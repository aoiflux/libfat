package libfat

import (
	"errors"
	"testing"
)

// TestReadAtOnTruncatedChainDoesNotPanic is a regression test. A deleted entry
// records a size but its FAT chain has been freed, so only the first cluster is
// locatable. Bounding reads against the recorded size instead of the recovered
// length sliced past the end of the buffer and panicked.
func TestReadAtOnTruncatedChainDoesNotPanic(t *testing.T) {
	im := newTestImage(t, FATType16)
	const size = 100000
	im.addFile(testFile{
		base: "GONE", ext: "BIN",
		clusters: []uint32{150}, size: size,
		content: patternBytes(512, 0x99), deleted: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "_ONE.BIN")
	if !entry.Deleted {
		t.Fatal("entry should be reported as deleted")
	}
	f := v.openDirEntry(entry)

	// Offsets inside the recorded size but past the recoverable data are the
	// panicking case.
	for _, off := range []int64{512, 1000, 60000, size - 1} {
		buf := make([]byte, 64)
		n, err := f.ReadAt(buf, off)
		if !errors.Is(err, ErrTruncatedChain) {
			t.Fatalf("ReadAt(%d) = %d, %v; want ErrTruncatedChain", off, n, err)
		}
	}

	// The recoverable prefix still reads correctly.
	buf := make([]byte, 64)
	n, err := f.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt(0) failed: %v", err)
	}
	if n != 64 {
		t.Fatalf("ReadAt(0) returned %d bytes, want 64", n)
	}

	data, err := f.ReadAll()
	if !errors.Is(err, ErrTruncatedChain) {
		t.Fatalf("ReadAll error = %v, want ErrTruncatedChain", err)
	}
	if len(data) != 512 {
		t.Fatalf("ReadAll recovered %d bytes, want the single locatable cluster (512)", len(data))
	}
}

func TestFragmentOffsetsDeletedWithoutAssumption(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "DELME", ext: "BIN",
		clusters: []uint32{160, 161, 162}, size: 1536,
		content: patternBytes(1536, 0xAA), deleted: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "_ELME.BIN")

	result, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{})
	if err != nil {
		t.Fatalf("FragmentOffsetsWithOptions failed: %v", err)
	}
	if result.ChainWalked {
		t.Fatal("a deleted entry's FAT chain is not authoritative and must not be walked")
	}
	if result.Assumed {
		t.Fatal("no assumption was requested")
	}
	if !result.Truncated {
		t.Fatal("one cluster cannot cover 1536 bytes; Truncated should be set")
	}
	assertRanges(t, result.Ranges, []Range{
		{StartByte: im.clusterOffset(160), Length: 512, StartCluster: 160, ClusterCount: 1},
	})

	// The convenience form surfaces the shortfall as an error while still
	// returning the usable range.
	ranges, err := v.FragmentOffsets(entry)
	if !errors.Is(err, ErrTruncatedChain) {
		t.Fatalf("FragmentOffsets error = %v, want ErrTruncatedChain", err)
	}
	if len(ranges) != 1 {
		t.Fatalf("partial ranges must still be returned, got %d", len(ranges))
	}
}

func TestFragmentOffsetsDeletedAssumeContiguous(t *testing.T) {
	im := newTestImage(t, FATType16)
	content := patternBytes(1536, 0xBB)
	im.addFile(testFile{
		base: "RECOV", ext: "BIN",
		clusters: []uint32{170, 171, 172}, size: 1536,
		content: content, deleted: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "_ECOV.BIN")

	result, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{AssumeContiguous: true})
	if err != nil {
		t.Fatalf("FragmentOffsetsWithOptions failed: %v", err)
	}
	if !result.Assumed {
		t.Fatal("reconstructed ranges must be flagged as assumed")
	}
	if result.Truncated {
		t.Fatal("the assumption covers the whole size")
	}
	assertRanges(t, result.Ranges, []Range{
		{StartByte: im.clusterOffset(170), Length: 1536, StartCluster: 170, ClusterCount: 3},
	})

	// The file was in fact laid out contiguously, so the assumption recovers it.
	f := v.openDirEntry(entry)
	f.SetFragmentOptions(FragmentOptions{AssumeContiguous: true})
	got, err := f.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after reconstruction failed: %v", err)
	}
	if len(got) != len(content) {
		t.Fatalf("recovered %d bytes, want %d", len(got), len(content))
	}
	for i := range content {
		if got[i] != content[i] {
			t.Fatalf("recovered content differs at byte %d", i)
		}
	}
}

// TestDeletedNeverFollowsReallocatedChain guards the most dangerous failure
// mode in deleted-file recovery: after deletion the first cluster may be
// reallocated to a new file, so walking the FAT from it would return the new
// file's clusters and attribute another file's data to the deleted entry.
func TestDeletedNeverFollowsReallocatedChain(t *testing.T) {
	im := newTestImage(t, FATType16)

	im.addFile(testFile{
		base: "VICTIM", ext: "BIN",
		clusters: []uint32{180}, size: 2048,
		content: patternBytes(512, 0xCC), deleted: true,
	})
	// A later file takes cluster 180 and chains on to distant clusters.
	im.addFile(testFile{
		base: "NEWFILE", ext: "BIN",
		clusters: []uint32{180, 900, 901, 902}, size: 2048,
		content: patternBytes(2048, 0xDD), terminate: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "_ICTIM.BIN")
	if !entry.ClusterAllocated {
		t.Fatal("the first cluster was reallocated and should report as allocated")
	}

	result, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{})
	if err != nil {
		t.Fatalf("FragmentOffsetsWithOptions failed: %v", err)
	}
	if !result.FirstClusterReallocated {
		t.Fatal("FirstClusterReallocated must warn that the data was overwritten")
	}
	for _, r := range result.Ranges {
		if r.StartCluster == 900 || r.StartCluster == 901 || r.StartCluster == 902 {
			t.Fatalf("deleted entry returned clusters belonging to a live file: %v", result.Ranges)
		}
	}
	if len(result.Ranges) != 1 || result.Ranges[0].StartCluster != 180 {
		t.Fatalf("expected only the recorded first cluster, got %v", result.Ranges)
	}
}

func TestDeletedLongNameRecoveryDisabledByDefault(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "DELFIL", ext: "TXT", longName: "deleted long name.txt",
		clusters: []uint32{190}, size: 100,
		content: patternBytes(100, 0xEE), deleted: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "_ELFIL.TXT")
	if entry.Name != "_ELFIL.TXT" {
		t.Fatalf("default name = %q, want the short-name placeholder", entry.Name)
	}
	if entry.NameSource != NameSourceShort {
		t.Fatalf("NameSource = %v, want short", entry.NameSource)
	}
	if entry.FirstCharRecovered {
		t.Fatal("no recovery was requested")
	}
}

func TestDeletedLongNameRecovery(t *testing.T) {
	cases := []struct {
		name     string
		base     string
		ext      string
		longName string
	}{
		// Short names follow the basis-name algorithm Windows uses, so that the
		// fixtures exercise the same association the library validates.
		{"two slots", "DELETE~1", "TXT", "deleted long name.txt"},
		{"single slot", "SHORTI~1", "TXT", "shortishname.txt"},
		{"exact multiple of 13", "EXACTL~1", "", "exactly13chrs"},
		{"non ascii", "CAF_-R~1", "TXT", "café-résumé.txt"},
		{"many slots", "A-REAL~1", "TXT", "a-really-quite-long-file-name-that-needs-several-slots.txt"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			im := newTestImage(t, FATType16)
			im.addFile(testFile{
				base: tc.base, ext: tc.ext, longName: tc.longName,
				clusters: []uint32{200}, size: 100,
				content: patternBytes(100, 0x12), deleted: true,
			})

			v := im.volumeWithOptions(OpenOptions{RecoverDeletedLongNames: true})
			entry := entryNamed(t, v, tc.longName)

			if entry.Name != tc.longName {
				t.Fatalf("recovered name = %q, want %q", entry.Name, tc.longName)
			}
			if entry.NameSource != NameSourceRecoveredLFN {
				t.Fatalf("NameSource = %v, want recovered-lfn", entry.NameSource)
			}
			if !entry.FirstCharRecovered {
				t.Fatal("the first character should have been recovered from the checksum")
			}
			wantShort := tc.base
			if tc.ext != "" {
				wantShort += "." + tc.ext
			}
			if entry.ShortName != wantShort {
				t.Fatalf("ShortName = %q, want %q with the 0xE5 byte restored", entry.ShortName, wantShort)
			}
			if entry.LFNEntryOffset < 0 {
				t.Fatal("LFNEntryOffset should point at the first recovered slot")
			}
		})
	}
}

// TestDeletedLongNameRecoveryRejectsForeignSlots checks that long-name slots
// left over from an unrelated entry are not grafted onto a deleted short entry
// that merely happens to follow them.
func TestDeletedLongNameRecoveryRejectsForeignSlots(t *testing.T) {
	im := newTestImage(t, FATType16)

	// Slots whose checksum belongs to a different short name.
	foreign := makeLFNEntries("someone elses name.txt", "OTHER   TXT")
	var raw [][]byte
	for _, e := range foreign {
		e[0] = 0xE5
		raw = append(raw, e)
	}
	short := makeTimestampedEntry("MYFILE", "TXT", 0x20, 210, 100)
	short[0] = 0xE5
	raw = append(raw, short)
	im.addRootRaw(raw...)
	im.setFATEntry(210, 0)

	v := im.volumeWithOptions(OpenOptions{RecoverDeletedLongNames: true})
	entry := entryNamed(t, v, "_YFILE.TXT")

	if entry.NameSource == NameSourceRecoveredLFN {
		t.Fatalf("slots with a foreign checksum were accepted, producing %q", entry.Name)
	}
	if entry.Name != "_YFILE.TXT" {
		t.Fatalf("name = %q, want the short-name placeholder", entry.Name)
	}
}

func TestClusterLoopReturnsPartialRangesAndFlag(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "LOOPY", ext: "BIN",
		clusters: []uint32{220, 221}, size: 4096, terminate: false,
	})
	im.setFATEntry(221, 220) // close the loop

	v := im.volume()
	entry := entryNamed(t, v, "LOOPY.BIN")

	result, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{})
	if err != nil {
		t.Fatalf("a looping chain must not fail outright: %v", err)
	}
	if !result.LoopDetected {
		t.Fatal("LoopDetected should be set")
	}
	if !result.ChainBroken {
		t.Fatal("ChainBroken should be set")
	}
	if result.ClustersWalked != 2 {
		t.Fatalf("walked %d clusters, want 2 before the repeat", result.ClustersWalked)
	}
	assertRanges(t, result.Ranges, []Range{
		{StartByte: im.clusterOffset(220), Length: 1024, StartCluster: 220, ClusterCount: 2},
	})
}

func TestBadClusterMidChainIsReported(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "BADCL", ext: "BIN",
		clusters: []uint32{230}, size: 2048, terminate: false,
	})
	im.setFATEntry(230, im.badCluster())

	v := im.volume()
	result, err := v.FragmentOffsetsWithOptions(entryNamed(t, v, "BADCL.BIN"), FragmentOptions{})
	if err != nil {
		t.Fatalf("a bad-cluster marker must not fail outright: %v", err)
	}
	if !result.ChainBroken {
		t.Fatal("ChainBroken should be set for a chain ending on a bad cluster")
	}
	if len(result.Ranges) != 1 {
		t.Fatalf("expected the clusters walked before the marker, got %v", result.Ranges)
	}
}

func TestOutOfRangeNextClusterIsReported(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "OORNG", ext: "BIN",
		clusters: []uint32{240}, size: 2048, terminate: false,
	})
	im.setFATEntry(240, im.clusterCount+100) // beyond the data area

	v := im.volume()
	result, err := v.FragmentOffsetsWithOptions(entryNamed(t, v, "OORNG.BIN"), FragmentOptions{})
	if err != nil {
		t.Fatalf("an out-of-range link must not fail outright: %v", err)
	}
	if !result.ChainBroken {
		t.Fatal("ChainBroken should be set")
	}
	for _, r := range result.Ranges {
		if r.StartCluster > im.clusterCount+1 {
			t.Fatalf("range escaped the data area: %v", r)
		}
	}
}

func TestMaxRunsCapsWork(t *testing.T) {
	im := newTestImage(t, FATType16)
	clusters := []uint32{250, 300, 350, 400, 450}
	im.addFile(testFile{
		base: "MANYRN", ext: "BIN",
		clusters: clusters, size: uint32(len(clusters) * 512), terminate: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "MANYRN.BIN")

	full, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{})
	if err != nil {
		t.Fatalf("FragmentOffsetsWithOptions failed: %v", err)
	}
	if len(full.Ranges) != 5 {
		t.Fatalf("expected 5 runs, got %d", len(full.Ranges))
	}

	capped, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{MaxRuns: 2})
	if err != nil {
		t.Fatalf("FragmentOffsetsWithOptions with cap failed: %v", err)
	}
	if len(capped.Ranges) != 2 {
		t.Fatalf("MaxRuns=2 returned %d runs", len(capped.Ranges))
	}
	if !capped.ChainBroken {
		t.Fatal("a capped walk must report that it stopped early")
	}
}

func TestFragmentOffsetsRejectsClusterOutsideDataArea(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()

	_, err := v.FragmentOffsets(DirEntry{
		Name: "BOGUS", Path: "/BOGUS", Size: 512,
		FirstCluster: im.clusterCount + 1000,
	})
	if !errors.Is(err, ErrCorruptStructure) {
		t.Fatalf("error = %v, want ErrCorruptStructure", err)
	}
}

func TestFragmentOffsetsEmptyFile(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()

	ranges, err := v.FragmentOffsets(DirEntry{Name: "EMPTY", Path: "/EMPTY", Size: 0})
	if err != nil {
		t.Fatalf("an empty file is not an error: %v", err)
	}
	if len(ranges) != 0 {
		t.Fatalf("expected no ranges, got %v", ranges)
	}
}
