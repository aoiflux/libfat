package libfat

import (
	"fmt"
	"testing"
)

// subdirEntries reads the entries of a subdirectory by path.
func subdirEntries(t *testing.T, v *Volume, dirPath string) []DirEntry {
	t.Helper()
	f, err := v.OpenPath(dirPath)
	if err != nil {
		t.Fatalf("OpenPath(%q) failed: %v", dirPath, err)
	}
	entries, err := f.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir(%q) failed: %v", dirPath, err)
	}
	return entries
}

func entryByName(t *testing.T, entries []DirEntry, name string) DirEntry {
	t.Helper()
	for _, e := range entries {
		if e.Name == name || e.ShortName == name {
			return e
		}
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name)
	}
	t.Fatalf("entry %q not found among %v", name, got)
	return DirEntry{}
}

func mustFileID(t *testing.T, v *Volume, e DirEntry) FileID {
	t.Helper()
	id, ok := v.FileID(e)
	if !ok {
		t.Fatalf("FileID(%q) reported no identity", e.Name)
	}
	return id
}

// spilledSubdirImage builds a FAT16 image with a subdirectory laid out across
// the two given clusters and enough entries that the last one lands in the
// second cluster. With 512-byte clusters a directory holds 16 slots, two of
// which the "." and ".." records take, so the fifteenth entry is the first to
// spill.
func spilledSubdirImage(t *testing.T, clusters []uint32) *testImage {
	t.Helper()
	im := newTestImage(t, FATType16)
	entries := make([][]byte, 0, 15)
	for i := range 15 {
		entries = append(entries, makeTimestampedEntry(fmt.Sprintf("F%02d", i), "TXT", 0x20, uint32(100+i), 4))
	}
	im.addSubdir("SUB", clusters, entries...)
	return im
}

// TestFileIDSurvivesDirectoryFragmentation is the central claim of the identity
// design: the slot index is logical, so it is the same whether the parent
// directory's clusters are adjacent or scattered, while the absolute offset of
// the same record is not.
func TestFileIDSurvivesDirectoryFragmentation(t *testing.T) {
	fragmented := spilledSubdirImage(t, []uint32{10, 30})
	contiguous := spilledSubdirImage(t, []uint32{10, 11})

	vFrag := fragmented.volume()
	defer vFrag.Close()
	vCont := contiguous.volume()
	defer vCont.Close()

	spilledFrag := entryByName(t, subdirEntries(t, vFrag, "/SUB"), "F14.TXT")
	spilledCont := entryByName(t, subdirEntries(t, vCont, "/SUB"), "F14.TXT")

	// The spilled entry is the first slot of the directory's second cluster.
	wantOffset := int64(fragmented.bytesPerCluster())
	if spilledFrag.EntryOffset != wantOffset {
		t.Fatalf("EntryOffset = %d, want %d", spilledFrag.EntryOffset, wantOffset)
	}

	// Physical addressing disagrees between the two layouts...
	if spilledFrag.EntryAbsoluteOffset != fragmented.clusterOffset(30) {
		t.Fatalf("fragmented EntryAbsoluteOffset = %d, want %d",
			spilledFrag.EntryAbsoluteOffset, fragmented.clusterOffset(30))
	}
	if spilledCont.EntryAbsoluteOffset != contiguous.clusterOffset(11) {
		t.Fatalf("contiguous EntryAbsoluteOffset = %d, want %d",
			spilledCont.EntryAbsoluteOffset, contiguous.clusterOffset(11))
	}
	if spilledFrag.EntryAbsoluteOffset == spilledCont.EntryAbsoluteOffset {
		t.Fatal("the two layouts were expected to place the record at different image offsets")
	}

	// ...while the identity does not.
	idFrag := mustFileID(t, vFrag, spilledFrag)
	idCont := mustFileID(t, vCont, spilledCont)
	if idFrag != idCont {
		t.Fatalf("FileID changed with directory layout: %v vs %v", idFrag, idCont)
	}
	if idFrag.EntrySlotIndex != 16 {
		t.Fatalf("EntrySlotIndex = %d, want 16", idFrag.EntrySlotIndex)
	}
	if idFrag.ParentFirstCluster != 10 {
		t.Fatalf("ParentFirstCluster = %d, want 10", idFrag.ParentFirstCluster)
	}
}

// TestFileIDDoesNotSurviveRenameChangingLongNameSlotCount pins the documented
// headline failure mode: a long name occupies ceil(len/13) slots immediately
// before the short entry, so renaming to a name of a different length moves the
// short entry and with it the identity.
func TestFileIDDoesNotSurviveRenameChangingLongNameSlotCount(t *testing.T) {
	build := func(longName string) (*Volume, DirEntry) {
		t.Helper()
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "LEAD", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
		im.addFile(testFile{
			base: "REPORT", ext: "TXT", longName: longName,
			clusters: []uint32{6}, size: 4, terminate: true,
		})
		v := im.volume()
		return v, entryNamed(t, v, longName)
	}

	vShort, short := build("report.txt")
	defer vShort.Close()
	vLong, long := build("quarterly report final.txt")
	defer vLong.Close()

	idShort := mustFileID(t, vShort, short)
	idLong := mustFileID(t, vLong, long)

	if idShort.ParentFirstCluster != idLong.ParentFirstCluster {
		t.Fatalf("the two images should share a parent: %v vs %v", idShort, idLong)
	}
	if idShort == idLong {
		t.Fatalf("FileID survived a rename that changed the long-name slot count: %v", idShort)
	}
	// One long-name slot becomes two, so the short entry moves one slot on.
	if idLong.EntrySlotIndex != idShort.EntrySlotIndex+1 {
		t.Fatalf("slot index moved from %d to %d, want a move of exactly one",
			idShort.EntrySlotIndex, idLong.EntrySlotIndex)
	}
}

// TestFileIDIsReusedAfterSlotReuse proves the collision the documentation warns
// about rather than merely asserting it: an unrelated file written into a
// deleted file's slot inherits its identity exactly, and nothing in the
// filesystem distinguishes the two.
func TestFileIDIsReusedAfterSlotReuse(t *testing.T) {
	before := newTestImage(t, FATType16)
	before.addFile(testFile{base: "ORIGINAL", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	vBefore := before.volume()
	defer vBefore.Close()

	// The same slot, now occupied by a different file with a different cluster
	// and a different size.
	after := newTestImage(t, FATType16)
	after.addFile(testFile{base: "REPLACE", ext: "BIN", clusters: []uint32{9}, size: 512, terminate: true})
	vAfter := after.volume()
	defer vAfter.Close()

	idBefore := mustFileID(t, vBefore, entryNamed(t, vBefore, "ORIGINAL.TXT"))
	idAfter := mustFileID(t, vAfter, entryNamed(t, vAfter, "REPLACE.BIN"))

	if idBefore != idAfter {
		t.Fatalf("expected the reused slot to collide: %v vs %v", idBefore, idAfter)
	}
}

func TestFileIDIsStableAcrossReReads(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "STABLE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	v := im.volume()
	defer v.Close()

	first := mustFileID(t, v, entryNamed(t, v, "STABLE.TXT"))
	second := mustFileID(t, v, entryNamed(t, v, "STABLE.TXT"))
	if first != second {
		t.Fatalf("FileID differed between reads: %v vs %v", first, second)
	}
}

func TestFileIDSlotIndexMatchesEntryOffset(t *testing.T) {
	im := spilledSubdirImage(t, []uint32{10, 30})
	v := im.volume()
	defer v.Close()

	for _, e := range subdirEntries(t, v, "/SUB") {
		id := mustFileID(t, v, e)
		if want := uint32(e.EntryOffset / dirEntrySize); id.EntrySlotIndex != want {
			t.Fatalf("%s: EntrySlotIndex = %d, want %d", e.Name, id.EntrySlotIndex, want)
		}
	}
}

func TestFileIDForFixedRootRegionUsesZeroParent(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			im.addFile(testFile{base: "ROOTED", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
			v := im.volume()
			defer v.Close()

			e := entryNamed(t, v, "ROOTED.TXT")
			if e.ParentFirstCluster != FixedRootCluster {
				t.Fatalf("ParentFirstCluster = %d, want FixedRootCluster", e.ParentFirstCluster)
			}
			id := mustFileID(t, v, e)
			if id.ParentFirstCluster != FixedRootCluster {
				t.Fatalf("FileID.ParentFirstCluster = %d, want FixedRootCluster", id.ParentFirstCluster)
			}
		})
	}
}

func TestFileIDForFAT32RootUsesRootCluster(t *testing.T) {
	im := newTestImage(t, FATType32)
	im.addFile(testFile{base: "ROOTED", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	v := im.volume()
	defer v.Close()

	id := mustFileID(t, v, entryNamed(t, v, "ROOTED.TXT"))
	if id.ParentFirstCluster != v.RootCluster() {
		t.Fatalf("ParentFirstCluster = %d, want root cluster %d", id.ParentFirstCluster, v.RootCluster())
	}
}

func TestFileIDForSubdirectoryChildUsesSubdirectoryCluster(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addSubdir("SUB", []uint32{10, 11}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))
	v := im.volume()
	defer v.Close()

	child := entryByName(t, subdirEntries(t, v, "/SUB"), "CHILD.TXT")
	if id := mustFileID(t, v, child); id.ParentFirstCluster != 10 {
		t.Fatalf("ParentFirstCluster = %d, want 10", id.ParentFirstCluster)
	}
}

func TestFileIDRejectsZeroParentClusterOnFAT32(t *testing.T) {
	im := newTestImage(t, FATType32)
	v := im.volume()
	defer v.Close()

	// A zero on FAT32 is an unpopulated field, not the fixed root region, which
	// FAT32 does not have.
	if _, ok := v.FileID(DirEntry{ParentFirstCluster: 0, EntryOffset: 64}); ok {
		t.Fatal("FileID accepted a zero parent cluster on FAT32")
	}
}

func TestFileIDRejectsVirtualEntry(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volumeWithOptions(OpenOptions{IncludeVirtualRootEntries: true})
	defer v.Close()

	if _, ok := v.FileID(entryNamed(t, v, "$MBR")); ok {
		t.Fatal("FileID accepted a virtual entry, which has no 32-byte record")
	}
}

func TestFileIDRejectsMisalignedEntryOffset(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	for _, offset := range []int64{-1, -32, 17, 33} {
		if _, ok := v.FileID(DirEntry{ParentFirstCluster: 10, EntryOffset: offset}); ok {
			t.Fatalf("FileID accepted EntryOffset %d", offset)
		}
	}
}

func TestFileIDRejectsOutOfRangeParentCluster(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	for _, parent := range []uint32{1, v.maxClusterNumber() + 1, ^uint32(0)} {
		if _, ok := v.FileID(DirEntry{ParentFirstCluster: parent, EntryOffset: 0}); ok {
			t.Fatalf("FileID accepted parent cluster %d", parent)
		}
	}
}

func TestFileIDForOrphanEntryUsesOrphanRunCluster(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addDeletedSubdir("GONE", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))
	v := im.volume()
	defer v.Close()

	result, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	entries := result.Entries()
	if len(entries) == 0 {
		t.Fatal("expected at least one orphaned entry")
	}
	id := mustFileID(t, v, entries[0])
	if id.ParentFirstCluster != 30 {
		t.Fatalf("ParentFirstCluster = %d, want the orphan run's first cluster 30", id.ParentFirstCluster)
	}
}

func TestFileIDOnClosedVolume(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "CLOSED", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	v := im.volume()
	e := entryNamed(t, v, "CLOSED.TXT")
	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, ok := v.FileID(e); ok {
		t.Fatal("FileID accepted an entry from a closed volume")
	}
}

func TestFileIDString(t *testing.T) {
	if got := (FileID{ParentFirstCluster: 10, EntrySlotIndex: 16}).String(); got != "10:16" {
		t.Fatalf("String() = %q, want %q", got, "10:16")
	}
}
