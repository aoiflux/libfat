package libfat

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestBuilderProducesValidVolumes(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			v := im.volume()
			if v.FATType() != fatType {
				t.Fatalf("expected %s, got %s", fatType, v.FATType())
			}
			if v.ClusterCount() != im.clusterCount {
				t.Fatalf("cluster count mismatch: library %d, builder %d", v.ClusterCount(), im.clusterCount)
			}
			if v.FirstDataSector() != im.firstDataSector {
				t.Fatalf("first data sector mismatch: library %d, builder %d", v.FirstDataSector(), im.firstDataSector)
			}
			// The library's cluster arithmetic must agree with the builder's,
			// which is derived independently from the BPB.
			got, err := v.ClusterToOffset(2)
			if err != nil {
				t.Fatalf("ClusterToOffset failed: %v", err)
			}
			if got != im.clusterOffset(2) {
				t.Fatalf("cluster 2 offset: library %d, builder %d", got, im.clusterOffset(2))
			}
		})
	}
}

func TestFragmentOffsetsContiguous(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			clusters := []uint32{10, 11, 12}
			size := uint32(3 * 512)
			im.addFile(testFile{
				base: "CONTIG", ext: "BIN",
				clusters: clusters, size: size,
				content: patternBytes(int(size), 0x11), terminate: true,
			})

			v := im.volume()
			entry := entryNamed(t, v, "CONTIG.BIN")
			ranges, err := v.FragmentOffsets(entry)
			if err != nil {
				t.Fatalf("FragmentOffsets failed: %v", err)
			}
			if len(ranges) != 1 {
				t.Fatalf("expected 1 coalesced run, got %d: %v", len(ranges), ranges)
			}
			if ranges[0].StartByte != im.clusterOffset(10) {
				t.Fatalf("start byte: got %d, want %d", ranges[0].StartByte, im.clusterOffset(10))
			}
			if ranges[0].Length != int64(size) {
				t.Fatalf("length: got %d, want %d", ranges[0].Length, size)
			}
			if ranges[0].ClusterCount != 3 {
				t.Fatalf("cluster count: got %d, want 3", ranges[0].ClusterCount)
			}
			if ranges[0].Sparse {
				t.Fatal("FAT ranges must never be sparse")
			}
			if IsFragmented(ranges) {
				t.Fatal("contiguous file reported as fragmented")
			}
		})
	}
}

func TestFragmentOffsetsFragmented(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			clusters := []uint32{100, 101, 105, 106, 107, 200}
			size := uint32(len(clusters) * 512)
			im.addFile(testFile{
				base: "FRAG", ext: "BIN",
				clusters: clusters, size: size,
				content: patternBytes(int(size), 0x22), terminate: true,
			})

			v := im.volume()
			entry := entryNamed(t, v, "FRAG.BIN")
			ranges, err := v.FragmentOffsets(entry)
			if err != nil {
				t.Fatalf("FragmentOffsets failed: %v", err)
			}

			want := []Range{
				{StartByte: im.clusterOffset(100), Length: 1024, StartCluster: 100, ClusterCount: 2},
				{StartByte: im.clusterOffset(105), Length: 1536, StartCluster: 105, ClusterCount: 3},
				{StartByte: im.clusterOffset(200), Length: 512, StartCluster: 200, ClusterCount: 1},
			}
			assertRanges(t, ranges, want)

			if TotalLength(ranges) != int64(size) {
				t.Fatalf("total length %d, want %d", TotalLength(ranges), size)
			}
			if !IsFragmented(ranges) {
				t.Fatal("fragmented file reported as contiguous")
			}
			frag, err := v.IsFragmented(entry)
			if err != nil || !frag {
				t.Fatalf("Volume.IsFragmented = %v, %v; want true, nil", frag, err)
			}
		})
	}
}

func TestFragmentOffsetsTrimsFinalRunToSize(t *testing.T) {
	im := newTestImage(t, FATType16)
	clusters := []uint32{50, 51, 60}
	size := uint32(1024 + 100) // final cluster only partly used
	im.addFile(testFile{
		base: "TRIM", ext: "BIN",
		clusters: clusters, size: size,
		content: patternBytes(int(size), 0x33), terminate: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "TRIM.BIN")
	ranges, err := v.FragmentOffsets(entry)
	if err != nil {
		t.Fatalf("FragmentOffsets failed: %v", err)
	}
	assertRanges(t, ranges, []Range{
		{StartByte: im.clusterOffset(50), Length: 1024, StartCluster: 50, ClusterCount: 2},
		{StartByte: im.clusterOffset(60), Length: 100, StartCluster: 60, ClusterCount: 1},
	})
	if TotalLength(ranges) != int64(size) {
		t.Fatalf("ranges must sum to the entry size: got %d, want %d", TotalLength(ranges), size)
	}
}

func TestSlackRangeCoversRemainderOfFinalCluster(t *testing.T) {
	im := newTestImage(t, FATType16)
	clusters := []uint32{70, 71}
	size := uint32(512 + 200)
	im.addFile(testFile{
		base: "SLACK", ext: "BIN",
		clusters: clusters, size: size,
		content: patternBytes(int(size), 0x44), terminate: true,
	})

	v := im.volume()
	entry := entryNamed(t, v, "SLACK.BIN")
	slack, ok, err := v.SlackRange(entry)
	if err != nil {
		t.Fatalf("SlackRange failed: %v", err)
	}
	if !ok {
		t.Fatal("expected slack for a file that does not end on a cluster boundary")
	}
	wantStart := im.clusterOffset(71) + 200
	if slack.StartByte != wantStart || slack.Length != 312 {
		t.Fatalf("slack = {%d, %d}, want {%d, 312}", slack.StartByte, slack.Length, wantStart)
	}
}

func TestSlackRangeAbsentOnClusterBoundary(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "EXACT", ext: "BIN",
		clusters: []uint32{80, 81}, size: 1024,
		content: patternBytes(1024, 0x55), terminate: true,
	})

	v := im.volume()
	_, ok, err := v.SlackRange(entryNamed(t, v, "EXACT.BIN"))
	if err != nil {
		t.Fatalf("SlackRange failed: %v", err)
	}
	if ok {
		t.Fatal("a cluster-aligned file has no slack")
	}
}

func TestRootDirectoryFragmentsFixedRegion(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			v := im.volume()
			ranges, err := v.RootDirectoryFragments()
			if err != nil {
				t.Fatalf("RootDirectoryFragments failed: %v", err)
			}
			if len(ranges) != 1 {
				t.Fatalf("the fixed root region is a single extent, got %d", len(ranges))
			}
			wantStart := int64(im.rootDirSector) * int64(im.bytesPerSector)
			wantLen := int64(im.rootDirSectors) * int64(im.bytesPerSector)
			if ranges[0].StartByte != wantStart || ranges[0].Length != wantLen {
				t.Fatalf("root range = {%d,%d}, want {%d,%d}",
					ranges[0].StartByte, ranges[0].Length, wantStart, wantLen)
			}
			if ranges[0].StartCluster != 0 || ranges[0].ClusterCount != 0 {
				t.Fatal("the fixed root region is not cluster-addressed and must report zero clusters")
			}
		})
	}
}

func TestRootDirectoryFragmentsFAT32IsChain(t *testing.T) {
	im := newTestImage(t, FATType32)
	// Extend the root directory onto a distant second cluster.
	im.setFATEntry(2, 900)
	im.setFATEntry(900, im.eoc())

	v := im.volume()
	ranges, err := v.RootDirectoryFragments()
	if err != nil {
		t.Fatalf("RootDirectoryFragments failed: %v", err)
	}
	assertRanges(t, ranges, []Range{
		{StartByte: im.clusterOffset(2), Length: 512, StartCluster: 2, ClusterCount: 1},
		{StartByte: im.clusterOffset(900), Length: 512, StartCluster: 900, ClusterCount: 1},
	})
}

func TestEntryAbsoluteOffsetInFragmentedDirectory(t *testing.T) {
	im := newTestImage(t, FATType16)
	dirClusters := []uint32{300, 400} // deliberately non-adjacent

	// 512-byte clusters hold 16 entries. "." and ".." take two, so 14 filler
	// entries push the target entry into the second, distant cluster.
	var entries [][]byte
	for i := 0; i < 14; i++ {
		entries = append(entries, makeTimestampedEntry(padName("FILL", i), "TXT", 0x20, 600+uint32(i), 10))
	}
	entries = append(entries, makeTimestampedEntry("TARGET", "TXT", 0x20, 700, 10))
	im.addSubdir("SUB", dirClusters, entries...)

	v := im.volume()
	dir, err := v.OpenPath("/SUB")
	if err != nil {
		t.Fatalf("OpenPath failed: %v", err)
	}
	listed, err := dir.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	var target DirEntry
	for _, e := range listed {
		if e.Name == "TARGET.TXT" {
			target = e
		}
	}
	if target.Name == "" {
		t.Fatal("TARGET.TXT not found in subdirectory")
	}

	// The entry sits at buffer offset 16*32 = 512, which is the first byte of
	// the second cluster. A naive implementation that treats EntryOffset as an
	// image offset, or that assumes the directory is contiguous, gets this
	// wrong by exactly the gap between clusters 300 and 400.
	if target.EntryOffset != 512 {
		t.Fatalf("EntryOffset = %d, want 512", target.EntryOffset)
	}
	want := im.clusterOffset(400)
	if target.EntryAbsoluteOffset != want {
		t.Fatalf("EntryAbsoluteOffset = %d, want %d (cluster 400)", target.EntryAbsoluteOffset, want)
	}

	// The mapped offset must actually contain the entry.
	got := make([]byte, 11)
	if _, err := v.ReadAt(got, target.EntryAbsoluteOffset); err != nil {
		t.Fatalf("ReadAt on mapped offset failed: %v", err)
	}
	if string(got) != "TARGET  TXT" {
		t.Fatalf("bytes at EntryAbsoluteOffset = %q, want %q", got, "TARGET  TXT")
	}
}

func TestFragmentReaderReadsAcrossFragments(t *testing.T) {
	im := newTestImage(t, FATType16)
	clusters := []uint32{500, 501, 800, 1200}
	size := uint32(len(clusters) * 512)
	content := patternBytes(int(size), 0x66)
	im.addFile(testFile{
		base: "SPAN", ext: "BIN",
		clusters: clusters, size: size,
		content: content, terminate: true,
	})

	v := im.volume()
	f, err := v.OpenPath("/SPAN.BIN")
	if err != nil {
		t.Fatalf("OpenPath failed: %v", err)
	}

	all, err := f.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(all, content) {
		t.Fatal("ReadAll returned data that does not match what was written across fragments")
	}

	// Reads that straddle a fragment boundary are the interesting case.
	for _, tc := range []struct{ off, n int }{
		{0, 10}, {500, 100}, {1020, 8}, {1500, 1000}, {2040, 16}, {int(size) - 1, 1},
	} {
		buf := make([]byte, tc.n)
		n, err := f.ReadAt(buf, int64(tc.off))
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("ReadAt(%d,%d) failed: %v", tc.off, tc.n, err)
		}
		if !bytes.Equal(buf[:n], content[tc.off:tc.off+n]) {
			t.Fatalf("ReadAt(%d,%d) returned wrong bytes", tc.off, tc.n)
		}
	}

	r, err := f.Reader()
	if err != nil {
		t.Fatalf("Reader failed: %v", err)
	}
	streamed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("streaming read failed: %v", err)
	}
	if !bytes.Equal(streamed, content) {
		t.Fatal("streaming reader returned data that does not match")
	}
}

func TestSectionReaderRejectsFragmentedFile(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "SPLIT", ext: "BIN",
		clusters: []uint32{900, 1000}, size: 1024,
		content: patternBytes(1024, 0x77), terminate: true,
	})
	im.addFile(testFile{
		base: "WHOLE", ext: "BIN",
		clusters: []uint32{1100, 1101}, size: 1024,
		content: patternBytes(1024, 0x88), terminate: true,
	})

	v := im.volume()

	split, err := v.OpenPath("/SPLIT.BIN")
	if err != nil {
		t.Fatalf("OpenPath failed: %v", err)
	}
	if _, err := split.SectionReader(); !errors.Is(err, ErrFragmented) {
		t.Fatalf("expected ErrFragmented, got %v", err)
	}

	whole, err := v.OpenPath("/WHOLE.BIN")
	if err != nil {
		t.Fatalf("OpenPath failed: %v", err)
	}
	sr, err := whole.SectionReader()
	if err != nil {
		t.Fatalf("SectionReader on a contiguous file failed: %v", err)
	}
	data, err := io.ReadAll(sr)
	if err != nil {
		t.Fatalf("reading section reader failed: %v", err)
	}
	if !bytes.Equal(data, patternBytes(1024, 0x88)) {
		t.Fatal("section reader returned wrong data")
	}
}

func assertRanges(t *testing.T, got, want []Range) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("run count = %d, want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].StartByte != want[i].StartByte ||
			got[i].Length != want[i].Length ||
			got[i].StartCluster != want[i].StartCluster ||
			got[i].ClusterCount != want[i].ClusterCount {
			t.Fatalf("run %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func padName(prefix string, i int) string {
	return prefix + string(rune('A'+i))
}
