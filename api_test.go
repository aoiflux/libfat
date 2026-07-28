package libfat

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// spanFile builds an image holding one deliberately fragmented file and returns
// the volume, the opened file, its content, and the image builder.
func spanFile(t *testing.T) (*testImage, *Volume, *File, []byte) {
	t.Helper()

	im := newTestImage(t, FATType16)
	clusters := []uint32{500, 501, 800, 1200}
	size := uint32(len(clusters)*512 - 200) // not cluster-aligned, so slack exists
	content := patternBytes(int(size), 0x3C)
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
	return im, v, f, content
}

func TestFileReadAdvancesCursorAcrossFragments(t *testing.T) {
	_, _, f, content := spanFile(t)

	var got []byte
	buf := make([]byte, 300)
	for {
		n, err := f.Read(buf)
		got = append(got, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if n == 0 {
			t.Fatal("Read returned 0 bytes without EOF")
		}
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("sequential Read returned %d bytes, want %d matching the written content",
			len(got), len(content))
	}

	// Once exhausted the reader must keep reporting EOF rather than looping.
	n, err := f.Read(buf)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read past the end = %d, %v; want 0, io.EOF", n, err)
	}
}

func TestFileReaderSeek(t *testing.T) {
	_, _, f, content := spanFile(t)

	r, err := f.Reader()
	if err != nil {
		t.Fatalf("Reader failed: %v", err)
	}

	// SeekStart into the second fragment.
	if _, err := r.Seek(600, io.SeekStart); err != nil {
		t.Fatalf("SeekStart failed: %v", err)
	}
	buf := make([]byte, 64)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("ReadFull after SeekStart failed: %v", err)
	}
	if !bytes.Equal(buf, content[600:664]) {
		t.Fatal("bytes after SeekStart do not match")
	}

	// SeekCurrent continues from where the read left off.
	if _, err := r.Seek(36, io.SeekCurrent); err != nil {
		t.Fatalf("SeekCurrent failed: %v", err)
	}
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("ReadFull after SeekCurrent failed: %v", err)
	}
	if !bytes.Equal(buf, content[700:764]) {
		t.Fatal("bytes after SeekCurrent do not match")
	}

	// SeekEnd reports the size the runs cover.
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("SeekEnd failed: %v", err)
	}
	if end != int64(len(content)) {
		t.Fatalf("SeekEnd = %d, want %d", end, len(content))
	}
	if _, err := r.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("reading at the end = %v, want io.EOF", err)
	}

	if _, err := r.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("seeking to a negative offset should fail")
	}
	if _, err := r.Seek(0, 99); err == nil {
		t.Fatal("an invalid whence should fail")
	}
}

func TestFileReaderAtIsIndependentOfCursor(t *testing.T) {
	_, _, f, content := spanFile(t)

	ra, err := f.ReaderAt()
	if err != nil {
		t.Fatalf("ReaderAt failed: %v", err)
	}

	// Interleaved reads at unrelated offsets must not interfere.
	offsets := []int64{1500, 0, 900, 300}
	for _, off := range offsets {
		buf := make([]byte, 128)
		n, err := ra.ReadAt(buf, off)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("ReadAt(%d) failed: %v", off, err)
		}
		if !bytes.Equal(buf[:n], content[off:off+int64(n)]) {
			t.Fatalf("ReadAt(%d) returned wrong bytes", off)
		}
	}

	if _, err := ra.ReadAt(make([]byte, 8), int64(len(content))); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt at the end = %v, want io.EOF", err)
	}
	if _, err := ra.ReadAt(make([]byte, 8), -1); err == nil {
		t.Fatal("a negative offset should fail")
	}
}

func TestFileSlackMatchesVolumeSlackRange(t *testing.T) {
	im, v, f, content := spanFile(t)

	slack, ok, err := f.Slack()
	if err != nil {
		t.Fatalf("File.Slack failed: %v", err)
	}
	if !ok {
		t.Fatal("a file that does not end on a cluster boundary has slack")
	}

	entry := entryNamed(t, v, "SPAN.BIN")
	viaVolume, okVolume, err := v.SlackRange(entry)
	if err != nil {
		t.Fatalf("Volume.SlackRange failed: %v", err)
	}
	if !okVolume || viaVolume != slack {
		t.Fatalf("File.Slack = %v, Volume.SlackRange = %v; want identical", slack, viaVolume)
	}

	// The slack begins exactly where the file's data ends in the last cluster.
	wantStart := im.clusterOffset(1200) + int64(len(content)%512)
	if slack.StartByte != wantStart {
		t.Fatalf("slack starts at %d, want %d", slack.StartByte, wantStart)
	}
	if slack.Length != 200 {
		t.Fatalf("slack length = %d, want 200", slack.Length)
	}

	// The root directory has no slack.
	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}
	if _, ok, err := root.Slack(); err != nil || ok {
		t.Fatalf("root slack = %v, %v; want false, nil", ok, err)
	}
}

func TestFileFragmentsWithOptions(t *testing.T) {
	_, v, f, content := spanFile(t)

	result, err := f.FragmentsWithOptions(FragmentOptions{})
	if err != nil {
		t.Fatalf("FragmentsWithOptions failed: %v", err)
	}
	if !result.ChainWalked {
		t.Fatal("a live file's ranges come from a FAT walk")
	}
	if result.BytesCovered != int64(len(content)) {
		t.Fatalf("BytesCovered = %d, want %d", result.BytesCovered, len(content))
	}
	if len(result.Ranges) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(result.Ranges))
	}

	// MaxRuns is honoured through the File-level entry point too.
	capped, err := f.FragmentsWithOptions(FragmentOptions{MaxRuns: 1})
	if err != nil {
		t.Fatalf("FragmentsWithOptions failed: %v", err)
	}
	if len(capped.Ranges) != 1 || !capped.ChainBroken {
		t.Fatalf("MaxRuns=1 gave %d runs, ChainBroken=%v", len(capped.Ranges), capped.ChainBroken)
	}

	// On the root the result describes the directory region itself.
	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}
	rootResult, err := root.FragmentsWithOptions(FragmentOptions{})
	if err != nil {
		t.Fatalf("root FragmentsWithOptions failed: %v", err)
	}
	if len(rootResult.Ranges) != 1 || rootResult.BytesCovered != TotalLength(rootResult.Ranges) {
		t.Fatalf("root result = %+v", rootResult)
	}
}

func TestFileFragmentsOnRootDirectory(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			v := im.volume()
			root, err := v.GetRootDirectory()
			if err != nil {
				t.Fatalf("GetRootDirectory failed: %v", err)
			}
			ranges, err := root.Fragments()
			if err != nil {
				t.Fatalf("root Fragments failed: %v", err)
			}
			viaVolume, err := v.RootDirectoryFragments()
			if err != nil {
				t.Fatalf("RootDirectoryFragments failed: %v", err)
			}
			assertRanges(t, ranges, viaVolume)
		})
	}
}

func TestVolumeClusterChain(t *testing.T) {
	_, v, _, _ := spanFile(t)

	chain, err := v.ClusterChain(500)
	if err != nil {
		t.Fatalf("ClusterChain failed: %v", err)
	}
	want := []uint32{500, 501, 800, 1200}
	if len(chain) != len(want) {
		t.Fatalf("chain = %v, want %v", chain, want)
	}
	for i := range want {
		if chain[i] != want[i] {
			t.Fatalf("chain = %v, want %v", chain, want)
		}
	}

	if _, err := v.ClusterChain(0); !errors.Is(err, ErrCorruptStructure) {
		t.Fatalf("ClusterChain(0) error = %v, want ErrCorruptStructure", err)
	}

	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, err := v.ClusterChain(500); err != ErrVolumeClosed {
		t.Fatalf("error after Close = %v, want ErrVolumeClosed", err)
	}
}

func TestClosedVolumeRejectsFragmentAPIs(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "ANY", ext: "BIN",
		clusters: []uint32{300}, size: 100,
		content: patternBytes(100, 0x01), terminate: true,
	})
	v := im.volume()
	entry := entryNamed(t, v, "ANY.BIN")
	f := v.openDirEntry(entry)

	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err := v.FragmentOffsets(entry); err != ErrVolumeClosed {
		t.Fatalf("FragmentOffsets = %v, want ErrVolumeClosed", err)
	}
	if _, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{}); err != ErrVolumeClosed {
		t.Fatalf("FragmentOffsetsWithOptions = %v, want ErrVolumeClosed", err)
	}
	if _, err := v.ClusterChainFragments(300, 100); err != ErrVolumeClosed {
		t.Fatalf("ClusterChainFragments = %v, want ErrVolumeClosed", err)
	}
	if _, err := v.RootDirectoryFragments(); err != ErrVolumeClosed {
		t.Fatalf("RootDirectoryFragments = %v, want ErrVolumeClosed", err)
	}
	if _, _, err := v.SlackRange(entry); err != ErrVolumeClosed {
		t.Fatalf("SlackRange = %v, want ErrVolumeClosed", err)
	}
	if _, err := f.Fragments(); err != ErrVolumeClosed {
		t.Fatalf("File.Fragments = %v, want ErrVolumeClosed", err)
	}
	if _, err := f.FragmentsWithOptions(FragmentOptions{}); err != ErrVolumeClosed {
		t.Fatalf("File.FragmentsWithOptions = %v, want ErrVolumeClosed", err)
	}
	if _, err := f.Reader(); err != ErrVolumeClosed {
		t.Fatalf("File.Reader = %v, want ErrVolumeClosed", err)
	}
	if _, err := f.ReadAll(); err != ErrVolumeClosed {
		t.Fatalf("File.ReadAll = %v, want ErrVolumeClosed", err)
	}
}

func TestSetFragmentOptionsDiscardsCachedReader(t *testing.T) {
	im := newTestImage(t, FATType16)
	const size = 4096
	im.addFile(testFile{
		base: "PARTIAL", ext: "BIN",
		clusters: []uint32{600}, size: size,
		content: patternBytes(512, 0x77), deleted: true,
	})

	v := im.volume()
	f := v.openDirEntry(entryNamed(t, v, "_ARTIAL.BIN"))

	// First read resolves and caches the conservative single-cluster view.
	data, err := f.ReadAll()
	if !errors.Is(err, ErrTruncatedChain) {
		t.Fatalf("ReadAll error = %v, want ErrTruncatedChain", err)
	}
	if len(data) != 512 {
		t.Fatalf("recovered %d bytes, want 512", len(data))
	}

	// Changing the options must invalidate that cache, not reuse it.
	f.SetFragmentOptions(FragmentOptions{AssumeContiguous: true})
	data, err = f.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after reconstruction failed: %v", err)
	}
	if len(data) != size {
		t.Fatalf("recovered %d bytes after reconstruction, want %d", len(data), size)
	}
}

// TestOpenEntryReachesDeletedAndOrphanedEntries covers the only route to the
// content of entries that no path leads to. Without it they can be enumerated
// but not read through the File API.
func TestOpenEntryReachesDeletedAndOrphanedEntries(t *testing.T) {
	im := newTestImage(t, FATType16)
	const size = 2048
	content := patternBytes(size, 0x2B)
	im.addFile(testFile{
		base: "ERASED", ext: "BIN",
		clusters: []uint32{640, 641, 642, 643}, size: size,
		content: content, deleted: true,
	})
	im.addDeletedSubdir("LOSTDIR", []uint32{660}, true,
		makeTimestampedEntry("INSIDE", "TXT", 0x20, 670, 64),
	)
	im.writeClusterData([]uint32{670}, patternBytes(64, 0x4D))

	v := im.volume()

	// A deleted entry cannot be reached by path, which is what OpenEntry is for.
	if _, err := v.OpenPath("/_RASED.BIN"); err == nil {
		t.Fatal("OpenPath should not resolve a deleted entry")
	}

	entry := entryNamed(t, v, "_RASED.BIN")
	f, err := v.OpenEntry(entry)
	if err != nil {
		t.Fatalf("OpenEntry failed: %v", err)
	}
	if f.Name() != entry.Name || f.Size() != int64(size) {
		t.Fatalf("opened file = %q/%d, want %q/%d", f.Name(), f.Size(), entry.Name, size)
	}

	// Default options locate only the first cluster.
	partial, err := f.ReadAll()
	if !errors.Is(err, ErrTruncatedChain) {
		t.Fatalf("ReadAll error = %v, want ErrTruncatedChain", err)
	}
	if len(partial) != 512 {
		t.Fatalf("recovered %d bytes, want 512", len(partial))
	}

	// Reconstruction recovers the whole file, which was in fact contiguous.
	f.SetFragmentOptions(FragmentOptions{AssumeContiguous: true})
	full, err := f.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after reconstruction failed: %v", err)
	}
	if !bytes.Equal(full, content) {
		t.Fatal("reconstructed content does not match what was written")
	}

	// The same route works for an entry recovered by an orphan scan.
	scan, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	var orphan DirEntry
	for _, e := range scan.Entries() {
		if e.Name == "INSIDE.TXT" {
			orphan = e
		}
	}
	if orphan.Name == "" {
		t.Fatalf("orphan not recovered; got %v", orphanNames(scan))
	}
	of, err := v.OpenEntry(orphan)
	if err != nil {
		t.Fatalf("OpenEntry on an orphan failed: %v", err)
	}
	data, err := of.ReadAll()
	if err != nil {
		t.Fatalf("reading an orphan failed: %v", err)
	}
	if !bytes.Equal(data, patternBytes(64, 0x4D)) {
		t.Fatal("orphan content does not match")
	}

	// Virtual entries have no backing data and must be refused.
	if _, err := v.OpenEntry(DirEntry{Name: "$MBR", Virtual: true}); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("OpenEntry on a virtual entry = %v, want ErrFileNotFound", err)
	}
	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, err := v.OpenEntry(entry); err != ErrVolumeClosed {
		t.Fatalf("OpenEntry after Close = %v, want ErrVolumeClosed", err)
	}
}

func TestStringHelpers(t *testing.T) {
	r := Range{StartByte: 1024, Length: 512, StartCluster: 7, ClusterCount: 2}
	if s := r.String(); !strings.Contains(s, "1024") || !strings.Contains(s, "clusters 7-8") {
		t.Fatalf("Range.String = %q", s)
	}
	// The fixed root region reports no clusters and must not render a range.
	region := Range{StartByte: 512, Length: 1024}
	if s := region.String(); strings.Contains(s, "clusters") {
		t.Fatalf("cluster-less Range.String = %q", s)
	}

	for source, want := range map[NameSource]string{
		NameSourceShort:        "short",
		NameSourceLFN:          "lfn",
		NameSourceRecoveredLFN: "recovered-lfn",
	} {
		if got := source.String(); got != want {
			t.Fatalf("NameSource(%d).String() = %q, want %q", source, got, want)
		}
	}
}

func TestCoalesceAndTotalLength(t *testing.T) {
	if got := Coalesce(nil); got != nil {
		t.Fatalf("Coalesce(nil) = %v", got)
	}
	if got := TotalLength(nil); got != 0 {
		t.Fatalf("TotalLength(nil) = %d", got)
	}

	adjacent := []Range{
		{StartByte: 0, Length: 100, StartCluster: 2, ClusterCount: 1},
		{StartByte: 100, Length: 100, StartCluster: 3, ClusterCount: 1},
		{StartByte: 500, Length: 100, StartCluster: 10, ClusterCount: 1},
	}
	merged := Coalesce(adjacent)
	if len(merged) != 2 {
		t.Fatalf("Coalesce merged into %d runs, want 2", len(merged))
	}
	if merged[0].Length != 200 || merged[0].ClusterCount != 2 {
		t.Fatalf("first merged run = %v", merged[0])
	}
	if TotalLength(adjacent) != 300 {
		t.Fatalf("TotalLength = %d, want 300", TotalLength(adjacent))
	}
	if !IsFragmented(adjacent) {
		t.Fatal("a gap between runs means fragmented")
	}
	if IsFragmented(adjacent[:2]) {
		t.Fatal("two adjacent runs coalesce into one and are not fragmented")
	}
}
