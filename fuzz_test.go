package libfat

import (
	"bytes"
	"testing"
)

// The fuzz targets assert one property: no input may cause a panic, an
// unbounded allocation, or a hang. Correctness of the parse is covered by the
// table-driven tests; these guard the panic-free requirement against images
// that no builder would produce.

func seedImages(f *testing.F) {
	f.Helper()

	im := newFuzzImage(f, FATType12)
	f.Add(im)
	f.Add(newFuzzImage(f, FATType16))

	// Structurally interesting mutations of a real image.
	truncated := make([]byte, len(im)/2)
	copy(truncated, im)
	f.Add(truncated)

	zeroed := make([]byte, 4096)
	copy(zeroed, im[:512])
	f.Add(zeroed)

	f.Add([]byte{})
	f.Add(make([]byte, 512))
	f.Add(bytes.Repeat([]byte{0xFF}, 1024))
}

// newFuzzImage builds a seed image without a *testing.T, which f.Add callers
// cannot supply.
func newFuzzImage(f *testing.F, fatType string) []byte {
	f.Helper()
	var im *testImage
	t := &testing.T{}
	im = newTestImage(t, fatType)
	im.addFile(testFile{
		base: "SEED", ext: "BIN",
		clusters: []uint32{10, 11, 30}, size: 1200,
		content: patternBytes(1200, 0x5A), terminate: true,
	})
	im.addFile(testFile{
		base: "DELSE~1", ext: "TXT", longName: "deleted seed.txt",
		clusters: []uint32{40}, size: 4096,
		content: patternBytes(512, 0x6B), deleted: true,
	})
	if t.Failed() {
		f.Fatal("failed to build seed image")
	}
	return im.data
}

func FuzzOpen(f *testing.F) {
	seedImages(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := OpenWithOptions(bytes.NewReader(data), OpenOptions{
			IncludeVolumeLabelEntries: true,
			RecoverDeletedLongNames:   true,
		})
		if err != nil {
			return
		}
		defer v.Close()

		root, err := v.GetRootDirectory()
		if err != nil {
			return
		}
		entries, err := root.ReadDir()
		if err != nil {
			return
		}

		// Bound the walk: a corrupt image may describe an enormous directory.
		for i, entry := range entries {
			if i >= 64 {
				break
			}
			_, _ = v.FragmentOffsets(entry)
			_, _ = v.FragmentOffsetsWithOptions(entry, FragmentOptions{AssumeContiguous: true, MaxRuns: 16})
			_, _, _ = v.SlackRange(entry)
			_, _ = v.IsFragmented(entry)

			if entry.IsDirectory || entry.Size > 1<<20 {
				continue
			}
			file := v.openDirEntry(entry)
			buf := make([]byte, 512)
			_, _ = file.ReadAt(buf, 0)
			_, _ = file.ReadAt(buf, int64(entry.Size)/2)
			_, _ = file.ReadAll()
		}
	})
}

func FuzzParseDirectoryEntries(f *testing.F) {
	im := newFuzzImage(f, FATType16)
	rootStart := int64(81) * 512
	f.Add(im[rootStart : rootStart+1024])
	f.Add(make([]byte, 32))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xE5}, 320))
	f.Add(bytes.Repeat([]byte{0x0F}, 320))

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, recover := range []bool{false, true} {
			ctx := &dirParseContext{
				dirPath:             "/",
				includeVolumeLabels: true,
				recoverDeletedLFN:   recover,
			}
			entries := parseDirectoryEntries(data, ctx)
			for _, e := range entries {
				if e.EntryAbsoluteOffset != -1 {
					t.Fatalf("absolute offset must be -1 without a mapper, got %d", e.EntryAbsoluteOffset)
				}
				if e.EntryOffset < 0 || e.EntryOffset >= int64(len(data)) {
					t.Fatalf("entry offset %d outside the %d-byte buffer", e.EntryOffset, len(data))
				}
			}
		}
	})
}

func FuzzFragmentOffsets(f *testing.F) {
	im := newFuzzImage(f, FATType16)
	f.Add(im, uint32(10), uint64(1200), false)
	f.Add(im, uint32(0), uint64(0), true)
	f.Add(im, uint32(0xFFFFFFFF), uint64(1<<40), true)
	f.Add(im[:1024], uint32(2), uint64(512), false)

	f.Fuzz(func(t *testing.T, data []byte, cluster uint32, size uint64, assume bool) {
		v, err := Open(bytes.NewReader(data))
		if err != nil {
			return
		}
		defer v.Close()

		entry := DirEntry{
			Name:         "FUZZ",
			Path:         "/FUZZ",
			Size:         size,
			FirstCluster: cluster,
		}
		result, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{
			AssumeContiguous: assume,
			MaxRuns:          64,
			MaxClusters:      4096,
		})
		if err != nil {
			return
		}

		// Every returned range must lie inside the volume: these offsets are
		// handed to callers as image coordinates, so an out-of-bounds range
		// would send them reading arbitrary parts of the disk.
		for _, r := range result.Ranges {
			if r.StartByte < 0 || r.Length < 0 {
				t.Fatalf("negative range %v", r)
			}
			if uint64(r.StartByte)+uint64(r.Length) > v.VolumeSize() {
				t.Fatalf("range %v extends past the %d-byte volume", r, v.VolumeSize())
			}
			if r.Sparse {
				t.Fatalf("FAT has no sparse runs, got %v", r)
			}
		}
		if TotalLength(result.Ranges) != result.BytesCovered {
			t.Fatalf("BytesCovered %d disagrees with the ranges (%d)",
				result.BytesCovered, TotalLength(result.Ranges))
		}
		if !result.Truncated && size > 0 && result.BytesCovered != int64(size) {
			t.Fatalf("untruncated result covers %d of %d bytes", result.BytesCovered, size)
		}
	})
}

func FuzzScanOrphans(f *testing.F) {
	f.Add(newFuzzImage(f, FATType16), false, false, false, uint32(0))
	f.Add(newFuzzImage(f, FATType12), true, true, true, uint32(64))
	f.Add(make([]byte, 512), false, true, false, uint32(0))

	f.Fuzz(func(t *testing.T, data []byte, allocated, looseDots, firstOnly bool, maxClusters uint32) {
		v, err := Open(bytes.NewReader(data))
		if err != nil {
			return
		}
		defer v.Close()

		result, err := v.ScanOrphans(OrphanScanOptions{
			ScanAllocatedClusters:  allocated,
			AllowMissingDotEntries: looseDots,
			OnlyFirstCluster:       firstOnly,
			// Bound the scan so a large synthesised volume cannot dominate the
			// fuzz budget; the property under test is safety, not coverage.
			MaxClusters:    maxClusters%4096 + 1,
			MaxDirectories: 32,
		})
		if err != nil {
			return
		}

		for _, dir := range result.Directories {
			for _, r := range dir.Ranges {
				if r.StartByte < 0 || r.Length < 0 {
					t.Fatalf("negative range %v", r)
				}
				if uint64(r.StartByte)+uint64(r.Length) > v.VolumeSize() {
					t.Fatalf("orphan range %v extends past the %d-byte volume", r, v.VolumeSize())
				}
			}
			for _, e := range dir.Entries {
				if !e.Orphaned {
					t.Fatalf("entry %q from a scan is not marked Orphaned", e.Name)
				}
				if e.EntryAbsoluteOffset >= 0 && uint64(e.EntryAbsoluteOffset)+dirEntrySize > v.VolumeSize() {
					t.Fatalf("entry offset %d lies outside the volume", e.EntryAbsoluteOffset)
				}
			}
		}
	})
}

func FuzzOpenPath(f *testing.F) {
	im := newFuzzImage(f, FATType16)
	f.Add("/SEED.BIN")
	f.Add("")
	f.Add("/")
	f.Add("C:\\SEED.BIN")
	f.Add("../../../etc/passwd")
	f.Add("/././//SEED.BIN/../")

	v, err := Open(bytes.NewReader(im))
	if err != nil {
		f.Fatalf("seed image failed to open: %v", err)
	}

	f.Fuzz(func(t *testing.T, path string) {
		file, err := v.OpenPath(path)
		if err != nil {
			return
		}
		if file.IsDirectory() {
			_, _ = file.ReadDir()
			return
		}
		_, _ = file.Fragments()
		_, _ = file.IsFragmented()
	})
}
