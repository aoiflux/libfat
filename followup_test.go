package libfat

import (
	"bytes"
	"errors"
	"testing"
)

// TestParseShortNameLowersASCIIOnly pins what the NTRes case bits mean:
// Windows lower-cases the ASCII letters A-Z and nothing else. Bytes from 0x80
// up are OEM code-page characters with no case mapping this package could
// know, and must come back exactly as stored - including 0x05, which stands
// for 0xE5.
func TestParseShortNameLowersASCIIOnly(t *testing.T) {
	raw := raw11("\x05B\x80C\x9a   T\x99X")
	for _, tc := range []struct {
		ntres byte
		want  string
	}{
		{0, "\xe5B\x80C\x9a.T\x99X"},
		{NTResLowerBase, "\xe5b\x80c\x9a.T\x99X"},
		{NTResLowerExt, "\xe5B\x80C\x9a.t\x99x"},
		{NTResLowerBase | NTResLowerExt, "\xe5b\x80c\x9a.t\x99x"},
	} {
		if got := ParseShortName(raw, tc.ntres); got != tc.want {
			t.Errorf("ntres %#02x: got %q, want %q", tc.ntres, got, tc.want)
		}
	}
}

func TestReadDirLowersASCIIOnly(t *testing.T) {
	im := newTestImage(t, FATType16)
	rec := makeTimestampedEntry("", "", AttrArchive, 0, 0)
	copy(rec[0:11], "\x05B\x80     TXT")
	rec[12] = NTResLowerBase | NTResLowerExt
	im.addRootRaw(rec)
	entries := mustReadRoot(t, im.volume())
	if len(entries) != 1 || entries[0].ShortName != "\xe5b\x80.txt" {
		t.Fatalf("listing = %+v", entries)
	}
}

func TestCloseReleasesFATCache(t *testing.T) {
	t.Run("loaded before close", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "A", ext: "TXT", clusters: []uint32{5}, size: 3, content: []byte("abc"), terminate: true})
		v := im.volumeWithOptions(OpenOptions{CacheFAT: true})
		if _, err := v.IsClusterAllocated(5); err != nil {
			t.Fatal(err)
		}
		if v.fatCache.tables == nil {
			t.Fatal("the cache was not loaded, so this test measures nothing")
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
		if v.fatCache.tables != nil {
			t.Fatal("Close kept the cached FAT copies")
		}
		if _, err := v.IsClusterAllocated(5); !errors.Is(err, ErrVolumeClosed) {
			t.Fatalf("lookup after Close = %v, want ErrVolumeClosed", err)
		}
		if v.fatCache.tables != nil {
			t.Fatal("a lookup after Close reloaded the cache")
		}
	})
	t.Run("never loaded", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		fatStart, fatEnd := im.fatRegion()
		counter := &countingReaderAt{inner: &mockReaderAt{data: im.data}}
		v, err := OpenWithOptions(counter, OpenOptions{CacheFAT: true})
		if err != nil {
			t.Fatal(err)
		}
		_ = v.Close()
		counter.reset()
		_, _ = v.IsClusterAllocated(5)
		if got := counter.readsWithin(fatStart, fatEnd); got != 0 {
			t.Fatalf("a closed volume read the FAT %d times", got)
		}
		if v.fatCache.tables != nil {
			t.Fatal("a closed volume loaded the cache")
		}
	})
}

// TestFAT16IgnoresEAIndex covers bytes 20-21 of a short entry, which hold the
// high word of the first cluster only on FAT32. On FAT12 and FAT16 they are
// the OS/2 extended-attribute index, and a non-zero value there must not move
// the file.
func TestFAT16IgnoresEAIndex(t *testing.T) {
	content := patternBytes(700, 9)
	build := func(t *testing.T) *testImage {
		im := newTestImage(t, FATType16)
		at := im.rootDirOffset() + int64(im.rootCursor)
		im.addFile(testFile{base: "EA", ext: "BIN", clusters: []uint32{5, 6}, size: 700, content: content, terminate: true})
		putUint16LE(im.data, int(at)+20, 0x0001)

		// A subdirectory whose own entry and "." record carry an EA index.
		at = im.rootDirOffset() + int64(im.rootCursor)
		im.addSubdir("SUB", []uint32{10}, makeTimestampedEntry("X", "TXT", AttrArchive, 0, 0))
		putUint16LE(im.data, int(at)+20, 0x0001)
		putUint16LE(im.data, int(im.clusterOffset(10))+20, 0x0001)
		return im
	}

	for _, opts := range []OpenOptions{{}, {Strict: true}} {
		im := build(t)
		v := im.volumeWithOptions(opts)
		e := entryNamed(t, v, "EA.BIN")
		if e.FirstCluster != 5 {
			t.Fatalf("strict=%v: FirstCluster = %#x, want 5", opts.Strict, e.FirstCluster)
		}
		data, err := readAllPath(v, "/EA.BIN")
		if err != nil || !bytes.Equal(data, content) {
			t.Fatalf("strict=%v: ReadAll = %d bytes, %v", opts.Strict, len(data), err)
		}
		sub, err := v.OpenPath("/SUB")
		if err != nil {
			t.Fatalf("strict=%v: %v", opts.Strict, err)
		}
		if _, err := sub.ReadDir(); err != nil {
			t.Fatalf("strict=%v: ReadDir /SUB: %v", opts.Strict, err)
		}
	}

	// The same bytes are the cluster's high word on FAT32, where they must
	// still be read.
	im := newTestImage(t, FATType32)
	im.addRootRaw(makeTimestampedEntry("HIGH", "BIN", AttrArchive, 0x10005, 10))
	if e := entryNamed(t, im.volume(), "HIGH.BIN"); e.FirstCluster != 0x10005 {
		t.Fatalf("FAT32 FirstCluster = %#x, want 0x10005", e.FirstCluster)
	}
}

func TestFAT16OrphanSweepIgnoresEAIndex(t *testing.T) {
	im := newTestImage(t, FATType16)
	child := makeTimestampedEntry("KEPT", "TXT", AttrArchive, 7, 3)
	putUint16LE(child, 20, 0x0001)
	im.addDeletedSubdir("GONE", []uint32{20}, false, child)
	putUint16LE(im.data, int(im.clusterOffset(20))+20, 0x0001) // "." record

	res, err := im.volume().ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range res.Directories {
		for _, e := range dir.Entries {
			if e.Name == "KEPT.TXT" {
				if e.FirstCluster != 7 {
					t.Fatalf("orphan FirstCluster = %#x, want 7", e.FirstCluster)
				}
				return
			}
		}
	}
	t.Fatal("the orphaned directory was not found")
}

func TestStrictDotDotNamesParent(t *testing.T) {
	for _, fatType := range []string{FATType16, FATType32} {
		t.Run(fatType+" well formed", func(t *testing.T) {
			// SUB below the root has ".." = 0, even on FAT32 where the root is
			// cluster 2; INNER below SUB names SUB's first cluster.
			im := newTestImage(t, fatType)
			im.writeSubdir([]uint32{60}, 50, makeTimestampedEntry("DEEP", "TXT", AttrArchive, 0, 0))
			im.addSubdir("SUB", []uint32{50}, makeTimestampedEntry("INNER", "", AttrDirectory, 60, 0))
			v := strictVolume(t, im)
			if _, err := v.OpenPath("/SUB/INNER/DEEP.TXT"); err != nil {
				t.Fatal(err)
			}
			inner, _ := v.OpenPath("/SUB/INNER")
			if _, err := inner.ReadDir(); err != nil {
				t.Fatal(err)
			}
		})
	}

	cases := []struct {
		name   string
		dir    string
		dirCl  uint32
		dotdot uint32
	}{
		{"below the root names a cluster", "/SUB", 50, 2},
		{"nested names the wrong parent", "/SUB/INNER", 60, 7},
		{"nested names the root", "/SUB/INNER", 60, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			im := newTestImage(t, FATType32)
			im.writeSubdir([]uint32{60}, 50, makeTimestampedEntry("DEEP", "TXT", AttrArchive, 0, 0))
			im.addSubdir("SUB", []uint32{50}, makeTimestampedEntry("INNER", "", AttrDirectory, 60, 0))
			putUint16LE(im.data, int(im.clusterOffset(tc.dirCl))+DirEntrySize+26, uint16(tc.dotdot))

			tolerant, err := im.volume().OpenPath(tc.dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tolerant.ReadDir(); err != nil {
				t.Fatalf("tolerant ReadDir: %v", err)
			}
			strict, err := strictVolume(t, im).OpenPath(tc.dir)
			if err != nil {
				t.Fatal(err)
			}
			_, err = strict.ReadDir()
			requireStrict(t, err, StrictDotEntries, tc.dirCl, DirEntrySize)
		})
	}
}
