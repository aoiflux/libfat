package libfat

import (
	"errors"
	"reflect"
	"testing"
)

// Each test here builds an image that breaks one strict rule, then checks two
// things: a Strict volume refuses it with a *StrictError naming the rule and
// the record at fault, and a default volume reads it as it always has. The
// second half is what "Strict never changes what non-strict callers see"
// means in practice.

func strictVolume(t *testing.T, im *testImage) *Volume {
	t.Helper()
	return im.volumeWithOptions(OpenOptions{Strict: true})
}

func requireStrict(t *testing.T, err error, check StrictCheck, dirCluster uint32, entryOffset int64) *StrictError {
	t.Helper()
	var se *StrictError
	if !errors.As(err, &se) {
		t.Fatalf("want *StrictError %s, got %v", check, err)
	}
	if !errors.Is(err, ErrStrict) {
		t.Fatalf("%v does not match ErrStrict", err)
	}
	if se.Check != check || se.DirCluster != dirCluster || se.EntryOffset != entryOffset {
		t.Fatalf("got %s at (%d, %d), want %s at (%d, %d): %v",
			se.Check, se.DirCluster, se.EntryOffset, check, dirCluster, entryOffset, err)
	}
	return se
}

func readRoot(t *testing.T, v *Volume) ([]DirEntry, error) {
	t.Helper()
	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatal(err)
	}
	return root.ReadDir()
}

func mustReadRoot(t *testing.T, v *Volume) []DirEntry {
	t.Helper()
	entries, err := readRoot(t, v)
	if err != nil {
		t.Fatalf("tolerant ReadDir failed: %v", err)
	}
	return entries
}

func readAllPath(v *Volume, p string) ([]byte, error) {
	f, err := v.OpenPath(p)
	if err != nil {
		return nil, err
	}
	return f.ReadAll()
}

// readTree lists every directory reachable from the root and reads every
// file, failing on the first error, which is how a writer checks its output.
func readTree(t *testing.T, v *Volume) int {
	t.Helper()
	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	var walk func(*File)
	walk = func(dir *File) {
		entries, err := dir.ReadDirWithOptions(ReadDirOptions{ExcludeDeleted: true})
		if err != nil {
			t.Fatalf("ReadDir %s: %v", dir.Path(), err)
		}
		for _, e := range entries {
			count++
			f, err := v.OpenEntry(e)
			if err != nil {
				t.Fatal(err)
			}
			if e.IsDirectory {
				walk(f)
				continue
			}
			if _, err := f.ReadAll(); err != nil {
				t.Fatalf("ReadAll %s: %v", e.Path, err)
			}
		}
	}
	walk(root)
	return count
}

// windowsShapedImage lays out what a Windows-formatted volume holds: long
// names, NTRes case bits, an unstamped empty file, nested directories and a
// fragmented file. Strict must accept all of it.
func windowsShapedImage(t *testing.T, fatType string) *testImage {
	im := newTestImage(t, fatType)
	im.addFile(testFile{base: "QUARTE~1", ext: "TXT", longName: "quarterly report.txt",
		clusters: []uint32{20}, size: 5, content: []byte("hello"), terminate: true})
	lower := makeTimestampedEntry("README", "TXT", AttrArchive, 21, 3)
	lower[12] = NTResLowerBase | NTResLowerExt
	im.linkChain([]uint32{21}, true)
	im.addRootRaw(lower)
	im.addRootRaw(unstampedEmptyEntry())
	im.addFile(testFile{base: "SPLIT", ext: "BIN", clusters: []uint32{30, 31, 40},
		size: 1300, content: patternBytes(1300, 3), terminate: true})

	// /OUTER/INNER/DEEP.TXT, with ".." in OUTER naming the root as 0.
	inner := []uint32{60}
	outer := []uint32{50, 55}
	im.linkChain([]uint32{61}, true)
	im.writeSubdir(inner, outer[0], makeTimestampedEntry("DEEP", "TXT", AttrArchive, 61, 4))
	im.addSubdir("OUTER", outer, makeTimestampedEntry("INNER", "", AttrDirectory, inner[0], 0))
	return im
}

func TestStrictAcceptsWellFormedVolume(t *testing.T) {
	for _, fatType := range []string{FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := windowsShapedImage(t, fatType)
			strictCount := readTree(t, strictVolume(t, im))
			tolerantCount := readTree(t, im.volume())
			if strictCount != tolerantCount || strictCount != 7 {
				t.Fatalf("strict saw %d entries, tolerant %d, want 7", strictCount, tolerantCount)
			}
			// Listings are identical, not merely both successful.
			strictRoot, _ := readRoot(t, strictVolume(t, im))
			if !reflect.DeepEqual(strictRoot, mustReadRoot(t, im.volume())) {
				t.Fatal("strict and tolerant listings differ")
			}
			if _, err := strictVolume(t, im).OpenPath("/OUTER/INNER/DEEP.TXT"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStrictListsUnstampedEmptyFile(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addRootRaw(unstampedEmptyEntry())
	v := strictVolume(t, im)
	entries, err := readRoot(t, v)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "EMPTY.TXT" {
		t.Fatalf("strict listing = %+v", entries)
	}
	if data, err := readAllPath(v, "/EMPTY.TXT"); err != nil || len(data) != 0 {
		t.Fatalf("ReadAll = %q, %v", data, err)
	}
}

func TestStrictChainRunsIntoFreeCluster(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "A", ext: "BIN", clusters: []uint32{5, 6, 7}, size: 1536, terminate: true})
		im.setFATEntry(6, 0)

		if _, err := readAllPath(im.volume(), "/A.BIN"); !errors.Is(err, ErrTruncatedChain) {
			t.Fatalf("tolerant ReadAll = %v, want ErrTruncatedChain", err)
		}
		_, err := readAllPath(strictVolume(t, im), "/A.BIN")
		if se := requireStrict(t, err, StrictChainFreeCluster, FixedRootCluster, 0); se.Cluster != 6 {
			t.Fatalf("Cluster = %d, want 6", se.Cluster)
		}
	})
	t.Run("directory", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addSubdir("SUB", []uint32{10, 11}, makeTimestampedEntry("X", "TXT", AttrArchive, 0, 0))
		im.setFATEntry(11, 0)

		tolerant, err := im.volume().OpenPath("/SUB")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tolerant.ReadDir(); err != nil {
			t.Fatalf("tolerant ReadDir: %v", err)
		}
		sub, err := strictVolume(t, im).OpenPath("/SUB")
		if err != nil {
			t.Fatal(err)
		}
		_, err = sub.ReadDir()
		requireStrict(t, err, StrictChainFreeCluster, FixedRootCluster, 0)
	})
	t.Run("FAT32 root", func(t *testing.T) {
		im := newTestImage(t, FATType32)
		im.linkChain([]uint32{2, 3}, false)
		_ = mustReadRoot(t, im.volume())
		_, err := readRoot(t, strictVolume(t, im))
		requireStrict(t, err, StrictChainFreeCluster, 2, -1)
	})
}

func TestStrictChainCorrupt(t *testing.T) {
	t.Run("loop", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "A", ext: "BIN", clusters: []uint32{5, 6}, size: 1024, content: patternBytes(1024, 1)})
		im.setFATEntry(6, 5)

		// The loop closes after exactly the clusters the size needs, so the
		// tolerant reading returns every byte.
		if _, err := readAllPath(im.volume(), "/A.BIN"); err != nil {
			t.Fatalf("tolerant ReadAll: %v", err)
		}
		_, err := readAllPath(strictVolume(t, im), "/A.BIN")
		requireStrict(t, err, StrictChainCorrupt, FixedRootCluster, 0)
		if !errors.Is(err, ErrCorruptStructure) {
			t.Fatal("a loop should also match ErrCorruptStructure")
		}
	})
	t.Run("first cluster out of range", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addRootRaw(makeTimestampedEntry("OK", "TXT", AttrArchive, 0, 0))
		im.addRootRaw(makeTimestampedEntry("FAR", "TXT", AttrArchive, 0xFFF0, 10))

		if len(mustReadRoot(t, im.volume())) != 2 {
			t.Fatal("tolerant ReadDir should list both records")
		}
		_, err := readRoot(t, strictVolume(t, im))
		requireStrict(t, err, StrictChainCorrupt, FixedRootCluster, 32)
	})
}

func TestStrictChainLength(t *testing.T) {
	t.Run("long chain", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "A", ext: "BIN", clusters: []uint32{5, 6, 7}, size: 600, terminate: true})

		if data, err := readAllPath(im.volume(), "/A.BIN"); err != nil || len(data) != 600 {
			t.Fatalf("tolerant ReadAll: %d bytes, %v", len(data), err)
		}
		_, err := readAllPath(strictVolume(t, im), "/A.BIN")
		requireStrict(t, err, StrictChainLength, FixedRootCluster, 0)
	})
	t.Run("short chain", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "A", ext: "BIN", clusters: []uint32{5, 6, 7}, size: 2000, terminate: true})

		if _, err := readAllPath(im.volume(), "/A.BIN"); !errors.Is(err, ErrTruncatedChain) {
			t.Fatalf("tolerant ReadAll = %v, want ErrTruncatedChain", err)
		}
		_, err := readAllPath(strictVolume(t, im), "/A.BIN")
		requireStrict(t, err, StrictChainLength, FixedRootCluster, 0)
		if !errors.Is(err, ErrTruncatedChain) {
			t.Fatal("a short chain should also match ErrTruncatedChain")
		}
	})
	t.Run("empty file with a cluster", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "A", ext: "BIN", clusters: []uint32{5}, size: 0, terminate: true})

		if _, err := readAllPath(im.volume(), "/A.BIN"); err != nil {
			t.Fatalf("tolerant ReadAll: %v", err)
		}
		_, err := readAllPath(strictVolume(t, im), "/A.BIN")
		requireStrict(t, err, StrictChainLength, FixedRootCluster, 0)
	})
	t.Run("bounded walk is not a violation", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "A", ext: "BIN", clusters: []uint32{5, 6, 7}, size: 1536, terminate: true})
		v := strictVolume(t, im)
		e := entryNamed(t, v, "A.BIN")
		res, err := v.FragmentOffsetsWithOptions(e, FragmentOptions{MaxClusters: 2})
		if err != nil {
			t.Fatalf("a caller-bounded walk failed under Strict: %v", err)
		}
		if !res.ChainBroken || !res.Truncated {
			t.Fatalf("bounded walk flags: %+v", res)
		}
	})
}

// lfnRecords returns the slots and short entry of a two-slot long name.
func lfnRecords() [][]byte {
	const short = "ALONGF~1TXT"
	recs := makeLFNEntries("a long file name.txt", short)
	if len(recs) != 2 {
		panic("want two slots")
	}
	entry := makeTimestampedEntry("ALONGF~1", "TXT", AttrArchive, 0, 0)
	return append(recs, entry)
}

func TestStrictLFN(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(recs [][]byte)
		check  StrictCheck
		offset int64
	}{
		{"slots out of order", func(r [][]byte) { r[0], r[1] = r[1], r[0] }, StrictLFNSequence, 0},
		{"missing last flag", func(r [][]byte) { r[0][0] &^= LFNLastFlag }, StrictLFNSequence, 0},
		{"slot checksums differ", func(r [][]byte) { r[1][13] ^= 0x01 }, StrictLFNChecksum, 32},
		{"checksum differs from short name", func(r [][]byte) { r[0][13] ^= 0x01; r[1][13] ^= 0x01 }, StrictLFNChecksum, 64},
		{"stops before slot 1", func(r [][]byte) { r[1] = makeTimestampedEntry("OTHER", "TXT", AttrArchive, 0, 0) }, StrictLFNSequence, 32},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			im := newTestImage(t, FATType16)
			recs := lfnRecords()
			tc.mutate(recs)
			im.addRootRaw(recs...)

			// The tolerant reading falls back to the 8.3 name and lists the file.
			found := false
			for _, e := range mustReadRoot(t, im.volume()) {
				if e.ShortName == "ALONGF~1.TXT" {
					found = true
					if e.NameSource != NameSourceShort {
						t.Fatalf("tolerant reading kept a long name: %q", e.Name)
					}
				}
			}
			if !found {
				t.Fatal("tolerant reading dropped the short entry")
			}
			_, err := readRoot(t, strictVolume(t, im))
			requireStrict(t, err, tc.check, FixedRootCluster, tc.offset)
		})
	}
}

func TestStrictDotEntries(t *testing.T) {
	t.Run("in the root", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addRootRaw(makeTimestampedEntry("A", "TXT", AttrArchive, 0, 0))
		im.addRootRaw(makeShortEntry(".", "", AttrDirectory, 0, 0))
		if len(mustReadRoot(t, im.volume())) != 1 {
			t.Fatal("tolerant ReadDir should skip the stray dot record")
		}
		_, err := readRoot(t, strictVolume(t, im))
		requireStrict(t, err, StrictDotEntries, FixedRootCluster, 32)
	})

	sub := func(t *testing.T, wantOffset int64, mutate func(cluster []byte)) {
		t.Helper()
		im := newTestImage(t, FATType16)
		im.addSubdir("SUB", []uint32{10}, makeTimestampedEntry("X", "TXT", AttrArchive, 0, 0))
		off := im.clusterOffset(10)
		mutate(im.data[off : off+int64(im.bytesPerCluster())])

		tolerant, err := im.volume().OpenPath("/SUB")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tolerant.ReadDir(); err != nil {
			t.Fatalf("tolerant ReadDir: %v", err)
		}
		strict, err := strictVolume(t, im).OpenPath("/SUB")
		if err != nil {
			t.Fatal(err)
		}
		_, err = strict.ReadDir()
		requireStrict(t, err, StrictDotEntries, 10, wantOffset)
	}
	t.Run("missing", func(t *testing.T) {
		sub(t, 0, func(c []byte) {
			copy(c, make([]byte, 64))
			copy(c, makeTimestampedEntry("X", "TXT", AttrArchive, 0, 0))
		})
	})
	t.Run("dot names another cluster", func(t *testing.T) {
		sub(t, 0, func(c []byte) { putUint16LE(c, 26, 11) })
	})
	t.Run("a third dot record", func(t *testing.T) {
		sub(t, 64, func(c []byte) { copy(c[64:96], makeShortEntry("..", "", AttrDirectory, 0, 0)) })
	})
}

func TestStrictDuplicateNames(t *testing.T) {
	t.Run("same short name", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addRootRaw(makeTimestampedEntry("A", "TXT", AttrArchive, 0, 0))
		im.addRootRaw(makeTimestampedEntry("A", "TXT", AttrArchive, 0, 0))
		if len(mustReadRoot(t, im.volume())) != 2 {
			t.Fatal("tolerant ReadDir should list both")
		}
		_, err := readRoot(t, strictVolume(t, im))
		requireStrict(t, err, StrictDuplicateName, FixedRootCluster, 32)
	})
	t.Run("long name against another's short name", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addRootRaw(makeTimestampedEntry("FOO", "TXT", AttrArchive, 0, 0))
		recs := makeLFNEntries("Foo.txt", "FOO~1   TXT")
		recs = append(recs, makeTimestampedEntry("FOO~1", "TXT", AttrArchive, 0, 0))
		im.addRootRaw(recs...)
		_ = mustReadRoot(t, im.volume())
		_, err := readRoot(t, strictVolume(t, im))
		requireStrict(t, err, StrictDuplicateName, FixedRootCluster, 64)
	})
	t.Run("deleted entries do not collide", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		gone := makeTimestampedEntry("A", "TXT", AttrArchive, 0, 0)
		gone[0] = DeletedMarker
		im.addRootRaw(makeTimestampedEntry("_", "TXT", AttrArchive, 0, 0), gone)
		if _, err := readRoot(t, strictVolume(t, im)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStrictAfterEndMarker(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addRootRaw(
		makeTimestampedEntry("A", "TXT", AttrArchive, 0, 0),
		make([]byte, 32),
		makeTimestampedEntry("B", "TXT", AttrArchive, 0, 0),
	)
	if got := mustReadRoot(t, im.volume()); len(got) != 1 {
		t.Fatalf("tolerant ReadDir = %d entries, want only A", len(got))
	}
	_, err := readRoot(t, strictVolume(t, im))
	requireStrict(t, err, StrictAfterEndMarker, FixedRootCluster, 64)

	// A deleted record past the marker is how deleted entries are recovered,
	// and is not a violation.
	im = newTestImage(t, FATType16)
	gone := makeTimestampedEntry("B", "TXT", AttrArchive, 0, 0)
	gone[0] = DeletedMarker
	im.addRootRaw(makeTimestampedEntry("A", "TXT", AttrArchive, 0, 0), make([]byte, 32), gone)
	if _, err := readRoot(t, strictVolume(t, im)); err != nil {
		t.Fatal(err)
	}
}

func TestStrictInvalidEntry(t *testing.T) {
	im := newTestImage(t, FATType16)
	bad := makeTimestampedEntry("A*B", "TXT", AttrArchive, 0, 0)
	im.addRootRaw(makeTimestampedEntry("OK", "TXT", AttrArchive, 0, 0), bad)
	if got := mustReadRoot(t, im.volume()); len(got) != 1 {
		t.Fatalf("tolerant ReadDir = %d entries, want the invalid one skipped", len(got))
	}
	_, err := readRoot(t, strictVolume(t, im))
	requireStrict(t, err, StrictInvalidEntry, FixedRootCluster, 32)
}

func TestStrictFATMirror(t *testing.T) {
	diverge := func(im *testImage, cluster, value uint32) {
		off := int(im.reserved+im.fatSectors)*int(im.bytesPerSector) + int(cluster)*4
		putUint32LE(im.data, off, value)
	}

	im := newTestImage(t, FATType32)
	im.addFile(testFile{base: "A", ext: "BIN", clusters: []uint32{5, 6}, size: 1024, terminate: true})
	diverge(im, 5, FAT32EOC)

	tolerant := im.volume()
	if _, err := readAllPath(tolerant, "/A.BIN"); err != nil {
		t.Fatalf("tolerant ReadAll: %v", err)
	}
	if tolerant.FATMirrorMismatches() == 0 {
		t.Fatal("tolerant reading did not count the mismatch")
	}
	strict := strictVolume(t, im)
	_, err := readRoot(t, strict)
	if se := requireStrict(t, err, StrictFATMirror, 2, 0); se.Cluster != 5 {
		t.Fatalf("Cluster = %d, want 5", se.Cluster)
	}

	// The orphan sweep is recovery and reads the FAT tolerantly whatever
	// Strict says.
	a, err := tolerant.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := strict.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Directories, b.Directories) {
		t.Fatal("Strict changed the orphan sweep")
	}

	// With mirroring switched off in BPB_ExtFlags the copies may differ.
	im.data[40] |= 0x80
	if _, err := readAllPath(strictVolume(t, im), "/A.BIN"); err != nil {
		t.Fatalf("Strict rejected a disagreement the volume declares legitimate: %v", err)
	}
}

func TestStrictIgnoresDeletedEntries(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 3, content: []byte("abc"), terminate: true})
	// A deleted file's chain is freed, which is exactly what Strict refuses in
	// a live one.
	im.addFile(testFile{base: "GONE", ext: "BIN", longName: "gone file.bin", clusters: []uint32{6, 7}, size: 1024, deleted: true})
	v := strictVolume(t, im)
	entries, err := readRoot(t, v)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Deleted {
			continue
		}
		f, err := v.OpenEntry(e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.ReadAll(); errors.Is(err, ErrStrict) {
			t.Fatalf("Strict applied to a deleted entry: %v", err)
		}
	}
}
