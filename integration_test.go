package libfat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Integration tests against real filesystem images.
//
// Everything else in this package is tested against images this package builds
// itself, which is what makes those tests fast, hermetic and able to construct
// damage on purpose. What they cannot do is catch an assumption that this
// library and its own fixture builder share. A volume produced by mkfs.fat and
// partitioned by Windows is written by implementations that have never seen
// this code.
//
// These tests are skipped unless LIBFAT_TEST_IMAGES names a directory holding
// the images described by realVolumes. They are opt-in rather than gated behind
// a build tag so that a contributor who has the images needs only an
// environment variable, and everyone else sees an explicit skip reason rather
// than silence.
const envImages = "LIBFAT_TEST_IMAGES"

// realVolume describes a FAT volume inside one of the dataset's images.
//
// The geometry here was decoded from each volume's BIOS parameter block
// independently of this library, so the geometry test is a comparison against
// an outside reading rather than against libfat's own output. base for the GPT
// image is partition 3's first LBA from the image's GUID partition table
// (4227072) times its 512-byte sectors; the volume is a 512 MiB FAT32 written
// with an MSDOS5.0 OEM name.
type realVolume struct {
	name string
	file string
	// base is the byte offset of the volume's boot sector within file.
	base int64
	// size is the volume's byte length, used to scope an io.SectionReader.
	size int64

	fatType           string
	bytesPerSector    uint32
	sectorsPerCluster uint32
	clusterCount      uint32
	volumeSize        uint64

	// fragmentedFiles and fragmentedDirs are how many non-empty regular files,
	// and how many directories, occupy more than one run.
	//
	// They are pinned because multi-run coverage is what matters most here and
	// is the easiest to lose silently: a contiguous entry exercises none of the
	// chain walking and coalescing that produces a multi-run extent map, so a
	// sampling rule that never reaches a fragmented one reports success while
	// testing nothing. On these images every fragmented entry is a directory -
	// the files were written once onto empty volumes, while the directories
	// grew a cluster at a time as files were added around them - which makes
	// the directory count the more important of the two to hold.
	fragmentedFiles int
	fragmentedDirs  int

	// oracle names the independent cross-check beside the image, when it is not
	// the default of file + ".fat-oracle.tsv". The GPT image needs one because
	// its oracle describes the partition rather than the whole disk.
	oracle string
}

var realVolumes = []realVolume{
	{
		name: "fat16-whole-file", file: "img9_fat16.dd",
		base: 0, size: 536870912,
		fatType: FATType16, bytesPerSector: 512, sectorsPerCluster: 16,
		clusterCount: 65500, volumeSize: 536868864,
		fragmentedFiles: 0, fragmentedDirs: 4,
	},
	{
		name: "fat32-whole-file", file: "img10_fat32.dd",
		base: 0, size: 536870912,
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 8,
		clusterCount: 130811, volumeSize: 536868864,
		fragmentedFiles: 0, fragmentedDirs: 6,
	},
	{
		// The reason F5 exists: a volume whose first byte is not the reader's.
		name: "fat32-gpt-partition", file: "img4_gpt.dd",
		base: 2164260864, size: 536870912,
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 8,
		clusterCount: 130048, volumeSize: 536870912,
		fragmentedFiles: 0, fragmentedDirs: 0,
		oracle: "img4_gpt.dd.p3.fat-oracle.tsv",
	},
}

func gptPartitionSpec(t *testing.T) realVolume {
	t.Helper()
	for _, spec := range realVolumes {
		if spec.base != 0 {
			return spec
		}
	}
	t.Fatal("no volume in realVolumes sits at a non-zero offset")
	return realVolume{}
}

// imagesDir returns the dataset directory, or skips.
func imagesDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv(envImages)
	if dir == "" {
		t.Skipf("set %s to the directory holding the test images to run this", envImages)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Skipf("%s=%q: %v", envImages, dir, err)
	}
	if !info.IsDir() {
		t.Skipf("%s=%q is not a directory", envImages, dir)
	}
	return dir
}

// openFile opens the image and registers its close, or skips when the dataset
// does not have it.
func (spec realVolume) openFile(t *testing.T) *os.File {
	t.Helper()
	path := filepath.Join(imagesDir(t), spec.file)
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("%s: %v", spec.file, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	spec.requireExpectedVolume(t, f)
	return f
}

// requireExpectedVolume skips when the bytes at spec.base are not the volume
// the manifest describes. A dataset holding a different image under the same
// name is a reason to skip rather than to fail: the failure would be about the
// dataset, and reporting it as a libfat defect would be a lie.
func (spec realVolume) requireExpectedVolume(t *testing.T, r io.ReaderAt) {
	t.Helper()
	buf := make([]byte, 512)
	if _, err := r.ReadAt(buf, spec.base); err != nil {
		t.Skipf("%s: cannot read a boot sector at offset %d: %v", spec.file, spec.base, err)
	}
	if buf[510] != 0x55 || buf[511] != 0xAA {
		t.Skipf("%s: no boot signature at offset %d; not the expected volume", spec.file, spec.base)
	}
	if got := uint32(buf[11]) | uint32(buf[12])<<8; got != spec.bytesPerSector {
		t.Skipf("%s: %d bytes per sector at offset %d, manifest says %d; not the expected volume",
			spec.file, got, spec.base, spec.bytesPerSector)
	}
}

// open returns the image file and a volume opened over the whole of it with
// BaseOffset set, which is the coordinate system a forensic caller wants: every
// offset the volume reports addresses the image directly.
func (spec realVolume) open(t *testing.T) (*os.File, *Volume) {
	t.Helper()
	f := spec.openFile(t)
	v, err := OpenWithOptions(f, OpenOptions{BaseOffset: spec.base})
	if err != nil {
		t.Fatalf("OpenWithOptions(%s, BaseOffset=%d) failed: %v", spec.file, spec.base, err)
	}
	t.Cleanup(func() { _ = v.Close() })
	return f, v
}

// TestRealVolumeGeometry checks libfat's reading of each boot sector against a
// decode made outside this library.
func TestRealVolumeGeometry(t *testing.T) {
	for _, spec := range realVolumes {
		t.Run(spec.name, func(t *testing.T) {
			_, v := spec.open(t)

			if got := v.FATType(); got != spec.fatType {
				t.Errorf("FATType = %q, want %q", got, spec.fatType)
			}
			if got := v.BytesPerSector(); got != spec.bytesPerSector {
				t.Errorf("BytesPerSector = %d, want %d", got, spec.bytesPerSector)
			}
			if got := v.SectorsPerCluster(); got != spec.sectorsPerCluster {
				t.Errorf("SectorsPerCluster = %d, want %d", got, spec.sectorsPerCluster)
			}
			if got := v.ClusterCount(); got != spec.clusterCount {
				t.Errorf("ClusterCount = %d, want %d", got, spec.clusterCount)
			}
			if got := v.VolumeSize(); got != spec.volumeSize {
				t.Errorf("VolumeSize = %d, want %d", got, spec.volumeSize)
			}
			if got := v.BaseOffset(); got != spec.base {
				t.Errorf("BaseOffset = %d, want %d", got, spec.base)
			}
			if v.UsedBackupBootSector() {
				t.Error("fell back to the backup boot sector on an undamaged image")
			}
		})
	}
}

// walkBudget bounds how much of a real volume a test reads.
//
// The bound is deliberately not a file count. These volumes hold a handful of
// very large files among a couple of thousand small ones, so a plain "first N
// files" or "first N bytes" rule spends everything on the large ones and never
// reaches the rest - which is how an earlier version of this test walked past
// every fragmented file on all three images and reported success. Extents are
// therefore checked for every file, whole-file hashing is capped per file so no
// single file can exhaust the budget, and fragmented files are always hashed in
// full because they are the ones whose extent map can be wrong.
type walkBudget struct {
	// bytes caps total whole-file hashing of contiguous files.
	bytes int64
	// perFile is the largest file hashed in full during that sampling.
	perFile int64
	// probe is how much of each run's head and tail is compared for every
	// file, whatever its size.
	probe int64
}

func budget(t *testing.T) walkBudget {
	t.Helper()
	if testing.Short() {
		return walkBudget{bytes: 8 << 20, perFile: 1 << 20, probe: 4 << 10}
	}
	return walkBudget{bytes: 192 << 20, perFile: 8 << 20, probe: 32 << 10}
}

var errBudgetSpent = errors.New("budget spent")

// verifyRunsInPlace checks each run of a file separately: the bytes at the run's
// image offset must be the bytes at that run's position within the file.
//
// This is a stronger statement than comparing a hash of the whole file, which
// only says the concatenation came out right and can hide two runs whose
// offsets are wrong in compensating ways. Probing the head and tail of every
// run keeps the cost independent of file size, so it runs for every file rather
// than for a sample.
func verifyRunsInPlace(img, inFile io.ReaderAt, ranges []Range, probe int64) error {
	head := make([]byte, probe)
	tail := make([]byte, probe)
	for i, run := range ranges {
		windows := []struct {
			at  int64
			buf []byte
		}{{0, head}}
		if run.Length > probe {
			windows = append(windows, struct {
				at  int64
				buf []byte
			}{run.Length - probe, tail})
		}
		for _, w := range windows {
			n := probe
			if run.Length < n {
				n = run.Length
			}
			fromImage := w.buf[:n]
			if _, err := img.ReadAt(fromImage, run.StartByte+w.at); err != nil {
				return fmt.Errorf("run %d: reading the image at %d: %w", i, run.StartByte+w.at, err)
			}
			fromFile := make([]byte, n)
			if _, err := inFile.ReadAt(fromFile, run.FileOffset+w.at); err != nil {
				return fmt.Errorf("run %d: reading the file at +%d: %w", i, run.FileOffset+w.at, err)
			}
			if !bytes.Equal(fromImage, fromFile) {
				return fmt.Errorf("run %d of %d: the %d bytes at image offset %d are not the file's bytes at +%d",
					i, len(ranges), n, run.StartByte+w.at, run.FileOffset+w.at)
			}
		}
	}
	return nil
}

// hashViaExtents reads a file's bytes straight out of the image at the offsets
// the extent map reports, rather than through Volume.ReadAt. That is what a
// consumer intersecting externally supplied byte ranges does, and it is the
// only way to prove the reported offsets address the right bytes of the image
// itself rather than merely being self-consistent with this library's reader.
func hashViaExtents(r io.ReaderAt, ranges []Range) (string, int64, error) {
	h := sha256.New()
	buf := make([]byte, 1<<20)
	var total int64
	for _, run := range ranges {
		at, left := run.StartByte, run.Length
		for left > 0 {
			n := int64(len(buf))
			if left < n {
				n = left
			}
			if _, err := r.ReadAt(buf[:n], at); err != nil {
				return "", total, fmt.Errorf("ReadAt(%d bytes at %d): %w", n, at, err)
			}
			h.Write(buf[:n])
			at, left, total = at+n, left-n, total+n
		}
	}
	return hex.EncodeToString(h.Sum(nil)), total, nil
}

// hashViaReader reads the same file through the library's own reader.
func hashViaReader(f *File) (string, int64, error) {
	rd, err := f.Reader()
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err := io.Copy(h, rd)
	if err != nil {
		return "", n, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// verifyEntryRecord reads the 32-byte directory record at the offset the volume
// reports for an entry, and checks that it is that entry's own record.
//
// EntryAbsoluteOffset is documented as being resolved through the parent
// directory's run list so that it stays correct when the directory is
// fragmented. That claim can only be tested on a volume that has fragmented
// directories. The three fields compared here live in the record itself, so an
// offset that is wrong fails even when it lands on some other valid record.
func verifyEntryRecord(img io.ReaderAt, e DirEntry) error {
	if e.EntryAbsoluteOffset < 0 {
		return nil
	}
	rec := make([]byte, 32)
	if _, err := img.ReadAt(rec, e.EntryAbsoluteOffset); err != nil {
		return fmt.Errorf("reading the entry record at %d: %w", e.EntryAbsoluteOffset, err)
	}
	if got := rec[11]; got != e.Attributes {
		return fmt.Errorf("the record at %d has attributes 0x%02x, the entry reports 0x%02x",
			e.EntryAbsoluteOffset, got, e.Attributes)
	}
	cluster := uint32(binary.LittleEndian.Uint16(rec[26:28])) |
		uint32(binary.LittleEndian.Uint16(rec[20:22]))<<16
	if cluster != e.FirstCluster {
		return fmt.Errorf("the record at %d names cluster %d, the entry reports %d",
			e.EntryAbsoluteOffset, cluster, e.FirstCluster)
	}
	if size := uint64(binary.LittleEndian.Uint32(rec[28:32])); size != e.Size {
		return fmt.Errorf("the record at %d records size %d, the entry reports %d",
			e.EntryAbsoluteOffset, size, e.Size)
	}
	return nil
}

// holdsOffset reports whether any run contains off.
func holdsOffset(runs []Range, off int64) bool {
	for _, r := range runs {
		if off >= r.StartByte && off < r.EndByte() {
			return true
		}
	}
	return false
}

// checkRunInvariants holds what Range documents, for any entry, and returns the
// bytes the runs cover.
func checkRunInvariants(t *testing.T, path string, ranges []Range, lo, hi int64) int64 {
	t.Helper()
	var sum int64
	for i, run := range ranges {
		if run.Sparse {
			t.Errorf("%s: run %d is sparse, which FAT cannot express", path, i)
		}
		if run.Length <= 0 {
			t.Errorf("%s: run %d has length %d", path, i, run.Length)
		}
		if run.StartByte < lo || run.EndByte() > hi {
			t.Errorf("%s: run %d spans [%d,%d), outside the volume [%d,%d)",
				path, i, run.StartByte, run.EndByte(), lo, hi)
		}
		if run.FileOffset != sum {
			t.Errorf("%s: run %d has FileOffset %d, want the %d bytes before it",
				path, i, run.FileOffset, sum)
		}
		sum += run.Length
	}
	return sum
}

// TestRealVolumeExtentMapReadsBackContent is the central claim of this library
// checked against volumes it did not create: for every entry, the byte ranges
// reported are exactly where that entry's bytes live in the image.
//
// Three things are checked for every entry on the volume, not for a sample:
// the Range invariants the type documents, that the entry's own 32-byte record
// really sits at the offset reported for it and inside its parent directory's
// runs, and that each run's bytes in the image are the bytes at that run's
// position within the file. Whole-file hashing is layered on top for as many
// files as the budget allows, and unconditionally for fragmented ones.
func TestRealVolumeExtentMapReadsBackContent(t *testing.T) {
	for _, spec := range realVolumes {
		t.Run(spec.name, func(t *testing.T) {
			f, v := spec.open(t)
			lo, hi := v.BaseOffset(), v.BaseOffset()+int64(v.VolumeSize())
			cluster := int64(v.BytesPerCluster())
			spent := budget(t)

			// The root directory has no record of its own anywhere on the
			// volume, so Walk never reports it. Its runs are still needed, to
			// place the records of everything directly beneath it.
			rootRuns, err := v.RootDirectoryFragments()
			if err != nil {
				t.Fatalf("RootDirectoryFragments failed: %v", err)
			}
			dirRuns := map[string][]Range{"/": rootRuns}

			var files, dirs, fragFiles, fragDirs, placed, hashedFiles int
			var hashed int64

			err = v.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
				if err := verifyEntryRecord(f, e); err != nil {
					t.Errorf("%s: %v", path, err)
				}
				// Walk is pre-order, so a directory's runs are recorded before
				// its children are reported.
				if runs, ok := dirRuns[e.DirectoryPath]; ok && e.EntryAbsoluteOffset >= 0 {
					placed++
					if !holdsOffset(runs, e.EntryAbsoluteOffset) {
						t.Errorf("%s: its record at %d lies outside the runs reported for %q: %v",
							path, e.EntryAbsoluteOffset, e.DirectoryPath, runs)
					}
				}

				ranges, err := v.FragmentOffsets(e)
				if err != nil {
					t.Errorf("%s: FragmentOffsets failed: %v", path, err)
					return nil
				}
				covered := checkRunInvariants(t, path, ranges, lo, hi)

				if e.IsDirectory {
					dirs++
					if len(ranges) > 1 {
						fragDirs++
					}
					// A directory has no size field to trim against, so its
					// runs are whole clusters to the end of the chain.
					if cluster > 0 && covered%cluster != 0 {
						t.Errorf("%s: a directory covers %d bytes, not a whole number of %d-byte clusters",
							path, covered, cluster)
					}
					dirRuns[path] = ranges
					return nil
				}
				if e.Size == 0 {
					return nil
				}
				files++
				if len(ranges) > 1 {
					fragFiles++
				}
				if covered != int64(e.Size) {
					t.Errorf("%s: runs cover %d bytes, the entry records %d", path, covered, e.Size)
				}

				file, err := v.OpenEntry(e)
				if err != nil {
					t.Errorf("%s: OpenEntry failed: %v", path, err)
					return nil
				}
				inFile, err := file.ReaderAt()
				if err != nil {
					t.Errorf("%s: ReaderAt failed: %v", path, err)
					return nil
				}
				// Every run of every file, at a cost independent of file size.
				if err := verifyRunsInPlace(f, inFile, ranges, spent.probe); err != nil {
					t.Errorf("%s: %v\n  ranges %v", path, err, ranges)
				}

				// Whole-file hashing on top: always for a fragmented file,
				// otherwise while the sampling budget lasts.
				if len(ranges) > 1 || (hashed < spent.bytes && int64(e.Size) <= spent.perFile) {
					fromImage, n, err := hashViaExtents(f, ranges)
					if err != nil {
						t.Errorf("%s: reading the image at the reported offsets failed: %v", path, err)
						return nil
					}
					fromReader, m, err := hashViaReader(file)
					if err != nil {
						t.Errorf("%s: reading through the library failed: %v", path, err)
						return nil
					}
					if n != m {
						t.Errorf("%s: extents yield %d bytes, the reader yields %d", path, n, m)
					}
					if fromImage != fromReader {
						t.Errorf("%s: the bytes at the reported offsets are not the file's content\n"+
							"  image  %s\n  reader %s\n  ranges %v", path, fromImage, fromReader, ranges)
					}
					hashedFiles++
					hashed += n
				}
				return nil
			})
			if err != nil && !errors.Is(err, errBudgetSpent) {
				t.Fatalf("Walk failed: %v", err)
			}

			if files == 0 || dirs == 0 {
				t.Fatalf("the volume yielded %d files and %d directories", files, dirs)
			}
			if placed == 0 {
				t.Fatal("no entry record was placed inside its parent's runs; that check proved nothing")
			}
			if fragFiles != spec.fragmentedFiles || fragDirs != spec.fragmentedDirs {
				t.Errorf("found %d fragmented files and %d fragmented directories, the manifest "+
					"records %d and %d; either this is not the image the manifest describes, or "+
					"chain walking has stopped splitting runs",
					fragFiles, fragDirs, spec.fragmentedFiles, spec.fragmentedDirs)
			}
			t.Logf("%d files and %d directories: every run checked in place, %d records placed "+
				"in their parent's runs, %d files hashed in full (%d bytes); %d fragmented files, "+
				"%d fragmented directories",
				files, dirs, placed, hashedFiles, hashed, fragFiles, fragDirs)
		})
	}
}

// offsetsForPath is every offset a volume reports about one entry.
type offsetsForPath struct {
	entry  int64
	lfn    int64
	slack  int64
	starts []int64
	// shape is what must be identical between the two views: the same runs, of
	// the same lengths, at the same positions in the file.
	shape []Range
}

// collectOffsets walks a volume and records what it says about each path.
func collectOffsets(t *testing.T, v *Volume, limit int) map[string]offsetsForPath {
	t.Helper()
	out := make(map[string]offsetsForPath)
	err := v.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
		if len(out) >= limit {
			return errBudgetSpent
		}
		got := offsetsForPath{entry: e.EntryAbsoluteOffset, lfn: e.LFNEntryOffset, slack: -1}

		file, err := v.OpenEntry(e)
		if err != nil {
			return fmt.Errorf("%s: OpenEntry: %w", path, err)
		}
		ranges, err := file.Fragments()
		if err != nil {
			return fmt.Errorf("%s: Fragments: %w", path, err)
		}
		for _, run := range ranges {
			got.starts = append(got.starts, run.StartByte)
			// Zero the one field that legitimately differs, so the rest can be
			// compared as a whole.
			run.StartByte = 0
			got.shape = append(got.shape, run)
		}
		if slack, ok, err := v.SlackRange(e); err == nil && ok {
			got.slack = slack.StartByte
		}
		out[path] = got
		return nil
	})
	if err != nil && !errors.Is(err, errBudgetSpent) {
		t.Fatalf("Walk failed: %v", err)
	}
	return out
}

// TestRealGPTPartitionBaseOffsetDifferential is F5's acceptance case on a real
// disk: the same FAT32 partition read two ways must describe the same bytes in
// two coordinate systems that differ by exactly the partition's start.
//
// Before BaseOffset existed the scoped reading was the only one available, and
// its offsets were silently partition-relative. Intersecting those against
// whole-disk ranges produced a confident wrong answer, which is the failure
// this test exists to make impossible.
func TestRealGPTPartitionBaseOffsetDifferential(t *testing.T) {
	spec := gptPartitionSpec(t)
	f := spec.openFile(t)

	// Scoped: the reader begins at the partition, so offsets are relative to it.
	scoped, err := OpenWithOptions(io.NewSectionReader(f, spec.base, spec.size), OpenOptions{})
	if err != nil {
		t.Fatalf("opening the partition through a SectionReader failed: %v", err)
	}
	defer scoped.Close()

	// Whole disk: the reader is the image, and BaseOffset says where the volume
	// starts within it.
	whole, err := OpenWithOptions(f, OpenOptions{BaseOffset: spec.base})
	if err != nil {
		t.Fatalf("opening the partition in place failed: %v", err)
	}
	defer whole.Close()

	if scoped.FATType() != whole.FATType() || scoped.ClusterCount() != whole.ClusterCount() {
		t.Fatalf("the two views disagree about the volume: %s/%d vs %s/%d",
			scoped.FATType(), scoped.ClusterCount(), whole.FATType(), whole.ClusterCount())
	}

	// The volume extent itself.
	sr, err := scoped.Report("scoped")
	if err != nil {
		t.Fatalf("Report on the scoped view failed: %v", err)
	}
	wr, err := whole.Report("whole")
	if err != nil {
		t.Fatalf("Report on the whole-disk view failed: %v", err)
	}
	for _, c := range []struct {
		what               string
		scoped, wholeValue int64
	}{
		{"report start_offset", sr.StartOffset, wr.StartOffset},
		{"report end_offset", sr.EndOffset, wr.EndOffset},
		{"meta offset", sr.Filesystem.Offset, wr.Filesystem.Offset},
	} {
		if got := c.wholeValue - c.scoped; got != spec.base {
			t.Errorf("%s: whole-disk %d - scoped %d = %d, want the partition start %d",
				c.what, c.wholeValue, c.scoped, got, spec.base)
		}
	}
	if sr.StartOffset != 0 {
		t.Errorf("scoped report starts at %d, want 0", sr.StartOffset)
	}
	if wr.StartOffset != spec.base {
		t.Errorf("whole-disk report starts at %d, want %d", wr.StartOffset, spec.base)
	}

	const limit = 400
	scopedOffsets := collectOffsets(t, scoped, limit)
	wholeOffsets := collectOffsets(t, whole, limit)

	if len(scopedOffsets) != len(wholeOffsets) {
		t.Fatalf("the two views found %d and %d entries", len(scopedOffsets), len(wholeOffsets))
	}
	if len(scopedOffsets) == 0 {
		t.Fatal("the partition walked but yielded no entries")
	}

	shifted := 0
	for path, s := range scopedOffsets {
		w, ok := wholeOffsets[path]
		if !ok {
			t.Errorf("%q is present in the scoped view and absent from the whole-disk view", path)
			continue
		}
		for _, c := range []struct {
			what               string
			scoped, wholeValue int64
		}{
			{"entry_absolute_offset", s.entry, w.entry},
			{"lfn_entry_offset", s.lfn, w.lfn},
			{"slack start", s.slack, w.slack},
		} {
			// -1 is "could not be located" and is not an offset to shift.
			if c.scoped == -1 || c.wholeValue == -1 {
				if c.scoped != c.wholeValue {
					t.Errorf("%s: %s is %d in one view and %d in the other",
						path, c.what, c.scoped, c.wholeValue)
				}
				continue
			}
			if got := c.wholeValue - c.scoped; got != spec.base {
				t.Errorf("%s: %s differs by %d, want %d", path, c.what, got, spec.base)
			} else {
				shifted++
			}
		}

		if len(s.starts) != len(w.starts) {
			t.Errorf("%s: %d runs scoped, %d runs whole-disk", path, len(s.starts), len(w.starts))
			continue
		}
		for i := range s.starts {
			if got := w.starts[i] - s.starts[i]; got != spec.base {
				t.Errorf("%s: run %d starts differ by %d, want %d", path, i, got, spec.base)
			} else {
				shifted++
			}
		}
		for i := range s.shape {
			if s.shape[i] != w.shape[i] {
				t.Errorf("%s: run %d has shape %+v scoped and %+v whole-disk",
					path, i, s.shape[i], w.shape[i])
			}
		}
	}
	if shifted == 0 {
		t.Fatal("no offset was compared; the test proved nothing")
	}
	t.Logf("%d entries, %d offsets shifted by exactly %d", len(scopedOffsets), shifted, spec.base)
}

// TestRealGPTPartitionBothViewsReadTheSameBytes closes the loop on the
// differential: matching arithmetic is not enough, both sets of offsets must
// actually land on the file's content in their own reader.
func TestRealGPTPartitionBothViewsReadTheSameBytes(t *testing.T) {
	spec := gptPartitionSpec(t)
	f := spec.openFile(t)

	section := io.NewSectionReader(f, spec.base, spec.size)
	scoped, err := OpenWithOptions(section, OpenOptions{})
	if err != nil {
		t.Fatalf("opening the partition through a SectionReader failed: %v", err)
	}
	defer scoped.Close()

	whole, err := OpenWithOptions(f, OpenOptions{BaseOffset: spec.base})
	if err != nil {
		t.Fatalf("opening the partition in place failed: %v", err)
	}
	defer whole.Close()

	spent := budget(t)
	var checked int
	var hashed int64

	err = scoped.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
		if e.IsDirectory || e.Size == 0 {
			return nil
		}
		if hashed >= spent.bytes {
			return errBudgetSpent
		}

		sFile, err := scoped.OpenEntry(e)
		if err != nil {
			return fmt.Errorf("%s: OpenEntry on the scoped view: %w", path, err)
		}
		sRanges, err := sFile.Fragments()
		if err != nil {
			return fmt.Errorf("%s: Fragments on the scoped view: %w", path, err)
		}

		wFile, err := whole.OpenPath(path)
		if err != nil {
			return fmt.Errorf("%s: OpenPath on the whole-disk view: %w", path, err)
		}
		wRanges, err := wFile.Fragments()
		if err != nil {
			return fmt.Errorf("%s: Fragments on the whole-disk view: %w", path, err)
		}

		// Each set of offsets is read against the reader it belongs to.
		sHash, sn, err := hashViaExtents(section, sRanges)
		if err != nil {
			return fmt.Errorf("%s: reading the partition at scoped offsets: %w", path, err)
		}
		wHash, wn, err := hashViaExtents(f, wRanges)
		if err != nil {
			return fmt.Errorf("%s: reading the disk at whole-disk offsets: %w", path, err)
		}
		if sn != wn || sHash != wHash {
			t.Errorf("%s: the two views read different bytes\n  scoped %s (%d bytes)\n  whole  %s (%d bytes)",
				path, sHash, sn, wHash, wn)
		}
		if sn != int64(e.Size) {
			t.Errorf("%s: read %d bytes for a %d-byte entry", path, sn, e.Size)
		}

		checked++
		hashed += sn
		return nil
	})
	if err != nil && !errors.Is(err, errBudgetSpent) {
		t.Fatalf("Walk failed: %v", err)
	}
	if checked == 0 {
		t.Fatal("no files were compared")
	}
	t.Logf("%d files read identically through both coordinate systems, %d bytes", checked, hashed)
}

// TestRealVolumeReportIsWellFormed exercises the whole reporting path, which is
// what a consumer actually calls, over volumes this package did not build.
func TestRealVolumeReportIsWellFormed(t *testing.T) {
	for _, spec := range realVolumes {
		t.Run(spec.name, func(t *testing.T) {
			_, v := spec.open(t)

			rep, err := v.ReportWithOptionsContext(context.Background(), spec.file, ReportOptions{})
			if err != nil {
				t.Fatalf("report failed: %v", err)
			}
			if rep.SchemaVersion != ReportSchemaVersion {
				t.Errorf("SchemaVersion = %d, want %d", rep.SchemaVersion, ReportSchemaVersion)
			}
			if rep.LibraryVersion == "" {
				t.Error("LibraryVersion is empty")
			}
			if rep.Generated.IsZero() {
				t.Error("Generated is the zero time")
			}
			if rep.StartOffset != spec.base {
				t.Errorf("StartOffset = %d, want %d", rep.StartOffset, spec.base)
			}
			if want := spec.base + int64(spec.volumeSize); rep.EndOffset != want {
				t.Errorf("EndOffset = %d, want %d", rep.EndOffset, want)
			}
			if rep.Filesystem.Type != spec.fatType {
				t.Errorf("Meta.Type = %q, want %q", rep.Filesystem.Type, spec.fatType)
			}

			for _, row := range rep.Files {
				if row.Path == "" {
					t.Errorf("a row has an empty path (filename %q)", row.Filename)
					break
				}
				if row.Name == "" {
					t.Errorf("row %q has an empty name", row.Path)
					break
				}
				if row.Filename != row.Path {
					t.Errorf("row %q: Filename and Path disagree (%q)", row.Path, row.Filename)
					break
				}
			}

			caps := v.Capabilities()
			if !caps.SecondFAT {
				t.Error("SecondFAT is false on a volume formatted with two FATs")
			}
			if want := spec.fatType == FATType32; caps.FSInfoSector != want {
				t.Errorf("FSInfoSector = %v on %s, want %v", caps.FSInfoSector, spec.fatType, want)
			}
		})
	}
}
