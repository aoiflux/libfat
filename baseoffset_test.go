package libfat

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// embedImage places the volume image at base bytes into a larger buffer, the
// way a partition sits inside a whole disk, and returns the disk.
func embedImage(volume []byte, base int64) []byte {
	disk := make([]byte, base+int64(len(volume))+4096)
	// Fill the space before the volume with something that is not zero, so a
	// read that lands there because an offset was not rebased returns obviously
	// wrong bytes rather than plausible ones.
	for i := range disk[:base] {
		disk[i] = 0xDB
	}
	copy(disk[base:], volume)
	return disk
}

// offsetsOf gathers every offset the library reports for one entry, keyed by
// what it is, so the two openings can be compared field by field.
func offsetsOf(t *testing.T, v *Volume, name string) map[string]int64 {
	t.Helper()
	e := entryNamed(t, v, name)

	got := map[string]int64{
		"entry_absolute_offset": e.EntryAbsoluteOffset,
		"lfn_entry_offset":      e.LFNEntryOffset,
		"boot_sector_offset":    v.GetBootSector().Offset,
	}

	ranges, err := v.FragmentOffsets(e)
	if err != nil {
		t.Fatalf("FragmentOffsets(%s) failed: %v", name, err)
	}
	for i, r := range ranges {
		got[string(rune('a'+i))+"_range_start"] = r.StartByte
	}

	if off, err := v.ClusterToOffset(e.FirstCluster); err == nil {
		got["cluster_to_offset"] = off
	}

	rep, err := v.Report("t")
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	got["report_start"] = rep.StartOffset
	got["report_end"] = rep.EndOffset
	got["meta_offset"] = rep.Filesystem.Offset

	row := rowFor(t, rep, e.Path)
	got["row_entry_offset"] = row.EntryAbsoluteOffset
	got["row_lfn_offset"] = row.LFNEntryOffset
	for i, f := range row.Fragments {
		got[string(rune('a'+i))+"_frag_start"] = f.StartOffset
		got[string(rune('a'+i))+"_frag_end"] = f.EndOffset
	}
	return got
}

// TestBaseOffsetShiftsEveryReportedOffset is the F5 acceptance case: the same
// volume read two ways must describe the same bytes, and the only difference
// between the two descriptions must be the base.
func TestBaseOffsetShiftsEveryReportedOffset(t *testing.T) {
	const base = int64(1024 * 1024)

	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			im.addFile(testFile{
				base:      "FRAG",
				ext:       "BIN",
				longName:  "fragmented-file.bin",
				clusters:  []uint32{5, 9, 10, 17},
				size:      uint32(im.bytesPerCluster()*3 + 11),
				content:   patternBytes(int(im.bytesPerCluster()*3+11), 0x40),
				terminate: true,
			})

			volumeImage := append([]byte(nil), im.data...)
			disk := embedImage(volumeImage, base)

			// Scoped: a reader that starts at the volume, base 0.
			scoped, err := OpenWithOptions(io.NewSectionReader(
				&mockReaderAt{data: disk}, base, int64(len(volumeImage))), OpenOptions{})
			if err != nil {
				t.Fatalf("scoped open failed: %v", err)
			}
			defer scoped.Close()

			// Whole-disk: the same volume addressed through the whole image.
			whole, err := OpenWithOptions(&mockReaderAt{data: disk}, OpenOptions{BaseOffset: base})
			if err != nil {
				t.Fatalf("whole-disk open failed: %v", err)
			}
			defer whole.Close()

			if whole.BaseOffset() != base {
				t.Fatalf("BaseOffset() = %d, want %d", whole.BaseOffset(), base)
			}
			if scoped.BaseOffset() != 0 {
				t.Fatalf("scoped BaseOffset() = %d, want 0", scoped.BaseOffset())
			}

			relative := offsetsOf(t, scoped, "fragmented-file.bin")
			absolute := offsetsOf(t, whole, "fragmented-file.bin")

			if len(relative) != len(absolute) {
				t.Fatalf("offset sets differ in size: %d vs %d", len(relative), len(absolute))
			}
			for key, rel := range relative {
				abs, ok := absolute[key]
				if !ok {
					t.Fatalf("%s missing from the whole-disk opening", key)
				}
				// -1 is the "could not be resolved" sentinel and is not an
				// offset, so it must not be shifted.
				if rel == -1 {
					if abs != -1 {
						t.Errorf("%s: sentinel -1 became %d", key, abs)
					}
					continue
				}
				if abs-rel != base {
					t.Errorf("%s: whole-disk %d - scoped %d = %d, want %d",
						key, abs, rel, abs-rel, base)
				}
			}

			// The offsets must not merely be shifted; they must address the
			// file's real bytes in the image they claim to be offsets into.
			assertRangesReadTheSame(t, scoped, disk[base:], whole, disk, "fragmented-file.bin")
		})
	}
}

// assertRangesReadTheSame reads each reported range straight out of the backing
// buffer the volume was opened over and requires both openings to yield the
// same content. This is what catches an offset that was shifted consistently
// but points at the wrong place.
func assertRangesReadTheSame(t *testing.T, a *Volume, aData []byte, b *Volume, bData []byte, name string) {
	t.Helper()

	read := func(v *Volume, data []byte, entryName string) []byte {
		e := entryNamed(t, v, entryName)
		ranges, err := v.FragmentOffsets(e)
		if err != nil {
			t.Fatalf("FragmentOffsets failed: %v", err)
		}
		var out []byte
		for _, r := range ranges {
			if r.StartByte < 0 || r.StartByte+r.Length > int64(len(data)) {
				t.Fatalf("range [%d,%d) outside a %d-byte image",
					r.StartByte, r.EndByte(), len(data))
			}
			out = append(out, data[r.StartByte:r.StartByte+r.Length]...)
		}
		return out
	}

	fromScoped := read(a, aData, name)
	fromWhole := read(b, bData, name)
	if !bytes.Equal(fromScoped, fromWhole) {
		t.Fatalf("%s: the two openings read different bytes (%d vs %d bytes)",
			name, len(fromScoped), len(fromWhole))
	}
	if len(fromScoped) == 0 {
		t.Fatalf("%s: read no bytes, so the comparison proved nothing", name)
	}

	// And the content must match what was actually written to the file.
	e := entryNamed(t, a, name)
	want := patternBytes(int(e.Size), 0x40)
	if !bytes.Equal(fromScoped, want) {
		t.Fatalf("%s: reported ranges do not reproduce the file content", name)
	}
}

// TestNegativeBaseOffsetRejected covers the F5 contract that a base offset which
// cannot describe a position in an image is refused at open.
func TestNegativeBaseOffsetRejected(t *testing.T) {
	im := newTestImage(t, FATType16)
	_, err := OpenWithOptions(&mockReaderAt{data: im.data}, OpenOptions{BaseOffset: -1})
	if err == nil {
		t.Fatal("OpenWithOptions accepted a negative BaseOffset")
	}
	if !errors.Is(err, ErrInvalidBaseOffset) {
		t.Fatalf("error = %v, want one matching ErrInvalidBaseOffset", err)
	}
}

// TestZeroBaseOffsetUnchanged pins the additive guarantee: the zero value must
// reproduce the behaviour of every release before this field existed.
func TestZeroBaseOffsetUnchanged(t *testing.T) {
	im := newTestImage(t, FATType32)
	im.addFile(testFile{base: "PLAIN", ext: "TXT", clusters: []uint32{4},
		size: 64, content: patternBytes(64, 7), terminate: true})

	plain := im.volume()
	defer plain.Close()
	explicit := im.volumeWithOptions(OpenOptions{BaseOffset: 0})
	defer explicit.Close()

	a := offsetsOf(t, plain, "PLAIN.TXT")
	b := offsetsOf(t, explicit, "PLAIN.TXT")
	for key, want := range a {
		if got := b[key]; got != want {
			t.Errorf("%s = %d with an explicit zero base, want %d", key, got, want)
		}
	}
}

// TestBaseOffsetWithBackupBootSector guards the one place where a relative and
// an absolute offset sit side by side: the boot sector candidates are volume
// relative sector indices, while the offset recorded on the parsed structure is
// image absolute. Confusing the two would either look past the backup sector or
// report it in the wrong coordinate space.
func TestBaseOffsetWithBackupBootSector(t *testing.T) {
	const base = int64(63 * 512)

	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       2,
		volumeLabel:       "FAT32BKP",
		fsTypeHint:        FATType32,
	})
	backup := append([]byte(nil), img[:BootSectorSize]...)
	copy(img[6*BootSectorSize:(6+1)*BootSectorSize], backup)
	clear(img[:BootSectorSize])

	disk := embedImage(img, base)
	v, err := OpenWithOptions(&mockReaderAt{data: disk}, OpenOptions{BaseOffset: base})
	if err != nil {
		t.Fatalf("OpenWithOptions failed: %v", err)
	}
	defer v.Close()

	if !v.UsedBackupBootSector() {
		t.Fatal("expected the backup boot sector to be used")
	}
	// UsedBackup is a statement about which of the volume's own candidates was
	// read, so it must stay true even though the image offset is not 6*512.
	if got, want := v.GetBootSector().Offset, base+6*BootSectorSize; got != want {
		t.Fatalf("boot sector offset = %d, want %d", got, want)
	}
	if got := v.VolumeLabel(); got != "FAT32BKP" {
		t.Fatalf("volume label = %q, want FAT32BKP", got)
	}
}

// rebasingReaderAt serves the same image no matter where it is read from, so a
// volume can be opened at an arbitrarily large base without the read failing
// first. It exists to reach the overflow guard, which an ordinary reader would
// shadow with an EOF.
type rebasingReaderAt struct {
	data []byte
	base int64
}

func (r *rebasingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	off -= r.base
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// TestBaseOffsetOverflowRejected covers the other half of the ErrInvalidBaseOffset
// contract: a base that cannot hold the volume is refused rather than wrapping
// around into a small negative or positive offset.
func TestBaseOffsetOverflowRejected(t *testing.T) {
	im := newTestImage(t, FATType16)
	base := int64(1<<63 - 1024)

	_, err := OpenWithOptions(&rebasingReaderAt{data: im.data, base: base},
		OpenOptions{BaseOffset: base})
	if err == nil {
		t.Fatal("OpenWithOptions accepted a base offset that cannot hold the volume")
	}
	if !errors.Is(err, ErrInvalidBaseOffset) {
		t.Fatalf("error = %v, want one matching ErrInvalidBaseOffset", err)
	}

	// The same reader at a sane base must still open, so the test above is
	// failing on the overflow and not on the reader.
	ok, err := OpenWithOptions(&rebasingReaderAt{data: im.data, base: 4096},
		OpenOptions{BaseOffset: 4096})
	if err != nil {
		t.Fatalf("OpenWithOptions at a sane base failed: %v", err)
	}
	ok.Close()
}
