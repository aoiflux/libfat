package libfat

import (
	"testing"
)

// TestRealVolumeCodecsAgreeWithImages checks the exported codecs against
// records that Windows and mkfs.fat wrote, which is the evidence the codec
// unit tests cannot supply: those compare this package against itself and
// against hand-worked values, these against implementations that have never
// seen it.
//
// For every live entry it re-reads the 32-byte record at EntryAbsoluteOffset
// and checks that ParseShortName reproduces ShortName, that ValidShortName
// accepts the name, that EncodeDateTime gives back the stored modification
// and creation fields, and, for a long-named entry, that LFNChecksum of the
// short name is the checksum byte its first slot carries.
func TestRealVolumeCodecsAgreeWithImages(t *testing.T) {
	for _, spec := range realVolumes {
		t.Run(spec.name, func(t *testing.T) {
			f, v := spec.open(t)
			var records, longNamed, stamped int
			rec := make([]byte, DirEntrySize)
			slot := make([]byte, DirEntrySize)

			err := v.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
				if e.EntryAbsoluteOffset < 0 {
					return nil
				}
				if _, err := f.ReadAt(rec, e.EntryAbsoluteOffset); err != nil {
					t.Fatalf("%s: %v", path, err)
				}
				records++
				var name [11]byte
				copy(name[:], rec[0:11])

				if got := ParseShortName(name, rec[12]); got != e.ShortName {
					t.Errorf("%s: ParseShortName = %q, ShortName = %q", path, got, e.ShortName)
				}
				if !ValidShortName(name) {
					t.Errorf("%s: ValidShortName rejected %q", path, name)
				}

				if !e.ModifiedAt.IsZero() {
					stamped++
					date, clock, _ := EncodeDateTime(e.ModifiedAt)
					if date != ReadUint16LE(rec, 24) || clock != ReadUint16LE(rec, 22) {
						t.Errorf("%s: modification time re-encodes as (%#04x, %#04x), record holds (%#04x, %#04x)",
							path, date, clock, ReadUint16LE(rec, 24), ReadUint16LE(rec, 22))
					}
				}
				if !e.CreatedAt.IsZero() {
					date, clock, tenths := EncodeDateTime(e.CreatedAt)
					if date != ReadUint16LE(rec, 16) || clock != ReadUint16LE(rec, 14) || tenths != rec[13] {
						t.Errorf("%s: creation time re-encodes as (%#04x, %#04x, %d), record holds (%#04x, %#04x, %d)",
							path, date, clock, tenths, ReadUint16LE(rec, 16), ReadUint16LE(rec, 14), rec[13])
					}
				}

				if e.NameSource == NameSourceLFN && e.LFNEntryOffset >= 0 {
					if _, err := f.ReadAt(slot, e.LFNEntryOffset); err != nil {
						t.Fatalf("%s: %v", path, err)
					}
					longNamed++
					if got := LFNChecksum(name); got != slot[13] {
						t.Errorf("%s: LFNChecksum = %#02x, the slot carries %#02x", path, got, slot[13])
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if records == 0 || longNamed == 0 || stamped == 0 {
				t.Fatalf("checked %d records, %d long-named, %d stamped; the volume no longer covers the codecs",
					records, longNamed, stamped)
			}
			t.Logf("%d records, %d long-named, %d stamped", records, longNamed, stamped)
		})
	}
}

// TestRealWindowsVolumePassesStrict is the claim Strict exists for: a volume
// written the way Windows writes one is well formed, so Strict accepts every
// directory and every chain on it. It runs on the GPT partition only, which
// Windows formatted and wrote; whether the mkfs.fat volumes pass is a
// statement about mkfs.fat, not about this package.
//
// Chains are checked through File.Fragments, which applies every strict chain
// rule without reading the content.
func TestRealWindowsVolumePassesStrict(t *testing.T) {
	spec := gptPartitionSpec(t)
	f := spec.openFile(t)
	spec.requireExpectedVolume(t, f)
	v, err := OpenWithOptions(f, OpenOptions{BaseOffset: spec.base, Strict: true, CacheFAT: true})
	if err != nil {
		t.Fatal(err)
	}
	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatal(err)
	}

	var dirs, files int
	var walk func(*File)
	walk = func(dir *File) {
		dirs++
		entries, err := dir.ReadDirWithOptions(ReadDirOptions{ExcludeDeleted: true})
		if err != nil {
			t.Fatalf("ReadDir %s: %v", dir.Path(), err)
		}
		for _, e := range entries {
			child, err := v.OpenEntry(e)
			if err != nil {
				t.Fatal(err)
			}
			if e.IsDirectory {
				walk(child)
				continue
			}
			files++
			if _, err := child.Fragments(); err != nil {
				t.Fatalf("Fragments %s: %v", e.Path, err)
			}
			if e.Size == 0 {
				if _, err := child.ReadAll(); err != nil {
					t.Fatalf("ReadAll %s: %v", e.Path, err)
				}
			}
		}
	}
	walk(root)
	if v.FATMirrorMismatches() != 0 {
		t.Fatalf("%d FAT mirror mismatches on a clean volume", v.FATMirrorMismatches())
	}
	t.Logf("%d directories and %d files pass Strict", dirs, files)
}
