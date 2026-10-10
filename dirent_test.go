package libfat

import (
	"testing"
)

// unstampedEmptyEntry is a live record for an empty file that carries no
// times at all: name "EMPTY.TXT", the archive attribute, and every other byte
// zero. Tools that do not stamp times write exactly this, and so do some
// embedded systems; it is a valid entry and must be listed.
func unstampedEmptyEntry() []byte {
	e := make([]byte, 32)
	copy(e[0:11], "EMPTY   TXT")
	e[11] = 0x20
	return e
}

func TestReadDirListsUnstampedEmptyFile(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			im.addRootRaw(unstampedEmptyEntry())

			e := entryNamed(t, im.volume(), "EMPTY.TXT")
			if e.Deleted || e.IsDirectory {
				t.Fatalf("entry flags wrong: deleted=%v dir=%v", e.Deleted, e.IsDirectory)
			}
			if e.Size != 0 || e.FirstCluster != 0 {
				t.Fatalf("size=%d cluster=%d, want 0 and 0", e.Size, e.FirstCluster)
			}
			if !e.CreatedAt.IsZero() || !e.ModifiedAt.IsZero() || !e.AccessedAt.IsZero() {
				t.Fatalf("times should be absent, got %v %v %v", e.CreatedAt, e.ModifiedAt, e.AccessedAt)
			}
		})
	}
}

// TestDeletedUnstampedEmptyEntryStillFiltered pins that the garbage heuristic
// moved to the recovery path rather than disappearing: a deleted record with
// no times, no cluster and no size carries nothing worth reporting.
func TestDeletedUnstampedEmptyEntryStillFiltered(t *testing.T) {
	im := newTestImage(t, FATType16)
	e := unstampedEmptyEntry()
	e[0] = 0xE5
	im.addRootRaw(e)

	root, err := im.volume().GetRootDirectory()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := root.ReadDir()
	if err != nil {
		t.Fatal(err)
	}
	for _, got := range entries {
		if got.ShortName == "_MPTY.TXT" {
			t.Fatalf("deleted unstamped empty record was listed: %+v", got)
		}
	}
}
