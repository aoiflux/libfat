package libfat

import (
	"fmt"
	"math"
)

// FixedRootCluster is the ParentFirstCluster reported for entries in the
// fixed-size root directory region of FAT12 and FAT16.
//
// Cluster numbering starts at 2 - 0 and 1 are reserved, and 0 doubles as the
// free-entry marker in the FAT - so 0 can never name a real directory and is
// available as the sentinel for the one directory that has no cluster number.
const FixedRootCluster uint32 = 0

// FileID is the closest thing FAT has to a file identifier: the directory the
// entry lives in, and which slot of that directory it occupies.
//
// # What it is
//
// A directory is an array of 32-byte slots. ParentFirstCluster names the
// directory - its first cluster, or FixedRootCluster for the FAT12/16 root
// region - and EntrySlotIndex is the entry's position in that array, counted
// across the directory's concatenated clusters. The pair is comparable,
// hashable and usable as a map key, and it is what two scans of the same
// unchanged volume will agree on.
//
// # What it is not
//
// FAT has no inode, no file record number, and above all no reuse or generation
// counter. NTFS pairs an MFT record with a sequence number that it increments
// each time the record is reused, and ext pairs an inode with a generation;
// both let a consumer tell "this file changed" from "a different file took this
// slot". FAT records nothing of the kind. A FileID is therefore an address, not
// an identity, and two files that were never related can share one.
//
// It is deliberately not derived from DirEntry.EntryAbsoluteOffset. That offset
// is the physical location of the record and it moves whenever the parent
// directory's clusters are relocated - a defragmentation pass rewrites it for
// every entry in the directory while changing no file. The slot index is
// logical and survives that, which is why it is the one used here.
//
// # When it does not survive
//
// In rough order of how often it bites:
//
//   - Slot reuse. Deleting a file leaves its slot marked 0xE5 and free for the
//     next creation in that directory. A new, unrelated file lands in the slot
//     and inherits the FileID exactly. This is the common case, and there is no
//     signal anywhere in the filesystem that distinguishes it.
//   - Rename. A long name occupies ceil(len/13) slots immediately before the
//     short entry, so renaming a file to a name of a different length moves the
//     short entry to a different slot index. "report.txt" (one slot) renamed to
//     "quarterly report final.txt" (two slots) changes the FileID even though
//     nothing else about the file did. FileID is therefore much better at
//     asserting that two observations are of the same file than at detecting
//     that a file was renamed.
//   - Directory compaction. Some implementations repack a directory's slots to
//     reclaim freed runs, shifting every entry after the gap.
//   - The parent directory itself being deleted and recreated, or its first
//     cluster reallocated. ParentFirstCluster then names a different directory,
//     or none.
//
// Rename detection on FAT is inference, never proof. Corroborate a FileID with
// the signals FAT does record and a rename does not touch: CreatedAt, which
// carries ten-millisecond precision and is the most discriminating field in the
// entry, along with FirstCluster and Size. A match on FileID plus CreatedAt
// plus FirstCluster is strong evidence of sameness; a FileID match alone, in a
// directory that has seen deletions, is not.
type FileID struct {
	// ParentFirstCluster is the first cluster of the directory holding the
	// entry, or FixedRootCluster for the FAT12/16 root region.
	ParentFirstCluster uint32
	// EntrySlotIndex is the entry's 32-byte slot number within that directory's
	// concatenated data.
	EntrySlotIndex uint32
}

func (id FileID) String() string {
	return fmt.Sprintf("%d:%d", id.ParentFirstCluster, id.EntrySlotIndex)
}

// FileID returns the composite identity of a directory entry.
//
// The boolean is false when no identity can be formed, which is a statement
// about the entry rather than an error condition:
//
//   - the entry is virtual ($MBR, $FAT1, $FAT2, $OrphanFiles), which has no
//     backing 32-byte record at all;
//   - EntryOffset is negative, not a multiple of the 32-byte slot size, or so
//     large that the slot index would not fit in a uint32 - none of which this
//     package produces, but DirEntry is an exported struct and callers
//     construct them;
//   - ParentFirstCluster is 1, or above the volume's last cluster;
//   - ParentFirstCluster is 0 on a FAT32 volume, where the root is an ordinary
//     cluster chain and no directory has cluster 0. A zero there means the
//     entry did not come from this package's parser, not that it lives in a
//     fixed root region;
//   - the volume is closed.
//
// A true does not promise the entry is live, reachable, or intact. Deleted and
// orphaned entries have identities, and that is the point: a deleted entry's
// FileID is what lets a consumer say the slot it occupies is the same slot a
// live file occupied in an earlier image. See FileID for what that does and
// does not survive.
func (v *Volume) FileID(e DirEntry) (FileID, bool) {
	if v.IsClosed() || e.Virtual {
		return FileID{}, false
	}
	if e.EntryOffset < 0 || e.EntryOffset%dirEntrySize != 0 {
		return FileID{}, false
	}
	slot := e.EntryOffset / dirEntrySize
	if slot > int64(math.MaxUint32) {
		return FileID{}, false
	}
	parent := e.ParentFirstCluster
	if parent == FixedRootCluster {
		// The fixed root region exists only on FAT12 and FAT16. On FAT32 a zero
		// is an unpopulated field, not a root.
		if v.fatType == FATType32 {
			return FileID{}, false
		}
		return FileID{ParentFirstCluster: FixedRootCluster, EntrySlotIndex: uint32(slot)}, true
	}
	if parent < defaultRootCluster || parent > v.maxClusterNumber() {
		return FileID{}, false
	}
	return FileID{ParentFirstCluster: parent, EntrySlotIndex: uint32(slot)}, true
}
