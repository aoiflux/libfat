package libfat

import (
	"encoding/json"
	"fmt"
	"time"
)

type BootSector struct {
	Jump               [3]byte `json:"jump"`
	OEMName            [8]byte `json:"oem_name"`
	Offset             int64   `json:"offset"`
	UsedBackup         bool    `json:"used_backup"`
	BytesPerSector     uint16  `json:"bytes_per_sector"`
	SectorsPerCluster  uint8   `json:"sectors_per_cluster"`
	ReservedSectors    uint16  `json:"reserved_sectors"`
	NumberOfFATs       uint8   `json:"number_of_fats"`
	RootEntryCount     uint16  `json:"root_entry_count"`
	TotalSectors16     uint16  `json:"total_sectors_16"`
	Media              uint8   `json:"media"`
	FATSize16          uint16  `json:"fat_size_16"`
	SectorsPerTrack    uint16  `json:"sectors_per_track"`
	NumberOfHeads      uint16  `json:"number_of_heads"`
	HiddenSectors      uint32  `json:"hidden_sectors"`
	TotalSectors32     uint32  `json:"total_sectors_32"`
	FATSize32          uint32  `json:"fat_size_32"`
	ExtFlags           uint16  `json:"ext_flags"`
	FSVersion          uint16  `json:"fs_version"`
	RootCluster        uint32  `json:"root_cluster"`
	FSInfoSector       uint16  `json:"fs_info_sector"`
	BackupBootSector   uint16  `json:"backup_boot_sector"`
	VolumeID           uint32  `json:"volume_id"`
	VolumeLabel        string  `json:"volume_label,omitempty"`
	FileSystemTypeHint string  `json:"file_system_type_hint,omitempty"`
	Magic              uint16  `json:"magic"`
}

type FAT32FSInfo struct {
	Sector             uint16 `json:"sector"`
	LeadSignature      uint32 `json:"lead_signature"`
	StructureSignature uint32 `json:"structure_signature"`
	TrailSignature     uint32 `json:"trail_signature"`
	FreeClusterCount   uint32 `json:"free_cluster_count"`
	NextFreeCluster    uint32 `json:"next_free_cluster"`
	Valid              bool   `json:"valid"`
}

type OpenOptions struct {
	FATType                   string `json:"fat_type,omitempty"`
	IncludeVolumeLabelEntries bool   `json:"include_volume_label_entries"`
	IncludeVirtualRootEntries bool   `json:"include_virtual_root_entries"`

	// RecoverDeletedLongNames enables reconstruction of long file names for
	// deleted entries. Deletion overwrites the sequence byte of every long-name
	// slot and the first byte of the short name, but leaves the short-name
	// checksum intact, which is enough to re-associate the slots and often to
	// recover the lost first character. It is off by default because enabling
	// it changes the Name of deleted entries.
	RecoverDeletedLongNames bool `json:"recover_deleted_long_names"`

	// BaseOffset is the byte offset within the reader at which the volume's
	// boot sector begins.
	//
	// Every offset this library reports already includes it - a Range.StartByte,
	// a DirEntry.EntryAbsoluteOffset, a DirEntry.LFNEntryOffset, a BootSector
	// .Offset, a report fragment - so those values address the image the volume
	// was opened over directly, with no further adjustment by the caller.
	//
	// Use 0 when the reader is already scoped to the volume, for example an
	// io.SectionReader over a single partition. Set it to the partition's start
	// when the reader is the whole disk and you need whole-disk offsets, which
	// is what intersecting against externally supplied byte ranges requires: a
	// partition-relative offset compared against a whole-disk range produces a
	// confident wrong answer rather than an error.
	//
	// It must not be negative. The zero value reproduces the behaviour of every
	// release before v0.4.0 exactly.
	BaseOffset int64 `json:"base_offset"`
}

// NameSource records how a DirEntry's Name was determined.
type NameSource uint8

const (
	// NameSourceShort means Name came from the 8.3 short-name field.
	NameSourceShort NameSource = iota
	// NameSourceLFN means Name was assembled from an intact long-name chain.
	NameSourceLFN
	// NameSourceRecoveredLFN means Name was reconstructed from the long-name
	// slots of a deleted entry by checksum matching. The name is a best-effort
	// recovery, not a value read directly from an intact structure.
	NameSourceRecoveredLFN
)

func (n NameSource) String() string {
	switch n {
	case NameSourceLFN:
		return "lfn"
	case NameSourceRecoveredLFN:
		return "recovered-lfn"
	default:
		return "short"
	}
}

// MarshalJSON renders a NameSource as the label String returns. The numeric
// values are an implementation detail; a report carrying "2" instead of
// "recovered-lfn" is both unreadable and fragile against future additions, and
// how a name was determined is exactly the kind of provenance a report exists
// to carry.
func (n NameSource) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.String())
}

// UnmarshalJSON accepts the labels MarshalJSON produces, so a written report
// round-trips. An unrecognised label is an error rather than a silent default:
// decoding "lfn" from a future version as "short" would quietly weaken a claim
// about a name, which is the opposite of what this type is for.
func (n *NameSource) UnmarshalJSON(b []byte) error {
	var label string
	if err := json.Unmarshal(b, &label); err != nil {
		return err
	}
	switch label {
	case "short":
		*n = NameSourceShort
	case "lfn":
		*n = NameSourceLFN
	case "recovered-lfn":
		*n = NameSourceRecoveredLFN
	default:
		return fmt.Errorf("unknown name source %q", label)
	}
	return nil
}

// Timestamps groups a directory entry's times for serialisation.
//
// FAT has no change time, so there is no ctime field; see the Timestamps
// section of the package documentation for the precision each one carries.
//
// A time the entry did not record is the zero time.Time and is absent from the
// JSON. That needs omitzero rather than omitempty, which does nothing for a
// struct field: a timestamp the volume never held must not appear in a report
// as 0001-01-01T00:00:00Z, because absent says "not recorded", which is the
// truth, while a rendered zero looks like an answer.
type Timestamps struct {
	Created  time.Time `json:"created,omitzero"`
	Modified time.Time `json:"modified,omitzero"`
	Accessed time.Time `json:"accessed,omitzero"`
}

// Timestamps returns the entry's times as a group.
func (d DirEntry) Timestamps() Timestamps {
	return Timestamps{
		Created:  d.CreatedAt,
		Modified: d.ModifiedAt,
		Accessed: d.AccessedAt,
	}
}

type DirEntry struct {
	Name             string    `json:"name"`
	Path             string    `json:"path"`
	ShortName        string    `json:"short_name"`
	IsDirectory      bool      `json:"is_directory"`
	Size             uint64    `json:"size"`
	FirstCluster     uint32    `json:"first_cluster"`
	ClusterAllocated bool      `json:"cluster_allocated"`
	Attributes       uint8     `json:"attributes"`
	CreatedAt        time.Time `json:"created_at,omitzero"`
	ModifiedAt       time.Time `json:"modified_at,omitzero"`
	AccessedAt       time.Time `json:"accessed_at,omitzero"`
	Deleted          bool      `json:"deleted"`
	Recovered        bool      `json:"recovered"`
	Virtual          bool      `json:"virtual"`

	// EntryOffset is the offset of the 32-byte directory entry within its
	// parent directory's concatenated data, not within the image. For a
	// fragmented directory it does not correspond to any single image
	// location. Use EntryAbsoluteOffset to address the entry on disk.
	EntryOffset int64 `json:"entry_offset"`

	// EntryAbsoluteOffset is the absolute image offset of the 32-byte directory
	// entry, computed through the parent directory's own fragment list so that
	// it is correct even when the directory is fragmented. It is -1 when the
	// offset could not be resolved.
	EntryAbsoluteOffset int64 `json:"entry_absolute_offset"`

	// LFNEntryOffset is the absolute image offset of the first long-name slot
	// belonging to this entry, or -1 when the entry has no long name.
	LFNEntryOffset int64 `json:"lfn_entry_offset"`

	DirectoryPath string `json:"directory_path"`

	// ParentFirstCluster is the first cluster of the directory this entry was
	// read from: the cluster that, with EntryOffset, locates the 32-byte record
	// logically rather than physically.
	//
	// It is FixedRootCluster (0) for entries in the fixed-size root directory
	// region of FAT12 and FAT16, which is not cluster-addressed. On FAT32 the
	// root is an ordinary cluster chain, so its entries carry the volume's root
	// cluster and 0 never occurs - a zero there means the field was not
	// populated, not that the entry lives in a root region.
	//
	// For an entry recovered by ScanOrphans it is the first cluster of the orphan
	// run the entry was found in, which is a real cluster even though no path
	// leads to it.
	//
	// With EntryOffset it forms the entry's FileID; see Volume.FileID for what
	// that pair does and does not survive.
	ParentFirstCluster uint32 `json:"parent_first_cluster"`

	// NameSource records how Name was determined.
	NameSource NameSource `json:"name_source"`

	// Orphaned is true when the entry was recovered by ScanOrphans from
	// directory data that is not reachable from the root. Its Path is rooted at
	// OrphanPath because the original path was lost with the directory chain
	// that named it.
	Orphaned bool `json:"orphaned"`

	// FirstCharRecovered is true when the entry is deleted and the first
	// character of its name, which deletion overwrites with 0xE5, was recovered
	// from long-name slots rather than replaced with a placeholder.
	FirstCharRecovered bool `json:"first_char_recovered"`
}
