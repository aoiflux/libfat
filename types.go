package libfat

import "time"

type BootSector struct {
	Jump               [3]byte
	OEMName            [8]byte
	Offset             int64
	UsedBackup         bool
	BytesPerSector     uint16
	SectorsPerCluster  uint8
	ReservedSectors    uint16
	NumberOfFATs       uint8
	RootEntryCount     uint16
	TotalSectors16     uint16
	Media              uint8
	FATSize16          uint16
	SectorsPerTrack    uint16
	NumberOfHeads      uint16
	HiddenSectors      uint32
	TotalSectors32     uint32
	FATSize32          uint32
	ExtFlags           uint16
	FSVersion          uint16
	RootCluster        uint32
	FSInfoSector       uint16
	BackupBootSector   uint16
	VolumeID           uint32
	VolumeLabel        string
	FileSystemTypeHint string
	Magic              uint16
}

type FAT32FSInfo struct {
	Sector             uint16
	LeadSignature      uint32
	StructureSignature uint32
	TrailSignature     uint32
	FreeClusterCount   uint32
	NextFreeCluster    uint32
	Valid              bool
}

type OpenOptions struct {
	FATType                   string
	IncludeVolumeLabelEntries bool
	IncludeVirtualRootEntries bool

	// RecoverDeletedLongNames enables reconstruction of long file names for
	// deleted entries. Deletion overwrites the sequence byte of every long-name
	// slot and the first byte of the short name, but leaves the short-name
	// checksum intact, which is enough to re-associate the slots and often to
	// recover the lost first character. It is off by default because enabling
	// it changes the Name of deleted entries.
	RecoverDeletedLongNames bool
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

type DirEntry struct {
	Name             string
	Path             string
	ShortName        string
	IsDirectory      bool
	Size             uint64
	FirstCluster     uint32
	ClusterAllocated bool
	Attributes       uint8
	CreatedAt        time.Time
	ModifiedAt       time.Time
	AccessedAt       time.Time
	Deleted          bool
	Recovered        bool
	Virtual          bool

	// EntryOffset is the offset of the 32-byte directory entry within its
	// parent directory's concatenated data, not within the image. For a
	// fragmented directory it does not correspond to any single image
	// location. Use EntryAbsoluteOffset to address the entry on disk.
	EntryOffset int64

	// EntryAbsoluteOffset is the absolute image offset of the 32-byte directory
	// entry, computed through the parent directory's own fragment list so that
	// it is correct even when the directory is fragmented. It is -1 when the
	// offset could not be resolved.
	EntryAbsoluteOffset int64

	// LFNEntryOffset is the absolute image offset of the first long-name slot
	// belonging to this entry, or -1 when the entry has no long name.
	LFNEntryOffset int64

	DirectoryPath string

	// NameSource records how Name was determined.
	NameSource NameSource

	// Orphaned is true when the entry was recovered by ScanOrphans from
	// directory data that is not reachable from the root. Its Path is rooted at
	// OrphanPath because the original path was lost with the directory chain
	// that named it.
	Orphaned bool

	// FirstCharRecovered is true when the entry is deleted and the first
	// character of its name, which deletion overwrites with 0xE5, was recovered
	// from long-name slots rather than replaced with a placeholder.
	FirstCharRecovered bool
}
