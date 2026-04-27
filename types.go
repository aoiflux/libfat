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
	EntryOffset      int64
	DirectoryPath    string
}
