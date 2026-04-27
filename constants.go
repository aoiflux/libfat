package libfat

const (
	BootSectorSize  = 512
	BootSectorMagic = 0xAA55

	FATType12 = "FAT12"
	FATType16 = "FAT16"
	FATType32 = "FAT32"

	defaultRootCluster = 2

	fat12ClusterThreshold = 4085
	fat16ClusterThreshold = 65525

	dirEntrySize = 32
)

const (
	attrReadOnly  = 0x01
	attrHidden    = 0x02
	attrSystem    = 0x04
	attrVolumeID  = 0x08
	attrDirectory = 0x10
	attrArchive   = 0x20
	attrLongName  = 0x0F
)