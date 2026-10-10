package libfat

const (
	BootSectorSize  = 512
	BootSectorMagic = 0xAA55

	FATType12 = "FAT12"
	FATType16 = "FAT16"
	FATType32 = "FAT32"

	// FAT32Signature is the BS_FilSysType field of a FAT32 boot sector: the
	// type name padded with spaces to eight bytes. The field is informational
	// and is not what decides the FAT type; see FAT12MaxClusters.
	FAT32Signature = "FAT32   "

	// DefaultRootCluster is the root directory's first cluster on a FAT32
	// volume formatted the usual way. It is also the first cluster number of
	// the data area: clusters 0 and 1 have FAT entries but no data.
	DefaultRootCluster = 2

	// FAT12MaxClusters and FAT32MinClusters are the cluster counts that decide
	// the FAT type, which is a function of the count alone: at most
	// FAT12MaxClusters is FAT12, at least FAT32MinClusters is FAT32, and
	// anything between is FAT16.
	FAT12MaxClusters = 4084
	FAT32MinClusters = 65525

	// DirEntrySize is the size of one directory record, short or long-name.
	DirEntrySize = 32

	// MediaFixed is the media descriptor for fixed disks, written to the
	// boot sector's BPB_Media and to the low byte of FAT entry 0.
	MediaFixed = 0xF8
)

// Directory entry attribute bits, as stored in byte 11 of a record.
const (
	AttrReadOnly  = 0x01
	AttrHidden    = 0x02
	AttrSystem    = 0x04
	AttrVolumeID  = 0x08
	AttrDirectory = 0x10
	AttrArchive   = 0x20

	// AttrLongName is the attribute byte of a long-name slot: read-only,
	// hidden, system and volume-ID together, a combination no short entry
	// carries.
	AttrLongName = 0x0F
)

// Values of a short entry's first byte, and its NTRes case bits.
const (
	// DeletedMarker in the first byte marks a deleted record.
	DeletedMarker = 0xE5
	// KanjiE5 in the first byte stands for a literal 0xE5 as the name's first
	// character, which would otherwise read as DeletedMarker.
	KanjiE5 = 0x05
	// DotEntry is the name byte of the "." and ".." records that begin every
	// subdirectory.
	DotEntry = '.'

	// NTResLowerBase in byte 12 (NTRes) says the 8-character base is displayed
	// in lower case; NTResLowerExt says the same of the extension. Windows
	// sets them for names such as "readme.txt" that need no long name.
	NTResLowerBase = 0x08
	NTResLowerExt  = 0x10
)

// Long-name slot layout.
const (
	// LFNLastFlag is set in the sequence byte of the slot holding the last part
	// of a long name, which is the first slot on disk.
	LFNLastFlag = 0x40
	// MaxLFNSlots is the largest number of slots one long name may occupy
	// (20 slots x 13 UTF-16 code units = 260, enough for MaxLongNameLen).
	MaxLFNSlots = 20
	// LFNCharsPerSlot is the number of UTF-16 code units one slot holds.
	LFNCharsPerSlot = 13
	// MaxLongNameLen is the longest long name, in UTF-16 code units.
	MaxLongNameLen = 255
)

// LFNCharOffsets are the byte offsets, within a long-name slot, of its
// LFNCharsPerSlot UTF-16 code units, in name order.
//
// It is a variable only because Go has no array constants. The parser keeps
// its own copy, so assigning to it changes nothing this package reads.
var LFNCharOffsets = lfnCharOffsets

var lfnCharOffsets = [LFNCharsPerSlot]int{1, 3, 5, 7, 9, 14, 16, 18, 20, 22, 24, 28, 30}

// FAT32 entry values. An entry is 32 bits on disk, of which only the low 28
// are the value; the top four are reserved and must be masked off.
const (
	FAT32Mask   = 0x0FFFFFFF
	FAT32EOC    = 0x0FFFFFFF // the end-of-chain value writers use
	FAT32EOCMin = 0x0FFFFFF8 // every value from here up ends a chain
	FAT32Bad    = 0x0FFFFFF7
)

// FAT32 FSInfo sector signatures.
const (
	FSInfoLeadSig   = 0x41615252
	FSInfoStructSig = 0x61417272
	FSInfoTrailSig  = 0xAA550000
)
