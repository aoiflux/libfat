package libfat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"sync"
	"sync/atomic"
)

type Volume struct {
	reader                    io.ReaderAt
	baseOffset                int64
	forcedFATType             string
	includeVolumeLabelEntries bool
	includeVirtualRootEntries bool
	recoverDeletedLongNames   bool

	closed  bool
	closeMu sync.RWMutex

	bootSector *BootSector
	fatType    string

	bytesPerSector      uint32
	sectorsPerCluster   uint32
	bytesPerCluster     uint32
	reservedSectors     uint32
	numberOfFATs        uint32
	rootEntryCount      uint32
	fatSizeSectors      uint32
	totalSectors        uint32
	rootDirSectors      uint32
	firstFATSector      uint32
	firstDataSector     uint32
	firstClusterSector  uint32
	rootDirFirstSector  uint32
	rootCluster         uint32
	clusterCount        uint32
	volumeSize          uint64
	volumeLabel         string
	rootVolumeLabel     string
	fsTypeHint          string
	fsInfo              *FAT32FSInfo
	fatMirrorMismatches uint64

	cacheFAT bool
	strict   bool
	// strictSkipFATMirror leaves the mirror comparison out of Strict.
	strictSkipFATMirror bool
	fatCache            fatCache
}

// fatCache holds every FAT copy in memory once loaded. tables is nil when the
// cache was not requested, has not been loaded yet, or could not be built; in
// each case lookups fall through to reading the image.
type fatCache struct {
	once   sync.Once
	tables [][]byte
}

// maxFATCacheBytes bounds the memory CacheFAT may take, summed across copies.
// It admits a FAT32 volume of 32 million clusters with two FATs - a terabyte
// of 32 KiB clusters - and refuses only the volumes where holding the FAT
// would cost more than the reads it saves.
const maxFATCacheBytes = 256 << 20

func Open(reader io.ReaderAt) (*Volume, error) {
	return OpenWithOptions(reader, OpenOptions{})
}

func OpenWithFATType(reader io.ReaderAt, fatType string) (*Volume, error) {
	return OpenWithOptions(reader, OpenOptions{FATType: fatType})
}

func OpenWithOptions(reader io.ReaderAt, options OpenOptions) (*Volume, error) {
	fatType := options.FATType
	if reader == nil {
		return nil, wrapVolumeError("open", fmt.Errorf("reader is nil"))
	}
	if fatType != "" && fatType != FATType12 && fatType != FATType16 && fatType != FATType32 {
		return nil, wrapVolumeError("open", fmt.Errorf("%w: %s", ErrUnsupportedFAT, fatType))
	}
	if options.BaseOffset < 0 {
		return nil, wrapVolumeError("open", fmt.Errorf("%w: %d", ErrInvalidBaseOffset, options.BaseOffset))
	}

	type statReader interface {
		Stat() (fs.FileInfo, error)
	}
	if sr, ok := reader.(statReader); ok {
		if info, err := sr.Stat(); err == nil && info.IsDir() {
			return nil, wrapVolumeError("open", fmt.Errorf("%w: %s", ErrInputIsDirectory, info.Name()))
		}
	}

	v := &Volume{
		reader:                    reader,
		baseOffset:                options.BaseOffset,
		forcedFATType:             fatType,
		includeVolumeLabelEntries: options.IncludeVolumeLabelEntries,
		includeVirtualRootEntries: options.IncludeVirtualRootEntries,
		recoverDeletedLongNames:   options.RecoverDeletedLongNames,
		cacheFAT:                  options.CacheFAT,
		strict:                    options.Strict,
		strictSkipFATMirror:       options.StrictSkipFATMirror,
	}
	if err := v.parseBootSector(); err != nil {
		return nil, wrapVolumeError("open", err)
	}
	v.rootVolumeLabel = v.readRootVolumeLabel()
	return v, nil
}

// readRootVolumeLabel returns the label held by the volume-label record in the
// root directory, or "" when there is none.
//
// FAT records a volume's label in two places, and only this one is
// authoritative. The boot sector's BS_VolLab field is documented as
// informational, and Windows leaves it reading "NO NAME" while writing the real
// label into the root directory, so any volume labelled after it was formatted
// disagrees with its own boot sector. Trusting the boot sector means reporting
// a name no other tool and no operating system will show.
//
// Failure is not an error here. An unreadable root, a torn record or a scan
// that reaches its bound all return "", and the boot sector's copy is used
// instead; refusing to open a volume because its label could not be confirmed
// would be a far worse trade. The bound exists because this runs on every open,
// including on images chosen by no one trustworthy: the record is conventionally
// the first in the root, so the limit is slack rather than a search.
func (v *Volume) readRootVolumeLabel() string {
	runs, err := v.RootDirectoryFragments()
	if err != nil {
		return ""
	}
	const maxScan = 1 << 20
	buf := make([]byte, 32*DirEntrySize)
	var scanned int64
	for _, run := range runs {
		for at := run.StartByte; at < run.EndByte(); {
			chunk := run.EndByte() - at
			if chunk > int64(len(buf)) {
				chunk = int64(len(buf))
			}
			chunk -= chunk % DirEntrySize
			if chunk == 0 || scanned >= maxScan {
				return ""
			}
			if _, err := v.reader.ReadAt(buf[:chunk], at); err != nil {
				return ""
			}
			scanned += chunk
			at += chunk
			for i := int64(0); i < chunk; i += DirEntrySize {
				rec := buf[i : i+DirEntrySize]
				switch {
				case rec[0] == 0x00:
					// End of directory: every record past here is unused.
					return ""
				case rec[0] == DeletedMarker:
					continue
				case rec[11] == AttrLongName:
					// A long-name slot sets the volume-ID bit without being a
					// label, so it has to be excluded before the test below.
					continue
				case rec[11]&AttrVolumeID != 0 && rec[11]&AttrDirectory == 0:
					return parseVolumeLabel(rec, false)
				}
			}
		}
	}
	return ""
}

func (v *Volume) Close() error {
	v.closeMu.Lock()
	defer v.closeMu.Unlock()
	v.closed = true
	// The cached FAT can be hundreds of megabytes; a closed volume that is
	// still referenced must not keep it alive. cachedFATTables refuses to load
	// it again once closed is set.
	v.fatCache.tables = nil
	return nil
}

func (v *Volume) IsClosed() bool {
	v.closeMu.RLock()
	defer v.closeMu.RUnlock()
	return v.closed
}

func (v *Volume) parseBootSector() error {
	bootOffsets := []int64{0, 6 * BootSectorSize, 12 * BootSectorSize}
	var lastErr error

	for _, offset := range bootOffsets {
		bs, err := v.parseBootSectorAt(offset)
		if err != nil {
			if !(offset != 0 && errors.Is(err, io.EOF)) {
				lastErr = err
			}
			continue
		}
		if offset != 0 && bs.FATSize16 != 0 {
			lastErr = fmt.Errorf("%w: backup boot sector is only valid for FAT32", ErrInvalidBootSector)
			continue
		}
		return v.applyBootSector(bs)
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("%w: no usable boot sector found", ErrInvalidBootSector)
}

// parseBootSectorAt parses the boot sector at offset, which is relative to the
// start of the volume: the caller passes the primary and backup candidates as
// the sector indices the format defines them as, and this rebases them onto the
// image. The parsed BootSector.Offset is image-absolute, like every other
// offset this library reports, while UsedBackup stays a statement about which
// of the volume's own candidates was read.
func (v *Volume) parseBootSectorAt(offset int64) (*BootSector, error) {
	imageOffset := v.abs(offset)
	buf := make([]byte, BootSectorSize)
	if _, err := v.reader.ReadAt(buf, imageOffset); err != nil {
		return nil, wrapParseError("boot sector", imageOffset, err)
	}

	bs := &BootSector{Offset: imageOffset, UsedBackup: offset != 0}
	copy(bs.Jump[:], buf[0:3])
	copy(bs.OEMName[:], buf[3:11])
	bs.BytesPerSector = ReadUint16LE(buf, 11)
	bs.SectorsPerCluster = buf[13]
	bs.ReservedSectors = ReadUint16LE(buf, 14)
	bs.NumberOfFATs = buf[16]
	bs.RootEntryCount = ReadUint16LE(buf, 17)
	bs.TotalSectors16 = ReadUint16LE(buf, 19)
	bs.Media = buf[21]
	bs.FATSize16 = ReadUint16LE(buf, 22)
	bs.SectorsPerTrack = ReadUint16LE(buf, 24)
	bs.NumberOfHeads = ReadUint16LE(buf, 26)
	bs.HiddenSectors = ReadUint32LE(buf, 28)
	bs.TotalSectors32 = ReadUint32LE(buf, 32)
	bs.FATSize32 = ReadUint32LE(buf, 36)
	bs.ExtFlags = ReadUint16LE(buf, 40)
	bs.FSVersion = ReadUint16LE(buf, 42)
	bs.RootCluster = ReadUint32LE(buf, 44)
	bs.FSInfoSector = ReadUint16LE(buf, 48)
	bs.BackupBootSector = ReadUint16LE(buf, 50)
	bs.Magic = ReadUint16LE(buf, 510)

	if bs.Magic != BootSectorMagic {
		return nil, fmt.Errorf("%w: boot sector magic %#x", ErrInvalidBootSector, bs.Magic)
	}
	if bs.BytesPerSector == 0 || bs.SectorsPerCluster == 0 || bs.NumberOfFATs == 0 || bs.ReservedSectors == 0 {
		return nil, fmt.Errorf("%w: invalid BPB geometry", ErrInvalidBootSector)
	}
	if bs.NumberOfFATs > 8 {
		return nil, fmt.Errorf("%w: invalid number of FATs %d", ErrInvalidBootSector, bs.NumberOfFATs)
	}
	if !isValidFATSectorSize(bs.BytesPerSector) {
		return nil, fmt.Errorf("%w: invalid sector size %d", ErrInvalidBootSector, bs.BytesPerSector)
	}
	if !isPowerOfTwo(uint32(bs.SectorsPerCluster)) {
		return nil, fmt.Errorf("%w: sector or cluster size is not a power of two", ErrInvalidBootSector)
	}

	bs.VolumeID = ReadUint32LE(buf, 39)
	bs.VolumeLabel = trimASCIISpaces(buf[43:54])
	bs.FileSystemTypeHint = trimASCIISpaces(buf[54:62])
	if bs.FATSize16 == 0 {
		bs.VolumeID = ReadUint32LE(buf, 67)
		bs.VolumeLabel = trimASCIISpaces(buf[71:82])
		bs.FileSystemTypeHint = trimASCIISpaces(buf[82:90])
	}

	totalSectors := uint32(bs.TotalSectors16)
	if totalSectors == 0 {
		totalSectors = bs.TotalSectors32
	}
	if totalSectors == 0 {
		return nil, fmt.Errorf("%w: total sector count is zero", ErrInvalidBootSector)
	}

	fatSize := uint32(bs.FATSize16)
	if fatSize == 0 {
		fatSize = bs.FATSize32
	}
	if fatSize == 0 {
		return nil, fmt.Errorf("%w: FAT size is zero", ErrInvalidBootSector)
	}

	return bs, nil
}

func (v *Volume) applyBootSector(bs *BootSector) error {
	totalSectors := uint32(bs.TotalSectors16)
	if totalSectors == 0 {
		totalSectors = bs.TotalSectors32
	}

	// The volume's extent has to be expressible as image offsets, since that is
	// what every offset this library reports is. Checked here rather than at
	// open because the volume's size is not known until the boot sector is, and
	// as early as it is known so that every rebased read below is covered.
	volumeSize := uint64(totalSectors) * uint64(bs.BytesPerSector)
	if volumeSize > uint64(math.MaxInt64)-uint64(v.baseOffset) {
		return fmt.Errorf("%w: base offset %d places the end of a %d-byte volume beyond the addressable range",
			ErrInvalidBaseOffset, v.baseOffset, volumeSize)
	}

	fatSize := uint32(bs.FATSize16)
	if fatSize == 0 {
		fatSize = bs.FATSize32
	}

	rootDirSectors := ((uint32(bs.RootEntryCount) * DirEntrySize) + (uint32(bs.BytesPerSector) - 1)) / uint32(bs.BytesPerSector)
	firstFATSector := uint32(bs.ReservedSectors)
	if firstFATSector > totalSectors {
		return fmt.Errorf("%w: invalid first FAT sector %d (total sectors %d)", ErrInvalidBootSector, firstFATSector, totalSectors)
	}
	firstDataSector := uint32(bs.ReservedSectors) + (uint32(bs.NumberOfFATs) * fatSize) + rootDirSectors
	if firstDataSector > totalSectors {
		return fmt.Errorf("%w: first data sector %d beyond total sectors %d", ErrInvalidBootSector, firstDataSector, totalSectors)
	}
	dataSectors := totalSectors - firstDataSector
	clusterCount := dataSectors / uint32(bs.SectorsPerCluster)

	fatType := fatTypeFromClusterCount(clusterCount)
	if fatType == "" {
		return fmt.Errorf("%w: %d clusters", ErrUnsupportedFAT, clusterCount)
	}
	if v.forcedFATType != "" {
		switch v.forcedFATType {
		case FATType12:
			if clusterCount > FAT12MaxClusters {
				return fmt.Errorf("%w: too many clusters for FAT12 (%d)", ErrInvalidBootSector, clusterCount)
			}
		case FATType16:
			if clusterCount <= FAT12MaxClusters || clusterCount >= FAT32MinClusters {
				return fmt.Errorf("%w: cluster count %d incompatible with FAT16", ErrInvalidBootSector, clusterCount)
			}
		case FATType32:
			if clusterCount < FAT32MinClusters {
				return fmt.Errorf("%w: too few clusters for FAT32 (%d)", ErrInvalidBootSector, clusterCount)
			}
		}
		fatType = v.forcedFATType
	}
	if fatType == FATType32 {
		if bs.FATSize16 != 0 {
			return fmt.Errorf("%w: FAT32 requires FATSize16 to be 0 (got %d)", ErrInvalidBootSector, bs.FATSize16)
		}
		if bs.FATSize32 == 0 {
			return fmt.Errorf("%w: FAT32 requires non-zero FATSize32", ErrInvalidBootSector)
		}
	} else {
		if bs.FATSize16 == 0 {
			return fmt.Errorf("%w: FAT12/16 requires non-zero FATSize16", ErrInvalidBootSector)
		}
	}

	rootCluster := uint32(DefaultRootCluster)
	rootDirFirstSector := uint32(bs.ReservedSectors) + (uint32(bs.NumberOfFATs) * fatSize)
	firstClusterSector := firstDataSector
	if fatType == FATType32 {
		if bs.RootEntryCount != 0 {
			return fmt.Errorf("%w: FAT32 requires root entry count 0 (got %d)", ErrInvalidBootSector, bs.RootEntryCount)
		}
		if bs.RootCluster < DefaultRootCluster {
			return fmt.Errorf("%w: invalid FAT32 root cluster %d", ErrInvalidBootSector, bs.RootCluster)
		}
		if bs.RootCluster-DefaultRootCluster >= clusterCount {
			return fmt.Errorf("%w: FAT32 root cluster %d outside data range", ErrInvalidBootSector, bs.RootCluster)
		}
		rootCluster = bs.RootCluster
		rootDirFirstSector = firstDataSector + ((rootCluster - DefaultRootCluster) * uint32(bs.SectorsPerCluster))
		if rootDirFirstSector >= totalSectors {
			return fmt.Errorf("%w: FAT32 root directory sector %d beyond total sectors %d", ErrInvalidBootSector, rootDirFirstSector, totalSectors)
		}
		if bs.UsedBackup && bs.BackupBootSector != 0 && bs.BackupBootSector != 6 && bs.BackupBootSector != 12 {
			return fmt.Errorf("%w: unsupported FAT32 backup boot sector %d", ErrInvalidBootSector, bs.BackupBootSector)
		}
		if fsInfo, err := v.readFAT32FSInfo(bs); err == nil {
			v.fsInfo = fsInfo
		}
	} else {
		if bs.RootEntryCount == 0 {
			return fmt.Errorf("%w: FAT12/16 requires non-zero root entry count", ErrInvalidBootSector)
		}
		firstClusterSector = firstDataSector
		v.fsInfo = nil
	}

	if clusterCount == 0 {
		return fmt.Errorf("%w: no data clusters in volume", ErrInvalidBootSector)
	}

	v.bootSector = bs
	v.fatType = fatType
	v.bytesPerSector = uint32(bs.BytesPerSector)
	v.sectorsPerCluster = uint32(bs.SectorsPerCluster)
	v.bytesPerCluster = uint32(bs.BytesPerSector) * uint32(bs.SectorsPerCluster)
	v.reservedSectors = uint32(bs.ReservedSectors)
	v.numberOfFATs = uint32(bs.NumberOfFATs)
	v.rootEntryCount = uint32(bs.RootEntryCount)
	v.fatSizeSectors = fatSize
	v.totalSectors = totalSectors
	v.rootDirSectors = rootDirSectors
	v.firstFATSector = firstFATSector
	v.firstDataSector = firstDataSector
	v.firstClusterSector = firstClusterSector
	v.rootDirFirstSector = rootDirFirstSector
	v.rootCluster = rootCluster
	v.clusterCount = clusterCount
	v.volumeSize = volumeSize
	v.volumeLabel = bs.VolumeLabel
	v.fsTypeHint = bs.FileSystemTypeHint

	return nil
}

func fatTypeFromClusterCount(clusterCount uint32) string {
	switch {
	case clusterCount <= FAT12MaxClusters:
		return FATType12
	case clusterCount < FAT32MinClusters:
		return FATType16
	default:
		return FATType32
	}
}

func isPowerOfTwo(value uint32) bool {
	return value != 0 && (value&(value-1)) == 0
}

func isValidFATSectorSize(value uint16) bool {
	switch value {
	case 512, 1024, 2048, 4096:
		return true
	default:
		return false
	}
}

func (v *Volume) GetBootSector() *BootSector {
	return v.bootSector
}

func (v *Volume) UsedBackupBootSector() bool {
	return v.bootSector != nil && v.bootSector.UsedBackup
}

func (v *Volume) FATType() string {
	return v.fatType
}

func (v *Volume) BytesPerSector() uint32 {
	return v.bytesPerSector
}

func (v *Volume) SectorsPerCluster() uint32 {
	return v.sectorsPerCluster
}

func (v *Volume) BytesPerCluster() uint32 {
	return v.bytesPerCluster
}

func (v *Volume) ClusterCount() uint32 {
	return v.clusterCount
}

func (v *Volume) VolumeSize() uint64 {
	return v.volumeSize
}

// VolumeLabel returns the volume's label, preferring the record in the root
// directory over the copy in the boot sector.
//
// The two can easily disagree: the boot sector copy is written once at format
// time and never updated, so a volume renamed afterwards - or formatted by
// Windows, which writes "NO NAME" there regardless - keeps a stale one. When
// the root directory holds no label record the boot sector's copy is returned,
// since a stale name is still better evidence than none. Use
// BootSectorVolumeLabel to see that copy on its own, and FATMeta's
// VolumeLabelSource to find out which of the two a report used.
func (v *Volume) VolumeLabel() string {
	if v.rootVolumeLabel != "" {
		return v.rootVolumeLabel
	}
	return v.volumeLabel
}

// BootSectorVolumeLabel returns the BS_VolLab copy of the label, which is what
// the boot sector claims rather than what the volume is called. A difference
// between this and VolumeLabel is itself evidence: it means the volume was
// labelled after it was formatted.
func (v *Volume) BootSectorVolumeLabel() string {
	return v.volumeLabel
}

// VolumeLabelSource reports where VolumeLabel's answer came from: "root
// directory", "boot sector", or "" when the volume carries no label at all.
func (v *Volume) VolumeLabelSource() string {
	switch {
	case v.rootVolumeLabel != "":
		return "root directory"
	case v.volumeLabel != "":
		return "boot sector"
	default:
		return ""
	}
}

func (v *Volume) RootCluster() uint32 {
	return v.rootCluster
}

func (v *Volume) FirstDataSector() uint32 {
	return v.firstDataSector
}

func (v *Volume) FirstRootDirSector() uint32 {
	return v.rootDirFirstSector
}

// Geometry returns the volume's layout; see Geometry.
func (v *Volume) Geometry() Geometry {
	return Geometry{
		BytesPerSector:     v.bytesPerSector,
		SectorsPerCluster:  v.sectorsPerCluster,
		BytesPerCluster:    v.bytesPerCluster,
		ReservedSectors:    v.reservedSectors,
		NumberOfFATs:       v.numberOfFATs,
		FATSizeSectors:     v.fatSizeSectors,
		FirstFATSector:     v.firstFATSector,
		RootDirFirstSector: v.rootDirFirstSector,
		RootDirSectors:     v.rootDirSectors,
		FirstDataSector:    v.firstDataSector,
		RootCluster:        v.rootCluster,
		ClusterCount:       v.clusterCount,
		TotalSectors:       v.totalSectors,
		BaseOffset:         v.baseOffset,
	}
}

// BaseOffset is the byte offset within the image at which this volume begins.
//
// It is what every offset this library returns is measured from - a Range's
// StartByte, a DirEntry's EntryAbsoluteOffset and LFNEntryOffset, the report's
// StartOffset and every fragment in it - so those values can be read straight
// from the image the volume was opened over, with no further adjustment.
// Subtract it to obtain a volume-relative offset.
//
// It is OpenOptions.BaseOffset as supplied, and 0 unless one was given.
func (v *Volume) BaseOffset() int64 {
	return v.baseOffset
}

// abs converts a volume-relative byte offset into an image-absolute one.
// Internal geometry is held relative to the volume, because that is what the
// boot sector describes and what the bounds checks are expressed in; every
// offset that leaves this package or reaches the reader goes through here.
func (v *Volume) abs(rel int64) int64 {
	return rel + v.baseOffset
}

// endOffset is the image-absolute offset one past the volume's last byte. It is
// the bound to test an already-absolute offset against, where a volume-relative
// one would be tested against volumeSize.
func (v *Volume) endOffset() int64 {
	return v.baseOffset + int64(v.volumeSize)
}

func (v *Volume) ClusterToOffset(cluster uint32) (int64, error) {
	if v.IsClosed() {
		return 0, ErrVolumeClosed
	}
	return v.clusterToOffset(cluster)
}

func (v *Volume) clusterToOffset(cluster uint32) (int64, error) {
	if cluster < DefaultRootCluster || cluster > v.maxClusterNumber() {
		return 0, fmt.Errorf("%w: invalid cluster %d", ErrCorruptStructure, cluster)
	}
	sector := uint64(v.firstDataSector) + (uint64(cluster-DefaultRootCluster) * uint64(v.sectorsPerCluster))
	offset := sector * uint64(v.bytesPerSector)
	if offset+uint64(v.bytesPerCluster) > v.volumeSize || offset > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("%w: cluster %d extends beyond volume", ErrCorruptStructure, cluster)
	}
	return v.abs(int64(offset)), nil
}

// ReadAt reads from the image the volume was opened over. The offset is
// image-absolute, in the same coordinate space as BaseOffset and every offset
// this library reports, so a Range's StartByte can be passed here unchanged.
// It is not volume-relative: a volume opened with a non-zero BaseOffset does
// not begin at offset 0 here.
func (v *Volume) ReadAt(p []byte, offset int64) (int, error) {
	if v.IsClosed() {
		return 0, ErrVolumeClosed
	}
	return v.reader.ReadAt(p, offset)
}

func (v *Volume) readSectors(firstSector, sectorCount uint32) ([]byte, error) {
	if sectorCount == 0 {
		return []byte{}, nil
	}
	size := int(sectorCount * v.bytesPerSector)
	buf := make([]byte, size)
	offset := v.abs(int64(firstSector) * int64(v.bytesPerSector))
	if _, err := v.ReadAt(buf, offset); err != nil && err != io.EOF {
		return nil, wrapParseError("sector data", offset, err)
	}
	return buf, nil
}

func (v *Volume) readCluster(cluster uint32) ([]byte, error) {
	offset, err := v.clusterToOffset(cluster)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, v.bytesPerCluster)
	if _, err := v.ReadAt(buf, offset); err != nil && err != io.EOF {
		return nil, wrapParseError("cluster data", offset, err)
	}
	return buf, nil
}

func (v *Volume) readFATEntry(cluster uint32) (uint32, error) {
	return v.readFATEntryMode(cluster, false)
}

// readFATEntryMode is readFATEntry with Strict's mirror check when strict is
// set. It is a parameter rather than read from the volume because only the
// live paths are strict: the orphan sweep and the label scan at open read the
// same FAT tolerantly on a strict volume.
func (v *Volume) readFATEntryMode(cluster uint32, strict bool) (uint32, error) {
	if cluster < DefaultRootCluster || cluster > v.maxClusterNumber() {
		return 0, fmt.Errorf("%w: FAT cluster %d out of range", ErrCorruptStructure, cluster)
	}

	primary, err := v.readFATEntryFromTable(cluster, 0)
	if err != nil {
		return 0, err
	}

	if v.numberOfFATs < 2 {
		return primary, nil
	}

	secondary, err := v.readFATEntryFromTable(cluster, 1)
	if err != nil {
		return primary, nil
	}

	if primary != secondary {
		atomic.AddUint64(&v.fatMirrorMismatches, 1)
		if strict && !v.strictSkipFATMirror && v.fatMirrored() {
			return 0, &StrictError{
				Check:       StrictFATMirror,
				EntryOffset: -1,
				Cluster:     cluster,
				Detail:      fmt.Sprintf("FAT #1 holds %#x, FAT #2 holds %#x", primary, secondary),
			}
		}
		if !v.isPlausibleFATValue(primary) && v.isPlausibleFATValue(secondary) {
			return secondary, nil
		}
	}

	return primary, nil
}

// fatMirrored reports whether the volume's FAT copies are meant to be kept
// identical. FAT32 can switch mirroring off with bit 7 of BPB_ExtFlags, after
// which only the active copy is maintained and the others may legitimately
// differ from it.
func (v *Volume) fatMirrored() bool {
	if v.numberOfFATs < 2 {
		return false
	}
	return v.fatType != FATType32 || v.bootSector == nil || v.bootSector.ExtFlags&0x80 == 0
}

func (v *Volume) readFATEntryFromTable(cluster, table uint32) (uint32, error) {
	if cluster < DefaultRootCluster || cluster > v.maxClusterNumber() {
		return 0, fmt.Errorf("%w: FAT cluster %d out of range", ErrCorruptStructure, cluster)
	}
	if table >= v.numberOfFATs {
		return 0, fmt.Errorf("%w: FAT table index %d out of range", ErrCorruptStructure, table)
	}

	entryOffset, entrySize, err := v.fatEntryLocation(cluster)
	if err != nil {
		return 0, err
	}

	fatByteLen := v.fatSizeSectors * v.bytesPerSector
	if entryOffset+entrySize > fatByteLen {
		return 0, fmt.Errorf("%w: FAT entry crosses FAT bounds (cluster=%d, offset=%d, size=%d, fatBytes=%d)",
			ErrCorruptStructure, cluster, entryOffset, entrySize, fatByteLen)
	}

	if tables := v.cachedFATTables(); tables != nil {
		return v.decodeFATEntry(tables[table][entryOffset:entryOffset+entrySize], cluster)
	}

	buf := make([]byte, entrySize)
	fatStartSector := v.firstFATSector + (table * v.fatSizeSectors)
	fatOffset := v.abs((int64(fatStartSector) * int64(v.bytesPerSector)) + int64(entryOffset))
	n, err := v.ReadAt(buf, fatOffset)
	if err != nil && err != io.EOF {
		return 0, wrapParseError("FAT entry", fatOffset, err)
	}
	if n != len(buf) {
		return 0, fmt.Errorf("%w: short FAT entry read at offset %d", ErrCorruptStructure, fatOffset)
	}
	return v.decodeFATEntry(buf, cluster)
}

// fatEntryLocation returns the byte offset of cluster's entry within one FAT
// copy and the number of bytes that must be read to decode it.
func (v *Volume) fatEntryLocation(cluster uint32) (offset, size uint32, err error) {
	switch v.fatType {
	case FATType12:
		return cluster + (cluster / 2), 2, nil
	case FATType16:
		return cluster * 2, 2, nil
	case FATType32:
		return cluster * 4, 4, nil
	default:
		return 0, 0, fmt.Errorf("%w: %s", ErrUnsupportedFAT, v.fatType)
	}
}

// decodeFATEntry decodes the entry for cluster from buf, which holds exactly
// the bytes fatEntryLocation names.
func (v *Volume) decodeFATEntry(buf []byte, cluster uint32) (uint32, error) {
	switch v.fatType {
	case FATType12:
		value := binary.LittleEndian.Uint16(buf)
		if cluster&1 == 0 {
			return uint32(value & 0x0FFF), nil
		}
		return uint32(value >> 4), nil
	case FATType16:
		return uint32(binary.LittleEndian.Uint16(buf)), nil
	case FATType32:
		return binary.LittleEndian.Uint32(buf) & FAT32Mask, nil
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFAT, v.fatType)
	}
}

// cachedFATTables returns every FAT copy held in memory, loading them on the
// first call, or nil when lookups must read the image instead. A volume opened
// without CacheFAT never loads anything.
func (v *Volume) cachedFATTables() [][]byte {
	if !v.cacheFAT {
		return nil
	}
	// Held across the load so that Close, which takes the write lock, cannot
	// interleave with it: either the load finishes first and Close drops what
	// it built, or Close goes first and nothing is loaded. loadFATTables reads
	// through v.reader directly and so does not take the lock again.
	v.closeMu.RLock()
	defer v.closeMu.RUnlock()
	if v.closed {
		return nil
	}
	v.fatCache.once.Do(func() {
		v.fatCache.tables = v.loadFATTables()
	})
	return v.fatCache.tables
}

// loadFATTables reads every FAT copy in full. It returns nil, so that lookups
// read entry by entry as they always have, when the copies are too large to be
// worth holding or when any of them cannot be read whole: a cache that covered
// only part of a table would make the same lookup succeed or fail depending on
// whether the cache had been built.
func (v *Volume) loadFATTables() [][]byte {
	tableBytes := int64(v.fatSizeSectors) * int64(v.bytesPerSector)
	if tableBytes <= 0 || tableBytes*int64(v.numberOfFATs) > maxFATCacheBytes {
		return nil
	}
	tables := make([][]byte, v.numberOfFATs)
	for table := uint32(0); table < v.numberOfFATs; table++ {
		buf := make([]byte, tableBytes)
		start := v.abs(int64(v.firstFATSector+table*v.fatSizeSectors) * int64(v.bytesPerSector))
		n, err := v.reader.ReadAt(buf, start)
		if n != len(buf) || (err != nil && err != io.EOF) {
			return nil
		}
		tables[table] = buf
	}
	return tables
}

func (v *Volume) isPlausibleFATValue(value uint32) bool {
	if value == 0 || v.isBadCluster(value) || v.isEndOfChain(value) {
		return true
	}
	return value >= DefaultRootCluster && value <= v.maxClusterNumber()
}

func (v *Volume) isEndOfChain(value uint32) bool {
	switch v.fatType {
	case FATType12:
		return value >= 0x0FF8
	case FATType16:
		return value >= 0xFFF8
	case FATType32:
		return value >= FAT32EOCMin
	default:
		return true
	}
}

func (v *Volume) isBadCluster(value uint32) bool {
	switch v.fatType {
	case FATType12:
		return value == 0x0FF7
	case FATType16:
		return value == 0xFFF7
	case FATType32:
		return value == FAT32Bad
	default:
		return false
	}
}

func (v *Volume) clusterChain(startCluster uint32) ([]uint32, error) {
	return v.clusterChainMode(startCluster, false)
}

// clusterChainMode is clusterChain, failing with a StrictError where Strict
// requires when strict is set.
func (v *Volume) clusterChainMode(startCluster uint32, strict bool) ([]uint32, error) {
	if startCluster < DefaultRootCluster || startCluster > v.maxClusterNumber() {
		return nil, fmt.Errorf("%w: invalid cluster %d", ErrCorruptStructure, startCluster)
	}

	seen := make(map[uint32]struct{})
	chain := make([]uint32, 0, 4)
	current := startCluster

	// Under Strict each failure is a StrictError naming the cluster; the record
	// that owns the chain is filled in by the caller, which knows it.
	corrupt := func(cluster uint32, err error) error {
		if !strict {
			return err
		}
		return &StrictError{Check: StrictChainCorrupt, EntryOffset: -1, Cluster: cluster, Err: err}
	}

	for {
		if _, ok := seen[current]; ok {
			return nil, corrupt(current, fmt.Errorf("%w: cluster loop at %d", ErrCorruptStructure, current))
		}
		seen[current] = struct{}{}
		chain = append(chain, current)

		next, err := v.readFATEntryMode(current, strict)
		if err != nil {
			return nil, err
		}
		if next == 0 {
			if strict {
				return nil, &StrictError{
					Check:       StrictChainFreeCluster,
					EntryOffset: -1,
					Cluster:     current,
					Detail:      fmt.Sprintf("chain runs into a free cluster after %d clusters", len(chain)),
				}
			}
			return chain, nil
		}
		if v.isBadCluster(next) {
			return nil, corrupt(current, fmt.Errorf("%w: bad cluster %d", ErrCorruptStructure, next))
		}
		if v.isEndOfChain(next) {
			return chain, nil
		}
		if next < DefaultRootCluster || next >= v.clusterCount+DefaultRootCluster {
			return nil, corrupt(current, fmt.Errorf("%w: next cluster %d out of range", ErrCorruptStructure, next))
		}
		current = next
	}
}

func (v *Volume) readClusterChain(startCluster uint32, sizeHint int64, strict bool) ([]byte, error) {
	chain, err := v.clusterChainMode(startCluster, strict)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, len(chain)*int(v.bytesPerCluster))
	for _, cluster := range chain {
		data, err := v.readCluster(cluster)
		if err != nil {
			return nil, err
		}
		buf = append(buf, data...)
	}

	if sizeHint >= 0 && int64(len(buf)) > sizeHint {
		buf = buf[:sizeHint]
	}
	return buf, nil
}

func (v *Volume) readRootDirectoryData(strict bool) ([]byte, error) {
	if v.fatType == FATType32 {
		return v.readClusterChain(v.rootCluster, -1, strict)
	}
	return v.readSectors(v.rootDirFirstSector, v.rootDirSectors)
}

func (v *Volume) IsClusterAllocated(cluster uint32) (bool, error) {
	if cluster < DefaultRootCluster {
		return false, nil
	}
	if cluster > v.maxClusterNumber() {
		return false, fmt.Errorf("%w: cluster %d out of range", ErrCorruptStructure, cluster)
	}
	value, err := v.readFATEntry(cluster)
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

func (v *Volume) FATMirrorMismatches() uint64 {
	return atomic.LoadUint64(&v.fatMirrorMismatches)
}

func (v *Volume) maxClusterNumber() uint32 {
	if v.clusterCount == 0 {
		return DefaultRootCluster - 1
	}
	return DefaultRootCluster + v.clusterCount - 1
}

func (v *Volume) readFAT32FSInfo(bs *BootSector) (*FAT32FSInfo, error) {
	sector := bs.FSInfoSector
	if sector == 0 {
		sector = 1
	}
	sectorSize := int64(bs.BytesPerSector)
	if sectorSize <= 0 {
		return nil, fmt.Errorf("%w: invalid sector size for FSInfo", ErrCorruptStructure)
	}
	buf := make([]byte, sectorSize)
	offset := v.abs(int64(sector) * sectorSize)
	if _, err := v.reader.ReadAt(buf, offset); err != nil && err != io.EOF {
		return nil, wrapParseError("FAT32 FSInfo", offset, err)
	}
	if len(buf) < BootSectorSize {
		return nil, fmt.Errorf("%w: FAT32 FSInfo sector too small", ErrCorruptStructure)
	}

	info := &FAT32FSInfo{
		Sector:             sector,
		LeadSignature:      ReadUint32LE(buf, 0),
		StructureSignature: ReadUint32LE(buf, 484),
		FreeClusterCount:   ReadUint32LE(buf, 488),
		NextFreeCluster:    ReadUint32LE(buf, 492),
		TrailSignature:     ReadUint32LE(buf, 508),
	}

	info.Valid = info.LeadSignature == FSInfoLeadSig &&
		info.StructureSignature == FSInfoStructSig &&
		info.TrailSignature == FSInfoTrailSig

	return info, nil
}

func (v *Volume) FSInfo() *FAT32FSInfo {
	return v.fsInfo
}

func (v *Volume) FreeClusterCountHint() (uint32, bool) {
	if v.fsInfo == nil || !v.fsInfo.Valid || v.fsInfo.FreeClusterCount == 0xFFFFFFFF {
		return 0, false
	}
	return v.fsInfo.FreeClusterCount, true
}

func (v *Volume) NextFreeClusterHint() (uint32, bool) {
	if v.fsInfo == nil || !v.fsInfo.Valid || v.fsInfo.NextFreeCluster == 0xFFFFFFFF {
		return 0, false
	}
	return v.fsInfo.NextFreeCluster, true
}

func (v *Volume) String() string {
	return fmt.Sprintf("%s volume: %d bytes (%d clusters of %d bytes)",
		v.fatType, v.volumeSize, v.clusterCount, v.bytesPerCluster)
}
