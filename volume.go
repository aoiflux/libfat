package libfat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
)

type Volume struct {
	reader                    io.ReaderAt
	forcedFATType             string
	includeVolumeLabelEntries bool
	includeVirtualRootEntries bool

	mu      sync.RWMutex
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
	fsTypeHint          string
	fsInfo              *FAT32FSInfo
	fatMirrorMismatches uint64
}

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
		forcedFATType:             fatType,
		includeVolumeLabelEntries: options.IncludeVolumeLabelEntries,
		includeVirtualRootEntries: options.IncludeVirtualRootEntries,
	}
	if err := v.parseBootSector(); err != nil {
		return nil, wrapVolumeError("open", err)
	}
	return v, nil
}

func (v *Volume) Close() error {
	v.closeMu.Lock()
	defer v.closeMu.Unlock()
	v.closed = true
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

func (v *Volume) parseBootSectorAt(offset int64) (*BootSector, error) {
	buf := make([]byte, BootSectorSize)
	if _, err := v.reader.ReadAt(buf, offset); err != nil {
		return nil, wrapParseError("boot sector", offset, err)
	}

	bs := &BootSector{Offset: offset, UsedBackup: offset != 0}
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

	fatSize := uint32(bs.FATSize16)
	if fatSize == 0 {
		fatSize = bs.FATSize32
	}

	rootDirSectors := ((uint32(bs.RootEntryCount) * dirEntrySize) + (uint32(bs.BytesPerSector) - 1)) / uint32(bs.BytesPerSector)
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
			if clusterCount >= fat12ClusterThreshold {
				return fmt.Errorf("%w: too many clusters for FAT12 (%d)", ErrInvalidBootSector, clusterCount)
			}
		case FATType16:
			if clusterCount < fat12ClusterThreshold || clusterCount >= fat16ClusterThreshold {
				return fmt.Errorf("%w: cluster count %d incompatible with FAT16", ErrInvalidBootSector, clusterCount)
			}
		case FATType32:
			if clusterCount < fat16ClusterThreshold {
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

	rootCluster := uint32(defaultRootCluster)
	rootDirFirstSector := uint32(bs.ReservedSectors) + (uint32(bs.NumberOfFATs) * fatSize)
	firstClusterSector := firstDataSector
	if fatType == FATType32 {
		if bs.RootEntryCount != 0 {
			return fmt.Errorf("%w: FAT32 requires root entry count 0 (got %d)", ErrInvalidBootSector, bs.RootEntryCount)
		}
		if bs.RootCluster < defaultRootCluster {
			return fmt.Errorf("%w: invalid FAT32 root cluster %d", ErrInvalidBootSector, bs.RootCluster)
		}
		if bs.RootCluster-defaultRootCluster >= clusterCount {
			return fmt.Errorf("%w: FAT32 root cluster %d outside data range", ErrInvalidBootSector, bs.RootCluster)
		}
		rootCluster = bs.RootCluster
		rootDirFirstSector = firstDataSector + ((rootCluster - defaultRootCluster) * uint32(bs.SectorsPerCluster))
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
	v.volumeSize = uint64(totalSectors) * uint64(bs.BytesPerSector)
	v.volumeLabel = bs.VolumeLabel
	v.fsTypeHint = bs.FileSystemTypeHint

	return nil
}

func fatTypeFromClusterCount(clusterCount uint32) string {
	switch {
	case clusterCount < fat12ClusterThreshold:
		return FATType12
	case clusterCount < fat16ClusterThreshold:
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

func (v *Volume) VolumeLabel() string {
	return v.volumeLabel
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

func (v *Volume) ClusterToOffset(cluster uint32) (int64, error) {
	if v.IsClosed() {
		return 0, ErrVolumeClosed
	}
	if cluster < defaultRootCluster {
		return 0, fmt.Errorf("%w: invalid cluster %d", ErrCorruptStructure, cluster)
	}
	sector := v.firstDataSector + ((cluster - defaultRootCluster) * v.sectorsPerCluster)
	return int64(sector) * int64(v.bytesPerSector), nil
}

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
	offset := int64(firstSector) * int64(v.bytesPerSector)
	if _, err := v.ReadAt(buf, offset); err != nil && err != io.EOF {
		return nil, wrapParseError("sector data", offset, err)
	}
	return buf, nil
}

func (v *Volume) readCluster(cluster uint32) ([]byte, error) {
	offset, err := v.ClusterToOffset(cluster)
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
	if cluster < defaultRootCluster || cluster > v.maxClusterNumber() {
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
		if !v.isPlausibleFATValue(primary) && v.isPlausibleFATValue(secondary) {
			return secondary, nil
		}
	}

	return primary, nil
}

func (v *Volume) readFATEntryFromTable(cluster, table uint32) (uint32, error) {
	if cluster < defaultRootCluster || cluster > v.maxClusterNumber() {
		return 0, fmt.Errorf("%w: FAT cluster %d out of range", ErrCorruptStructure, cluster)
	}
	if table >= v.numberOfFATs {
		return 0, fmt.Errorf("%w: FAT table index %d out of range", ErrCorruptStructure, table)
	}

	var entryOffset uint32
	var entrySize uint32

	switch v.fatType {
	case FATType12:
		entryOffset = cluster + (cluster / 2)
		entrySize = 2
	case FATType16:
		entryOffset = cluster * 2
		entrySize = 2
	case FATType32:
		entryOffset = cluster * 4
		entrySize = 4
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFAT, v.fatType)
	}

	fatByteLen := v.fatSizeSectors * v.bytesPerSector
	if entryOffset+entrySize > fatByteLen {
		return 0, fmt.Errorf("%w: FAT entry crosses FAT bounds (cluster=%d, offset=%d, size=%d, fatBytes=%d)",
			ErrCorruptStructure, cluster, entryOffset, entrySize, fatByteLen)
	}

	buf := make([]byte, entrySize)
	fatStartSector := v.firstFATSector + (table * v.fatSizeSectors)
	fatOffset := (int64(fatStartSector) * int64(v.bytesPerSector)) + int64(entryOffset)
	n, err := v.ReadAt(buf, fatOffset)
	if err != nil && err != io.EOF {
		return 0, wrapParseError("FAT entry", fatOffset, err)
	}
	if n != len(buf) {
		return 0, fmt.Errorf("%w: short FAT entry read at offset %d", ErrCorruptStructure, fatOffset)
	}

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
		return binary.LittleEndian.Uint32(buf) & 0x0FFFFFFF, nil
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFAT, v.fatType)
	}
}

func (v *Volume) isPlausibleFATValue(value uint32) bool {
	if value == 0 || v.isBadCluster(value) || v.isEndOfChain(value) {
		return true
	}
	return value >= defaultRootCluster && value <= v.maxClusterNumber()
}

func (v *Volume) isEndOfChain(value uint32) bool {
	switch v.fatType {
	case FATType12:
		return value >= 0x0FF8
	case FATType16:
		return value >= 0xFFF8
	case FATType32:
		return value >= 0x0FFFFFF8
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
		return value == 0x0FFFFFF7
	default:
		return false
	}
}

func (v *Volume) clusterChain(startCluster uint32) ([]uint32, error) {
	if startCluster < defaultRootCluster || startCluster > v.maxClusterNumber() {
		return nil, fmt.Errorf("%w: invalid cluster %d", ErrCorruptStructure, startCluster)
	}

	seen := make(map[uint32]struct{})
	chain := make([]uint32, 0, 4)
	current := startCluster

	for {
		if _, ok := seen[current]; ok {
			return nil, fmt.Errorf("%w: cluster loop at %d", ErrCorruptStructure, current)
		}
		seen[current] = struct{}{}
		chain = append(chain, current)

		next, err := v.readFATEntry(current)
		if err != nil {
			return nil, err
		}
		if next == 0 {
			return chain, nil
		}
		if v.isBadCluster(next) {
			return nil, fmt.Errorf("%w: bad cluster %d", ErrCorruptStructure, next)
		}
		if v.isEndOfChain(next) {
			return chain, nil
		}
		if next < defaultRootCluster || next >= v.clusterCount+defaultRootCluster {
			return nil, fmt.Errorf("%w: next cluster %d out of range", ErrCorruptStructure, next)
		}
		current = next
	}
}

func (v *Volume) readClusterChain(startCluster uint32, sizeHint int64) ([]byte, error) {
	chain, err := v.clusterChain(startCluster)
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

func (v *Volume) readRootDirectoryData() ([]byte, error) {
	if v.fatType == FATType32 {
		return v.readClusterChain(v.rootCluster, -1)
	}
	return v.readSectors(v.rootDirFirstSector, v.rootDirSectors)
}

func (v *Volume) IsClusterAllocated(cluster uint32) (bool, error) {
	if cluster < defaultRootCluster {
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
		return defaultRootCluster - 1
	}
	return defaultRootCluster + v.clusterCount - 1
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
	offset := int64(sector) * sectorSize
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

	info.Valid = info.LeadSignature == 0x41615252 &&
		info.StructureSignature == 0x61417272 &&
		info.TrailSignature == 0xAA550000

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
