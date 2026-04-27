package libfat

import (
	"fmt"
	"io"
	"path"
	"strings"
	"time"
	"unicode/utf16"
)

type File struct {
	volume       *Volume
	entry        DirEntry
	name         string
	path         string
	isDir        bool
	size         uint64
	firstCluster uint32
	readOffset   int64
	isRoot       bool
}

func normalizeFATPath(filePath string) string {
	filePath = strings.TrimSpace(filePath)
	filePath = strings.ReplaceAll(filePath, "\\", "/")
	if len(filePath) >= 2 && filePath[1] == ':' {
		filePath = filePath[2:]
	}
	filePath = path.Clean(filePath)
	if filePath == "" || filePath == "." {
		return "/"
	}
	if !strings.HasPrefix(filePath, "/") {
		filePath = "/" + filePath
	}
	return filePath
}

func (v *Volume) GetRootDirectory() (*File, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}
	return &File{
		volume:       v,
		name:         "/",
		path:         "/",
		isDir:        true,
		isRoot:       true,
		firstCluster: v.rootCluster,
	}, nil
}

func (v *Volume) OpenPath(filePath string) (*File, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}

	filePath = normalizeFATPath(filePath)
	current, err := v.GetRootDirectory()
	if err != nil {
		return nil, wrapPathError("open", filePath, "/", err)
	}
	if filePath == "/" {
		return current, nil
	}

	parts := strings.Split(strings.Trim(filePath, "/"), "/")
	for i, part := range parts {
		if part == "" || part == "." {
			continue
		}
		pathSoFar := "/" + strings.Join(parts[:i+1], "/")
		if !current.IsDirectory() {
			return nil, wrapPathError("traverse", filePath, pathSoFar, ErrNotDirectory)
		}

		entries, err := current.ReadDir()
		if err != nil {
			return nil, wrapPathError("readdir", filePath, pathSoFar, err)
		}

		found := false
		for _, entry := range entries {
			if entry.Deleted {
				continue
			}
			if entry.Virtual {
				continue
			}
			if strings.EqualFold(entry.Name, part) || strings.EqualFold(entry.ShortName, part) {
				current = v.openDirEntry(entry)
				found = true
				break
			}
		}
		if !found {
			return nil, wrapPathError("lookup", filePath, pathSoFar, fmt.Errorf("%w: %s", ErrFileNotFound, pathSoFar))
		}
	}

	return current, nil
}

func (v *Volume) openDirEntry(entry DirEntry) *File {
	return &File{
		volume:       v,
		entry:        entry,
		name:         entry.Name,
		path:         entry.Path,
		isDir:        entry.IsDirectory,
		size:         entry.Size,
		firstCluster: entry.FirstCluster,
	}
}

func (f *File) Name() string {
	return f.name
}

func (f *File) Path() string {
	return f.path
}

func (f *File) IsDirectory() bool {
	return f.isDir
}

func (f *File) Size() int64 {
	return int64(f.size)
}

func (f *File) Entry() DirEntry {
	return f.entry
}

func (f *File) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.readOffset)
	f.readOffset += int64(n)
	return n, err
}

func (f *File) ReadAt(p []byte, offset int64) (int, error) {
	if f.volume.IsClosed() {
		return 0, ErrVolumeClosed
	}
	if f.isDir {
		return 0, ErrNotFile
	}
	if offset < 0 {
		return 0, ErrInvalidPath
	}
	if offset >= int64(f.size) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	data, err := f.readFileData()
	if err != nil {
		return 0, err
	}
	n := copy(p, data[offset:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *File) ReadAll() ([]byte, error) {
	if f.isDir {
		return nil, ErrIsDirectory
	}
	return f.readFileData()
}

func (f *File) ReadDir() ([]DirEntry, error) {
	if f.volume.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if !f.isDir {
		return nil, ErrNotDirectory
	}

	var data []byte
	var err error
	if f.isRoot {
		data, err = f.volume.readRootDirectoryData()
	} else {
		data, err = f.volume.readClusterChain(f.firstCluster, -1)
	}
	if err != nil {
		return nil, err
	}
	entries := parseDirectoryEntries(data, f.path, f.volume.IsClusterAllocated, f.volume.includeVolumeLabelEntries)
	if f.isRoot && f.volume.includeVirtualRootEntries {
		entries = append(entries, virtualRootEntries()...)
	}
	return entries, nil
}

func (f *File) ListFiles() ([]DirEntry, error) {
	entries, err := f.ReadDir()
	if err != nil {
		return nil, err
	}
	files := make([]DirEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDirectory {
			files = append(files, entry)
		}
	}
	return files, nil
}

func (f *File) ListDirectories() ([]DirEntry, error) {
	entries, err := f.ReadDir()
	if err != nil {
		return nil, err
	}
	dirs := make([]DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDirectory {
			dirs = append(dirs, entry)
		}
	}
	return dirs, nil
}

func (f *File) readFileData() ([]byte, error) {
	if f.size == 0 {
		return []byte{}, nil
	}
	if f.firstCluster < defaultRootCluster {
		return nil, fmt.Errorf("%w: file %s has no data cluster", ErrCorruptStructure, f.path)
	}
	return f.volume.readClusterChain(f.firstCluster, int64(f.size))
}

func parseDirectoryEntries(data []byte, dirPath string, isClusterAllocated func(cluster uint32) (bool, error), includeVolumeLabels bool) []DirEntry {
	entries := make([]DirEntry, 0, len(data)/dirEntrySize)
	var lfnParts [][]uint16
	var lfnActive bool
	var lfnExpectedSeq byte
	var lfnChecksum byte
	seenEndMarker := false

	resetLFN := func() {
		lfnParts = nil
		lfnActive = false
		lfnExpectedSeq = 0
		lfnChecksum = 0
	}

	for offset := 0; offset+dirEntrySize <= len(data); offset += dirEntrySize {
		entry := data[offset : offset+dirEntrySize]
		firstByte := entry[0]
		if firstByte == 0x00 {
			seenEndMarker = true
			resetLFN()
			continue
		}
		deleted := firstByte == 0xE5
		attributes := entry[11]
		recovered := seenEndMarker && deleted

		if seenEndMarker && !deleted && attributes != attrLongName {
			resetLFN()
			continue
		}

		if attributes == attrLongName {
			if !isValidLFNEntry(entry, deleted) {
				resetLFN()
				continue
			}

			if deleted {
				// Deleted LFN sequence bytes are frequently damaged; do not trust them.
				resetLFN()
				continue
			}

			seq := entry[0]
			ord := seq & 0x3F
			chk := entry[13]

			if seq&0x40 != 0 {
				resetLFN()
				lfnActive = true
				lfnExpectedSeq = ord
				lfnChecksum = chk
			} else {
				if !lfnActive || chk != lfnChecksum || lfnExpectedSeq <= 1 || ord != lfnExpectedSeq-1 {
					resetLFN()
					continue
				}
				lfnExpectedSeq = ord
			}

			lfnParts = append(lfnParts, parseLFNPart(entry))
			continue
		}

		if !isValidShortEntry(entry) {
			resetLFN()
			continue
		}

		if attributes&attrVolumeID != 0 {
			if includeVolumeLabels {
				label := parseVolumeLabel(entry, deleted)
				if label != "" {
					entries = append(entries, DirEntry{
						Name:          label,
						Path:          path.Join(dirPath, label),
						ShortName:     label,
						IsDirectory:   false,
						Size:          0,
						Attributes:    attributes,
						Deleted:       deleted,
						Recovered:     recovered,
						EntryOffset:   int64(offset),
						DirectoryPath: dirPath,
					})
				}
			}
			resetLFN()
			continue
		}

		shortName := parseShortName(entry, deleted)
		name := shortName
		if lfnActive && len(lfnParts) > 0 && lfnExpectedSeq == 1 && lfnChecksum == lfnShortNameChecksum(entry[0:11]) {
			if longName := assembleLFN(lfnParts); longName != "" {
				name = longName
			}
		}
		resetLFN()

		if name == "." || name == ".." || name == "" {
			continue
		}

		firstCluster := uint32(ReadUint16LE(entry, 26)) | (uint32(ReadUint16LE(entry, 20)) << 16)
		clusterAllocated := false
		if isClusterAllocated != nil {
			if allocated, err := isClusterAllocated(firstCluster); err == nil {
				clusterAllocated = allocated
			}
		}
		fullPath := path.Join(dirPath, name)
		if dirPath == "/" {
			fullPath = "/" + name
		}

		entries = append(entries, DirEntry{
			Name:             name,
			Path:             fullPath,
			ShortName:        shortName,
			IsDirectory:      attributes&attrDirectory != 0,
			Size:             uint64(ReadUint32LE(entry, 28)),
			FirstCluster:     firstCluster,
			ClusterAllocated: clusterAllocated,
			Attributes:       attributes,
			CreatedAt:        decodeFATDateTime(ReadUint16LE(entry, 16), ReadUint16LE(entry, 14), entry[13]),
			ModifiedAt:       decodeFATDateTime(ReadUint16LE(entry, 24), ReadUint16LE(entry, 22), 0),
			AccessedAt:       decodeFATDateTime(ReadUint16LE(entry, 18), 0, 0),
			Deleted:          deleted,
			Recovered:        recovered,
			EntryOffset:      int64(offset),
			DirectoryPath:    dirPath,
		})
	}

	return entries
}

func lfnShortNameChecksum(shortName []byte) byte {
	var sum byte
	for i := 0; i < len(shortName); i++ {
		sum = ((sum & 1) << 7) + (sum >> 1) + shortName[i]
	}
	return sum
}

func isValidLFNEntry(entry []byte, deleted bool) bool {
	seq := entry[0]
	if deleted {
		return true
	}
	if seq == 0x00 {
		return false
	}
	masked := seq & 0x3F
	if masked == 0 || masked > 0x14 {
		return false
	}
	return entry[12] == 0x00 && entry[26] == 0x00 && entry[27] == 0x00
}

func isValidShortEntry(entry []byte) bool {
	attributes := entry[11]
	if attributes&^byte(attrReadOnly|attrHidden|attrSystem|attrVolumeID|attrDirectory|attrArchive) != 0 {
		return false
	}
	if attributes&attrVolumeID != 0 && attributes&(attrDirectory|attrReadOnly|attrArchive) != 0 {
		return false
	}

	cluster := uint32(ReadUint16LE(entry, 26)) | (uint32(ReadUint16LE(entry, 20)) << 16)
	size := ReadUint32LE(entry, 28)
	if size > 0 && cluster == 0 {
		return false
	}
	if attributes&attrDirectory != 0 && size != 0 {
		return false
	}
	if attributes&attrVolumeID != 0 {
		return true
	}
	if !looksLikeShortName(entry[0:8], entry[8:11]) {
		return false
	}

	hasTimestamp := ReadUint16LE(entry, 14) != 0 || ReadUint16LE(entry, 16) != 0 || ReadUint16LE(entry, 18) != 0 || ReadUint16LE(entry, 22) != 0 || ReadUint16LE(entry, 24) != 0
	if !hasTimestamp && cluster == 0 && size == 0 {
		return false
	}
	return true
}

func looksLikeShortName(base, ext []byte) bool {
	seenSpace := false
	for i, value := range base {
		if value == ' ' {
			seenSpace = true
			continue
		}
		if i == 0 && (value == 0x05 || value == 0x2E || value == 0xE5) {
			continue
		}
		if seenSpace || !isValid83Char(value, i < 2) {
			return false
		}
	}
	seenSpace = false
	for _, value := range ext {
		if value == ' ' {
			seenSpace = true
			continue
		}
		if seenSpace || !isValid83Char(value, false) || value >= 0x7F {
			return false
		}
	}
	return true
}

func isValid83Char(value byte, allowDot bool) bool {
	if allowDot && value == 0x2E {
		return true
	}
	if value < 0x20 || value == 0x22 || value == 0x2F || value == 0x7C {
		return false
	}
	if value >= 0x2A && value <= 0x2C {
		return false
	}
	if value == 0x2E {
		return false
	}
	if value >= 0x3A && value <= 0x3F {
		return false
	}
	if value >= 0x5B && value <= 0x5D {
		return false
	}
	return true
}

func parseShortName(entry []byte, deleted bool) string {
	name := make([]byte, 8)
	copy(name, entry[0:8])
	if deleted {
		name[0] = '_'
	}
	base := strings.TrimRight(string(name), " ")
	ext := strings.TrimRight(string(entry[8:11]), " ")
	lowercase := entry[12]
	if lowercase&0x08 != 0 {
		base = strings.ToLower(base)
	}
	if lowercase&0x10 != 0 {
		ext = strings.ToLower(ext)
	}
	if ext == "" {
		return base
	}
	return base + "." + ext
}

func parseVolumeLabel(entry []byte, deleted bool) string {
	name := make([]byte, 8)
	copy(name, entry[0:8])
	if deleted {
		name[0] = '_'
	}
	base := strings.TrimRight(string(name), " ")
	ext := strings.TrimRight(string(entry[8:11]), " ")
	return strings.TrimSpace(base + ext)
}

func virtualRootEntries() []DirEntry {
	return []DirEntry{
		{Name: "$MBR", Path: "/$MBR", ShortName: "$MBR", Virtual: true},
		{Name: "$FAT1", Path: "/$FAT1", ShortName: "$FAT1", Virtual: true},
		{Name: "$FAT2", Path: "/$FAT2", ShortName: "$FAT2", Virtual: true},
		{Name: "$OrphanFiles", Path: "/$OrphanFiles", ShortName: "$OrphanFiles", IsDirectory: true, Virtual: true},
	}
}

func parseLFNPart(entry []byte) []uint16 {
	part := make([]uint16, 0, 13)
	for _, offset := range []int{1, 3, 5, 7, 9, 14, 16, 18, 20, 22, 24, 28, 30} {
		value := ReadUint16LE(entry, offset)
		if value == 0x0000 || value == 0xFFFF {
			continue
		}
		part = append(part, value)
	}
	return part
}

func assembleLFN(parts [][]uint16) string {
	if len(parts) == 0 {
		return ""
	}
	combined := make([]uint16, 0, len(parts)*13)
	for i := len(parts) - 1; i >= 0; i-- {
		combined = append(combined, parts[i]...)
	}
	return string(utf16.Decode(combined))
}

func decodeFATDateTime(date, clock uint16, tenths byte) time.Time {
	if date == 0 {
		return time.Time{}
	}
	year := int((date>>9)&0x7F) + 1980
	month := time.Month((date >> 5) & 0x0F)
	day := int(date & 0x1F)
	hour := int((clock >> 11) & 0x1F)
	minute := int((clock >> 5) & 0x3F)
	second := int(clock&0x1F) * 2
	nsec := int(tenths) * 10 * 1_000_000
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}
	}
	return time.Date(year, month, day, hour, minute, second, nsec, time.UTC)
}

func (f *File) String() string {
	if f.isDir {
		return fmt.Sprintf("Directory: %s", f.path)
	}
	return fmt.Sprintf("File: %s (%d bytes)", f.path, f.size)
}

func (d *DirEntry) String() string {
	kind := "File"
	if d.IsDirectory {
		kind = "Dir "
	}
	return fmt.Sprintf("[%s] %-40s %10d bytes", kind, d.Name, d.Size)
}
