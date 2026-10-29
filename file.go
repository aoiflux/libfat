package libfat

import (
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"unicode"
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

	// reader is built on first read and caches the file's resolved runs, so
	// that repeated reads do not re-walk the FAT. A File is not safe for
	// concurrent use; open the path again for a second goroutine.
	reader          *fragmentReader
	truncated       bool
	fragmentOptions FragmentOptions
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

// OpenPath resolves a path from the root and returns the File it names.
//
// Matching is deliberately forgiving, in the way Windows is, and the rules are
// these. The path is trimmed of surrounding spaces, '\' is accepted as a
// separator, a leading drive designator such as "C:" is dropped, and the result
// is cleaned with path.Clean, so "." and ".." components are resolved
// lexically rather than through the directory records. Each component matches
// an entry whose Name or ShortName equals it under strings.EqualFold, which is
// Unicode simple case folding rather than the upcase table the volume was
// written with. The first matching record wins. Deleted and virtual entries
// never match; use OpenEntry for those.
//
// On a volume opened with OpenOptions.Strict, a directory crossed on the way
// that fails a strict check fails the lookup with its *StrictError.
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

// OpenEntry returns a File for a directory entry obtained from ReadDir or
// ScanOrphans.
//
// OpenPath deliberately refuses deleted and orphaned entries: no live path
// leads to them, so there is nothing to resolve. This is how their content is
// read instead. Enumerate the entries, then open the one you want:
//
//	f, err := v.OpenEntry(entry)
//	f.SetFragmentOptions(libfat.FragmentOptions{AssumeContiguous: true})
//	data, err := f.ReadAll()
//
// For a deleted entry the default options locate only the first cluster and
// ReadAll reports ErrTruncatedChain alongside the recovered prefix; see
// FragmentOptions for the reconstruction alternative.
func (v *Volume) OpenEntry(entry DirEntry) (*File, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if entry.Virtual {
		return nil, fmt.Errorf("%w: %s is a virtual entry with no backing data",
			ErrFileNotFound, entry.Name)
	}
	return v.openDirEntry(entry), nil
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

	r, err := f.fragments()
	if err != nil {
		return 0, err
	}
	// The runs may cover fewer bytes than the directory entry claims when the
	// chain is truncated, which is the normal case for a deleted entry. Bound
	// against what was actually located, never against the recorded size.
	if offset >= r.Size() {
		return 0, fmt.Errorf("%w: %d of %d bytes recoverable for %s",
			ErrTruncatedChain, r.Size(), f.size, f.path)
	}
	if limit := int64(f.size) - offset; int64(len(p)) > limit {
		p = p[:limit]
	}
	return r.ReadAt(p, offset)
}

// ReadAll returns the file's contents. When the FAT chain accounts for fewer
// bytes than the directory entry's size, the recovered prefix is returned
// together with an error wrapping ErrTruncatedChain, so that a partially
// recoverable deleted file is never mistaken for a small intact one.
func (f *File) ReadAll() ([]byte, error) {
	if f.isDir {
		return nil, ErrIsDirectory
	}
	if f.volume.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if f.size == 0 {
		if f.volume.strict {
			// An empty file owns no clusters; Strict checks that it names none.
			if _, err := f.fragments(); err != nil {
				return nil, err
			}
		}
		return []byte{}, nil
	}

	r, err := f.fragments()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, r.Size())
	n, rerr := r.ReadAt(buf, 0)
	buf = buf[:n]
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return buf, rerr
	}
	if int64(n) < int64(f.size) {
		return buf, fmt.Errorf("%w: %d of %d bytes for %s",
			ErrTruncatedChain, n, f.size, f.path)
	}
	return buf, nil
}

// ReadDir lists the directory's entries in the order their records appear,
// deleted entries included, and without the "." and ".." records. It is
// ReadDirWithOptions with the zero ReadDirOptions.
func (f *File) ReadDir() ([]DirEntry, error) {
	return f.ReadDirWithOptions(ReadDirOptions{})
}

// ReadDirOptions selects what ReadDirWithOptions reports. The zero value is
// what ReadDir has always reported.
type ReadDirOptions struct {
	// ExcludeDeleted leaves out every entry with Deleted set, including those
	// recovered from beyond the end-of-directory marker. ReadDir reports them,
	// with '_' standing in for the first character deletion overwrote, because
	// a listing that hid deleted entries would hide the point of this package;
	// a caller that wants only the live tree asks for that here.
	ExcludeDeleted bool `json:"exclude_deleted"`

	// IncludeDotEntries reports the "." and ".." records that begin every
	// subdirectory, which ReadDir drops. Their Path is the directory's path
	// with "/." or "/.." appended literally rather than cleaned. They are
	// reported as records, not as a way to move around the tree: ".." in a
	// directory directly below the root carries cluster 0, which names no
	// cluster, so OpenEntry on it does not open the root.
	IncludeDotEntries bool `json:"include_dot_entries"`
}

// ReadDirWithOptions lists the directory's entries as ReadDir does, filtered
// by opts.
//
// On a volume opened with OpenOptions.Strict it also checks the directory's
// records and returns a *StrictError for the first violation instead of a
// listing; see the package documentation's Strict mode section.
func (f *File) ReadDirWithOptions(opts ReadDirOptions) ([]DirEntry, error) {
	if f.volume.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if !f.isDir {
		return nil, ErrNotDirectory
	}

	// A directory reached through a deleted or orphaned record is being
	// recovered, not listed, and Strict does not apply to it.
	strict := f.volume.strict && !f.entry.Deleted && !f.entry.Orphaned
	dirCluster := f.identityCluster()

	var data []byte
	var ranges []Range
	var err error
	if f.isRoot {
		data, err = f.volume.readRootDirectoryData(strict)
		ranges, _ = f.volume.RootDirectoryFragments()
	} else {
		data, err = f.volume.readClusterChain(f.firstCluster, -1, strict)
		ranges, _ = f.volume.ClusterChainFragments(f.firstCluster, 0)
	}
	if err != nil {
		var se *StrictError
		if errors.As(err, &se) {
			// The chain belongs to this directory, so the record at fault is
			// this directory's own entry in its parent. The root has none.
			if f.isRoot {
				return nil, se.atEntry(dirCluster, -1)
			}
			return nil, se.atEntry(f.entry.ParentFirstCluster, f.entry.EntryOffset)
		}
		return nil, err
	}
	if strict {
		// ".." names the parent's first cluster, except that a parent which
		// is the root is named 0: always on FAT12 and FAT16, where the root
		// is no cluster, and by convention on FAT32, where it is one.
		// A directory opened through one of its own "." or ".." records was
		// reached without its parent, so there is nothing to hold its ".."
		// to; every other check still applies.
		dotdot, knownParent := f.entry.ParentFirstCluster, f.entry.Name != "." && f.entry.Name != ".."
		if f.volume.fatType == FATType32 && dotdot == f.volume.rootCluster {
			dotdot = 0
		}
		if se := f.volume.strictDirectory(data, dirCluster, f.isRoot, dotdot, knownParent); se != nil {
			return nil, se
		}
	}

	// A failure to resolve the directory's own fragments must not fail the
	// listing: entries simply report -1 for their absolute offsets.
	ctx := &dirParseContext{
		dirPath:             f.path,
		isClusterAllocated:  f.volume.IsClusterAllocated,
		includeVolumeLabels: f.volume.includeVolumeLabelEntries,
		recoverDeletedLFN:   f.volume.recoverDeletedLongNames,
		mapOffset:           rangeOffsetMapper(ranges),
		parentFirstCluster:  dirCluster,
		fat32:               f.volume.fatType == FATType32,
		includeDots:         opts.IncludeDotEntries,
	}
	entries := parseDirectoryEntries(data, ctx)
	if strict {
		if se := strictDuplicates(entries, dirCluster); se != nil {
			return nil, se
		}
	}
	if f.isRoot && f.volume.includeVirtualRootEntries {
		entries = append(entries, virtualRootEntries()...)
	}
	if opts.ExcludeDeleted {
		live := entries[:0]
		for _, e := range entries {
			if !e.Deleted {
				live = append(live, e)
			}
		}
		entries = live
	}
	return entries, nil
}

// identityCluster is the cluster number that identifies this directory for the
// purposes of DirEntry.ParentFirstCluster.
//
// It is not always f.firstCluster. GetRootDirectory sets firstCluster to
// v.rootCluster, which applyBootSector fills with 2 on FAT12 and FAT16 even
// though those volumes' roots are a fixed region and not cluster-addressed at
// all. Reporting 2 there would collide with whatever directory really occupies
// cluster 2, so the fixed root region reports FixedRootCluster instead.
func (f *File) identityCluster() uint32 {
	if f.isRoot && f.volume.fatType != FATType32 {
		return FixedRootCluster
	}
	return f.firstCluster
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

// deletedDirectoryChildren parses the surviving records in the first cluster of
// a deleted directory. The boolean is false when the cluster no longer holds
// that directory's data, which is the common outcome and not an error.
//
// Only one cluster is read. Deletion frees the FAT chain, so there is no chain
// to follow; walking the FAT from the first cluster would report whatever now
// owns it as this directory's contents, which is the error FragmentOffsets
// refuses to make for deleted files. Contiguity is not assumed either. Two
// conditions must hold before anything is returned: the FAT must still mark the
// cluster free, and the cluster must still begin with the "." and ".." records
// with "." pointing back at itself, which is the guard that separates surviving
// directory data from a later file that happens to start there.
func (v *Volume) deletedDirectoryChildren(e DirEntry) ([]DirEntry, bool, error) {
	if e.ClusterAllocated {
		// Reallocated: the cluster now belongs to something else.
		return nil, false, nil
	}
	if e.FirstCluster < DefaultRootCluster || e.FirstCluster > v.maxClusterNumber() {
		return nil, false, nil
	}
	if v.bytesPerCluster == 0 {
		return nil, false, fmt.Errorf("%w: volume has no cluster size", ErrCorruptStructure)
	}

	buf := make([]byte, v.bytesPerCluster)
	if err := v.readClusterInto(buf, e.FirstCluster); err != nil {
		return nil, false, err
	}
	if _, hasDots := dotEntryParent(buf, e.FirstCluster, v.fatType == FATType32); !hasDots {
		// A stale copy, or overwritten by a later file.
		return nil, false, nil
	}

	offset, err := v.clusterToOffset(e.FirstCluster)
	if err != nil {
		return nil, false, err
	}
	ranges := []Range{{
		StartByte:    offset,
		Length:       int64(v.bytesPerCluster),
		StartCluster: e.FirstCluster,
		ClusterCount: 1,
	}}
	entries := parseDirectoryEntries(buf, &dirParseContext{
		dirPath:             e.Path,
		isClusterAllocated:  v.IsClusterAllocated,
		includeVolumeLabels: v.includeVolumeLabelEntries,
		recoverDeletedLFN:   v.recoverDeletedLongNames,
		mapOffset:           rangeOffsetMapper(ranges),
		parentFirstCluster:  e.FirstCluster,
		fat32:               v.fatType == FATType32,
		recovery:            true,
	})
	return entries, true, nil
}

// dirParseContext carries everything parseDirectoryEntries needs beyond the raw
// directory bytes.
type dirParseContext struct {
	dirPath             string
	isClusterAllocated  func(cluster uint32) (bool, error)
	includeVolumeLabels bool
	recoverDeletedLFN   bool
	// mapOffset translates an offset within the directory's concatenated data
	// into an absolute image offset. It is nil when the mapping is unavailable.
	mapOffset func(int64) int64
	// parentFirstCluster is the first cluster of the directory whose data this
	// is, or FixedRootCluster for the FAT12/16 root region. It is copied onto
	// every entry so that an entry carries the logical address of its own record
	// and not only the physical one, which relocation invalidates.
	parentFirstCluster uint32
	// recovery marks data that is being recovered rather than listed: a deleted
	// directory's surviving cluster or an orphan run. Every record in it is held
	// to isPlausibleRecoveredEntry, as deleted records are everywhere.
	recovery bool
	// includeDots reports the "." and ".." records instead of skipping them.
	includeDots bool
	// fat32 says whether bytes 20-21 of a record are the high word of its first
	// cluster; see entryFirstCluster.
	fat32 bool
}

func (c *dirParseContext) absolute(bufOffset int) int64 {
	if c.mapOffset == nil {
		return -1
	}
	return c.mapOffset(int64(bufOffset))
}

// deletedLFNSlot is a long-name slot belonging to a deleted entry. Deletion
// overwrites the sequence byte, so the physical position is the only reliable
// ordering signal and the checksum is the only reliable association signal.
type deletedLFNSlot struct {
	offset   int
	checksum byte
	part     []uint16
	// full is true when the slot carries all 13 code units with no padding,
	// meaning further parts of the name follow it logically. The first slot
	// that is not full is the final part of the name, which is the only
	// structural end-of-name signal that survives deletion.
	full bool
}

func parseDirectoryEntries(data []byte, ctx *dirParseContext) []DirEntry {
	entries := make([]DirEntry, 0, len(data)/DirEntrySize)
	var lfnParts [][]uint16
	var lfnActive bool
	var lfnExpectedSeq byte
	var lfnChecksum byte
	var lfnStartOffset int
	var deletedSlots []deletedLFNSlot
	seenEndMarker := false

	resetLFN := func() {
		lfnParts = nil
		lfnActive = false
		lfnExpectedSeq = 0
		lfnChecksum = 0
		lfnStartOffset = -1
		deletedSlots = nil
	}
	resetLFN()

	for offset := 0; offset+DirEntrySize <= len(data); offset += DirEntrySize {
		entry := data[offset : offset+DirEntrySize]
		firstByte := entry[0]
		if firstByte == 0x00 {
			seenEndMarker = true
			resetLFN()
			continue
		}
		deleted := firstByte == DeletedMarker
		attributes := entry[11]
		recovered := seenEndMarker && deleted

		if seenEndMarker && !deleted && attributes != AttrLongName {
			resetLFN()
			continue
		}

		if attributes == AttrLongName {
			if !isValidLFNEntry(entry, deleted) {
				resetLFN()
				continue
			}

			if deleted {
				// The sequence byte is destroyed by deletion, so the live-entry
				// ordering checks cannot be applied. Buffer the slot and defer
				// association to the short entry's checksum.
				if !ctx.recoverDeletedLFN {
					resetLFN()
					continue
				}
				lfnParts = nil
				lfnActive = false
				if len(deletedSlots) >= MaxLFNSlots {
					deletedSlots = deletedSlots[1:]
				}
				part := parseLFNPart(entry)
				deletedSlots = append(deletedSlots, deletedLFNSlot{
					offset:   offset,
					checksum: entry[13],
					part:     part,
					full:     len(part) == LFNCharsPerSlot,
				})
				continue
			}
			deletedSlots = nil

			seq := entry[0]
			ord := seq & 0x3F
			chk := entry[13]

			if seq&LFNLastFlag != 0 {
				resetLFN()
				lfnActive = true
				lfnExpectedSeq = ord
				lfnChecksum = chk
				lfnStartOffset = offset
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

		valid := isValidShortEntry(entry, ctx.fat32)
		if valid && (deleted || ctx.recovery) {
			valid = isPlausibleRecoveredEntry(entry, ctx.fat32)
		}
		if !valid {
			resetLFN()
			continue
		}

		if attributes&AttrVolumeID != 0 {
			if ctx.includeVolumeLabels {
				label := parseVolumeLabel(entry, deleted)
				if label != "" {
					entries = append(entries, DirEntry{
						Name:                label,
						Path:                path.Join(ctx.dirPath, label),
						ShortName:           label,
						IsDirectory:         false,
						Size:                0,
						Attributes:          attributes,
						Deleted:             deleted,
						Recovered:           recovered,
						EntryOffset:         int64(offset),
						EntryAbsoluteOffset: ctx.absolute(offset),
						LFNEntryOffset:      -1,
						DirectoryPath:       ctx.dirPath,
						ParentFirstCluster:  ctx.parentFirstCluster,
					})
				}
			}
			resetLFN()
			continue
		}

		var (
			name           string
			shortName      string
			nameSource     = NameSourceShort
			firstCharFixed bool
			lfnOffset      = int64(-1)
		)

		if deleted && ctx.recoverDeletedLFN && len(deletedSlots) > 0 {
			longName, firstChar, slotOffset, ok := recoverDeletedLFNName(entry, deletedSlots)
			if ok {
				shortName = parseShortNameWithFirstChar(entry, deleted, firstChar)
				firstCharFixed = firstChar != 0
				name = longName
				nameSource = NameSourceRecoveredLFN
				lfnOffset = ctx.absolute(slotOffset)
			}
		}

		if name == "" {
			shortName = parseShortName(entry, deleted)
			name = shortName
			if lfnActive && len(lfnParts) > 0 && lfnExpectedSeq == 1 && lfnChecksum == lfnShortNameChecksum(entry[0:11]) {
				if longName := assembleLFN(lfnParts); longName != "" {
					name = longName
					nameSource = NameSourceLFN
					lfnOffset = ctx.absolute(lfnStartOffset)
				}
			}
		}
		resetLFN()

		isDot := name == "." || name == ".."
		if name == "" || (isDot && !ctx.includeDots) {
			continue
		}

		firstCluster := entryFirstCluster(entry, ctx.fat32)
		clusterAllocated := false
		if ctx.isClusterAllocated != nil {
			if allocated, err := ctx.isClusterAllocated(firstCluster); err == nil {
				clusterAllocated = allocated
			}
		}
		fullPath := path.Join(ctx.dirPath, name)
		if ctx.dirPath == "/" {
			fullPath = "/" + name
		}
		if isDot {
			// Joined literally: path.Join would clean "/DIR/.." to "/".
			fullPath = strings.TrimSuffix(ctx.dirPath, "/") + "/" + name
		}

		entries = append(entries, DirEntry{
			Name:                name,
			Path:                fullPath,
			ShortName:           shortName,
			IsDirectory:         attributes&AttrDirectory != 0,
			Size:                uint64(ReadUint32LE(entry, 28)),
			FirstCluster:        firstCluster,
			ClusterAllocated:    clusterAllocated,
			Attributes:          attributes,
			CreatedAt:           DecodeDateTime(ReadUint16LE(entry, 16), ReadUint16LE(entry, 14), entry[13]),
			ModifiedAt:          DecodeDateTime(ReadUint16LE(entry, 24), ReadUint16LE(entry, 22), 0),
			AccessedAt:          DecodeDateTime(ReadUint16LE(entry, 18), 0, 0),
			Deleted:             deleted,
			Recovered:           recovered,
			EntryOffset:         int64(offset),
			EntryAbsoluteOffset: ctx.absolute(offset),
			LFNEntryOffset:      lfnOffset,
			DirectoryPath:       ctx.dirPath,
			ParentFirstCluster:  ctx.parentFirstCluster,
			NameSource:          nameSource,
			FirstCharRecovered:  firstCharFixed,
		})
	}

	return entries
}

// recoverDeletedLFNName reconstructs the long name of a deleted short entry
// from the buffered long-name slots that physically precede it.
//
// Deletion overwrites the first byte of every slot and of the short entry, so
// neither the sequence numbers nor the original first character survive. Three
// signals do survive, and all three must agree before a name is reported:
//
//   - the slots are physically adjacent to the short entry, since directory
//     entries for one file are always written consecutively;
//   - every slot carries the same checksum byte, computed over the short
//     entry's 11 name bytes;
//   - exactly one slot is not full, and it is the one logically last in the
//     name, which bounds the run and proves it was not truncated mid-name.
//
// The checksum recovers the lost first character rather than validating the
// association: the map from first byte to checksum is a bijection, so some byte
// always matches. The association rests on adjacency and on the run being
// properly bounded, which is strong in practice but remains a reconstruction.
// Entries recovered this way are marked NameSourceRecoveredLFN so that callers
// can treat them as a hypothesis.
//
// It returns the assembled name, the recovered first character, the buffer
// offset of the first slot, and whether recovery succeeded.
func recoverDeletedLFNName(shortEntry []byte, slots []deletedLFNSlot) (string, byte, int, bool) {
	if len(slots) == 0 {
		return "", 0, 0, false
	}

	// Walk backwards from the short entry. Physical order is reverse logical
	// order, so this yields the name's parts in order starting from the first.
	target := slots[len(slots)-1].checksum
	run := make([]deletedLFNSlot, 0, len(slots))
	nextOffset := -1
	bounded := false
	for i := len(slots) - 1; i >= 0; i-- {
		slot := slots[i]
		if slot.checksum != target {
			break
		}
		if nextOffset >= 0 && slot.offset+DirEntrySize != nextOffset {
			break
		}
		nextOffset = slot.offset
		run = append(run, slot)
		if !slot.full {
			// The logically last part of the name; the run is complete.
			bounded = true
			break
		}
		if len(run) >= MaxLFNSlots {
			break
		}
	}
	if len(run) == 0 {
		return "", 0, 0, false
	}
	// Consuming every buffered slot also bounds the run: the buffer is cleared
	// at each live or invalid entry, so its start is a real boundary. This is
	// what covers names whose length is an exact multiple of 13, where the
	// final slot is full and carries no terminator.
	if !bounded && len(run) == len(slots) {
		bounded = true
	}
	if !bounded {
		return "", 0, 0, false
	}

	name11 := make([]byte, 11)
	copy(name11, shortEntry[0:11])
	candidate, ok := recoverFirstChar(name11, target)
	if !ok {
		return "", 0, 0, false
	}

	parts := make([][]uint16, 0, len(run))
	for i := len(run) - 1; i >= 0; i-- {
		parts = append(parts, run[i].part)
	}

	name := assembleLFN(parts)
	if !isPlausibleRecoveredName(name) {
		return "", 0, 0, false
	}
	if !longNameMatchesShortName(name, name11, candidate) {
		return "", 0, 0, false
	}
	return name, candidate, run[len(run)-1].offset, true
}

// longNameMatchesShortName checks that the recovered long name could have
// generated the surviving short name, which is the association evidence the
// checksum cannot supply.
//
// Short names are derived from the long name by upper-casing it, discarding
// spaces and embedded dots, substituting characters that are illegal in 8.3,
// and appending a numeric tail. Every variant of that algorithm, including the
// hashed form used after many collisions, preserves the first two characters of
// the basis name. The second character survives deletion intact, so comparing
// it is independent of the checksum-recovered first character and rejects
// long-name slots that belong to some other entry.
//
// The check is deliberately strict: for a forensic tool, falling back to the
// short name costs little, whereas attributing another file's name to a deleted
// entry is a substantive error.
func longNameMatchesShortName(longName string, shortName11 []byte, firstChar byte) bool {
	basis := shortNameBasis(longName)
	if basis == "" {
		return false
	}
	if basis[0] != firstChar {
		return false
	}
	if len(basis) >= 2 && shortName11[1] != ' ' && basis[1] != shortName11[1] {
		return false
	}
	return true
}

// shortNameBasis reproduces the basis name from which a short name is derived:
// the long name up to its final dot, upper-cased, with spaces and dots removed
// and illegal characters replaced by underscores.
func shortNameBasis(longName string) string {
	name := strings.TrimRight(longName, ". ")
	if i := strings.LastIndex(name, "."); i > 0 {
		name = name[:i]
	}

	var basis strings.Builder
	for _, r := range name {
		if r == ' ' || r == '.' {
			continue
		}
		var c byte
		if r > 0x7F {
			c = '_'
		} else {
			c = byte(unicode.ToUpper(r))
			if !isValid83Char(c, false) {
				c = '_'
			}
		}
		basis.WriteByte(c)
		if basis.Len() >= 8 {
			break
		}
	}
	return basis.String()
}

// recoverFirstChar finds the byte that, in position 0 of the 11-byte short
// name, produces the given checksum. It reports failure when the solution is
// not a byte that could legally have appeared in a short name, which is the one
// way the checksum can contradict the association.
func recoverFirstChar(name11 []byte, target byte) (byte, bool) {
	scratch := make([]byte, 11)
	copy(scratch, name11)
	for value := 0; value < 256; value++ {
		scratch[0] = byte(value)
		if lfnShortNameChecksum(scratch) != target {
			continue
		}
		b := byte(value)
		if b == ' ' || b == DeletedMarker || !isValid83Char(b, false) {
			return 0, false
		}
		return b, true
	}
	return 0, false
}

// isPlausibleRecoveredName rejects reconstructions that decoded into control
// characters or path separators, which indicate that the slots were overwritten
// with unrelated data rather than genuinely recovered.
func isPlausibleRecoveredName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7F {
			return false
		}
		if r == '/' || r == '\\' || r == ':' {
			return false
		}
		if r == 0xFFFD {
			return false
		}
	}
	return true
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
		// The sequence byte is gone, but the reserved byte and the zeroed
		// cluster field are untouched by deletion and still discriminate a real
		// long-name slot from unrelated data left in the directory.
		return entry[12] == 0x00 && entry[26] == 0x00 && entry[27] == 0x00
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

func isValidShortEntry(entry []byte, fat32 bool) bool {
	attributes := entry[11]
	if attributes&^byte(AttrReadOnly|AttrHidden|AttrSystem|AttrVolumeID|AttrDirectory|AttrArchive) != 0 {
		return false
	}
	if attributes&AttrVolumeID != 0 && attributes&(AttrDirectory|AttrReadOnly|AttrArchive) != 0 {
		return false
	}

	cluster := entryFirstCluster(entry, fat32)
	size := ReadUint32LE(entry, 28)
	if size > 0 && cluster == 0 {
		return false
	}
	if attributes&AttrDirectory != 0 && size != 0 {
		return false
	}
	if attributes&AttrVolumeID != 0 {
		return true
	}
	return looksLikeShortName(entry[0:8], entry[8:11])
}

// isPlausibleRecoveredEntry is isValidShortEntry plus a heuristic that rejects
// records carrying no times, no cluster and no size. Such a record is a valid
// empty file when it is live, and is listed as one, but in data that is being
// recovered - a deleted record, or a cluster swept by the orphan scan - it is
// far more often residue that happens to parse, and it carries nothing that
// would be worth reporting even when it is genuine.
func isPlausibleRecoveredEntry(entry []byte, fat32 bool) bool {
	if !isValidShortEntry(entry, fat32) {
		return false
	}
	if entry[11]&AttrVolumeID != 0 {
		return true
	}
	cluster := entryFirstCluster(entry, fat32)
	size := ReadUint32LE(entry, 28)
	hasTimestamp := ReadUint16LE(entry, 14) != 0 || ReadUint16LE(entry, 16) != 0 || ReadUint16LE(entry, 18) != 0 || ReadUint16LE(entry, 22) != 0 || ReadUint16LE(entry, 24) != 0
	return hasTimestamp || cluster != 0 || size != 0
}

func looksLikeShortName(base, ext []byte) bool {
	seenSpace := false
	for i, value := range base {
		if value == ' ' {
			seenSpace = true
			continue
		}
		if i == 0 && (value == KanjiE5 || value == DotEntry || value == DeletedMarker) {
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
		if seenSpace || !isValid83Char(value, false) {
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
	return parseShortNameWithFirstChar(entry, deleted, 0)
}

// parseShortNameWithFirstChar decodes the 8.3 name. For a deleted entry the
// first byte has been overwritten with 0xE5; firstChar supplies the recovered
// original when one is known, and 0 falls back to the '_' placeholder.
func parseShortNameWithFirstChar(entry []byte, deleted bool, firstChar byte) string {
	var name [11]byte
	copy(name[:], entry[0:11])
	if !deleted {
		// 0x05 encodes a literal 0xE5 as the first character of a live entry,
		// so that it is not mistaken for a deletion marker.
		return ParseShortName(name, entry[12])
	}
	if firstChar != 0 && firstChar != ' ' {
		name[0] = firstChar
	} else {
		name[0] = '_'
	}
	return formatShortName(name, entry[12])
}

// lowerASCII lower-cases the letters A-Z in place and leaves every other byte
// alone. That is all the NTRes case bits ask for: Windows sets them only for
// names whose letters are ASCII, and a byte from 0x80 up is an OEM code-page
// character whose case this package cannot know. strings.ToLower would treat
// such a byte as invalid UTF-8 and replace it with U+FFFD.
func lowerASCII(b []byte) {
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
}

// formatShortName trims, joins and case-folds an 11-byte name field whose first
// byte has already been resolved.
func formatShortName(name [11]byte, ntres byte) string {
	if ntres&NTResLowerBase != 0 {
		lowerASCII(name[0:8])
	}
	if ntres&NTResLowerExt != 0 {
		lowerASCII(name[8:11])
	}
	base := strings.TrimRight(string(name[0:8]), " ")
	ext := strings.TrimRight(string(name[8:11]), " ")
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
		{Name: "$MBR", Path: "/$MBR", ShortName: "$MBR", Virtual: true, EntryAbsoluteOffset: -1, LFNEntryOffset: -1},
		{Name: "$FAT1", Path: "/$FAT1", ShortName: "$FAT1", Virtual: true, EntryAbsoluteOffset: -1, LFNEntryOffset: -1},
		{Name: "$FAT2", Path: "/$FAT2", ShortName: "$FAT2", Virtual: true, EntryAbsoluteOffset: -1, LFNEntryOffset: -1},
		{Name: "$OrphanFiles", Path: "/$OrphanFiles", ShortName: "$OrphanFiles", IsDirectory: true, Virtual: true, EntryAbsoluteOffset: -1, LFNEntryOffset: -1},
	}
}

func parseLFNPart(entry []byte) []uint16 {
	part := make([]uint16, 0, 13)
	for _, offset := range lfnCharOffsets {
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
