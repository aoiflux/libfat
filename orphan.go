package libfat

import (
	"fmt"
	"io"
)

// OrphanScanOptions tunes the search for unreachable directory data.
//
// The zero value is the precision-first configuration: it scans only clusters
// the FAT marks free, requires the "." and ".." records that identify the first
// cluster of a directory, and follows contiguous continuation clusters.
type OrphanScanOptions struct {
	// ScanAllocatedClusters also examines clusters the FAT marks in use. Those
	// clusters belong to live files, so any directory data found in them is
	// stale content that a later file was written over. Off by default.
	ScanAllocatedClusters bool

	// AllowMissingDotEntries accepts any cluster whose records all parse as
	// directory entries, rather than only the first cluster of a directory.
	// It finds directory fragments whose start has been overwritten, at the
	// cost of false positives from files that happen to contain entry-shaped
	// bytes.
	AllowMissingDotEntries bool

	// OnlyFirstCluster reports just the cluster where a directory was found,
	// without gathering the contiguous clusters that follow it. Directories are
	// usually laid out contiguously, so following them recovers entries beyond
	// the first cluster of a large directory.
	OnlyFirstCluster bool

	// MinValidEntries is the number of well-formed records a cluster must hold
	// before it is treated as directory data. Zero selects 1.
	MinValidEntries int

	// MaxClusters bounds how many clusters are examined. Zero scans the whole
	// data area.
	MaxClusters uint32

	// MaxDirectories bounds how many orphan directories are returned. Zero
	// means unlimited.
	MaxDirectories int

	// MaxDepth bounds the walk of the reachable tree that determines which
	// clusters are already accounted for. Zero selects 64.
	MaxDepth int
}

// OrphanDirectory is a run of clusters holding directory data that is not
// reachable from the root.
type OrphanDirectory struct {
	// Ranges are the absolute image byte ranges of the clusters the directory
	// data was found in.
	Ranges []Range
	// FirstCluster is the cluster where the directory data starts.
	FirstCluster uint32
	// ParentCluster is the first cluster of the parent directory, taken from
	// the ".." record. Zero means the root directory, or that the record was
	// absent.
	ParentCluster uint32
	// HasDotEntries is true when the run began with the "." and ".." records
	// that mark the first cluster of a directory. When false, the run was
	// identified only by the shape of its records and is a weaker finding.
	HasDotEntries bool
	// Entries are the directory entries recovered from the run. Their Path is
	// rooted at /$OrphanFiles because the original path is not recoverable:
	// the parent chain that named this directory is gone.
	Entries []DirEntry
}

// OrphanScanResult reports the outcome of a scan.
type OrphanScanResult struct {
	Directories []OrphanDirectory
	// ClustersScanned counts clusters read, whether or not they matched.
	ClustersScanned uint32
	// ClustersSkipped counts clusters not examined because they were reachable
	// from the root or allocated.
	ClustersSkipped uint32
	// Truncated is true when a bound in OrphanScanOptions stopped the scan
	// before the data area was exhausted.
	Truncated bool
	// ReachableWalkFailed is true when part of the live directory tree could
	// not be walked, so some reported orphans may in fact be reachable.
	ReachableWalkFailed bool
}

// Entries flattens the recovered entries across all orphan directories.
func (r *OrphanScanResult) Entries() []DirEntry {
	var out []DirEntry
	for _, dir := range r.Directories {
		out = append(out, dir.Entries...)
	}
	return out
}

// OrphanPath is the virtual directory under which recovered orphan entries are
// reported, mirroring the convention used by other forensic tooling.
const OrphanPath = "/$OrphanFiles"

// ScanOrphans searches the data area for directory entries that cannot be
// reached from the root directory.
//
// Deleting a directory marks its entry in the parent and frees its FAT chain,
// but leaves the directory's own clusters untouched. The entries describing its
// children survive there, unreferenced by any reachable directory. This scan
// finds those clusters and parses the entries out of them.
//
// The scan reads every candidate cluster, so it costs one pass over the data
// area and is far more expensive than ordinary traversal. Use MaxClusters to
// bound it on large or untrusted images.
//
// Recovered entries carry Orphaned set and a Path under OrphanPath. Their
// original paths are not recoverable, because the directory chain that named
// them is exactly what was lost.
func (v *Volume) ScanOrphans(opts OrphanScanOptions) (*OrphanScanResult, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if v.bytesPerCluster == 0 {
		return nil, fmt.Errorf("%w: volume has no cluster size", ErrCorruptStructure)
	}

	minEntries := opts.MinValidEntries
	if minEntries <= 0 {
		minEntries = 1
	}
	maxDepth := opts.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 64
	}
	limit := opts.MaxClusters
	if limit == 0 || limit > v.clusterCount {
		limit = v.clusterCount
	}

	reachable, walkOK := v.reachableDirectoryClusters(maxDepth)
	result := &OrphanScanResult{ReachableWalkFailed: !walkOK}

	consumed := make(map[uint32]struct{})
	buf := make([]byte, v.bytesPerCluster)
	last := v.maxClusterNumber()

	for cluster := uint32(defaultRootCluster); cluster <= last; cluster++ {
		if result.ClustersScanned >= limit {
			result.Truncated = true
			break
		}
		if opts.MaxDirectories > 0 && len(result.Directories) >= opts.MaxDirectories {
			result.Truncated = true
			break
		}
		if _, ok := consumed[cluster]; ok {
			continue
		}
		if _, ok := reachable[cluster]; ok {
			result.ClustersSkipped++
			continue
		}
		if !opts.ScanAllocatedClusters {
			allocated, err := v.IsClusterAllocated(cluster)
			if err != nil {
				continue
			}
			if allocated {
				result.ClustersSkipped++
				continue
			}
		}

		if err := v.readClusterInto(buf, cluster); err != nil {
			continue
		}
		result.ClustersScanned++

		parent, hasDots := dotEntryParent(buf, cluster)
		if !hasDots {
			if !opts.AllowMissingDotEntries {
				continue
			}
			if !looksLikeDirectoryData(buf, minEntries) {
				continue
			}
		}

		dir := v.gatherOrphanDirectory(cluster, buf, parent, hasDots, opts, minEntries, reachable, consumed, result)
		if len(dir.Entries) > 0 {
			result.Directories = append(result.Directories, dir)
		}
	}

	return result, nil
}

// gatherOrphanDirectory collects the run of clusters starting at first and
// parses the entries out of it.
func (v *Volume) gatherOrphanDirectory(
	first uint32,
	firstData []byte,
	parent uint32,
	hasDots bool,
	opts OrphanScanOptions,
	minEntries int,
	reachable, consumed map[uint32]struct{},
	result *OrphanScanResult,
) OrphanDirectory {
	data := make([]byte, 0, len(firstData)*2)
	data = append(data, firstData...)
	clusters := []uint32{first}
	consumed[first] = struct{}{}

	if !opts.OnlyFirstCluster {
		buf := make([]byte, v.bytesPerCluster)
		for next := first + 1; next <= v.maxClusterNumber(); next++ {
			if _, ok := consumed[next]; ok {
				break
			}
			if _, ok := reachable[next]; ok {
				break
			}
			if !opts.ScanAllocatedClusters {
				allocated, err := v.IsClusterAllocated(next)
				if err != nil || allocated {
					break
				}
			}
			if err := v.readClusterInto(buf, next); err != nil {
				break
			}
			result.ClustersScanned++
			// A continuation cluster carries no "." records, so it is accepted
			// only on the shape of its records, and only while it still holds
			// entries: the zero-filled tail of a directory ends the run.
			if !looksLikeDirectoryData(buf, minEntries) {
				break
			}
			data = append(data, buf...)
			clusters = append(clusters, next)
			consumed[next] = struct{}{}
		}
	}

	ranges := v.clustersToRanges(clusters)
	ctx := &dirParseContext{
		dirPath:             OrphanPath,
		isClusterAllocated:  v.IsClusterAllocated,
		includeVolumeLabels: false,
		recoverDeletedLFN:   v.recoverDeletedLongNames,
		mapOffset:           rangeOffsetMapper(ranges),
	}
	entries := parseDirectoryEntries(data, ctx)
	for i := range entries {
		entries[i].Orphaned = true
	}

	return OrphanDirectory{
		Ranges:        ranges,
		FirstCluster:  first,
		ParentCluster: parent,
		HasDotEntries: hasDots,
		Entries:       entries,
	}
}

// clustersToRanges converts an ascending cluster list into coalesced ranges.
func (v *Volume) clustersToRanges(clusters []uint32) []Range {
	ranges := make([]Range, 0, len(clusters))
	for _, c := range clusters {
		offset, err := v.clusterToOffset(c)
		if err != nil {
			continue
		}
		ranges = append(ranges, Range{
			StartByte:    offset,
			Length:       int64(v.bytesPerCluster),
			StartCluster: c,
			ClusterCount: 1,
		})
	}
	return Coalesce(ranges)
}

func (v *Volume) readClusterInto(buf []byte, cluster uint32) error {
	offset, err := v.clusterToOffset(cluster)
	if err != nil {
		return err
	}
	n, err := v.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return err
	}
	if n < len(buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// dotEntryParent reports whether the cluster begins with the "." and ".."
// records that mark the first cluster of a directory, and returns the parent's
// first cluster from the ".." record.
//
// The "." record's own cluster field must point at the cluster the record was
// found in. That check rejects stale copies of directory data that survive in
// clusters the directory no longer occupies.
func dotEntryParent(data []byte, cluster uint32) (uint32, bool) {
	if len(data) < 2*dirEntrySize {
		return 0, false
	}
	dot := data[0:dirEntrySize]
	dotdot := data[dirEntrySize : 2*dirEntrySize]

	if !isDotRecord(dot, ".") || !isDotRecord(dotdot, "..") {
		return 0, false
	}
	if entryFirstCluster(dot) != cluster {
		return 0, false
	}
	return entryFirstCluster(dotdot), true
}

func isDotRecord(entry []byte, name string) bool {
	if entry[11]&attrDirectory == 0 {
		return false
	}
	if entry[11]&(attrVolumeID|attrLongName) != 0 {
		return false
	}
	for i := 0; i < len(name); i++ {
		if entry[i] != '.' {
			return false
		}
	}
	for i := len(name); i < 11; i++ {
		if entry[i] != ' ' {
			return false
		}
	}
	return true
}

func entryFirstCluster(entry []byte) uint32 {
	return uint32(ReadUint16LE(entry, 26)) | (uint32(ReadUint16LE(entry, 20)) << 16)
}

// looksLikeDirectoryData reports whether every record in the buffer parses as a
// directory entry and at least minEntries of them carry real content.
//
// Requiring that no record fails to classify is what keeps the false-positive
// rate low: arbitrary file data almost always contains a byte pattern that no
// directory entry could have.
func looksLikeDirectoryData(data []byte, minEntries int) bool {
	if len(data) < dirEntrySize {
		return false
	}
	valid := 0
	for offset := 0; offset+dirEntrySize <= len(data); offset += dirEntrySize {
		entry := data[offset : offset+dirEntrySize]
		switch {
		case entry[0] == 0x00:
			// Free record; the remainder of a directory cluster is zero-filled.
			if !isZeroed(entry) {
				return false
			}
		case entry[11] == attrLongName:
			if !isValidLFNEntry(entry, entry[0] == 0xE5) {
				return false
			}
			valid++
		case isDotRecord(entry, "."), isDotRecord(entry, ".."):
			// The dot records are structural rather than content. ".." in a
			// directory directly below the root carries cluster 0 and no
			// timestamps, which the general short-entry validator rejects, so
			// they are classified here and not counted towards minEntries.
		default:
			if !isValidShortEntry(entry) {
				return false
			}
			valid++
		}
	}
	return valid >= minEntries
}

func isZeroed(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// reachableDirectoryClusters returns the clusters occupied by directories that
// can be reached from the root. Anything outside this set is a candidate for
// orphan recovery. The boolean reports whether the whole tree was walked; a
// false value means some subtree failed to parse and its clusters are missing
// from the set.
func (v *Volume) reachableDirectoryClusters(maxDepth int) (map[uint32]struct{}, bool) {
	seen := make(map[uint32]struct{})
	visited := make(map[uint32]struct{})
	ok := true

	if v.fatType == FATType32 {
		v.addChainClusters(v.rootCluster, seen)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		return seen, false
	}
	v.walkReachable(root, 0, maxDepth, seen, visited, &ok)
	return seen, ok
}

func (v *Volume) walkReachable(dir *File, depth, maxDepth int, seen, visited map[uint32]struct{}, ok *bool) {
	if depth >= maxDepth {
		*ok = false
		return
	}

	entries, err := dir.ReadDir()
	if err != nil {
		*ok = false
		return
	}

	for _, entry := range entries {
		if !entry.IsDirectory || entry.Deleted || entry.Virtual {
			continue
		}
		if entry.FirstCluster < defaultRootCluster || entry.FirstCluster > v.maxClusterNumber() {
			continue
		}
		if _, done := visited[entry.FirstCluster]; done {
			continue
		}
		visited[entry.FirstCluster] = struct{}{}

		v.addChainClusters(entry.FirstCluster, seen)
		v.walkReachable(v.openDirEntry(entry), depth+1, maxDepth, seen, visited, ok)
	}
}

// addChainClusters records every cluster of a chain, tolerating breakage: a
// partially walked chain still tells us which clusters are accounted for.
func (v *Volume) addChainClusters(start uint32, seen map[uint32]struct{}) {
	result, err := v.walkChainRuns(start, FragmentOptions{})
	if err != nil {
		return
	}
	for _, r := range result.Ranges {
		for i := uint32(0); i < r.ClusterCount; i++ {
			seen[r.StartCluster+i] = struct{}{}
		}
	}
}
