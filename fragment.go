package libfat

import (
	"fmt"
	"math"
	"sort"
)

// Range is one contiguous run of bytes in the image passed to Open.
//
// StartByte is an absolute offset in that io.ReaderAt. When the reader is an
// io.SectionReader positioned at a partition offset, StartByte is relative to
// the partition, and the caller adds the partition base to obtain a whole-disk
// offset. Ranges are the unit that lets a caller compute image byte ranges for
// a file without re-walking the FAT.
type Range struct {
	// StartByte is the absolute byte offset of the run within the volume.
	StartByte int64 `json:"start_byte"`
	// Length is the number of bytes in the run.
	Length int64 `json:"length"`
	// Sparse is always false on FAT: the format has no sparse allocation and
	// every run is backed by real clusters. The field exists so that callers
	// can treat FAT ranges uniformly with filesystems that do have holes.
	Sparse bool `json:"sparse"`
	// StartCluster is the first cluster of the run, or 0 for the fixed-size
	// root directory region of FAT12 and FAT16, which is not cluster-addressed.
	StartCluster uint32 `json:"start_cluster"`
	// ClusterCount is the number of clusters in the run, or 0 for the fixed
	// root directory region.
	ClusterCount uint32 `json:"cluster_count"`
}

// EndByte returns the offset one past the last byte of the run.
func (r Range) EndByte() int64 {
	return r.StartByte + r.Length
}

func (r Range) String() string {
	if r.ClusterCount == 0 {
		return fmt.Sprintf("[%d,%d) %d bytes", r.StartByte, r.EndByte(), r.Length)
	}
	return fmt.Sprintf("[%d,%d) %d bytes, clusters %d-%d",
		r.StartByte, r.EndByte(), r.Length, r.StartCluster, r.StartCluster+r.ClusterCount-1)
}

// FragmentResult reports a file's runs together with how they were derived.
// Forensic callers must be able to tell a FAT-verified chain from a contiguity
// assumption, so every degraded outcome is flagged rather than hidden.
type FragmentResult struct {
	// Ranges are the coalesced runs, in file order. Always usable even when a
	// degradation flag is set: partial results are returned, never discarded.
	Ranges []Range `json:"ranges"`
	// BytesCovered is the sum of Range.Length.
	BytesCovered int64 `json:"bytes_covered"`
	// ChainWalked is true when Ranges came from an actual FAT chain walk. It is
	// false for deleted entries, whose FAT entries no longer describe them, and
	// for the fixed root directory region.
	ChainWalked bool `json:"chain_walked"`
	// Truncated is true when BytesCovered is less than the entry's size.
	Truncated bool `json:"truncated"`
	// Assumed is true when runs were synthesized under FragmentOptions'
	// AssumeContiguous rather than read from the FAT. Data located through
	// assumed ranges is a hypothesis, not a fact.
	Assumed bool `json:"assumed"`
	// ChainBroken is true when the walk stopped on a free, bad, or
	// out-of-range FAT entry instead of a proper end-of-chain marker.
	ChainBroken bool `json:"chain_broken"`
	// LoopDetected is true when the chain revisited a cluster. The walk stops
	// at the repeat; earlier runs remain valid.
	LoopDetected bool `json:"loop_detected"`
	// FirstClusterReallocated is true for a deleted entry whose first cluster
	// is now marked in use, meaning its content was likely overwritten by a
	// later file. Recovery from these ranges is unlikely to succeed.
	FirstClusterReallocated bool `json:"first_cluster_reallocated"`
	// ClustersWalked counts clusters visited, including those coalesced away.
	ClustersWalked uint32 `json:"clusters_walked"`
}

// FragmentOptions tunes how runs are derived.
type FragmentOptions struct {
	// AssumeContiguous reconstructs runs for entries with no usable FAT chain,
	// typically deleted files, by assuming that ceil(Size/BytesPerCluster)
	// clusters follow FirstCluster contiguously. It sets FragmentResult.Assumed
	// and is off by default so that callers never receive fabricated offsets
	// they did not ask for.
	AssumeContiguous bool `json:"assume_contiguous"`
	// MaxRuns caps the number of runs returned. Zero means unlimited. Use it to
	// bound work on hostile images with pathologically fragmented chains.
	MaxRuns int `json:"max_runs"`
	// MaxClusters caps the number of clusters walked. Zero defaults to the
	// volume's total cluster count, which is already a hard upper bound.
	MaxClusters uint32 `json:"max_clusters"`
}

// FragmentOffsets returns the absolute byte ranges occupied by entry.
//
// The FAT chain is walked and consecutive clusters are coalesced, so a
// contiguous file yields exactly one Range and len(result) > 1 means the file
// is fragmented. The final run is trimmed so that the ranges sum to the entry's
// recorded size; cluster slack past the size is available from SlackRange.
//
// Directories are walked to end-of-chain and are not trimmed, since their
// directory-entry size field is zero.
//
// When the chain accounts for fewer bytes than the entry's size the recovered
// ranges are returned together with an error wrapping ErrTruncatedChain. This
// is the expected outcome for deleted entries; use FragmentOffsetsWithOptions
// with AssumeContiguous to reconstruct them instead.
func (v *Volume) FragmentOffsets(entry DirEntry) ([]Range, error) {
	result, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{})
	if err != nil {
		return nil, err
	}
	if result.Truncated {
		return result.Ranges, fmt.Errorf("%w: %d of %d bytes for %s",
			ErrTruncatedChain, result.BytesCovered, entry.Size, entry.Path)
	}
	return result.Ranges, nil
}

// FragmentOffsetsWithOptions returns entry's runs along with provenance flags
// describing how they were derived. It returns an error only when no ranges can
// be produced at all; every partial or degraded outcome is reported through
// FragmentResult's flags with the usable ranges intact.
func (v *Volume) FragmentOffsetsWithOptions(entry DirEntry, opts FragmentOptions) (*FragmentResult, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}

	size := int64(0)
	if entry.Size > uint64(math.MaxInt64) {
		return nil, fmt.Errorf("%w: entry size %d out of range", ErrCorruptStructure, entry.Size)
	}
	if !entry.IsDirectory {
		size = int64(entry.Size)
	}

	if entry.FirstCluster < defaultRootCluster {
		if entry.Size == 0 {
			return &FragmentResult{Ranges: nil}, nil
		}
		return nil, fmt.Errorf("%w: %s has size %d but no first cluster",
			ErrNoDataClusters, entry.Path, entry.Size)
	}
	if entry.FirstCluster > v.maxClusterNumber() {
		return nil, fmt.Errorf("%w: first cluster %d of %s outside data area",
			ErrCorruptStructure, entry.FirstCluster, entry.Path)
	}

	// A deleted entry's FAT chain has been freed. Worse, if the first cluster
	// has since been reallocated, walking it would follow the *new* owner's
	// chain and return ranges belonging to an unrelated file. Never walk.
	if entry.Deleted {
		return v.deletedFragments(entry, size, opts)
	}

	result, err := v.walkChainRuns(entry.FirstCluster, opts)
	if err != nil {
		return nil, err
	}
	v.finalizeRuns(result, size)

	if result.Truncated && opts.AssumeContiguous {
		assumed, aerr := v.contiguousRuns(entry.FirstCluster, size, opts)
		if aerr == nil {
			assumed.ChainBroken = result.ChainBroken
			assumed.LoopDetected = result.LoopDetected
			return assumed, nil
		}
	}
	return result, nil
}

func (v *Volume) deletedFragments(entry DirEntry, size int64, opts FragmentOptions) (*FragmentResult, error) {
	if opts.AssumeContiguous {
		result, err := v.contiguousRuns(entry.FirstCluster, size, opts)
		if err != nil {
			return nil, err
		}
		result.FirstClusterReallocated = entry.ClusterAllocated
		return result, nil
	}

	// Without an assumption, only the first cluster is defensible: it is the
	// one location the directory entry itself records.
	offset, err := v.clusterToOffset(entry.FirstCluster)
	if err != nil {
		return nil, err
	}
	length := int64(v.bytesPerCluster)
	if size > 0 && size < length {
		length = size
	}
	result := &FragmentResult{
		Ranges: []Range{{
			StartByte:    offset,
			Length:       length,
			StartCluster: entry.FirstCluster,
			ClusterCount: 1,
		}},
		BytesCovered:            length,
		ClustersWalked:          1,
		Truncated:               size > length,
		FirstClusterReallocated: entry.ClusterAllocated,
	}
	return result, nil
}

// contiguousRuns synthesizes a single run covering size bytes starting at
// startCluster, on the assumption that the file was laid out contiguously.
func (v *Volume) contiguousRuns(startCluster uint32, size int64, opts FragmentOptions) (*FragmentResult, error) {
	offset, err := v.clusterToOffset(startCluster)
	if err != nil {
		return nil, err
	}
	if size <= 0 {
		return &FragmentResult{
			Ranges:         []Range{{StartByte: offset, Length: int64(v.bytesPerCluster), StartCluster: startCluster, ClusterCount: 1}},
			BytesCovered:   int64(v.bytesPerCluster),
			ClustersWalked: 1,
			Assumed:        true,
		}, nil
	}

	perCluster := int64(v.bytesPerCluster)
	needed := (size + perCluster - 1) / perCluster
	available := int64(v.maxClusterNumber()) - int64(startCluster) + 1
	truncated := false
	if needed > available {
		needed = available
		truncated = true
	}
	if opts.MaxClusters > 0 && needed > int64(opts.MaxClusters) {
		needed = int64(opts.MaxClusters)
		truncated = true
	}

	covered := needed * perCluster
	if covered > size {
		covered = size
	}
	return &FragmentResult{
		Ranges: []Range{{
			StartByte:    offset,
			Length:       covered,
			StartCluster: startCluster,
			ClusterCount: uint32(needed),
		}},
		BytesCovered:   covered,
		ClustersWalked: uint32(needed),
		Assumed:        true,
		Truncated:      truncated || covered < size,
	}, nil
}

// ClusterChainFragments returns the coalesced byte runs of the chain starting at
// startCluster, trimmed to size bytes when size is positive. A size of zero
// walks the chain to its end without trimming, which is what directories need.
func (v *Volume) ClusterChainFragments(startCluster uint32, size uint64) ([]Range, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if size > uint64(math.MaxInt64) {
		return nil, fmt.Errorf("%w: size %d out of range", ErrCorruptStructure, size)
	}
	result, err := v.walkChainRuns(startCluster, FragmentOptions{})
	if err != nil {
		return nil, err
	}
	v.finalizeRuns(result, int64(size))
	if result.Truncated {
		return result.Ranges, fmt.Errorf("%w: %d of %d bytes from cluster %d",
			ErrTruncatedChain, result.BytesCovered, size, startCluster)
	}
	return result.Ranges, nil
}

// RootDirectoryFragments returns the byte ranges of the root directory.
//
// On FAT32 the root directory is an ordinary cluster chain. On FAT12 and FAT16
// it is a fixed-size region between the FATs and the data area that is not
// cluster-addressed; there it is always a single Range whose StartCluster and
// ClusterCount are zero.
func (v *Volume) RootDirectoryFragments() ([]Range, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if v.fatType == FATType32 {
		return v.ClusterChainFragments(v.rootCluster, 0)
	}

	offset := int64(v.rootDirFirstSector) * int64(v.bytesPerSector)
	length := int64(v.rootDirSectors) * int64(v.bytesPerSector)
	if length == 0 {
		return nil, nil
	}
	if uint64(offset)+uint64(length) > v.volumeSize {
		return nil, fmt.Errorf("%w: root directory region extends beyond volume", ErrCorruptStructure)
	}
	return []Range{{StartByte: offset, Length: length}}, nil
}

// ClusterChain returns every cluster in the chain starting at startCluster, in
// order. Prefer ClusterChainFragments for locating data: it coalesces runs and
// returns byte offsets directly. This is exposed for callers that need the raw
// cluster numbers, for example to cross-reference allocation bitmaps.
func (v *Volume) ClusterChain(startCluster uint32) ([]uint32, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}
	return v.clusterChain(startCluster)
}

// walkChainRuns walks the FAT from startCluster, emitting coalesced runs. It
// never discards work: when the chain is broken, loops, or hits a cap, the runs
// gathered so far are returned with the corresponding flag set.
func (v *Volume) walkChainRuns(startCluster uint32, opts FragmentOptions) (*FragmentResult, error) {
	if startCluster < defaultRootCluster || startCluster > v.maxClusterNumber() {
		return nil, fmt.Errorf("%w: invalid start cluster %d", ErrCorruptStructure, startCluster)
	}

	maxClusters := opts.MaxClusters
	if maxClusters == 0 || maxClusters > v.clusterCount {
		maxClusters = v.clusterCount
	}

	result := &FragmentResult{ChainWalked: true}

	// A valid chain cannot exceed the volume's cluster count, so the walked
	// counter alone guarantees termination. The seen set only sharpens the
	// diagnosis to LoopDetected, so it is capped to keep memory bounded on
	// multi-gigabyte files rather than growing one entry per cluster.
	seen := make(map[uint32]struct{})
	const maxSeenTracked = 1 << 20

	runStart := startCluster
	runCount := uint32(0)
	current := startCluster

	flush := func() error {
		if runCount == 0 {
			return nil
		}
		offset, err := v.clusterToOffset(runStart)
		if err != nil {
			return err
		}
		result.Ranges = append(result.Ranges, Range{
			StartByte:    offset,
			Length:       int64(runCount) * int64(v.bytesPerCluster),
			StartCluster: runStart,
			ClusterCount: runCount,
		})
		runCount = 0
		return nil
	}

	for {
		if _, ok := seen[current]; ok {
			result.LoopDetected = true
			result.ChainBroken = true
			break
		}
		if len(seen) < maxSeenTracked {
			seen[current] = struct{}{}
		}

		if runCount > 0 && current != runStart+runCount {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if runCount == 0 {
			runStart = current
			if opts.MaxRuns > 0 && len(result.Ranges) >= opts.MaxRuns {
				result.ChainBroken = true
				break
			}
		}
		runCount++
		result.ClustersWalked++

		next, err := v.readFATEntry(current)
		if err != nil {
			result.ChainBroken = true
			break
		}
		if next == 0 {
			// A free entry mid-chain: the chain does not terminate properly.
			result.ChainBroken = true
			break
		}
		if v.isBadCluster(next) {
			result.ChainBroken = true
			break
		}
		if v.isEndOfChain(next) {
			break
		}
		if next < defaultRootCluster || next > v.maxClusterNumber() {
			result.ChainBroken = true
			break
		}
		// The cap is checked only once a genuine continuation is known, so a
		// chain that ends exactly at the cap is not misreported as broken.
		if result.ClustersWalked >= maxClusters {
			result.ChainBroken = true
			break
		}
		current = next
	}

	if err := flush(); err != nil {
		return nil, err
	}
	for _, r := range result.Ranges {
		result.BytesCovered += r.Length
	}
	return result, nil
}

// finalizeRuns trims the run list to size bytes and sets Truncated. A size of
// zero or less leaves the runs whole, which is correct for directories.
func (v *Volume) finalizeRuns(result *FragmentResult, size int64) {
	if size <= 0 {
		return
	}
	if result.BytesCovered < size {
		result.Truncated = true
		return
	}
	remaining := size
	trimmed := result.Ranges[:0]
	for _, r := range result.Ranges {
		if remaining <= 0 {
			break
		}
		if r.Length > remaining {
			r.Length = remaining
			r.ClusterCount = uint32((remaining + int64(v.bytesPerCluster) - 1) / int64(v.bytesPerCluster))
		}
		remaining -= r.Length
		trimmed = append(trimmed, r)
	}
	result.Ranges = trimmed
	result.BytesCovered = size
}

// SlackRange returns the unused tail of the entry's final cluster: the bytes
// between the end of the file's data and the end of the cluster that holds it.
// Slack frequently retains fragments of previously deleted files. The boolean
// is false when the file ends exactly on a cluster boundary, leaving no slack.
func (v *Volume) SlackRange(entry DirEntry) (Range, bool, error) {
	if v.IsClosed() {
		return Range{}, false, ErrVolumeClosed
	}
	if entry.IsDirectory || entry.Size == 0 {
		return Range{}, false, nil
	}

	result, err := v.FragmentOffsetsWithOptions(entry, FragmentOptions{})
	if err != nil {
		return Range{}, false, err
	}
	if len(result.Ranges) == 0 || result.Truncated {
		return Range{}, false, nil
	}

	last := result.Ranges[len(result.Ranges)-1]
	perCluster := int64(v.bytesPerCluster)
	used := last.Length % perCluster
	if used == 0 {
		return Range{}, false, nil
	}
	slackStart := last.EndByte()
	slackLen := perCluster - used
	if uint64(slackStart)+uint64(slackLen) > v.volumeSize {
		return Range{}, false, nil
	}
	return Range{
		StartByte:    slackStart,
		Length:       slackLen,
		StartCluster: last.StartCluster + last.ClusterCount - 1,
		ClusterCount: 1,
	}, true, nil
}

// rangeOffsetMapper builds a translator from an offset within the concatenated
// data of the given ranges to an absolute image offset. It returns nil when
// there are no ranges; the resulting function returns -1 for offsets that fall
// outside the mapped extent.
func rangeOffsetMapper(ranges []Range) func(int64) int64 {
	if len(ranges) == 0 {
		return nil
	}
	starts := make([]int64, len(ranges))
	var total int64
	for i, r := range ranges {
		starts[i] = total
		total += r.Length
	}
	return func(logical int64) int64 {
		if logical < 0 || logical >= total {
			return -1
		}
		i := sort.Search(len(starts), func(i int) bool { return starts[i] > logical }) - 1
		if i < 0 {
			return -1
		}
		return ranges[i].StartByte + (logical - starts[i])
	}
}

// TotalLength sums the lengths of the given ranges.
func TotalLength(ranges []Range) int64 {
	var total int64
	for _, r := range ranges {
		total += r.Length
	}
	return total
}

// IsFragmented reports whether the ranges describe a discontiguous layout.
// Ranges produced by this package are already coalesced, so this is true
// whenever more than one run is present.
func IsFragmented(ranges []Range) bool {
	return len(Coalesce(ranges)) > 1
}

// Coalesce merges runs that are adjacent in the image into single runs. Ranges
// returned by this package are coalesced already; the function is exported for
// callers that assemble range lists from other sources.
func Coalesce(ranges []Range) []Range {
	if len(ranges) < 2 {
		return ranges
	}
	merged := make([]Range, 0, len(ranges))
	current := ranges[0]
	for _, next := range ranges[1:] {
		if current.EndByte() == next.StartByte && current.Sparse == next.Sparse {
			current.Length += next.Length
			current.ClusterCount += next.ClusterCount
			continue
		}
		merged = append(merged, current)
		current = next
	}
	return append(merged, current)
}
