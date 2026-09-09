package libfat

import "context"

// maxWalkDepth bounds how deep a walk descends.
//
// The cycle guard already stops a directory being entered twice, so this is the
// backstop for a crafted tree that is deep without repeating, which would
// otherwise grow the stack without limit. The cap is far past anything a real
// filesystem produces.
const maxWalkDepth = 4096

// WalkOptions tunes a walk.
//
// The zero value is the precision-first configuration, matching
// OrphanScanOptions: the live, reachable tree only. Deleted records and orphan
// runs are evidence a caller must ask for, because including them changes what
// a change-detection pass sees and a caller who did not ask should not silently
// receive records for files that are not there.
//
// Note that this is deliberately not ReadDir's behaviour. ReadDir has always
// returned deleted entries unconditionally, since a directory listing that hid
// them would hide the thing this package exists to show. A walk is a different
// operation with a different default, and the difference is called out here
// because it is the one place the two disagree.
type WalkOptions struct {
	// IncludeDeleted reports entries whose first byte is the 0xE5 deletion
	// marker. They are reported at the slot they physically occupy, so a
	// deleted entry appears among its live siblings in disk order rather than
	// appended at the end.
	IncludeDeleted bool

	// DescendDeletedDirectories walks into a deleted directory and reports the
	// children whose records survive in its clusters. It has no effect unless
	// IncludeDeleted is set, since the directory itself would not be reported.
	//
	// A deleted directory's FAT chain has been freed, so there is no chain to
	// follow and this reads exactly one cluster: the first, which is the only
	// location the surviving parent entry records. It never assumes contiguity
	// and never walks the FAT, for the same reason FragmentOffsets does not - if
	// the cluster has been reallocated, the FAT now describes its new owner.
	//
	// Two further conditions must hold before the descent happens at all. The
	// first cluster must still be free in the FAT, which DirEntry.ClusterAllocated
	// reports, and it must still begin with the "." and ".." records with "."
	// pointing back at the cluster it was found in. That last check is the orphan
	// scanner's stale-copy guard, and it is what separates surviving directory
	// data from a later file that happens to start there.
	DescendDeletedDirectories bool

	// IncludeOrphans appends the entries ScanOrphans recovers, after the whole
	// reachable tree has been reported. Their paths are rooted at OrphanPath and
	// their parent cluster is the first cluster of the orphan run they were
	// found in.
	//
	// This costs considerably more than a plain walk: the scan performs its own
	// reachability pre-pass over the live tree and then sweeps the data area.
	// The two traversals are kept separate rather than shared because they
	// compute different sets - this walk tracks directory first clusters, while
	// the scan needs every cluster of every live directory chain.
	//
	// With DescendDeletedDirectories, one record can be reported twice: once
	// under the path its deleted parent still names, and once under OrphanPath
	// because the sweep found the same cluster unreferenced. Those are two
	// genuinely different findings - one recovers the record's path, the other
	// establishes that nothing reaches its cluster - so neither is suppressed.
	// They carry the same FileID, which is how a caller that wants one row per
	// record collapses them.
	IncludeOrphans bool

	// OrphanScan is passed through to the scan when IncludeOrphans is set. Its
	// zero value is the precision-first configuration described on
	// OrphanScanOptions.
	OrphanScan OrphanScanOptions

	// MaxDepth is the number of directory levels reported, counting the root's
	// children as the first. A MaxDepth of 1 reports the root's children and
	// descends no further; 2 adds their children, and so on. Zero selects
	// maxWalkDepth, and larger values are clamped to it: the constant is a
	// ceiling on stack growth, not a default to be raised.
	//
	// A directory sitting on the limit is still reported; only its contents are
	// not read. Exceeding the limit is not an error, because a crafted image
	// that nests a million directories should truncate a walk rather than fail
	// it, and because the truncation is visible in the absence of children under
	// a directory the callback was handed.
	MaxDepth int

	// StopOnReadError makes a directory that cannot be read abort the walk and
	// return the underlying error. By default such a directory is reported - its
	// own record is real evidence whether or not what it points at survives -
	// its contents are skipped, and the walk continues, which is the same choice
	// FragmentResult makes when it returns the runs it managed to walk.
	StopOnReadError bool
}

// Walk calls fn for every entry in the volume's directory tree, depth first and
// in disk order.
//
// It is WalkWithOptions with the zero WalkOptions: the live, reachable tree,
// without deleted records or orphan runs.
func (v *Volume) Walk(ctx context.Context,
	fn func(path string, parentFirstCluster uint32, e DirEntry) error,
) error {
	return v.walk(ctx, WalkOptions{}, fn)
}

// WalkWithOptions calls fn for every entry the options select.
//
// # Order
//
// Pre-order and depth first. Within a directory, entries are reported in the
// order their 32-byte records appear in the directory's concatenated clusters -
// disk order, not sorted order - because that order is itself evidence: a
// deleted record's position tells you which live records were written around
// it. A subdirectory is reported and then immediately descended into, so its
// whole subtree precedes its next sibling.
//
// # The root
//
// The root directory is not reported. It has no 32-byte record anywhere on the
// volume: no name, no timestamps, no attributes, no parent. Synthesising a
// DirEntry for it would be fabricating a structure that does not exist. The
// walk begins by reading the root and reporting its children.
//
// # The callback's arguments
//
// path always equals e.Path, and parentFirstCluster always equals
// e.ParentFirstCluster. Both are passed separately so that a caller who wants
// only the path, or only the identity, need not reach into the entry, and so
// that the shape matches the walk callbacks in the sibling filesystem
// libraries.
//
// parentFirstCluster is FixedRootCluster (0) for the children of a FAT12 or
// FAT16 root, which is a fixed region and not cluster-addressed. Cluster
// numbers start at 2, so 0 cannot collide with a real directory. With
// e.EntryOffset it forms the entry's FileID.
//
// # What is reported
//
// Deleted records and orphan runs are opt-in; see WalkOptions. Virtual entries
// ($MBR, $FAT1, $FAT2, $OrphanFiles) are reported when, and only when, the
// volume was opened with OpenOptions.IncludeVirtualRootEntries - the walk does
// not second-guess that choice - and are never descended into, since they name
// no directory data. Volume-label records likewise follow
// OpenOptions.IncludeVolumeLabelEntries. The "." and ".." records are never
// reported; the parser drops them.
//
// A child of a deleted directory carries the path it had, because the chain of
// names leading to it is intact and only the records are marked deleted.
// Pre-order guarantees the deleted parent was reported first, so a caller can
// mark the whole subtree. This is unlike an orphan, whose path really was lost
// with the directory chain that named it and which is reported under
// OrphanPath.
//
// # Errors and limits
//
// An error returned by fn stops the walk immediately and is returned unchanged;
// there are no SkipDir or SkipAll sentinels. A directory that cannot be read is
// skipped and the walk continues unless StopOnReadError is set; a root that
// cannot be read is always an error, since there is then nothing to walk. A
// directory reachable more than once - which a valid FAT volume cannot produce
// - is reported each time it is named but descended into only once, keyed on
// its first cluster. Depth is capped; see WalkOptions.MaxDepth.
//
// A skipped directory is not otherwise observable: this package has no warning
// sink, so a walk that stepped over an unreadable directory returns the same
// nil a clean one does. To find out why a directory reported no children, call
// OpenEntry on it and then ReadDir, which returns the underlying error.
//
// Cancelling ctx stops the walk and returns ctx.Err() unchanged. Entries
// already handed to fn are the partial result. A nil ctx or a nil fn is an
// error.
func (v *Volume) WalkWithOptions(ctx context.Context, opts WalkOptions,
	fn func(path string, parentFirstCluster uint32, e DirEntry) error,
) error {
	return v.walk(ctx, opts, fn)
}

// fatWalk is the state of one traversal. Both exported shapes go through it, so
// the cycle guard, the depth cap, the pacing of cancellation checks and the
// order entries are reported in cannot drift apart between them.
type fatWalk struct {
	v    *Volume
	opts WalkOptions
	fn   func(string, uint32, DirEntry) error
	prog *scanProgress
	// seen holds the first cluster of every directory already descended into.
	seen     map[uint32]struct{}
	maxDepth int
}

func (v *Volume) walk(ctx context.Context, opts WalkOptions,
	fn func(path string, parentFirstCluster uint32, e DirEntry) error,
) error {
	if v.IsClosed() {
		return ErrVolumeClosed
	}
	if ctx == nil {
		return ErrNilContext
	}
	if fn == nil {
		return ErrNilCallback
	}

	depth := opts.MaxDepth
	if depth <= 0 || depth > maxWalkDepth {
		depth = maxWalkDepth
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		return err
	}

	w := &fatWalk{
		v:        v,
		opts:     opts,
		fn:       fn,
		prog:     &scanProgress{ctx: ctx},
		seen:     make(map[uint32]struct{}),
		maxDepth: depth,
	}
	// Only FAT32 has a root with a cluster number of its own. On FAT12 and
	// FAT16 rootCluster holds the placeholder 2, and seeding that would make a
	// real directory at cluster 2 look like a cycle.
	if v.fatType == FATType32 {
		w.seen[v.rootCluster] = struct{}{}
	}

	entries, err := root.ReadDir()
	if err != nil {
		// A root that cannot be read leaves nothing to walk, so unlike any other
		// directory this is always fatal.
		return err
	}
	if err := w.report(entries, root.identityCluster(), 0); err != nil {
		return err
	}

	if opts.IncludeOrphans {
		return w.orphans(ctx)
	}
	return nil
}

// report hands one directory's entries to the callback and descends into the
// subdirectories among them.
func (w *fatWalk) report(entries []DirEntry, parent uint32, depth int) error {
	for _, e := range entries {
		if err := w.prog.check(); err != nil {
			return err
		}
		if e.Deleted && !w.opts.IncludeDeleted {
			continue
		}

		if err := w.fn(e.Path, parent, e); err != nil {
			return err
		}

		if !e.IsDirectory || e.Virtual {
			continue
		}
		if depth+1 >= w.maxDepth {
			continue
		}
		if e.FirstCluster < defaultRootCluster || e.FirstCluster > w.v.maxClusterNumber() {
			continue
		}
		if _, done := w.seen[e.FirstCluster]; done {
			continue
		}

		// Marked before the attempt, not after it: a corrupt image can name the
		// same unreadable cluster many times, and one failed read of it is
		// enough.
		w.seen[e.FirstCluster] = struct{}{}

		children, ok, err := w.children(e)
		if err != nil {
			if w.opts.StopOnReadError {
				return err
			}
			continue
		}
		if !ok {
			continue
		}
		if err := w.report(children, e.FirstCluster, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// children reads a directory entry's contents. The boolean is false when the
// entry is a deleted directory that the options or the surviving evidence do
// not permit descending into, which is not an error.
func (w *fatWalk) children(e DirEntry) ([]DirEntry, bool, error) {
	if !e.Deleted {
		entries, err := w.v.openDirEntry(e).ReadDir()
		if err != nil {
			return nil, false, err
		}
		return entries, true, nil
	}
	if !w.opts.DescendDeletedDirectories {
		return nil, false, nil
	}
	return w.v.deletedDirectoryChildren(e)
}

// orphans reports the entries recovered from directory data that no path
// reaches, after the whole reachable tree.
func (w *fatWalk) orphans(ctx context.Context) error {
	result, err := w.v.ScanOrphansContext(ctx, w.opts.OrphanScan)
	if result == nil {
		return err
	}
	for _, dir := range result.Directories {
		for _, e := range dir.Entries {
			if perr := w.prog.check(); perr != nil {
				return perr
			}
			// IncludeDeleted governs the reachable tree only. An orphan run's
			// records are recovery material in their entirety, and filtering
			// them by the deletion marker would drop exactly the entries the
			// scan exists to find.
			if ferr := w.fn(e.Path, dir.FirstCluster, e); ferr != nil {
				return ferr
			}
		}
	}
	// A cancelled or bounded scan still yields what it found.
	return err
}
