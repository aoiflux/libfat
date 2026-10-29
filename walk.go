package libfat

import "context"

// maxWalkDepth bounds how deep a walk descends.
//
// The cycle guard already stops a directory being entered twice, so this is the
// backstop for a crafted tree that is deep without repeating, which would
// otherwise grow the stack without limit. The cap is far past anything a real
// filesystem produces.
const maxWalkDepth = 4096

// SkipReason names why a walk reported a directory but did not descend into
// it. It is passed to WalkOptions.OnSkip, which is the only way to observe a
// skip: the alternative signal, a directory reported without children, is what
// an empty directory looks like too.
type SkipReason string

const (
	// SkipReadError: the directory's contents could not be read. The
	// underlying error is passed to OnSkip alongside this reason. Under
	// OpenOptions.Strict it is a *StrictError naming the record at fault, so a
	// strict walk can learn which directory failed which check without
	// aborting.
	SkipReadError SkipReason = "read-error"
	// SkipDepthLimit: the directory sits on WalkOptions.MaxDepth, so it is
	// reported but not read.
	SkipDepthLimit SkipReason = "depth-limit"
	// SkipRevisit: the directory's first cluster had already been descended
	// into. A valid FAT volume cannot produce this; it is a cycle or a
	// cross-link, and the record naming it is still reported as evidence.
	SkipRevisit SkipReason = "revisit"
	// SkipInvalidCluster: the first cluster is not a data-area cluster, so
	// there is nothing to read and no read to fail.
	SkipInvalidCluster SkipReason = "invalid-cluster"
	// SkipDeletedUnrecoverable: DescendDeletedDirectories was set, but the
	// deleted directory's first cluster is no longer free in the FAT or no
	// longer begins with "." and ".." pointing at itself. See
	// DescendDeletedDirectories for why both conditions are required before a
	// descent happens at all.
	SkipDeletedUnrecoverable SkipReason = "deleted-unrecoverable"
)

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
	// it. Set OnSkip to learn which directories were truncated this way: the
	// only other signal is the absence of children under a directory the
	// callback was handed, which is indistinguishable from an empty one.
	MaxDepth int

	// StopOnReadError makes a directory that cannot be read abort the walk and
	// return the underlying error. By default such a directory is reported - its
	// own record is real evidence whether or not what it points at survives -
	// its contents are skipped, and the walk continues, which is the same choice
	// FragmentResult makes when it returns the runs it managed to walk. OnSkip
	// reports those directories without aborting anything.
	StopOnReadError bool

	// OnSkip is called for every directory the walk reported but did not
	// descend into, with the reason and, for SkipReadError, the error the read
	// failed with. It is how a caller tells an empty directory from a truncated
	// one, which is otherwise impossible: both are reported without children.
	//
	// It is only ever called for an entry already handed to the walk callback,
	// and at most once for each. The conditions are tested in the order
	// SkipDepthLimit, SkipInvalidCluster, SkipRevisit, SkipReadError,
	// SkipDeletedUnrecoverable, and the first that holds is the one reported.
	// A directory named twice whose first read failed is reported SkipReadError
	// and then SkipRevisit, because the cycle guard is marked before the
	// attempt.
	//
	// Nothing is reported for a file, for a virtual entry - which names no
	// directory data - for a deleted record IncludeDeleted left out, or for a
	// deleted directory not descended because DescendDeletedDirectories is off:
	// that last is the caller's own choice rather than something the walk
	// decided. With StopOnReadError set, an unreadable directory aborts the walk
	// and OnSkip is not called, because nothing was skipped.
	//
	// An error returned by OnSkip stops the walk immediately and is returned
	// unchanged, exactly as the walk callback's own error is.
	OnSkip func(path string, e DirEntry, reason SkipReason, err error) error

	// OnOrphanScan is called once with the result of the scan IncludeOrphans
	// performs, before any orphan entry is reported, and only when the scan
	// returned a result. It exists because the walk reports the scan's entries
	// but otherwise discards its provenance: OrphanScanResult.Truncated says a
	// bound in OrphanScan stopped the sweep before the data area was exhausted,
	// and ReachableWalkFailed says part of the live tree could not be walked, so
	// some reported orphans may in fact be reachable.
	//
	// An error returned from it stops the walk with that error, and no orphan
	// entry is reported. The walk does not use the result afterwards, so the
	// callback may keep it.
	OnOrphanScan func(result *OrphanScanResult) error
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
// Set WalkOptions.OnSkip to learn about every directory the walk declined to
// descend into - unreadable, depth-capped, already walked, naming a cluster
// outside the data area, or a deleted directory whose surviving data the
// stale-copy guard refused - with the reason and the read error if there was
// one. Without it a skipped directory is not otherwise observable: this package
// has no warning sink, so a walk that stepped over an unreadable directory
// returns the same nil a clean one does, and the only remaining way to find out
// why a directory reported no children is to call OpenEntry on it and then
// ReadDir, which returns the underlying error.
//
// With IncludeOrphans, WalkOptions.OnOrphanScan likewise reports what the
// orphan scan itself could not cover.
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
			if serr := w.skip(e, SkipDepthLimit, nil); serr != nil {
				return serr
			}
			continue
		}
		if e.FirstCluster < DefaultRootCluster || e.FirstCluster > w.v.maxClusterNumber() {
			if serr := w.skip(e, SkipInvalidCluster, nil); serr != nil {
				return serr
			}
			continue
		}
		if _, done := w.seen[e.FirstCluster]; done {
			if serr := w.skip(e, SkipRevisit, nil); serr != nil {
				return serr
			}
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
			if serr := w.skip(e, SkipReadError, err); serr != nil {
				return serr
			}
			continue
		}
		if !ok {
			// children reports false only for a deleted directory. With the
			// option off the caller chose not to descend, which is not the
			// walk's finding to report; with it on, the stale-copy guard
			// refused, which is.
			if w.opts.DescendDeletedDirectories {
				if serr := w.skip(e, SkipDeletedUnrecoverable, nil); serr != nil {
					return serr
				}
			}
			continue
		}
		if err := w.report(children, e.FirstCluster, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// skip reports a directory the walk did not descend into, if the caller asked
// to hear about them. The nil check lives here so that the five call sites in
// report read the same way.
func (w *fatWalk) skip(e DirEntry, reason SkipReason, err error) error {
	if w.opts.OnSkip == nil {
		return nil
	}
	return w.opts.OnSkip(e.Path, e, reason, err)
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
	// The scan's own account of what it could not cover, which the entries
	// alone do not carry. Before the entries, so that a caller refusing a
	// truncated or blind scan refuses it whole.
	if w.opts.OnOrphanScan != nil {
		if herr := w.opts.OnOrphanScan(result); herr != nil {
			return herr
		}
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
