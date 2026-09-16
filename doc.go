// Package libfat provides read-only, panic-free parsing of FAT12, FAT16, and
// FAT32 volumes and disk images for forensic use.
//
// A volume is opened over an io.ReaderAt. All byte offsets the package returns
// are absolute within that reader, and already include OpenOptions.BaseOffset.
//
// # Partitions and coordinate spaces
//
// A partition inside a whole-disk image can be opened either way round, and the
// choice decides what coordinate space every reported offset is in.
//
// To work in whole-disk coordinates - which is what comparing against byte
// ranges obtained from somewhere else requires - open the whole image and say
// where the volume starts:
//
//	v, err := libfat.OpenWithOptions(image, libfat.OpenOptions{
//		BaseOffset: partitionOffset,
//	})
//
// Every offset then addresses the whole disk, so no adjustment is needed and
// none must be applied.
//
// To work in partition-relative coordinates, scope the reader instead and leave
// BaseOffset at zero:
//
//	section := io.NewSectionReader(image, partitionOffset, partitionLength)
//	v, err := libfat.Open(section)
//
// Both are correct; mixing them is not. Adding partitionOffset to an offset
// that already includes BaseOffset counts it twice, and comparing a
// partition-relative offset against a whole-disk range produces a confident
// wrong answer rather than an error, which is why BaseOffset exists.
//
// # Locating file data
//
// The central operation for forensic work is mapping a file to the byte ranges
// it occupies in the image, without extracting it and without re-walking the
// FAT. FragmentOffsets returns those ranges directly:
//
//	root, _ := v.GetRootDirectory()
//	entries, _ := root.ReadDir()
//	for _, entry := range entries {
//		ranges, err := v.FragmentOffsets(entry)
//		if err != nil && !errors.Is(err, libfat.ErrTruncatedChain) {
//			continue
//		}
//		for _, r := range ranges {
//			fmt.Printf("%s: %d bytes at image offset %d\n", entry.Path, r.Length, r.StartByte)
//		}
//	}
//
// Consecutive clusters are coalesced, so a contiguous file yields exactly one
// Range and len(ranges) > 1 means the file is fragmented. The ranges sum to the
// entry's recorded size; the unused tail of the last cluster is reported
// separately by SlackRange.
//
// Each Range also carries FileOffset, where that run begins in the file's own
// byte space, so a byte range known in image coordinates maps back to a
// position within the file without the caller accumulating lengths:
//
//	for _, r := range ranges {
//		if changedStart < r.StartByte+r.Length && changedEnd > r.StartByte {
//			at := r.FileOffset + (changedStart - r.StartByte) // byte N of the file
//			_ = at
//		}
//	}
//
// The runs are in file order and gap-free, so FileOffset on run i is the sum of
// the lengths before it. FAT has no sparse allocation, so no run is a hole.
//
// # Deleted files
//
// Deleting a file frees its FAT entries, so the chain that described its layout
// no longer exists. Only the first cluster, recorded in the directory entry
// itself, survives. Worse, if that cluster has since been reallocated, walking
// the FAT from it would follow the new owner's chain and attribute another
// file's data to the deleted entry. This package therefore never walks the FAT
// for a deleted entry. By default it reports only the first cluster, with
// FragmentResult.Truncated set:
//
//	result, _ := v.FragmentOffsetsWithOptions(entry, libfat.FragmentOptions{})
//	// result.ChainWalked == false, result.Truncated == true for a deleted entry
//
// Passing AssumeContiguous reconstructs the full extent on the assumption that
// the file was laid out contiguously, which is common but not guaranteed:
//
//	result, _ := v.FragmentOffsetsWithOptions(entry, libfat.FragmentOptions{
//		AssumeContiguous: true,
//	})
//	// result.Assumed == true: these ranges are a hypothesis, not a fact.
//
// Check FragmentResult.FirstClusterReallocated before trusting recovered data:
// when it is set, the content has most likely been overwritten.
//
// OpenPath refuses deleted entries, because no live path leads to one. Use
// OpenEntry to read their content:
//
//	f, _ := v.OpenEntry(entry)
//	f.SetFragmentOptions(libfat.FragmentOptions{AssumeContiguous: true})
//	data, err := f.ReadAll()
//
// # Orphaned files
//
// Deleting a directory marks its entry in the parent and frees its FAT chain,
// but leaves the directory's own clusters untouched. The entries describing its
// children survive there, reachable from no path. ScanOrphans finds them by
// sweeping the data area for clusters that hold directory data:
//
//	result, _ := v.ScanOrphans(libfat.OrphanScanOptions{})
//	for _, dir := range result.Directories {
//		for _, entry := range dir.Entries {
//			fmt.Println(entry.Path, entry.Size) // rooted at libfat.OrphanPath
//		}
//	}
//
// The zero-value options are precision-first: only clusters the FAT marks free
// are examined, and a cluster is accepted only when it carries the "." and ".."
// records that begin a directory, with "." pointing back at the cluster it was
// found in. AllowMissingDotEntries relaxes that to any cluster whose records
// all parse as directory entries, which recovers fragments whose start was
// overwritten at the cost of false positives.
//
// Original paths are not recoverable: the directory chain that named these
// entries is exactly what was lost. Entries are reported under OrphanPath with
// DirEntry.Orphaned set, and OrphanDirectory.ParentCluster carries the ".."
// record's cluster for callers that want to rebuild the hierarchy themselves.
//
// A scan reads every candidate cluster, so it costs a pass over the data area
// and is far more expensive than ordinary traversal. Bound it with MaxClusters
// and MaxDirectories on large or untrusted images.
//
// # Walking the tree
//
// Walk visits every entry in the volume, depth first, so that a caller need not
// hand-roll recursion over ReadDir:
//
//	ctx := context.Background()
//	err := v.Walk(ctx, func(path string, parentFirstCluster uint32, e libfat.DirEntry) error {
//		fmt.Println(path, e.Size)
//		return nil
//	})
//
// Entries within a directory are reported in the order their 32-byte records
// appear, not sorted, because that order is itself evidence: a deleted record's
// position tells you which live records were written around it. A subdirectory
// is reported and then descended into immediately, so its whole subtree
// precedes its next sibling.
//
// The root directory is not reported. It has no directory record anywhere on
// the volume - no name, no timestamps, no parent - and synthesising a DirEntry
// for it would fabricate a structure that does not exist.
//
// parentFirstCluster is FixedRootCluster, which is 0, for the children of a
// FAT12 or FAT16 root. That region is a fixed area of the volume rather than a
// cluster chain, and since cluster numbering starts at 2 the value cannot
// collide with a real directory. On FAT32 the root is an ordinary chain and its
// children carry the volume's root cluster.
//
// The zero WalkOptions reports the live, reachable tree. Deleted records and
// orphan runs are opt-in, which is deliberately unlike ReadDir - a directory
// listing that hid deleted entries would hide the point of this package, but a
// walk is a different operation and says so:
//
//	err := v.WalkWithOptions(ctx, libfat.WalkOptions{
//		IncludeDeleted:            true,
//		DescendDeletedDirectories: true,
//		IncludeOrphans:            true,
//	}, fn)
//
// DescendDeletedDirectories reads exactly one cluster of a deleted directory:
// the first, which is the only location its surviving parent record names. The
// FAT chain was freed by the deletion, so there is no chain to follow, and
// following one would report whatever now owns those clusters as this
// directory's contents. Two conditions must also hold - the FAT must still mark
// the cluster free, and the cluster must still begin with the "." and ".."
// records with "." pointing back at itself. See Deleted files above for why
// nothing further is defensible. Entries beyond that first cluster are found by
// IncludeOrphans instead, which applies the same guard while sweeping the data
// area, and costs a great deal more.
//
// With both options set, one record can appear twice: once under the path its
// deleted parent still names, and once under OrphanPath because the sweep found
// the same cluster unreferenced. Those are two different findings and neither
// is suppressed. They carry the same FileID, which is how a caller wanting one
// row per record collapses them.
//
// A callback error stops the walk and is returned unchanged; there are no
// SkipDir or SkipAll sentinels. A directory that cannot be read is reported -
// its own record is evidence whether or not what it points at survives - its
// contents are skipped, and the walk continues, which mirrors what
// FragmentResult does with a chain it could only partly follow. Set
// StopOnReadError to fail instead.
//
// That skipping is not otherwise observable: this package has no warning sink,
// so a walk that stepped over an unreadable directory returns the same nil a
// clean one does. To find out why a directory reported no children, call
// OpenEntry on it and then ReadDir, which returns the underlying error.
//
// # Absolute directory entry offsets
//
// DirEntry.EntryOffset is the offset of the 32-byte entry within its parent
// directory's data, not within the image; for a fragmented directory it maps to
// no single image location. DirEntry.EntryAbsoluteOffset is the image offset,
// resolved through the parent directory's own fragment list, and is the field
// to use when addressing an entry on disk.
//
// # File identity
//
// EntryAbsoluteOffset says where a record is now. FileID says which record it
// is. The two come apart exactly when a directory is relocated: a
// defragmentation pass rewrites every entry's absolute offset while changing no
// file, and the slot index, being logical rather than physical, does not move.
//
//	id, ok := v.FileID(entry)  // {ParentFirstCluster, EntrySlotIndex}
//
// A directory is an array of 32-byte slots. The pair names the directory - by
// its first cluster, or FixedRootCluster for the FAT12/16 root region - and the
// entry's position within that directory's concatenated clusters.
//
// It is an address, not an identity. FAT has no inode, no file record number,
// and above all no reuse or generation counter. NTFS pairs an MFT record with a
// sequence number it increments on every reuse, and ext pairs an inode with a
// generation; either lets a consumer tell "this file changed" from "a different
// file took this slot". FAT records nothing of the kind, so two files that were
// never related can share a FileID. In rough order of how often it bites:
//
//   - Slot reuse. A deleted file's slot is marked 0xE5 and handed to the next
//     creation in that directory. The new file inherits the identity exactly,
//     and nothing in the filesystem distinguishes the case.
//   - Rename. A long name occupies ceil(len/13) slots immediately before the
//     short entry, so renaming to a name of a different length moves the short
//     entry to a different slot. Renaming report.txt to
//     "quarterly report final.txt" changes the FileID although nothing else
//     about the file did. FileID is therefore much better at asserting that two
//     observations are of the same file than at detecting a rename.
//   - Directory compaction, which repacks slots after a freed run.
//   - The parent directory itself being deleted and recreated, or its first
//     cluster reallocated.
//
// Rename detection on FAT is inference, never proof. Corroborate a FileID with
// what FAT does record and a rename does not touch: CreatedAt, which carries
// ten-millisecond precision and is the most discriminating field in the entry,
// together with FirstCluster and Size. A match on all of those is strong
// evidence of sameness. A FileID match alone, in a directory that has seen
// deletions, is not.
//
// # Timestamps
//
// FAT records creation, modification, and last-access times. It has no change
// time, so DirEntry has no ctime field. Access times carry date precision only,
// modification times two-second precision, and creation times ten-millisecond
// precision. All are decoded as UTC because FAT stores no time zone; the values
// were written in the recording system's local time and may need adjusting.
//
// # Cancellation
//
// The operations worth interrupting take a context: ScanOrphansContext, Walk,
// WalkWithOptions, ReportWithOptionsContext and WriteReportWithOptionsContext.
// ScanOrphans and the plain Report and WriteReport forms are one-line delegates
// passing context.Background(); Walk was introduced with a context and has no
// second form.
//
// Three rules hold across all of them. A nil context is an error, ErrNilContext,
// rather than a silent default: a caller who forgot to thread one through has an
// interruptible operation that cannot be interrupted, and silence hides that. A
// cancelled context yields ctx.Err() unchanged, so errors.Is(err,
// context.Canceled) works. And the check is paced - one consultation of the
// context every cancellationCheckInterval units of work, over a counter that
// spans the whole operation rather than resetting per directory or per cluster
// run, tested before that counter advances so an already-cancelled context is
// caught on the first unit rather than the thousandth.
//
// Unlike the sibling filesystem libraries, a cancelled operation here returns
// its partial result alongside the error. That follows this package's own rule
// rather than theirs: FragmentOffsets returns the runs it walked alongside
// ErrTruncatedChain, and a half-finished scan is worth the same. A caller
// wanting all-or-nothing discards the result on any non-nil error. The one
// exception is WriteReportWithOptionsContext, which writes nothing when the
// report could not be completed: a truncated JSON document that looks complete
// is worse than none, and the written form carries no flag to say otherwise.
//
// One caveat bounds how promptly a cancellation lands. The fragment API has no
// Context form, deliberately - it is already bounded by
// FragmentOptions.MaxClusters, and a second bounding mechanism on the same
// function invites the two to disagree. But ScanOrphansContext's reachability
// pre-pass walks a FAT chain per live directory through that unbounded code, so
// the worst-case latency between two checks is one full directory chain walk.
// On a large image with a pathological chain that is not instant. Bound it with
// OrphanScanOptions.MaxClusters and MaxDepth rather than relying on the context
// alone.
//
// # JSON reports
//
// Report renders the volume as a FATReport, in the shape the sibling filesystem
// libraries emit so that reports from several filesystems can be consumed
// together:
//
//	err := v.WriteReportDeep("evidence.img", os.Stdout)
//
// The zero ReportOptions covers the live, reachable tree with FAT-verified
// extents. ReportDeep adds deleted records, the surviving first cluster of
// deleted directories, and the orphan sweep. Deep means more places searched,
// never weaker evidence: it does not set Fragments.AssumeContiguous, because a
// report labelled deep that silently contained hypothesised extents would be
// the worst possible default - the caller who most wants recovery data is the
// one least able to tell a reconstruction from a fact. Ask for that explicitly,
// and read layout.assumed on every row.
//
// Each row carries a layout object holding FragmentResult's provenance flags:
// chain_walked, assumed, truncated, chain_broken, loop_detected and
// first_cluster_reallocated. None of them is omitted when false. A
// "chain_walked": false is the statement that these extents did not come from a
// FAT chain, and dropping the key would leave a consumer unable to tell that
// from a field this version does not emit.
//
// Every report names the shape it is in. schema_version identifies the document
// layout and increments only when a key is removed, renamed, or changes
// meaning, never on an addition, so a consumer that ignores unknown keys can
// pin to a major shape rather than to a library release. library_version
// records the build that wrote it, and generated the time it was written - the
// one key that varies between two reports of the same volume, and so the one to
// exclude when hashing a report for comparison.
//
// Fragment offsets follow Range: EndOffset is exclusive, one past the last
// byte. Timestamps the entry never recorded are absent rather than rendered as
// 0001-01-01, since absent says "not recorded", which is the truth, while a
// rendered zero looks like an answer. The identity scalars are never omitted,
// even when zero, because a report row is a thing that gets diffed and wants a
// stable key set; see File identity above for what they do and do not
// guarantee.
//
// # Volume labels
//
// A FAT volume records its label in two independent places, and they are free
// to disagree. BS_VolLab in the boot sector is written once at format time and
// is never updated afterwards by Windows, which leaves it reading "NO NAME"
// however the volume is subsequently named. The authoritative copy is a record
// in the root directory carrying the volume-ID attribute, which is what every
// operating system and every other tool displays.
//
// VolumeLabel returns the root directory's record, falling back to the boot
// sector only when the volume has no such record. Both readings stay available,
// because on a volume where they differ the difference is itself evidence about
// how the volume was created and handled:
//
//	v.VolumeLabel()           // what the volume is called
//	v.BootSectorVolumeLabel() // what the boot sector still says
//	v.VolumeLabelSource()     // "root directory", "boot sector", or ""
//
// The report carries all three, as volume_label, boot_sector_volume_label and
// volume_label_source.
//
// # Capabilities
//
// Capabilities reports what this volume can and cannot tell a caller, so that
// code consuming several filesystems through the sibling libraries can ask
// rather than special-case on the format's name:
//
//	if !v.Capabilities().StableFileIdentity {
//		// FAT has no inode and no reuse counter; see File identity above.
//	}
//
// Most fields are fixed by the format. Three are read from this volume's own
// boot record and differ between volumes: SecondFAT, FSInfoSector and
// BackupBootSector.
//
// Two are worth reading closely before relying on them. SubSecondTimestamps is
// true, but only creation times carry sub-second precision - modification times
// are accurate to two seconds and access times to a whole day - so equal
// timestamps are not proof that a file did not change. TimezoneOffsets is
// false: see Timestamps above.
//
// # Robustness
//
// Every exported entry point is read-only and panic-free on arbitrary input,
// which the package's fuzz targets exercise. Corrupt structures are reported
// rather than assumed away: a chain that loops, hits a bad cluster, or leaves
// the data area yields the ranges walked so far with the corresponding flag set
// on FragmentResult, never a discarded result or a silent truncation.
//
// Walking a chain is O(clusters) and an adversarial image can present chains as
// long as the volume's cluster count. Callers processing untrusted images can
// bound the work with FragmentOptions.MaxClusters and MaxRuns.
//
// A Volume is safe for concurrent use when the io.ReaderAt it was opened over
// is, which io.ReaderAt implementations are required to be. A File is not: it
// caches its resolved ranges and a read cursor. Open the path again for a
// second goroutine, or use File.ReaderAt, which holds no cursor.
package libfat
