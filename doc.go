// Package libfat provides read-only, panic-free parsing of FAT12, FAT16, and
// FAT32 volumes and disk images for forensic use.
//
// A volume is opened over an io.ReaderAt, so a partition inside a whole-disk
// image is handled by layering an io.SectionReader at the partition offset:
//
//	section := io.NewSectionReader(image, partitionOffset, partitionLength)
//	v, err := libfat.Open(section)
//
// All byte offsets the package returns are relative to that reader. To obtain
// whole-disk offsets, add the partition base.
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
// # Absolute directory entry offsets
//
// DirEntry.EntryOffset is the offset of the 32-byte entry within its parent
// directory's data, not within the image; for a fragmented directory it maps to
// no single image location. DirEntry.EntryAbsoluteOffset is the image offset,
// resolved through the parent directory's own fragment list, and is the field
// to use when addressing an entry on disk.
//
// # Timestamps
//
// FAT records creation, modification, and last-access times. It has no change
// time, so DirEntry has no ctime field. Access times carry date precision only,
// modification times two-second precision, and creation times ten-millisecond
// precision. All are decoded as UTC because FAT stores no time zone; the values
// were written in the recording system's local time and may need adjusting.
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
