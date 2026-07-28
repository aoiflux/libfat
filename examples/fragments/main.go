// Command fragments prints the absolute image byte ranges occupied by every
// file in a FAT volume, including deleted entries.
//
// This is the offset contract a carving or indexing layer needs: for each file,
// where its bytes physically live in the image, without extracting them.
//
// Usage:
//
//	fragments <image> [partition-offset]
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/aoiflux/libfat"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: fragments <image> [partition-offset]")
		os.Exit(2)
	}

	if err := run(os.Args[1], partitionOffset()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func partitionOffset() int64 {
	if len(os.Args) < 3 {
		return 0
	}
	offset, err := strconv.ParseInt(os.Args[2], 0, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid partition offset: %v\n", err)
		os.Exit(2)
	}
	return offset
}

func run(path string, base int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	// Layering a SectionReader at the partition offset is how a volume inside a
	// whole-disk image is opened. Every offset the library reports is then
	// relative to the partition, so add base for whole-disk coordinates.
	section := io.NewSectionReader(file, base, info.Size()-base)
	v, err := libfat.OpenWithOptions(section, libfat.OpenOptions{
		// Reconstructing long names for deleted entries is what makes a
		// deleted-file listing readable, so a forensic tool wants it on.
		RecoverDeletedLongNames: true,
	})
	if err != nil {
		return err
	}
	defer v.Close()

	fmt.Printf("%s volume, %d clusters of %d bytes\n\n",
		v.FATType(), v.ClusterCount(), v.BytesPerCluster())

	root, err := v.GetRootDirectory()
	if err != nil {
		return err
	}
	if err := walk(v, root, base); err != nil {
		return err
	}
	return reportOrphans(v, base)
}

// reportOrphans lists entries that no path leads to, recovered by scanning the
// data area for directory clusters the root tree cannot reach.
func reportOrphans(v *libfat.Volume, base int64) error {
	result, err := v.ScanOrphans(libfat.OrphanScanOptions{})
	if err != nil {
		return err
	}
	if len(result.Directories) == 0 {
		return nil
	}

	fmt.Printf("%s (%d directories recovered from %d clusters scanned)\n",
		libfat.OrphanPath, len(result.Directories), result.ClustersScanned)
	if result.ReachableWalkFailed {
		fmt.Println("  warning: part of the live tree could not be walked; some")
		fmt.Println("           entries below may in fact be reachable")
	}
	fmt.Println()

	for _, dir := range result.Directories {
		confidence := "dot-records intact"
		if !dir.HasDotEntries {
			confidence = "shape-matched only"
		}
		fmt.Printf("  directory at cluster %d (%s), parent cluster %d\n",
			dir.FirstCluster, confidence, dir.ParentCluster)
		for _, r := range dir.Ranges {
			fmt.Printf("    data: image offset %d-%d\n", base+r.StartByte, base+r.EndByte())
		}
		for _, entry := range dir.Entries {
			state := ""
			if entry.Deleted {
				state = " [deleted]"
			}
			fmt.Printf("    %-32s %8d bytes  entry at %d%s\n",
				entry.Name, entry.Size, base+entry.EntryAbsoluteOffset, state)
		}
		fmt.Println()
	}
	return nil
}

func walk(v *libfat.Volume, dir *libfat.File, base int64) error {
	entries, err := dir.ReadDir()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.Virtual || entry.Name == "." || entry.Name == ".." {
			continue
		}

		if entry.IsDirectory {
			if entry.Deleted {
				continue
			}
			sub, err := v.OpenPath(entry.Path)
			if err != nil {
				continue
			}
			if err := walk(v, sub, base); err != nil {
				return err
			}
			continue
		}

		report(v, entry, base)
	}
	return nil
}

func report(v *libfat.Volume, entry libfat.DirEntry, base int64) {
	// Deleted files have no FAT chain left, so ask for the contiguity-based
	// reconstruction and label it as such in the output.
	opts := libfat.FragmentOptions{AssumeContiguous: entry.Deleted}
	result, err := v.FragmentOffsetsWithOptions(entry, opts)
	if err != nil {
		if !errors.Is(err, libfat.ErrNoDataClusters) {
			fmt.Printf("%-40s  error: %v\n", entry.Path, err)
		}
		return
	}

	status := "ok"
	switch {
	case result.FirstClusterReallocated:
		status = "OVERWRITTEN"
	case result.Assumed:
		status = "assumed-contiguous"
	case result.LoopDetected:
		status = "chain-loop"
	case result.ChainBroken:
		status = "chain-broken"
	case result.Truncated:
		status = "truncated"
	}

	label := entry.Path
	if entry.Deleted {
		label = "[deleted] " + label
	}

	fmt.Printf("%-50s %8d bytes  %d run(s)  %s\n",
		label, entry.Size, len(result.Ranges), status)
	fmt.Printf("%-50s modified %s\n", "", entry.ModifiedAt.Format("2006-01-02 15:04:05"))

	for i, r := range result.Ranges {
		fmt.Printf("    run %d: image offset %d-%d (%d bytes, clusters %d+%d)\n",
			i, base+r.StartByte, base+r.EndByte(), r.Length, r.StartCluster, r.ClusterCount)
	}

	if slack, ok, err := v.SlackRange(entry); err == nil && ok {
		fmt.Printf("    slack: image offset %d-%d (%d bytes)\n",
			base+slack.StartByte, base+slack.EndByte(), slack.Length)
	}
	fmt.Println()
}
