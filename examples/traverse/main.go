// Command traverse lists a FAT volume's directory tree using Volume.Walk.
//
// The recursion, the cycle guard and the depth cap all live in the library, so
// this program is a callback and a counter. Pass -deleted to include deleted
// records and the surviving first cluster of deleted directories, and -orphans
// to append the directory data no path reaches.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/aoiflux/libfat"
)

type stats struct {
	dirCount  int
	fileCount int
	deleted   int
	orphaned  int
	totalSize uint64
}

func main() {
	var (
		includeDeleted = flag.Bool("deleted", false, "include deleted records and descend into deleted directories")
		includeOrphans = flag.Bool("orphans", false, "append directory data that no path reaches")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <fat_volume_or_image>\n\nFlags:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	file, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatalf("Failed to open volume: %v", err)
	}
	defer file.Close()

	volume, err := libfat.Open(file)
	if err != nil {
		log.Fatalf("Failed to parse FAT volume: %v", err)
	}
	defer volume.Close()

	// A walk over a large image is worth being able to interrupt.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var s stats
	opts := libfat.WalkOptions{
		IncludeDeleted:            *includeDeleted,
		DescendDeletedDirectories: *includeDeleted,
		IncludeOrphans:            *includeOrphans,
	}

	// The parent cluster the callback is handed always equals
	// e.ParentFirstCluster, so this only needs the entry.
	err = volume.WalkWithOptions(ctx, opts, func(path string, _ uint32, e libfat.DirEntry) error {
		// The path is already absolute and built with forward slashes by the
		// library, so it needs no rebuilding here.
		depth := strings.Count(strings.Trim(path, "/"), "/")
		indent := strings.Repeat("  ", depth)

		kind := "FILE"
		if e.IsDirectory {
			kind = "DIR "
			s.dirCount++
		} else {
			s.fileCount++
			s.totalSize += e.Size
		}

		var notes []string
		if e.Deleted {
			s.deleted++
			notes = append(notes, "deleted")
			if e.ClusterAllocated {
				// The first cluster is in use again, so the content is most
				// likely gone even though the record survives.
				notes = append(notes, "overwritten")
			}
		}
		if e.Orphaned {
			s.orphaned++
			notes = append(notes, "orphaned")
		}
		if id, ok := volume.FileID(e); ok {
			notes = append(notes, "id="+id.String())
		}

		suffix := ""
		if len(notes) > 0 {
			suffix = "  (" + strings.Join(notes, ", ") + ")"
		}
		if e.IsDirectory {
			fmt.Printf("%s[%s] %s%s\n", indent, kind, e.Name, suffix)
		} else {
			fmt.Printf("%s[%s] %s (%d bytes)%s\n", indent, kind, e.Name, e.Size, suffix)
		}
		return nil
	})
	if err != nil {
		// A cancelled walk still printed everything it reached.
		log.Printf("Traversal stopped: %v", err)
	}

	fmt.Println()
	fmt.Printf("Directories: %d\n", s.dirCount)
	fmt.Printf("Files: %d\n", s.fileCount)
	fmt.Printf("Total Size: %d bytes\n", s.totalSize)
	if s.deleted > 0 {
		fmt.Printf("Deleted records: %d\n", s.deleted)
	}
	if s.orphaned > 0 {
		fmt.Printf("Orphaned records: %d\n", s.orphaned)
	}
}
