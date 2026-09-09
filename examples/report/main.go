// Command report writes a JSON forensic report of a FAT volume to stdout.
//
// Pass -deep to include deleted records, the surviving first cluster of deleted
// directories, and the directory data no path reaches. Deep widens the search
// but never weakens the evidence: every row carries a "layout" object saying
// how its extents were derived, and nothing is ever assumed contiguous unless
// -assume is given as well.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/aoiflux/libfat"
)

func main() {
	var (
		deep   = flag.Bool("deep", false, "include deleted records and unreachable directory data")
		assume = flag.Bool("assume", false, "reconstruct deleted extents by assuming contiguity (flagged as assumed)")
		slack  = flag.Bool("slack", false, "include each file's cluster slack")
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := libfat.ReportOptions{
		IncludeSlack: *slack,
		Fragments:    libfat.FragmentOptions{AssumeContiguous: *assume},
	}
	if *deep {
		opts.IncludeDeleted = true
		opts.DescendDeletedDirectories = true
		opts.IncludeOrphans = true
	}

	name := filepath.Base(flag.Arg(0))
	if err := volume.WriteReportWithOptionsContext(ctx, name, opts, os.Stdout); err != nil {
		// Nothing was written: a truncated report that looks complete is worse
		// than none.
		log.Fatalf("Report failed: %v", err)
	}
}
