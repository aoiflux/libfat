package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aoiflux/libfat"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: %s <fat_volume_or_image> <file_path> <output_file>\n", os.Args[0])
		os.Exit(1)
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to open volume: %v", err)
	}
	defer file.Close()

	volume, err := libfat.Open(file)
	if err != nil {
		log.Fatalf("Failed to parse FAT volume: %v", err)
	}
	defer volume.Close()

	target, err := volume.OpenPath(os.Args[2])
	if err != nil {
		log.Fatalf("Failed to open %s: %v", os.Args[2], err)
	}
	if target.IsDirectory() {
		log.Fatalf("%s is a directory", os.Args[2])
	}

	// A truncated chain still yields every byte that could be located, which is
	// the useful outcome for a deleted file. Write the recovered prefix and say
	// so, rather than discarding it.
	data, err := target.ReadAll()
	truncated := errors.Is(err, libfat.ErrTruncatedChain)
	if err != nil && !truncated {
		log.Fatalf("Failed to read %s: %v", os.Args[2], err)
	}

	if err := os.WriteFile(os.Args[3], data, 0o644); err != nil {
		log.Fatalf("Failed to write %s: %v", os.Args[3], err)
	}

	if truncated {
		fmt.Printf("Extracted %d of %d bytes to %s (chain truncated; the rest could not be located)\n",
			len(data), target.Size(), os.Args[3])
		return
	}
	fmt.Printf("Extracted %d bytes to %s\n", len(data), os.Args[3])
}
