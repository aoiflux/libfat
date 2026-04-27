package main

import (
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

	data, err := target.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read %s: %v", os.Args[2], err)
	}

	if err := os.WriteFile(os.Args[3], data, 0o644); err != nil {
		log.Fatalf("Failed to write %s: %v", os.Args[3], err)
	}

	fmt.Printf("Extracted %d bytes to %s\n", len(data), os.Args[3])
}
