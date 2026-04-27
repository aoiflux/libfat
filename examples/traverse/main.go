package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aoiflux/libfat"
)

type Stats struct {
	DirCount  int
	FileCount int
	TotalSize uint64
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <fat_volume_or_image> <directory_path>\n", os.Args[0])
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

	dir, err := volume.OpenPath(os.Args[2])
	if err != nil {
		log.Fatalf("Failed to open directory %s: %v", os.Args[2], err)
	}
	if !dir.IsDirectory() {
		log.Fatalf("%s is not a directory", os.Args[2])
	}

	stats := &Stats{}
	if err := traverse(volume, dir, os.Args[2], 0, stats); err != nil {
		log.Fatalf("Traversal error: %v", err)
	}

	fmt.Println()
	fmt.Printf("Directories: %d\n", stats.DirCount)
	fmt.Printf("Files: %d\n", stats.FileCount)
	fmt.Printf("Total Size: %d bytes\n", stats.TotalSize)
}

func traverse(volume *libfat.Volume, dir *libfat.File, currentPath string, depth int, stats *Stats) error {
	entries, err := dir.ReadDir()
	if err != nil {
		return err
	}

	indent := strings.Repeat("  ", depth)
	for _, entry := range entries {
		fullPath := filepath.Join(currentPath, entry.Name)
		if entry.IsDirectory {
			stats.DirCount++
			fmt.Printf("%s[DIR]  %s\n", indent, entry.Name)
			subdir, err := volume.OpenPath(fullPath)
			if err != nil {
				fmt.Printf("%s  (open error: %v)\n", indent, err)
				continue
			}
			if err := traverse(volume, subdir, fullPath, depth+1, stats); err != nil {
				fmt.Printf("%s  (traverse error: %v)\n", indent, err)
			}
			continue
		}

		stats.FileCount++
		stats.TotalSize += entry.Size
		fmt.Printf("%s[FILE] %s (%d bytes)\n", indent, entry.Name, entry.Size)
	}

	return nil
}
