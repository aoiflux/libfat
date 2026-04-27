package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aoiflux/libfat"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <fat_volume_or_image> [FAT12|FAT16|FAT32]\n", os.Args[0])
		os.Exit(1)
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to open volume: %v", err)
	}
	defer file.Close()

	var volume *libfat.Volume
	if len(os.Args) >= 3 {
		volume, err = libfat.OpenWithFATType(file, strings.ToUpper(os.Args[2]))
	} else {
		volume, err = libfat.Open(file)
	}
	if err != nil {
		log.Fatalf("Failed to parse FAT volume: %v", err)
	}
	defer volume.Close()

	bs := volume.GetBootSector()
	fmt.Println("=== FAT Volume Information ===")
	fmt.Printf("Type: %s\n", volume.FATType())
	fmt.Printf("Volume Size: %d bytes\n", volume.VolumeSize())
	fmt.Printf("Sector Size: %d bytes\n", volume.BytesPerSector())
	fmt.Printf("Cluster Size: %d bytes\n", volume.BytesPerCluster())
	fmt.Printf("Cluster Count: %d\n", volume.ClusterCount())
	fmt.Printf("Volume Label: %s\n", volume.VolumeLabel())
	fmt.Printf("OEM Name: %s\n", strings.TrimSpace(string(bs.OEMName[:])))
	fmt.Println()

	root, err := volume.GetRootDirectory()
	if err != nil {
		log.Fatalf("Failed to open root directory: %v", err)
	}

	entries, err := root.ReadDir()
	if err != nil {
		log.Fatalf("Failed to read root directory: %v", err)
	}

	fmt.Println("=== Root Directory Contents ===")
	for _, entry := range entries {
		kind := "FILE"
		if entry.IsDirectory {
			kind = "DIR"
		}
		fmt.Printf("[%s] %-32s %10d bytes\n", kind, entry.Name, entry.Size)
	}
}
