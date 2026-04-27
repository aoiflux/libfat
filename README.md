# libfat

Pure Go parsing for FAT12, FAT16, and FAT32 volumes and disk images.

A small core package with `Open`, `Volume`, `File`, `DirEntry`, focused binary
helpers, typed errors, and example programs for listing, traversal, and
extraction.

## Status

Implemented now:

- FAT12, FAT16, and FAT32 boot sector parsing and validation
- Volume geometry derivation from the BPB
- FAT table cluster-chain resolution
- Root directory access for FAT12/16 and FAT32
- Short-name and long-file-name directory parsing
- Path-based file and directory opening
- File reads via `Read`, `ReadAt`, and `ReadAll`
- Directory enumeration via `ReadDir`
- Deleted-entry recovery metadata (`Deleted`, `Recovered`, `ClusterAllocated`)
- FAT32 FSInfo parsing with free-cluster and next-free hints
- Optional forced FAT type parsing via `OpenWithFATType`
- Focused unit tests with synthetic FAT images
- exFAT support is provided by the companion library:
  https://github.com/aoiflux/libxfat

Current limitations:

- Read-only library
- No write support

## Installation

```bash
go get github.com/aoiflux/libfat
```

## Go Version

- Requires Go 1.25+

## Quick Start

```go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aoiflux/libfat"
)

func main() {
	img, err := os.Open("disk.img")
	if err != nil {
		log.Fatal(err)
	}
	defer img.Close()

	vol, err := libfat.Open(img)
	if err != nil {
		log.Fatal(err)
	}
	defer vol.Close()

	root, err := vol.GetRootDirectory()
	if err != nil {
		log.Fatal(err)
	}

	entries, err := root.ReadDir()
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {
		kind := "FILE"
		if entry.IsDirectory {
			kind = "DIR "
		}
		fmt.Printf("[%s] %s (%d bytes)\n", kind, entry.Name, entry.Size)
	}
}
```

Optional forced FAT type (TSK-style claimed-type checking):

```go
vol, err := libfat.OpenWithFATType(img, libfat.FAT16)
if err != nil {
	log.Fatal(err)
}
defer vol.Close()
```

## API Highlights

Volume-level:

- `Open(reader io.ReaderAt) (*Volume, error)`
- `OpenWithFATType(reader io.ReaderAt, fatType string) (*Volume, error)`
- `(*Volume).GetRootDirectory() (*File, error)`
- `(*Volume).OpenPath(path string) (*File, error)`
- `(*Volume).GetBootSector() *BootSector`

File-level:

- `(*File).Read(p []byte) (int, error)`
- `(*File).ReadAt(p []byte, offset int64) (int, error)`
- `(*File).ReadAll() ([]byte, error)`
- `(*File).ReadDir() ([]DirEntry, error)`

## Examples

- `examples/basic`: open a volume/image (optionally force FAT12/FAT16/FAT32),
  print volume metadata, list root directory
- `examples/traverse`: recursively walk a directory subtree with summary stats
- `examples/extract`: extract a file from a FAT volume to a local output path

Run one example:

```bash
cd examples/basic
go run . <fat_volume_or_image> [FAT12|FAT16|FAT32]
```

## Related Projects

- libxfat (exFAT parser): https://github.com/aoiflux/libxfat

## Development

```bash
go test ./...
go vet ./...
```
