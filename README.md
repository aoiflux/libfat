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
- Per-file fragment / data-run export: absolute image byte ranges via
  `FragmentOffsets`, with provenance flags for degraded chains
- Fragment-aware streaming reads (`Reader`, `ReaderAt`, `SectionReader`) that do
  not buffer the whole file
- Absolute directory-entry offsets (`EntryAbsoluteOffset`), correct even for
  fragmented directories
- File slack ranges via `SlackRange`
- Deleted-entry recovery metadata (`Deleted`, `Recovered`, `ClusterAllocated`)
- Optional long-name reconstruction for deleted entries
  (`OpenOptions.RecoverDeletedLongNames`)
- Orphan recovery (`ScanOrphans`): finds children of deleted directories that no
  path leads to
- FAT32 FSInfo parsing with free-cluster and next-free hints
- Optional forced FAT type parsing via `OpenWithFATType`
- Unit tests over synthetic images with explicitly chosen cluster layouts, an
  adversarial corpus, and fuzz targets asserting panic-freedom
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

## Locating file data

For forensic work the central operation is mapping a file to the byte ranges it
occupies in the image, without extracting it:

```go
ranges, err := vol.FragmentOffsets(entry)
if err != nil && !errors.Is(err, libfat.ErrTruncatedChain) {
	log.Fatal(err)
}
for _, r := range ranges {
	fmt.Printf("%d bytes at image offset %d\n", r.Length, r.StartByte)
}
```

Consecutive clusters are coalesced, so one `Range` means the file is contiguous
and more than one means it is fragmented. Ranges sum to the entry's size; the
unused tail of the final cluster is reported separately by `SlackRange`.

Offsets are relative to the `io.ReaderAt` passed to `Open`. For a partition
inside a whole-disk image, layer an `io.SectionReader` at the partition offset
and add that base to obtain whole-disk coordinates.

### Deleted files

Deletion frees a file's FAT entries, so the chain describing its layout is gone
and only the first cluster survives in the directory entry. If that cluster has
since been reallocated, walking the FAT from it would follow the *new* owner's
chain. This library therefore never walks the FAT for a deleted entry:

```go
result, err := vol.FragmentOffsetsWithOptions(entry, libfat.FragmentOptions{
	AssumeContiguous: true, // opt in to contiguity-based reconstruction
})
// result.Assumed                 -> ranges are a hypothesis, not a fact
// result.FirstClusterReallocated -> content was most likely overwritten
```

Without `AssumeContiguous` only the recorded first cluster is reported, with
`Truncated` set.

### Orphaned files

Deleting a directory frees its chain and marks its entry in the parent, but the
directory's own clusters keep intact entries for its children. Those children
are reachable from no path. `ScanOrphans` sweeps the data area for them:

```go
result, err := vol.ScanOrphans(libfat.OrphanScanOptions{})
for _, dir := range result.Directories {
	for _, entry := range dir.Entries {
		fmt.Println(entry.Path, entry.Size) // rooted at libfat.OrphanPath
	}
}
```

The zero value is precision-first: only free clusters are examined, and only
those carrying self-consistent `.` and `..` records are accepted.
`AllowMissingDotEntries` trades precision for recall. Original paths are not
recoverable — the chain that named these entries is what was lost — so they are
reported under `/$OrphanFiles` with `DirEntry.Orphaned` set.

A scan reads every candidate cluster, so it is far more expensive than ordinary
traversal; bound it with `MaxClusters` on large images.

## API Highlights

Volume-level:

- `Open(reader io.ReaderAt) (*Volume, error)`
- `OpenWithFATType(reader io.ReaderAt, fatType string) (*Volume, error)`
- `OpenWithOptions(reader io.ReaderAt, options OpenOptions) (*Volume, error)`
- `(*Volume).GetRootDirectory() (*File, error)`
- `(*Volume).OpenPath(path string) (*File, error)`
- `(*Volume).GetBootSector() *BootSector`

Fragments and offsets:

- `(*Volume).FragmentOffsets(entry DirEntry) ([]Range, error)`
- `(*Volume).FragmentOffsetsWithOptions(entry DirEntry, opts FragmentOptions) (*FragmentResult, error)`
- `(*Volume).ClusterChainFragments(startCluster uint32, size uint64) ([]Range, error)`
- `(*Volume).RootDirectoryFragments() ([]Range, error)`
- `(*Volume).ClusterChain(startCluster uint32) ([]uint32, error)`
- `(*Volume).SlackRange(entry DirEntry) (Range, bool, error)`
- `(*Volume).IsFragmented(entry DirEntry) (bool, error)`
- `(*Volume).ScanOrphans(opts OrphanScanOptions) (*OrphanScanResult, error)`
- `IsFragmented(ranges []Range) bool`, `TotalLength(ranges []Range) int64`,
  `Coalesce(ranges []Range) []Range`

File-level:

- `(*File).Read(p []byte) (int, error)`
- `(*File).ReadAt(p []byte, offset int64) (int, error)`
- `(*File).ReadAll() ([]byte, error)`
- `(*File).ReadDir() ([]DirEntry, error)`
- `(*File).Fragments() ([]Range, error)`
- `(*File).IsFragmented() (bool, error)`
- `(*File).Reader() (io.ReadSeeker, error)`, `(*File).ReaderAt() (io.ReaderAt, error)`
- `(*File).SectionReader() (*io.SectionReader, error)` (contiguous files only)
- `(*File).SetFragmentOptions(opts FragmentOptions)`

## Examples

- `examples/basic`: open a volume/image (optionally force FAT12/FAT16/FAT32),
  print volume metadata, list root directory
- `examples/traverse`: recursively walk a directory subtree with summary stats
- `examples/extract`: extract a file from a FAT volume to a local output path
- `examples/fragments`: print the absolute image byte ranges of every file,
  including deleted entries, slack, and orphans recovered from deleted
  directories

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
go test -race ./...
go vet ./...

# Fuzz targets assert panic-freedom and that every returned range lies
# inside the volume.
go test -run XXX -fuzz '^FuzzFragmentOffsets$' -fuzztime 60s .
```
