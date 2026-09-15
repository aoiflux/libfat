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

## Walking the tree

`Walk` visits every entry depth first, in the order records appear on disk, so
callers do not hand-roll recursion over `ReadDir`:

```go
ctx := context.Background()
err := v.Walk(ctx, func(path string, parentFirstCluster uint32, e libfat.DirEntry) error {
    fmt.Println(path, e.Size)
    return nil
})
```

The root is not reported: it has no directory record anywhere on the volume, and
synthesising one would fabricate a structure that does not exist. The zero
`WalkOptions` covers the live, reachable tree; deleted records and orphan runs
are opt-in, which is deliberately unlike `ReadDir`.

```go
err := v.WalkWithOptions(ctx, libfat.WalkOptions{
    IncludeDeleted:            true,
    DescendDeletedDirectories: true,
    IncludeOrphans:            true,
}, fn)
```

### File identity

`EntryAbsoluteOffset` says where a record is now; `FileID` says which record it
is. A defragmentation pass rewrites the first for every entry in a directory
while changing no file — the slot index, being logical, does not move.

```go
id, ok := v.FileID(entry) // {ParentFirstCluster, EntrySlotIndex}
```

It is an address, not an identity. FAT has no inode and **no reuse or generation
counter**, so a slot reused after a deletion carries its previous occupant's
`FileID` exactly, and a rename that changes the number of long-name slots moves
the entry to a different one. **Rename detection on FAT is inference, never
proof** — corroborate with `CreatedAt`, `FirstCluster` and `Size`. See the *File
identity* section of the package documentation for the full account.

## JSON reports

```go
err := v.WriteReportDeep("evidence.img", os.Stdout)
```

Every row carries a `layout` object holding the provenance flags — `chain_walked`,
`assumed`, `truncated`, `chain_broken`, `loop_detected`,
`first_cluster_reallocated` — none of them omitted when false, so a hypothesised
extent is never indistinguishable from a verified one. `ReportDeep` searches more
places but never relaxes the evidence: it does not set `AssumeContiguous`.

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

Offsets are absolute within the `io.ReaderAt` passed to `Open`, and include
`OpenOptions.BaseOffset`. For a partition inside a whole-disk image, either open
the whole image and set `BaseOffset` to the partition start, which makes every
reported offset a whole-disk offset:

```go
v, err := libfat.OpenWithOptions(image, libfat.OpenOptions{
	BaseOffset: partitionOffset,
})
```

or layer an `io.SectionReader` at the partition offset and leave `BaseOffset` at
zero, which gives partition-relative offsets. Do one or the other: adding the
partition base to an offset that already includes it counts it twice.

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
- `(*Volume).OpenEntry(entry DirEntry) (*File, error)` — the route to deleted
  and orphaned entries, which no path resolves to
- `(*Volume).GetBootSector() *BootSector`
- `(*Volume).BaseOffset() int64` — where the volume begins in the image, which
  every reported offset includes
- `(*Volume).Capabilities() Capabilities` — what FAT records, as distinct from
  what this volume happens to record

Walking the tree:

- `(*Volume).Walk(ctx, func(path string, parentFirstCluster uint32, e DirEntry) error) error`
- `(*Volume).WalkWithOptions(ctx, opts WalkOptions, fn) error` — deleted records
  and unreachable directory data are opt-in, so one pass serves both change
  detection and recovery
- `(*Volume).FileID(entry DirEntry) (FileID, bool)` — the composite
  `{ParentFirstCluster, EntrySlotIndex}` identity, and `FixedRootCluster` for
  the FAT12/16 root region

Reports:

- `(*Volume).Report(name string) (*FATReport, error)`, `ReportDeep`,
  `ReportWithOptions`, `ReportWithOptionsContext`
- `(*Volume).WriteReport(name string, w io.Writer) error`, `WriteReportDeep`,
  `WriteReportWithOptions`, `WriteReportWithOptionsContext`
- `(*FATReport).Summary()`, `FilterFiles`, `FilesByType`, `DeletedFiles`,
  `OrphanedFiles`, `FragmentedFiles`, `AssumedFiles`
- `FATReport` carries `SchemaVersion` (`ReportSchemaVersion`), `LibraryVersion`
  and `Generated`; rows carry `Path` and `Name` alongside `Filename`

Fragments and offsets:

- `(*Volume).FragmentOffsets(entry DirEntry) ([]Range, error)`
- `(*Volume).FragmentOffsetsWithOptions(entry DirEntry, opts FragmentOptions) (*FragmentResult, error)`
- `(*Volume).ClusterChainFragments(startCluster uint32, size uint64) ([]Range, error)`
- `(*Volume).RootDirectoryFragments() ([]Range, error)`
- `(*Volume).ClusterChain(startCluster uint32) ([]uint32, error)`
- `(*Volume).SlackRange(entry DirEntry) (Range, bool, error)`
- `(*Volume).IsFragmented(entry DirEntry) (bool, error)`
- `(*Volume).ScanOrphans(opts OrphanScanOptions) (*OrphanScanResult, error)`
- `(*Volume).ScanOrphansContext(ctx, opts OrphanScanOptions) (*OrphanScanResult, error)`
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
- `examples/traverse`: walk the whole tree with `Volume.Walk`, printing each
  entry's `FileID`; `-deleted` and `-orphans` widen it to recovery material
- `examples/extract`: extract a file from a FAT volume to a local output path
- `examples/fragments`: print the absolute image byte ranges of every file,
  including deleted entries, slack, and orphans recovered from deleted
  directories
- `examples/report`: write a JSON forensic report to stdout; `-deep` adds
  deleted records and unreachable directory data

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

### Integration tests

The suite above builds its own images, which keeps it fast and lets it construct
damage on purpose, but cannot catch an assumption this library and its fixture
builder share. `integration_test.go` runs the same checks against volumes
written by `mkfs.fat` and by Windows, including a FAT32 partition inside a GPT
disk. They skip unless told where the images are:

```bash
LIBFAT_TEST_IMAGES=/path/to/images go test -run TestReal -v .
```

`integration_oracle_test.go` goes further and compares libfat's reading against
one made by 7-Zip, whose FAT handler shares no code with this library, which is
the only way to catch a misreading that libfat makes consistently. Generate the
oracle once per image:

```bash
python3 testdata/make_fat_oracle.py /path/to/images/disk.dd
```

`realVolumes` in `integration_test.go` lists the images expected, along with
geometry decoded outside this library. A volume that is present but does not
match its entry is skipped rather than failed, since that is a fact about the
image rather than about libfat.

### The synthetic corpus

Real images are the better evidence, but the three on hand cover one sector size,
two of the three FAT types, no deletions, no orphans and no fragmented regular
file. `testdata/gen_corpus.sh` builds fourteen volumes that cover the rest, using
`dosfstools` and `mtools` — neither of which needs root, and neither of which
shares any code with libfat:

```bash
wsl.exe -- sh testdata/gen_corpus.sh /mnt/e/dataset/fat_synth
```

`corpus_test.go` reads them from `$LIBFAT_TEST_IMAGES/fat_synth`, or from
`LIBFAT_CORPUS`. Each volume's manifest entry records what only that volume
covers, and the generated `README.md` beside the images explains each one and
what the corpus still does not reach.
