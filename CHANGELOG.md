# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - Unreleased

Every change is additive: no exported symbol was removed, renamed, or
re-signatured. Two behaviour changes are called out under *Fixed* and *Changed*
because they alter what existing callers observe at runtime.

### Added

- **Fragment / data-run export.** `Volume.FragmentOffsets(entry)` returns the
  absolute image byte ranges a file occupies, so callers can compute image
  offsets without re-walking the FAT. Consecutive clusters are coalesced and the
  final run is trimmed to the entry size, so the ranges sum to the file's size
  and a single run means the file is contiguous.
- `Volume.FragmentOffsetsWithOptions` returning `*FragmentResult`, which reports
  how the ranges were derived: `ChainWalked`, `Truncated`, `Assumed`,
  `ChainBroken`, `LoopDetected`, `FirstClusterReallocated`, `ClustersWalked`.
- `FragmentOptions` with `AssumeContiguous`, `MaxRuns`, and `MaxClusters`.
  Contiguity reconstruction for deleted files is opt-in so that callers never
  receive fabricated offsets they did not ask for.
- `Range` type (`StartByte`, `Length`, `Sparse`, `StartCluster`,
  `ClusterCount`). `Sparse` is always false on FAT and exists for parity with
  filesystems that have holes.
- `Volume.ClusterChainFragments`, `Volume.RootDirectoryFragments`,
  `Volume.ClusterChain`, `Volume.SlackRange`.
- `File.Fragments`, `File.FragmentsWithOptions`, `File.Slack`,
  `File.SetFragmentOptions`.
- `File.IsFragmented`, `Volume.IsFragmented(entry)`, and package-level
  `IsFragmented(ranges)`, `TotalLength(ranges)`, `Coalesce(ranges)`.
- `File.Reader`, `File.ReaderAt`, and `File.SectionReader` for fragment-aware
  streaming reads that do not buffer the whole file.
- `DirEntry.EntryAbsoluteOffset`: the image offset of the 32-byte directory
  entry, resolved through the parent directory's own fragment list so that it is
  correct for fragmented directories. `DirEntry.LFNEntryOffset` locates the
  first long-name slot.
- `DirEntry.NameSource` and `DirEntry.FirstCharRecovered`, reporting how a name
  was determined.
- `OpenOptions.RecoverDeletedLongNames` reconstructs long names for deleted
  entries and recovers the first character that deletion overwrites. Off by
  default, since enabling it changes the `Name` of deleted entries.
- **Orphan-file recovery.** `Volume.ScanOrphans(OrphanScanOptions)` sweeps the
  data area for directory clusters that cannot be reached from the root,
  recovering the children of deleted directories whose entries survive in place.
  Returns `*OrphanScanResult` with per-directory `Ranges`, `FirstCluster`,
  `ParentCluster` (from the `..` record) and `HasDotEntries`. Options:
  `ScanAllocatedClusters`, `AllowMissingDotEntries`, `OnlyFirstCluster`,
  `MinValidEntries`, `MaxClusters`, `MaxDirectories`, `MaxDepth`. The zero value
  is the precision-first configuration. Adds `DirEntry.Orphaned` and the
  `OrphanPath` constant.
- `Volume.OpenEntry(DirEntry) (*File, error)` opens an entry obtained from
  `ReadDir` or `ScanOrphans`. `OpenPath` refuses deleted and orphaned entries
  because no live path leads to them, which previously left no public way to
  read their content through the `File` API.
- Sentinel errors `ErrTruncatedChain`, `ErrFragmented`, `ErrNoDataClusters`.
- Test image builder placing files at explicitly chosen clusters, a
  fragmentation matrix across FAT12/16/32, adversarial corpus (cluster loops,
  bad-cluster markers, out-of-range links, reallocated deleted clusters), and
  four fuzz targets asserting panic-freedom and range containment.

### Fixed

- **`File.ReadAt` could panic.** Reads were bounded against the directory
  entry's recorded size rather than the number of bytes the cluster chain
  actually covers. For a deleted entry, whose chain is freed, any offset between
  the end of the first cluster and the recorded size sliced past the end of the
  buffer. Such offsets now return an error wrapping `ErrTruncatedChain`.
  Callers that previously relied on the panic-free path for intact files are
  unaffected.
- `Volume.ClusterToOffset` accepted cluster numbers past the end of the data
  area and returned offsets outside the volume. It now rejects them.
- `ReadUint16LE` and `ReadUint32LE`, both exported, panicked on short input.
  They now return 0.
- `0x05` as the first byte of a short name, which encodes a literal `0xE5`, is
  now decoded back to `0xE5` instead of being reported as `0x05`.
- Removed an unused `sync.RWMutex` field on `Volume` that implied a locking
  discipline the type did not have.

### Changed

- `File.ReadAll` returns the recovered prefix together with an error wrapping
  `ErrTruncatedChain` when the chain covers fewer bytes than the entry's size.
  It previously returned the short buffer with a nil error, which made a
  partially recoverable deleted file indistinguishable from a small intact one.
- `File.ReadAt` no longer reads and buffers the entire file on every call; it
  resolves the file's runs once and reads only the clusters it needs.
- Reading a file whose first cluster is below 2 now reports `ErrNoDataClusters`
  rather than `ErrCorruptStructure`.
- `DirEntry` gained fields. Code using unkeyed composite literals for it will
  no longer compile; keyed literals are unaffected.
- `DirEntry.EntryOffset` is unchanged but now documented as
  directory-buffer-relative. Callers treating it as an image offset should move
  to `EntryAbsoluteOffset`.

## [0.1.0]

- Initial release: FAT12/16/32 boot sector parsing, directory traversal,
  deleted-entry enumeration, LFN assembly, FAT mirror comparison, FAT32 FSInfo.
