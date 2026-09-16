# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing yet.

## [0.3.1] - 2026-09-16

The Go API is additive: no exported symbol was removed, renamed, or
re-signatured, and no JSON key was removed or renamed. Reports gain keys, which
a consumer that ignores unknown keys is unaffected by.

One existing value changes. `Volume.VolumeLabel()`, and the report's
`volume_label`, now read the volume's label from the root directory rather than
from the boot sector, which is a fix rather than an addition and is described
under *Fixed*.

### Added

- **Configurable base offset.** `OpenOptions.BaseOffset` states where the volume
  begins inside the reader, and `Volume.BaseOffset()` reports it back. Every
  offset the library returns - `Range.StartByte`, `DirEntry.EntryAbsoluteOffset`,
  `DirEntry.LFNEntryOffset`, `BootSector.Offset`, `FATReport.StartOffset` and
  `EndOffset`, `FATMeta.Offset` and every report fragment - now includes it, so
  a volume opened at a partition offset yields whole-disk offsets directly.
- Previously the only way to open a partition was to scope the reader with an
  `io.SectionReader`, which silently yielded partition-relative offsets.
  Comparing those against whole-disk byte ranges produces a confident wrong
  answer rather than an error, which is the failure this field removes. Both
  approaches remain valid; mixing them double-counts the base, and the package
  documentation now says so.
- A negative `BaseOffset`, or one large enough that the end of the volume would
  overflow an `int64`, is rejected at open with the new `ErrInvalidBaseOffset`.
  The zero value reproduces the previous behaviour exactly.
- **File-relative offsets on runs.** `Range.FileOffset` (`file_offset`) is where
  a run begins in the file's own byte space, so mapping a changed image range
  back to a position in the file no longer requires the caller to accumulate
  lengths themselves. `Coalesce` renumbers the runs it returns.
- `Range`'s documentation now states its semantics in full: offsets are
  image-absolute, `Length` excludes cluster slack, holes do not occur, adjacent
  runs are coalesced, and the slice is sorted and gap-free.
- **Capability introspection.** `Capabilities` and `Volume.Capabilities()` report
  what FAT records as distinct from what a given volume happens to record, so a
  consumer can tell "the format does not keep that" from "that was absent here".
  `SubSecondTimestamps` is true, and its documentation carries the granularities
  that matter: creation to 10 ms, modification to 2 s, last access to the day.
  Timestamp equality is therefore not proof that nothing changed on FAT.
- `SecondFAT`, `FSInfoSector` and `BackupBootSector` are read from the volume's
  own boot record rather than being format constants.
- **Report provenance.** `FATReport` gains `SchemaVersion` (`schema_version`,
  currently `1`, exposed as `ReportSchemaVersion`), `LibraryVersion` and
  `Generated`. `Generated` is wall-clock time and is the only field that differs
  between two reports of an unchanged volume, so a consumer hashing a report to
  detect change must exclude it.
- **Paths on report rows.** `FATFile` gains `Path` (the full path, matching the
  sibling libxfat report's key) and `Name` (the basename, which the report could
  not previously express). `Filename` is unchanged and still carries the full
  path; it is retained because removing it would break existing consumers.
- **Volume label provenance.** `Volume.BootSectorVolumeLabel()` returns the boot
  sector's copy of the label on its own, and `Volume.VolumeLabelSource()` says
  which of the two copies `VolumeLabel()` used. `FATMeta` carries both as
  `boot_sector_volume_label` and `volume_label_source`. A disagreement between
  them is evidence in itself: it means the volume was labelled after it was
  formatted.

### Changed

- `FATReport.StartOffset` is now the volume's base rather than always zero, and
  `EndOffset` is the base plus the volume size. On a volume opened without a
  base offset both are unchanged.
- The package documentation and the `fragments` example no longer add the
  partition base to reported offsets by hand; the example uses `BaseOffset`.

### Fixed

- **The wrong volume label was reported.** FAT records a label twice - in the
  boot sector's `BS_VolLab` field and as a record in the root directory carrying
  the volume-ID attribute - and only the second is authoritative. libfat read
  the first. Because the boot sector's copy is written once at format time and
  never updated, and because Windows leaves it reading `NO NAME` whatever the
  volume is called, libfat reported names that no operating system and no other
  tool would show. It was found by cross-checking a real Windows-formatted FAT32
  partition against 7-Zip, which read its label as `P3_FAT32` where libfat said
  `NO NAME`.

  `VolumeLabel()` now prefers the root directory's record and falls back to the
  boot sector's copy when there is none. The root is scanned once at open,
  bounded, and a failure to read it is not an error: the boot sector's copy is
  used instead. Consumers that compare `volume_label` across reports written
  before and after this change will see a difference on affected volumes without
  the volume having changed.
- `constants.go` had no trailing newline, the only `gofmt` deviation in the tree.
- **`Version` was stale.** The constant read `0.2.0` while the repository was
  tagged `v0.3.0`. It was referenced nowhere until this release, which exposes it
  as the report's `library_version`, so every report written between that field
  landing and now names the wrong build. It now reads `0.3.1` and is part of the
  release checklist.

### Tests

- **Integration tests against real images.** `integration_test.go` and
  `integration_oracle_test.go` run against FAT12, FAT16 and FAT32 volumes written
  by `mkfs.fat` and by Windows, including a FAT32 partition sitting 2 GB inside a
  GPT disk - the case `BaseOffset` exists for, previously covered only by
  fixtures this package builds itself. They are skipped unless
  `LIBFAT_TEST_IMAGES` names a directory holding the images.
- Every entry on each volume is checked, not a sample: the `Range` invariants,
  that each entry's 32-byte record really sits at its reported
  `EntryAbsoluteOffset` and inside its parent directory's runs, and that each
  run's bytes in the image are the file's bytes at that run's position. An
  earlier sampling rule spent its budget on a few large files and walked past
  every fragmented entry on all three images, so the fragmented counts are now
  pinned in the manifest.
- `testdata/make_fat_oracle.py` generates an independent oracle with 7-Zip, whose
  FAT handler shares no code with libfat. Cross-checking against it confirmed
  agreement on every path, size and 8.3 short name, on 795 MB of file content,
  and on 3,724 modification timestamps - and is what surfaced the volume label
  defect above.
- **A synthetic corpus.** `testdata/gen_corpus.sh` builds fourteen volumes with
  `dosfstools` and `mtools`, covering what the real images cannot: FAT12 at all,
  a 4096-byte and a 1024-byte sector, a 1024-entry root region, a single-FAT
  volume, deletions on each of the three FAT types, a fragmented *regular* file,
  an orphaned directory, disagreeing FATs, a destroyed primary boot sector, and
  a boot sector label that contradicts the root directory's. Neither tool needs
  root, and mtools edits an image in place rather than mounting it, so the whole
  corpus builds under WSL.
- `corpus_test.go` pins what each volume covers and cross-checks thirteen of the
  fourteen against 7-Zip. The fourteenth has no oracle because 7-Zip cannot open
  a volume whose primary boot sector is destroyed, while libfat reads it through
  the backup.
- `TestCorpusRecoversDeletedContent` is the claim this library exists to make,
  checked end to end: a file whose record is deleted and whose FAT chain has been
  released reads back byte for byte under `FragmentOptions.AssumeContiguous`,
  against the sha256 of what the generator deleted. Two of the three deleted
  files per volume also have their long names recovered from the orphaned slots.

### Note for the sibling libxfat

`libfat.Range` and `libxfat.Range` are deliberately identical field for field.
This release adds `FileOffset` to libfat's; libxfat needs the same field, with
the same name, the same `json:"file_offset"` tag and the same meaning, populated
by its `FragmentOffsets`. Until that lands the two types differ by one field.

## [0.3.0] - 2026-09-10

The Go API is additive: no exported symbol was removed, renamed, or
re-signatured. The *JSON* output of the previously untagged exported structs
does change, which is called out under *Changed*.

### Added

- **Recursive walk.** `Volume.Walk(ctx, fn)` and
  `Volume.WalkWithOptions(ctx, opts, fn)` call
  `fn(path, parentFirstCluster, entry)` for every entry in the tree, depth first
  and in the order records appear on disk. Callers no longer hand-roll recursion
  over `ReadDir`. The root is not reported: it has no directory record anywhere
  on the volume, and synthesising one would fabricate a structure that does not
  exist.
- `WalkOptions` selects deleted records (`IncludeDeleted`,
  `DescendDeletedDirectories`) and unreachable directory data (`IncludeOrphans`,
  `OrphanScan`), and bounds the walk (`MaxDepth`, `StopOnReadError`). The zero
  value is the live reachable tree, so one pass can serve both change detection
  and recovery without either silently becoming the other.
- Descending into a deleted directory reads only its first cluster, requires
  that cluster to still be free in the FAT and to still begin with `.` and `..`
  pointing at itself, and never walks the FAT - the same rule
  `FragmentOffsets` applies to deleted files, for the same reason.
- **Composite file identity.** `FileID{ParentFirstCluster, EntrySlotIndex}`,
  `Volume.FileID(entry) (FileID, bool)`, the new `DirEntry.ParentFirstCluster`
  field, and the `FixedRootCluster` constant for the FAT12/16 root region, which
  is not cluster-addressed.
- The identity is documented as what it is: an address, not an identity. FAT has
  no inode and no reuse counter, so a slot reused after a deletion carries its
  previous occupant's `FileID` exactly, and a rename that changes the number of
  long-name slots moves the entry to a different one. It survives the parent
  directory being relocated, which `EntryAbsoluteOffset` does not. Rename
  detection on FAT is inference, never proof; see the *File identity* section of
  the package documentation for the corroborating signals.
- **Cancellation.** `Volume.ScanOrphansContext(ctx, opts)`, with `ScanOrphans`
  now a one-line delegate. Cancellation reaches all three phases of a scan - the
  reachability pre-pass, the cluster sweep and the continuation-run gather -
  paced by one counter spanning the whole operation. A cancelled scan returns
  its partial `*OrphanScanResult` with `Truncated` set alongside `ctx.Err()`.
  New sentinels `ErrNilContext` and `ErrNilCallback`.
- **JSON report.** `Volume.Report`, `ReportDeep`, `ReportWithOptions` and
  `ReportWithOptionsContext`, with the matching `WriteReport` quartet writing
  indented JSON to an `io.Writer`. `FATReport` carries `FATMeta` and `FATFile`
  rows in the shape the sibling filesystem libraries emit.
- `FileFragment` is the report's extent type, converted from `Range`, with an
  exclusive `EndOffset` and the FAT-native cluster addressing alongside the byte
  offsets. `FragmentProvenance` carries `FragmentResult`'s `ChainWalked`,
  `Assumed`, `Truncated`, `ChainBroken`, `LoopDetected` and
  `FirstClusterReallocated` into every row, none of them omitted when false: a
  hypothesised extent must never be indistinguishable from a verified one.
- `ReportDeep` widens the search but never relaxes the evidence. It does not set
  `AssumeContiguous`, because a report labelled deep that silently contained
  hypotheses would be the worst possible default.
- `FATReportSummary` and the `Summary` / `FilterFiles` / `FilesByType` /
  `DeletedFiles` / `OrphanedFiles` / `FragmentedFiles` / `AssumedFiles` query
  helpers on `*FATReport`.
- `Timestamps` groups an entry's times for serialisation, with `DirEntry.Timestamps()`.
- `NameSource` marshals as its label rather than its number, and round-trips.
- Fuzz targets `FuzzWalk` and `FuzzReport`, bringing the total to seven. `Walk`
  is the first API here that recurses on attacker-controlled structure, with
  three independent termination arguments that all have to hold at once.
- Package documentation gains *Walking the tree*, *File identity*,
  *Cancellation* and *JSON reports*.

### Changed

- **The JSON field names of the exported structs changed.** `Range`,
  `FragmentResult`, `FragmentOptions`, `DirEntry`, `BootSector`, `FAT32FSInfo`,
  `OpenOptions`, `OrphanScanOptions`, `OrphanDirectory` and `OrphanScanResult`
  gained snake_case tags, so a caller already marshalling them sees `name` where
  it previously saw `Name`. Timestamps an entry never recorded are now absent
  rather than rendered as `0001-01-01T00:00:00Z`, and `NameSource` marshals as
  `"recovered-lfn"` rather than `2`. The Go types are unchanged, except that a
  caller converting `DirEntry` to an identically-shaped untagged struct will no
  longer compile, since struct tags participate in type identity.
- `Volume.Walk` does not report deleted entries by default, unlike `File.ReadDir`
  which has always returned them unconditionally. A directory listing that hid
  them would hide the point of this package; a walk is a different operation and
  makes the choice explicit.

## [0.2.0] - 2026-07-28

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
