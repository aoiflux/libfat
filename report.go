package libfat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"time"
)

// FATReport is a volume-level view of a FAT image, in the shape the sibling
// filesystem libraries emit so that reports from several filesystems can be
// consumed together.
//
// StartOffset and EndOffset bound the volume within the reader it was opened
// over. StartOffset is the volume's BaseOffset rather than zero, and EndOffset
// is exclusive, matching Range.EndByte and FileFragment: every offset in this
// document is absolute within that reader, so the bounds are stated the same
// way.
type FATReport struct {
	// SchemaVersion identifies the shape of this document, so that a consumer
	// reading one produced years earlier can pin what it understands and
	// recognise one it does not.
	//
	// It increments when a field is removed, renamed, or changes meaning.
	// Adding a field does not increment it, because a reader that ignores
	// unknown keys is unaffected by one.
	SchemaVersion int `json:"schema_version"`
	// LibraryVersion is the libfat version that produced this document, and
	// Generated is when it did.
	//
	// Generated is wall-clock time, so it is the one field here that differs
	// between two reports of an unchanged volume. A consumer hashing a report
	// to detect change must exclude it.
	LibraryVersion string    `json:"library_version"`
	Generated      time.Time `json:"generated"`

	Name        string    `json:"name"`
	StartOffset int64     `json:"start_offset"`
	EndOffset   int64     `json:"end_offset"`
	Filesystem  FATMeta   `json:"fat_meta"`
	Files       []FATFile `json:"files"`
}

// ReportSchemaVersion is the SchemaVersion this build of libfat writes.
const ReportSchemaVersion = 1

// FATMeta describes the volume's geometry and the provenance of the structures
// it was read from.
//
// Type, BlockSize and Offset are the fields every report in this family carries.
// The rest are FAT-specific and are here because they are evidence: a volume
// parsed from its backup boot sector, or one whose two FATs disagree, is a
// different kind of finding from a clean one, and a report that omitted that
// would describe a healthier image than the one on disk.
type FATMeta struct {
	// Type is FAT12, FAT16 or FAT32.
	Type string `json:"type"`
	// BlockSize is the cluster size in bytes, which is the allocation unit
	// every offset in this report is a multiple of.
	BlockSize int `json:"block_size"`
	// Offset is the byte offset of the first data cluster.
	Offset int64 `json:"offset"`

	SectorSize   int    `json:"sector_size"`
	ClusterCount uint32 `json:"cluster_count"`

	// VolumeLabel is what the volume is called, taken from the label record in
	// the root directory when there is one and from the boot sector otherwise.
	// BootSectorVolumeLabel is the boot sector's copy on its own, and
	// VolumeLabelSource says which of the two VolumeLabel came from.
	//
	// The two can easily disagree: the boot sector's is written at format time
	// and never updated afterwards, and Windows leaves it reading "NO NAME"
	// whatever the volume is called. Both are reported because the
	// disagreement is itself evidence that the volume was labelled after it was
	// formatted.
	VolumeLabel           string `json:"volume_label,omitempty"`
	BootSectorVolumeLabel string `json:"boot_sector_volume_label,omitempty"`
	VolumeLabelSource     string `json:"volume_label_source,omitempty"`

	VolumeSerial uint32 `json:"volume_serial"`

	// UsedBackupBootSector is true when the primary boot sector was unusable
	// and a backup copy was parsed instead, which means the geometry every
	// offset here derives from came from a copy rather than the original.
	UsedBackupBootSector bool `json:"used_backup_boot_sector"`

	// FATMirrorMismatches counts FAT entries whose copies in the two tables
	// disagreed. It is a running counter sampled after the walk, so it reflects
	// the entries this report touched rather than the whole volume.
	FATMirrorMismatches uint64 `json:"fat_mirror_mismatches"`
}

// FATFile is one directory entry.
//
// The identity fields are what make two reports of the same volume comparable.
// A path is not an identity: it changes when a file or any of its ancestors is
// renamed, and it is reused when a new file takes an old name.
// ParentFirstCluster and EntrySlotIndex are the closest FAT comes to one - the
// directory, and which slot of it - but FAT has no reuse or generation counter
// of the kind NTFS and ext carry, so the pair is an address rather than an
// identity and a slot reused after a deletion carries the previous occupant's
// exactly. FirstCluster and the creation time are the corroborating signals.
// See FileID for the full account of what the pair does and does not survive,
// and note in particular that a rename changing the number of long-name slots
// moves the entry: rename detection on FAT is inference, never proof.
//
// None of the identity scalars is omitted when zero. A report row is a thing
// that gets diffed, which wants a stable key set: parent cluster 0 is a real
// value on FAT12 and FAT16, slot 0 is the first slot of every directory, and an
// entry_absolute_offset of -1 is the statement that the record could not be
// located - all of which would otherwise be indistinguishable from "not
// reported".
type FATFile struct {
	// Path is the entry's full path from the volume root, and Name is its
	// basename alone. The sibling libxfat report names these the same way.
	//
	// Filename is the full path too: it predates Path and is retained because
	// removing it would break every existing consumer. Prefer Path in new code;
	// the two are always equal.
	Path     string `json:"path"`
	Name     string `json:"name"`
	Filename string `json:"filename"`
	// Type is file, directory, volume_label or virtual.
	Type string `json:"type"`

	ParentFirstCluster uint32 `json:"parent_first_cluster"`
	EntrySlotIndex     uint32 `json:"entry_slot_index"`
	FirstCluster       uint32 `json:"first_cluster"`

	// EntryAbsoluteOffset is the image offset of the 32-byte record, resolved
	// through the parent directory's fragment list so that it is correct even
	// when that directory is fragmented, and -1 when it could not be resolved.
	// LFNEntryOffset locates the first long-name slot, or -1 when the entry has
	// none. Unlike EntrySlotIndex these are physical, and they do not survive
	// the parent directory being relocated.
	EntryAbsoluteOffset int64 `json:"entry_absolute_offset"`
	LFNEntryOffset      int64 `json:"lfn_entry_offset"`

	ShortName  string     `json:"short_name"`
	NameSource NameSource `json:"name_source"`

	IsFragmented bool  `json:"is_fragmented"`
	IsDeleted    bool  `json:"is_deleted"`
	IsOrphaned   bool  `json:"is_orphaned"`
	IsVirtual    bool  `json:"is_virtual"`
	Size         int64 `json:"size"`
	Attributes   uint8 `json:"attributes"`

	// ClusterAllocated is the FAT's opinion of the first cluster. On a deleted
	// entry a true here means the cluster has been handed to a later file and
	// the content is most likely gone.
	ClusterAllocated bool `json:"cluster_allocated"`

	Timestamps Timestamps `json:"timestamps"`

	// Layout records how Fragments was derived. It is the part of this row that
	// separates a fact from a hypothesis, and it is never omitted.
	Layout    FragmentProvenance `json:"layout"`
	Fragments []FileFragment     `json:"fragments"`

	// Slack is the unused tail of the file's last cluster, present only when
	// ReportOptions.IncludeSlack is set and the file does not end on a cluster
	// boundary.
	Slack *FileFragment `json:"slack,omitempty"`
}

// FileFragment is one contiguous on-disk span of a file, converted from Range.
//
// EndOffset is exclusive - it is Range.EndByte, one past the last byte - which
// matches this package's Range and the equivalent types in the NTFS and XFS
// libraries. The ext library's FileFragment is inclusive; if you consume both,
// that is the difference to watch.
type FileFragment struct {
	StartOffset int64 `json:"start_offset"`
	EndOffset   int64 `json:"end_offset"`
	// FileOffset is where this span begins within the file, so consecutive
	// fragments tile the file without gaps.
	FileOffset int64 `json:"file_offset"`
	Length     int64 `json:"length"`

	// Sparse is always false on FAT, which has no sparse allocation and backs
	// every run with real clusters. The field exists so that a consumer can
	// treat these fragments uniformly with those from filesystems that do have
	// holes.
	Sparse bool `json:"sparse"`

	// StartCluster and ClusterCount are the FAT-native addressing of the same
	// span, for cross-referencing against the allocation tables. Both are zero
	// for the fixed root directory region, which is not cluster-addressed.
	StartCluster uint32 `json:"start_cluster"`
	ClusterCount uint32 `json:"cluster_count"`
}

// FragmentProvenance carries FragmentResult's degradation flags into the JSON.
//
// These are the distinguishing content of a libfat report and none of them is
// omitted when false. A chain_walked of false is the statement that these
// fragments did not come from a FAT chain; dropping the key would leave a
// consumer unable to tell that from a field this version did not emit, and a
// hypothesised extent would become indistinguishable from a verified one.
type FragmentProvenance struct {
	// ChainWalked is true when the fragments came from an actual FAT chain
	// walk. It is false for deleted entries, whose FAT entries no longer
	// describe them, and for the fixed root directory region.
	ChainWalked bool `json:"chain_walked"`
	// Assumed is true when the fragments were synthesised under
	// FragmentOptions.AssumeContiguous rather than read from the FAT. Data
	// located through them is a hypothesis, not a fact.
	Assumed bool `json:"assumed"`
	// Truncated is true when the fragments cover less than the entry's size.
	Truncated bool `json:"truncated"`
	// ChainBroken is true when the walk stopped on a free, bad or out-of-range
	// FAT entry rather than a proper end-of-chain marker.
	ChainBroken bool `json:"chain_broken"`
	// LoopDetected is true when the chain revisited a cluster.
	LoopDetected bool `json:"loop_detected"`
	// FirstClusterReallocated is true for a deleted entry whose first cluster
	// is now marked in use, meaning its content was likely overwritten.
	FirstClusterReallocated bool `json:"first_cluster_reallocated"`

	ClustersWalked uint32 `json:"clusters_walked"`
	BytesCovered   int64  `json:"bytes_covered"`

	// Error is the reason no fragments could be derived, when there is one. The
	// row is kept either way: an entry whose extent could not be resolved is
	// still an entry, and why it failed is itself a finding.
	Error string `json:"error,omitempty"`
}

// ReportOptions controls what a report collects.
//
// The zero value is the reachable, live tree with FAT-verified extents. Every
// field either widens the search or relaxes the evidence, and none is on by
// default.
type ReportOptions struct {
	// IncludeDeleted, DescendDeletedDirectories, IncludeOrphans and OrphanScan
	// are passed through to the walk; see WalkOptions, which documents what
	// each one costs and what it risks.
	IncludeDeleted            bool
	DescendDeletedDirectories bool
	IncludeOrphans            bool
	OrphanScan                OrphanScanOptions

	// Fragments is passed to FragmentOffsetsWithOptions for every row. Its zero
	// value walks the FAT and never fabricates an offset. Setting
	// AssumeContiguous fills in extents for deleted entries under an
	// assumption; rows produced that way carry layout.assumed, and no report
	// constructor sets it for you.
	Fragments FragmentOptions

	// IncludeSlack adds each file's cluster slack to its row. It costs one
	// extra resolution per file.
	IncludeSlack bool

	// MaxDepth bounds the walk; see WalkOptions.MaxDepth.
	MaxDepth int
}

// FATReportSummary is the aggregate view of a report.
type FATReportSummary struct {
	Total      int `json:"total"`
	Deleted    int `json:"deleted"`
	Orphaned   int `json:"orphaned"`
	Fragmented int `json:"fragmented"`
	// Assumed counts rows whose extents are hypotheses rather than facts, and
	// Truncated those whose extents account for less than the recorded size.
	Assumed    int            `json:"assumed"`
	Truncated  int            `json:"truncated"`
	TypeCounts map[string]int `json:"type_counts"`
	TotalSize  int64          `json:"total_size"`
}

// Report returns a report of the volume's live, reachable tree.
//
// It is ReportWithOptions with the zero ReportOptions. name identifies the
// image in the report and is not read from the volume.
func (v *Volume) Report(name string) (*FATReport, error) {
	return v.ReportWithOptions(name, ReportOptions{})
}

// ReportDeep returns a report that also covers deleted records and the
// directory data no path reaches.
//
// Deep means more places searched, never weaker evidence. It sets
// IncludeDeleted, DescendDeletedDirectories and IncludeOrphans, and
// deliberately does not set Fragments.AssumeContiguous: a report labelled deep
// that silently contained hypothesised extents would be the worst possible
// default, because the caller who most wants recovery data is the one least
// able to tell a reconstruction from a fact. Ask for that explicitly through
// ReportWithOptions when you want it, and read layout.assumed on every row.
func (v *Volume) ReportDeep(name string) (*FATReport, error) {
	return v.ReportWithOptions(name, ReportOptions{
		IncludeDeleted:            true,
		DescendDeletedDirectories: true,
		IncludeOrphans:            true,
	})
}

// ReportWithOptions returns a report of what the options select.
//
// It is ReportWithOptionsContext with context.Background().
func (v *Volume) ReportWithOptions(name string, opts ReportOptions) (*FATReport, error) {
	return v.ReportWithOptionsContext(context.Background(), name, opts)
}

// ReportWithOptionsContext is ReportWithOptions with cancellation.
//
// A report walks the whole tree and resolves every file's extents, so it is
// worth interrupting on a large image. On cancellation the rows gathered so far
// are returned alongside ctx.Err(), following this package's rule that a
// partial result is reported rather than discarded. A caller who wants
// all-or-nothing discards the report on any non-nil error.
func (v *Volume) ReportWithOptionsContext(ctx context.Context, name string, opts ReportOptions) (*FATReport, error) {
	if v.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if ctx == nil {
		return nil, ErrNilContext
	}

	report := &FATReport{
		SchemaVersion:  ReportSchemaVersion,
		LibraryVersion: Version,
		Generated:      time.Now().UTC(),

		Name:        name,
		StartOffset: v.BaseOffset(),
		EndOffset:   v.endOffset(),
		Filesystem:  v.reportMeta(),
		Files:       []FATFile{},
	}

	err := v.WalkWithOptions(ctx, WalkOptions{
		IncludeDeleted:            opts.IncludeDeleted,
		DescendDeletedDirectories: opts.DescendDeletedDirectories,
		IncludeOrphans:            opts.IncludeOrphans,
		OrphanScan:                opts.OrphanScan,
		MaxDepth:                  opts.MaxDepth,
	}, func(_ string, _ uint32, e DirEntry) error {
		report.Files = append(report.Files, v.reportFile(e, opts))
		return nil
	})

	// Sampled after the walk, so the count covers the entries this report
	// actually touched.
	report.Filesystem.FATMirrorMismatches = v.FATMirrorMismatches()
	if err != nil {
		return report, err
	}
	return report, nil
}

func (v *Volume) reportMeta() FATMeta {
	meta := FATMeta{
		Type:                  v.FATType(),
		BlockSize:             int(v.BytesPerCluster()),
		SectorSize:            int(v.BytesPerSector()),
		ClusterCount:          v.ClusterCount(),
		VolumeLabel:           v.VolumeLabel(),
		BootSectorVolumeLabel: v.BootSectorVolumeLabel(),
		VolumeLabelSource:     v.VolumeLabelSource(),
		UsedBackupBootSector:  v.UsedBackupBootSector(),
	}
	if offset, err := v.clusterToOffset(defaultRootCluster); err == nil {
		meta.Offset = offset
	}
	if bs := v.GetBootSector(); bs != nil {
		meta.VolumeSerial = bs.VolumeID
	}
	return meta
}

// reportFile builds one row. It never fails: an entry whose extents cannot be
// resolved keeps its row with the reason recorded in Layout.Error, because the
// entry is evidence whether or not its data can be located.
func (v *Volume) reportFile(e DirEntry, opts ReportOptions) FATFile {
	row := FATFile{
		Path:                e.Path,
		Name:                e.Name,
		Filename:            e.Path,
		Type:                entryType(e),
		FirstCluster:        e.FirstCluster,
		EntryAbsoluteOffset: e.EntryAbsoluteOffset,
		LFNEntryOffset:      e.LFNEntryOffset,
		ShortName:           e.ShortName,
		NameSource:          e.NameSource,
		IsDeleted:           e.Deleted,
		IsOrphaned:          e.Orphaned,
		IsVirtual:           e.Virtual,
		Attributes:          e.Attributes,
		ClusterAllocated:    e.ClusterAllocated,
		Timestamps:          e.Timestamps(),
		Fragments:           []FileFragment{},
	}
	// Size is read from a 32-bit field, so this only guards a DirEntry a caller
	// built by hand.
	if e.Size <= math.MaxInt64 {
		row.Size = int64(e.Size)
	}
	if id, ok := v.FileID(e); ok {
		row.ParentFirstCluster = id.ParentFirstCluster
		row.EntrySlotIndex = id.EntrySlotIndex
	} else {
		// Keep whatever the entry itself recorded rather than inventing a zero
		// that would read as a valid FAT12/16 root slot.
		row.ParentFirstCluster = e.ParentFirstCluster
		if e.EntryOffset >= 0 {
			row.EntrySlotIndex = uint32(e.EntryOffset / dirEntrySize)
		}
	}
	if e.Virtual {
		// A virtual entry names no clusters, so there is nothing to resolve and
		// nothing to report as a failure.
		return row
	}

	result, err := v.FragmentOffsetsWithOptions(e, opts.Fragments)
	if err != nil {
		row.Layout.Error = err.Error()
		return row
	}
	row.Layout = FragmentProvenance{
		ChainWalked:             result.ChainWalked,
		Assumed:                 result.Assumed,
		Truncated:               result.Truncated,
		ChainBroken:             result.ChainBroken,
		LoopDetected:            result.LoopDetected,
		FirstClusterReallocated: result.FirstClusterReallocated,
		ClustersWalked:          result.ClustersWalked,
		BytesCovered:            result.BytesCovered,
	}
	row.Fragments = toFileFragments(result.Ranges)
	row.IsFragmented = len(result.Ranges) > 1

	if opts.IncludeSlack && !e.IsDirectory {
		if slack, ok, serr := v.SlackRange(e); serr == nil && ok {
			// Slack sits past the end of the file, so it has no offset within
			// it; SlackRange reports the file's size, which is where the slack
			// begins, and toFileFragments carries that through.
			frag := toFileFragments([]Range{slack})[0]
			row.Slack = &frag
		}
	}
	return row
}

func entryType(e DirEntry) string {
	switch {
	case e.Virtual:
		return "virtual"
	case e.Attributes&attrVolumeID != 0:
		return "volume_label"
	case e.IsDirectory:
		return "directory"
	default:
		return "file"
	}
}

// toFileFragments converts the package's Range list into the report's extent
// type. The two carry the same facts; the report states the run's end as well
// as its length because a document is read without the help of EndByte.
func toFileFragments(ranges []Range) []FileFragment {
	out := make([]FileFragment, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, FileFragment{
			StartOffset:  r.StartByte,
			EndOffset:    r.EndByte(),
			FileOffset:   r.FileOffset,
			Length:       r.Length,
			Sparse:       r.Sparse,
			StartCluster: r.StartCluster,
			ClusterCount: r.ClusterCount,
		})
	}
	return out
}

// WriteReport writes a report of the live tree to w as indented JSON.
func (v *Volume) WriteReport(name string, w io.Writer) error {
	return v.WriteReportWithOptions(name, ReportOptions{}, w)
}

// WriteReportDeep writes a ReportDeep to w as indented JSON.
func (v *Volume) WriteReportDeep(name string, w io.Writer) error {
	return v.WriteReportWithOptions(name, ReportOptions{
		IncludeDeleted:            true,
		DescendDeletedDirectories: true,
		IncludeOrphans:            true,
	}, w)
}

// WriteReportWithOptions writes a report to w as indented JSON.
//
// It is WriteReportWithOptionsContext with context.Background().
func (v *Volume) WriteReportWithOptions(name string, opts ReportOptions, w io.Writer) error {
	return v.WriteReportWithOptionsContext(context.Background(), name, opts, w)
}

// WriteReportWithOptionsContext writes a report to w as indented JSON, with
// cancellation.
//
// Nothing is written when the report could not be completed. A truncated
// listing that looks complete is worse than none, and unlike the in-memory
// forms there is no flag on the written document to say otherwise.
func (v *Volume) WriteReportWithOptionsContext(ctx context.Context, name string, opts ReportOptions, w io.Writer) error {
	if w == nil {
		return errors.New("writer is nil")
	}
	report, err := v.ReportWithOptionsContext(ctx, name, opts)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// Summary returns the report's aggregate counters.
func (r *FATReport) Summary() FATReportSummary {
	summary := FATReportSummary{TypeCounts: make(map[string]int)}
	for _, f := range r.Files {
		summary.Total++
		summary.TypeCounts[f.Type]++
		summary.TotalSize += f.Size
		if f.IsDeleted {
			summary.Deleted++
		}
		if f.IsOrphaned {
			summary.Orphaned++
		}
		if f.IsFragmented {
			summary.Fragmented++
		}
		if f.Layout.Assumed {
			summary.Assumed++
		}
		if f.Layout.Truncated {
			summary.Truncated++
		}
	}
	return summary
}

// FilterFiles returns the rows fn accepts.
func (r *FATReport) FilterFiles(fn func(FATFile) bool) []FATFile {
	var out []FATFile
	for _, f := range r.Files {
		if fn(f) {
			out = append(out, f)
		}
	}
	return out
}

// FilesByType returns the rows of the given type: file, directory,
// volume_label or virtual.
func (r *FATReport) FilesByType(t string) []FATFile {
	return r.FilterFiles(func(f FATFile) bool { return f.Type == t })
}

// DeletedFiles returns the rows whose records carry the deletion marker.
func (r *FATReport) DeletedFiles() []FATFile {
	return r.FilterFiles(func(f FATFile) bool { return f.IsDeleted })
}

// OrphanedFiles returns the rows recovered from directory data no path reaches.
func (r *FATReport) OrphanedFiles() []FATFile {
	return r.FilterFiles(func(f FATFile) bool { return f.IsOrphaned })
}

// FragmentedFiles returns the rows occupying more than one run.
func (r *FATReport) FragmentedFiles() []FATFile {
	return r.FilterFiles(func(f FATFile) bool { return f.IsFragmented })
}

// AssumedFiles returns the rows whose extents are hypotheses rather than facts,
// which is the set a caller must not treat as located data without saying so.
func (r *FATReport) AssumedFiles() []FATFile {
	return r.FilterFiles(func(f FATFile) bool { return f.Layout.Assumed })
}
