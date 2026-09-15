package libfat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
)

func mustReport(t *testing.T, v *Volume, name string, opts ReportOptions) *FATReport {
	t.Helper()
	r, err := v.ReportWithOptions(name, opts)
	if err != nil {
		t.Fatalf("ReportWithOptions failed: %v", err)
	}
	return r
}

func rowFor(t *testing.T, r *FATReport, filename string) FATFile {
	t.Helper()
	for _, f := range r.Files {
		if f.Filename == filename {
			return f
		}
	}
	var got []string
	for _, f := range r.Files {
		got = append(got, f.Filename)
	}
	t.Fatalf("row %q not in report; got %v", filename, got)
	return FATFile{}
}

func hasRow(r *FATReport, filename string) bool {
	for _, f := range r.Files {
		if f.Filename == filename {
			return true
		}
	}
	return false
}

// decodeReport marshals the report and decodes it into untyped maps, which is
// how the JSON shape is asserted rather than the Go struct.
func decodeReport(t *testing.T, r *FATReport) map[string]any {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	return out
}

func keysOf(t *testing.T, m map[string]any) []string {
	t.Helper()
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestReportIncludesReachableFiles(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "TOP", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addSubdir("SUB", []uint32{10, 11}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))

	v := im.volume()
	defer v.Close()

	r := mustReport(t, v, "image.img", ReportOptions{})
	if r.Name != "image.img" {
		t.Fatalf("Name = %q, want image.img", r.Name)
	}
	for _, want := range []string{"/TOP.TXT", "/SUB", "/SUB/CHILD.TXT"} {
		if !hasRow(r, want) {
			t.Fatalf("%s missing from the report", want)
		}
	}
	if got := rowFor(t, r, "/SUB").Type; got != "directory" {
		t.Fatalf("/SUB type = %q, want directory", got)
	}
	if got := rowFor(t, r, "/TOP.TXT").Type; got != "file" {
		t.Fatalf("/TOP.TXT type = %q, want file", got)
	}
}

func TestReportZeroValueExcludesDeletedAndOrphans(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6}, size: 4, deleted: true})
	im.addDeletedSubdir("LOSTDIR", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))

	v := im.volume()
	defer v.Close()

	r := mustReport(t, v, "image.img", ReportOptions{})
	if len(r.DeletedFiles()) != 0 {
		t.Fatalf("zero-value report carried %d deleted rows", len(r.DeletedFiles()))
	}
	if len(r.OrphanedFiles()) != 0 {
		t.Fatalf("zero-value report carried %d orphaned rows", len(r.OrphanedFiles()))
	}
	if !hasRow(r, "/LIVE.TXT") {
		t.Fatal("the live file is missing")
	}
}

func TestReportDeepIncludesDeletedAndOrphans(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6}, size: 4, deleted: true})
	im.addDeletedSubdir("LOSTDIR", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))

	v := im.volume()
	defer v.Close()

	r, err := v.ReportDeep("image.img")
	if err != nil {
		t.Fatalf("ReportDeep failed: %v", err)
	}
	if len(r.DeletedFiles()) == 0 {
		t.Fatal("ReportDeep carried no deleted rows")
	}
	if len(r.OrphanedFiles()) == 0 {
		t.Fatal("ReportDeep carried no orphaned rows")
	}
}

// TestReportDeepDoesNotAssumeContiguity pins the rule that deep means more
// places searched, never weaker evidence.
func TestReportDeepDoesNotAssumeContiguity(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6, 7, 8}, size: 1536, deleted: true})

	v := im.volume()
	defer v.Close()

	r, err := v.ReportDeep("image.img")
	if err != nil {
		t.Fatalf("ReportDeep failed: %v", err)
	}
	if len(r.Files) == 0 {
		t.Fatal("the report is empty")
	}
	for _, f := range r.Files {
		if f.Layout.Assumed {
			t.Fatalf("%s carried an assumed extent from ReportDeep", f.Filename)
		}
	}
	if len(r.AssumedFiles()) != 0 {
		t.Fatalf("AssumedFiles returned %d rows", len(r.AssumedFiles()))
	}

	// The caller who wants the hypothesis has to ask for it, and gets it
	// flagged.
	assumed, err := v.ReportWithOptions("image.img", ReportOptions{
		IncludeDeleted: true,
		Fragments:      FragmentOptions{AssumeContiguous: true},
	})
	if err != nil {
		t.Fatalf("ReportWithOptions failed: %v", err)
	}
	if len(assumed.AssumedFiles()) == 0 {
		t.Fatal("AssumeContiguous produced no rows flagged as assumed")
	}
}

func TestReportFragmentEndOffsetIsExclusive(t *testing.T) {
	im := newTestImage(t, FATType16)
	// Clusters 10 and 11 coalesce into one run; 30 is a second run.
	im.addFile(testFile{
		base: "FRAG", ext: "BIN", clusters: []uint32{10, 11, 30},
		size: 1536, terminate: true,
	})

	v := im.volume()
	defer v.Close()

	row := rowFor(t, mustReport(t, v, "image.img", ReportOptions{}), "/FRAG.BIN")
	if !row.IsFragmented {
		t.Fatal("the file should be reported as fragmented")
	}
	if len(row.Fragments) != 2 {
		t.Fatalf("got %d fragments, want 2", len(row.Fragments))
	}

	first := row.Fragments[0]
	if first.StartOffset != im.clusterOffset(10) {
		t.Fatalf("first fragment starts at %d, want %d", first.StartOffset, im.clusterOffset(10))
	}
	// Exclusive: one past the last byte, which is the start of cluster 12.
	if first.EndOffset != im.clusterOffset(12) {
		t.Fatalf("first fragment ends at %d, want %d (exclusive)", first.EndOffset, im.clusterOffset(12))
	}
	if first.FileOffset != 0 {
		t.Fatalf("first fragment FileOffset = %d, want 0", first.FileOffset)
	}

	second := row.Fragments[1]
	if second.FileOffset != first.Length {
		t.Fatalf("fragments do not tile the file: %d then %d", first.Length, second.FileOffset)
	}
	for i, f := range row.Fragments {
		if f.EndOffset != f.StartOffset+f.Length {
			t.Fatalf("fragment %d: EndOffset %d != StartOffset+Length %d", i, f.EndOffset, f.StartOffset+f.Length)
		}
		if f.Sparse {
			t.Fatalf("fragment %d reported Sparse on FAT", i)
		}
	}
}

func TestReportFragmentsCarryClusterAddressing(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "FRAG", ext: "BIN", clusters: []uint32{10, 11, 30},
		size: 1536, terminate: true,
	})

	v := im.volume()
	defer v.Close()

	entry := entryNamed(t, v, "FRAG.BIN")
	ranges, err := v.FragmentOffsets(entry)
	if err != nil {
		t.Fatalf("FragmentOffsets failed: %v", err)
	}
	row := rowFor(t, mustReport(t, v, "image.img", ReportOptions{}), "/FRAG.BIN")
	if len(row.Fragments) != len(ranges) {
		t.Fatalf("got %d fragments, want %d", len(row.Fragments), len(ranges))
	}
	for i := range ranges {
		if row.Fragments[i].StartCluster != ranges[i].StartCluster {
			t.Fatalf("fragment %d: StartCluster %d != %d", i,
				row.Fragments[i].StartCluster, ranges[i].StartCluster)
		}
		if row.Fragments[i].ClusterCount != ranges[i].ClusterCount {
			t.Fatalf("fragment %d: ClusterCount %d != %d", i,
				row.Fragments[i].ClusterCount, ranges[i].ClusterCount)
		}
	}
}

// TestReportProvenanceFlagsSurviveJSON is the point of the report type: the
// flags that separate a FAT-verified extent from a hypothesis must reach the
// consumer, and a false must be present rather than omitted.
func TestReportProvenanceFlagsSurviveJSON(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6, 7}, size: 1024, deleted: true})

	v := im.volume()
	defer v.Close()

	r, err := v.ReportDeep("image.img")
	if err != nil {
		t.Fatalf("ReportDeep failed: %v", err)
	}

	decoded := decodeReport(t, r)
	files, ok := decoded["files"].([]any)
	if !ok || len(files) == 0 {
		t.Fatalf("files missing from the encoded report: %v", keysOf(t, decoded))
	}

	var deleted map[string]any
	for _, f := range files {
		row := f.(map[string]any)
		if row["is_deleted"] == true {
			deleted = row
		}
	}
	if deleted == nil {
		t.Fatal("no deleted row in the encoded report")
	}

	layout, ok := deleted["layout"].(map[string]any)
	if !ok {
		t.Fatalf("layout missing from the row: %v", keysOf(t, deleted))
	}
	want := []string{
		"assumed", "bytes_covered", "chain_broken", "chain_walked",
		"clusters_walked", "first_cluster_reallocated", "loop_detected", "truncated",
	}
	for _, key := range want {
		if _, present := layout[key]; !present {
			t.Fatalf("layout key %q was omitted; got %v", key, keysOf(t, layout))
		}
	}
	// A deleted entry's extent is not chain-derived, and the false must be
	// present rather than dropped: an omitted key is indistinguishable from a
	// field this version does not emit.
	if layout["chain_walked"] != false {
		t.Fatalf("chain_walked = %v, want a present false for a deleted entry", layout["chain_walked"])
	}
	if layout["truncated"] != true {
		t.Fatalf("truncated = %v, want true for a deleted entry", layout["truncated"])
	}
}

func TestReportRowForUnresolvableExtentKeepsTheRow(t *testing.T) {
	im := newTestImage(t, FATType16)
	v0 := im.volume()
	beyond := v0.maxClusterNumber() + 5
	v0.Close()

	im.addRootRaw(makeTimestampedEntry("BADFILE", "TXT", 0x20, beyond, 64))

	v := im.volume()
	defer v.Close()

	row := rowFor(t, mustReport(t, v, "image.img", ReportOptions{}), "/BADFILE.TXT")
	if len(row.Fragments) != 0 {
		t.Fatalf("got %d fragments for an unresolvable extent, want 0", len(row.Fragments))
	}
	if row.Layout.Error == "" {
		t.Fatal("the row kept no reason for the failed resolution")
	}
}

func TestReportTimestampsOmitZero(t *testing.T) {
	im := newTestImage(t, FATType16)
	// addFile writes a modification time but no creation or access date, so
	// those decode to the zero time.
	im.addFile(testFile{base: "TIMED", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	entry := entryNamed(t, v, "TIMED.TXT")
	if !entry.CreatedAt.IsZero() {
		t.Skip("the builder now records a creation time, so this case no longer applies")
	}

	r := mustReport(t, v, "image.img", ReportOptions{})
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	if bytes.Contains(raw, []byte("0001-01-01")) {
		t.Fatal("a timestamp the volume never recorded was rendered as a zero date")
	}

	files := decodeReport(t, r)["files"].([]any)
	row := files[0].(map[string]any)
	stamps := row["timestamps"].(map[string]any)
	if _, present := stamps["created"]; present {
		t.Fatalf("an unrecorded creation time was emitted: %v", stamps)
	}
	if _, present := stamps["modified"]; !present {
		t.Fatalf("the recorded modification time was omitted: %v", stamps)
	}
}

// TestReportIdentityScalarsAreNeverOmitted guards the rule that a report row is
// a thing that gets diffed, so its key set must be stable even when every
// identity value is zero.
func TestReportIdentityScalarsAreNeverOmitted(t *testing.T) {
	im := newTestImage(t, FATType16)
	// The first entry of a FAT16 root: parent cluster 0, slot 0.
	im.addFile(testFile{base: "FIRST", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	r := mustReport(t, v, "image.img", ReportOptions{})
	row := rowFor(t, r, "/FIRST.TXT")
	if row.ParentFirstCluster != 0 || row.EntrySlotIndex != 0 {
		t.Fatalf("expected the zero identity, got %d:%d", row.ParentFirstCluster, row.EntrySlotIndex)
	}

	files := decodeReport(t, r)["files"].([]any)
	encoded := files[0].(map[string]any)
	for _, key := range []string{"parent_first_cluster", "entry_slot_index", "first_cluster"} {
		if _, present := encoded[key]; !present {
			t.Fatalf("identity key %q was omitted when zero; got %v", key, keysOf(t, encoded))
		}
	}
}

func TestReportJSONKeysAreSnakeCase(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "ONE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	decoded := decodeReport(t, mustReport(t, v, "image.img", ReportOptions{}))

	wantTop := []string{
		"fat_meta", "files", "name", "start_offset", "end_offset",
		"schema_version", "library_version", "generated",
	}
	sort.Strings(wantTop)
	if got := keysOf(t, decoded); !equalStrings(got, wantTop) {
		t.Fatalf("top-level keys = %v, want %v", got, wantTop)
	}

	wantMeta := []string{
		"block_size", "cluster_count", "fat_mirror_mismatches", "offset",
		"sector_size", "type", "used_backup_boot_sector", "volume_label", "volume_serial",
	}
	sort.Strings(wantMeta)
	if got := keysOf(t, decoded["fat_meta"].(map[string]any)); !equalStrings(got, wantMeta) {
		t.Fatalf("fat_meta keys = %v, want %v", got, wantMeta)
	}

	wantRow := []string{
		"attributes", "cluster_allocated", "entry_absolute_offset", "entry_slot_index",
		"filename", "first_cluster", "fragments", "is_deleted", "is_fragmented",
		"is_orphaned", "is_virtual", "layout", "lfn_entry_offset", "name", "name_source",
		"parent_first_cluster", "path", "short_name", "size", "timestamps", "type",
	}
	sort.Strings(wantRow)
	files := decoded["files"].([]any)
	if got := keysOf(t, files[0].(map[string]any)); !equalStrings(got, wantRow) {
		t.Fatalf("row keys = %v, want %v", got, wantRow)
	}

	for _, key := range keysOf(t, decoded) {
		if strings.ToLower(key) != key {
			t.Fatalf("key %q is not snake_case", key)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReportNameSourceMarshalsAsLabel(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{
		base: "LONGNM", ext: "TXT", longName: "a long name.txt",
		clusters: []uint32{5}, size: 4, terminate: true,
	})

	v := im.volume()
	defer v.Close()

	files := decodeReport(t, mustReport(t, v, "image.img", ReportOptions{}))["files"].([]any)
	row := files[0].(map[string]any)
	if got := row["name_source"]; got != "lfn" {
		t.Fatalf("name_source = %v, want the label \"lfn\"", got)
	}
}

func TestNameSourceRoundTripsThroughJSON(t *testing.T) {
	for _, source := range []NameSource{NameSourceShort, NameSourceLFN, NameSourceRecoveredLFN} {
		raw, err := json.Marshal(source)
		if err != nil {
			t.Fatalf("Marshal(%v) failed: %v", source, err)
		}
		var back NameSource
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("Unmarshal(%s) failed: %v", raw, err)
		}
		if back != source {
			t.Fatalf("round trip changed %v to %v", source, back)
		}
	}

	var unknown NameSource
	if err := json.Unmarshal([]byte(`"something-else"`), &unknown); err == nil {
		t.Fatal("an unknown name source decoded silently instead of erroring")
	}
}

func TestReportSummaryCounts(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6}, size: 4, deleted: true})
	im.addSubdir("SUB", []uint32{10, 11})
	im.addFile(testFile{base: "FRAG", ext: "BIN", clusters: []uint32{20, 21, 40}, size: 1536, terminate: true})

	v := im.volume()
	defer v.Close()

	r, err := v.ReportDeep("image.img")
	if err != nil {
		t.Fatalf("ReportDeep failed: %v", err)
	}
	s := r.Summary()
	if s.Total != len(r.Files) {
		t.Fatalf("Total = %d, want %d", s.Total, len(r.Files))
	}
	if s.Deleted != len(r.DeletedFiles()) {
		t.Fatalf("Deleted = %d, want %d", s.Deleted, len(r.DeletedFiles()))
	}
	if s.Fragmented != len(r.FragmentedFiles()) {
		t.Fatalf("Fragmented = %d, want %d", s.Fragmented, len(r.FragmentedFiles()))
	}
	if s.TypeCounts["directory"] == 0 {
		t.Fatalf("TypeCounts recorded no directories: %v", s.TypeCounts)
	}
}

func TestReportFilterHelpers(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addSubdir("SUB", []uint32{10, 11})

	v := im.volume()
	defer v.Close()

	r := mustReport(t, v, "image.img", ReportOptions{})
	if len(r.FilesByType("directory")) != 1 {
		t.Fatalf("FilesByType(directory) = %d, want 1", len(r.FilesByType("directory")))
	}
	if len(r.FilesByType("file")) != 1 {
		t.Fatalf("FilesByType(file) = %d, want 1", len(r.FilesByType("file")))
	}
	if got := r.FilterFiles(func(f FATFile) bool { return f.Size > 0 }); len(got) != 1 {
		t.Fatalf("FilterFiles matched %d rows, want 1", len(got))
	}
}

func TestReportEndOffsetIsVolumeSize(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	r := mustReport(t, v, "image.img", ReportOptions{})
	if r.StartOffset != 0 {
		t.Fatalf("StartOffset = %d, want 0", r.StartOffset)
	}
	if r.EndOffset != int64(v.VolumeSize()) {
		t.Fatalf("EndOffset = %d, want the volume size %d", r.EndOffset, v.VolumeSize())
	}
}

func TestReportMetaDescribesTheVolume(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	meta := mustReport(t, v, "image.img", ReportOptions{}).Filesystem
	if meta.Type != FATType16 {
		t.Fatalf("Type = %q, want %q", meta.Type, FATType16)
	}
	if meta.BlockSize != int(v.BytesPerCluster()) {
		t.Fatalf("BlockSize = %d, want %d", meta.BlockSize, v.BytesPerCluster())
	}
	if meta.SectorSize != int(v.BytesPerSector()) {
		t.Fatalf("SectorSize = %d, want %d", meta.SectorSize, v.BytesPerSector())
	}
	if meta.ClusterCount != v.ClusterCount() {
		t.Fatalf("ClusterCount = %d, want %d", meta.ClusterCount, v.ClusterCount())
	}
	wantOffset, err := v.ClusterToOffset(2)
	if err != nil {
		t.Fatalf("ClusterToOffset failed: %v", err)
	}
	if meta.Offset != wantOffset {
		t.Fatalf("Offset = %d, want the first data cluster at %d", meta.Offset, wantOffset)
	}
	if meta.UsedBackupBootSector {
		t.Fatal("a clean image reported that its backup boot sector was used")
	}
}

func TestReportIncludeSlack(t *testing.T) {
	im := newTestImage(t, FATType16)
	// 100 bytes in a 512-byte cluster leaves 412 bytes of slack.
	im.addFile(testFile{base: "SHORT", ext: "TXT", clusters: []uint32{5}, size: 100, terminate: true})

	v := im.volume()
	defer v.Close()

	without := rowFor(t, mustReport(t, v, "image.img", ReportOptions{}), "/SHORT.TXT")
	if without.Slack != nil {
		t.Fatal("slack was reported without IncludeSlack")
	}

	with := rowFor(t, mustReport(t, v, "image.img", ReportOptions{IncludeSlack: true}), "/SHORT.TXT")
	if with.Slack == nil {
		t.Fatal("IncludeSlack produced no slack range")
	}
	if with.Slack.Length != int64(im.bytesPerCluster())-100 {
		t.Fatalf("slack length = %d, want %d", with.Slack.Length, int64(im.bytesPerCluster())-100)
	}
	if with.Slack.FileOffset != 100 {
		t.Fatalf("slack FileOffset = %d, want the file size 100", with.Slack.FileOffset)
	}
}

func TestWriteReportProducesIndentedJSON(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "ONE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	var buf bytes.Buffer
	if err := v.WriteReport("image.img", &buf); err != nil {
		t.Fatalf("WriteReport failed: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("\n  \"name\": \"image.img\"")) {
		t.Fatalf("output is not indented JSON:\n%s", buf.String())
	}

	var back FATReport
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatalf("the written report does not decode: %v", err)
	}
	if back.Name != "image.img" {
		t.Fatalf("decoded Name = %q", back.Name)
	}
}

func TestWriteReportRejectsNilWriter(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	if err := v.WriteReport("image.img", nil); err == nil {
		t.Fatal("WriteReport accepted a nil writer")
	}
}

func TestWriteReportWithOptionsContextCancelledWritesNothing(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "ONE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	err := v.WriteReportWithOptionsContext(ctx, "image.img", ReportOptions{}, &buf)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("a cancelled report wrote %d bytes; a truncated document that looks complete is worse than none",
			buf.Len())
	}
}

// TestReportCancelledReturnsPartialReport pins this package's rule that a
// partial result is reported rather than discarded, unlike the sibling
// libraries' all-or-nothing reports. The cancellation is arranged to land well
// after the walk has begun but before it would finish, so the report stops at
// the first checkpoint that follows it.
func TestReportCancelledReturnsPartialReport(t *testing.T) {
	im, total := pacedTreeImage(t)

	// The walk consults the context every cancellationCheckInterval entries, so
	// a cancellation between the first and second checkpoints is noticed at the
	// second. Five hundred reads is comfortably inside that window: the tree
	// takes several thousand reads to walk in full.
	v, ctx := im.volumeCancellingAfter(500)
	defer v.Close()

	report, err := v.ReportWithOptionsContext(ctx, "image.img", ReportOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if report == nil {
		t.Fatal("a cancelled report must still return the rows it gathered")
	}
	if len(report.Files) != cancellationCheckInterval {
		t.Fatalf("partial report carried %d rows, want %d: the walk should stop at the checkpoint "+
			"following the cancellation", len(report.Files), cancellationCheckInterval)
	}
	if len(report.Files) >= total {
		t.Fatal("the report was not truncated at all")
	}
}

func TestReportOnClosedVolume(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err := v.Report("image.img"); !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("Report on a closed volume = %v, want ErrVolumeClosed", err)
	}
}

func TestReportRejectsNilContext(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	//lint:ignore SA1012 passing nil is exactly what this test asserts about.
	_, err := v.ReportWithOptionsContext(nil, "image.img", ReportOptions{}) //nolint:staticcheck
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("err = %v, want ErrNilContext", err)
	}
}

func TestDirEntryJSONTagsAreSnakeCase(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "TAGGED", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	raw, err := json.Marshal(entryNamed(t, v, "TAGGED.TXT"))
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	for _, key := range []string{
		"name", "path", "short_name", "is_directory", "size", "first_cluster",
		"cluster_allocated", "attributes", "deleted", "entry_offset",
		"entry_absolute_offset", "lfn_entry_offset", "directory_path",
		"parent_first_cluster", "name_source", "orphaned",
	} {
		if _, present := decoded[key]; !present {
			t.Fatalf("DirEntry key %q missing; got %v", key, keysOf(t, decoded))
		}
	}
	if _, present := decoded["Name"]; present {
		t.Fatal("DirEntry still marshals an untagged Go field name")
	}
	if _, present := decoded["created_at"]; present {
		t.Fatal("an unrecorded creation time was emitted rather than omitted")
	}
}

func TestRangeAndFragmentResultJSONTags(t *testing.T) {
	raw, err := json.Marshal(Range{StartByte: 1, Length: 2, StartCluster: 3, ClusterCount: 4})
	if err != nil {
		t.Fatalf("json.Marshal(Range) failed: %v", err)
	}
	var r map[string]any
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	wantRange := []string{"cluster_count", "file_offset", "length", "sparse", "start_byte", "start_cluster"}
	if got := keysOf(t, r); !equalStrings(got, wantRange) {
		t.Fatalf("Range keys = %v, want %v", got, wantRange)
	}

	raw, err = json.Marshal(FragmentResult{})
	if err != nil {
		t.Fatalf("json.Marshal(FragmentResult) failed: %v", err)
	}
	var fr map[string]any
	if err := json.Unmarshal(raw, &fr); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	for _, key := range []string{
		"ranges", "bytes_covered", "chain_walked", "truncated", "assumed",
		"chain_broken", "loop_detected", "first_cluster_reallocated", "clusters_walked",
	} {
		if _, present := fr[key]; !present {
			t.Fatalf("FragmentResult key %q missing; got %v", key, keysOf(t, fr))
		}
	}
}
