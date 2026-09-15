package libfat

import (
	"encoding/json"
	"path"
	"testing"
	"time"
)

// TestReportSchemaVersionRoundTrips is the F8 acceptance case.
func TestReportSchemaVersionRoundTrips(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "A", ext: "TXT", clusters: []uint32{3},
		size: 8, content: patternBytes(8, 1), terminate: true})

	v := im.volume()
	defer v.Close()

	before := time.Now().UTC().Add(-time.Second)
	rep, err := v.Report("image.img")
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	if rep.SchemaVersion != ReportSchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", rep.SchemaVersion, ReportSchemaVersion)
	}
	if rep.SchemaVersion == 0 {
		t.Error("SchemaVersion is zero, which is indistinguishable from absent")
	}
	if rep.LibraryVersion != Version {
		t.Errorf("LibraryVersion = %q, want %q", rep.LibraryVersion, Version)
	}
	if rep.Generated.Before(before) || rep.Generated.After(after) {
		t.Errorf("Generated = %v, outside the window the report was built in", rep.Generated)
	}

	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	var back FATReport
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if back.SchemaVersion != rep.SchemaVersion {
		t.Errorf("SchemaVersion did not survive the round trip: %d", back.SchemaVersion)
	}
	if back.SchemaVersion == 0 {
		t.Error("SchemaVersion is zero after the round trip")
	}
	if back.LibraryVersion != rep.LibraryVersion {
		t.Errorf("LibraryVersion did not survive: %q", back.LibraryVersion)
	}
	if !back.Generated.Equal(rep.Generated) {
		t.Errorf("Generated did not survive: %v want %v", back.Generated, rep.Generated)
	}
}

// TestReportProvenanceOnEveryConstructor pins the provenance to all four entry
// points, since a document produced by any of them is evidence.
func TestReportProvenanceOnEveryConstructor(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	reports := map[string]func() (*FATReport, error){
		"Report":     func() (*FATReport, error) { return v.Report("i") },
		"ReportDeep": func() (*FATReport, error) { return v.ReportDeep("i") },
		"ReportWithOptions": func() (*FATReport, error) {
			return v.ReportWithOptions("i", ReportOptions{})
		},
		"ReportWithOptionsContext": func() (*FATReport, error) {
			return v.ReportWithOptionsContext(t.Context(), "i", ReportOptions{})
		},
	}
	for name, build := range reports {
		t.Run(name, func(t *testing.T) {
			rep, err := build()
			if err != nil {
				t.Fatalf("%s failed: %v", name, err)
			}
			if rep.SchemaVersion != ReportSchemaVersion {
				t.Errorf("SchemaVersion = %d, want %d", rep.SchemaVersion, ReportSchemaVersion)
			}
			if rep.LibraryVersion == "" {
				t.Error("LibraryVersion is empty")
			}
			if rep.Generated.IsZero() {
				t.Error("Generated is the zero time")
			}
		})
	}
}

// TestReportRowCarriesPathAndName is the F9 acceptance case: a report of a
// nested volume says where a file was without the caller re-walking the tree.
func TestReportRowCarriesPathAndName(t *testing.T) {
	im := newTestImage(t, FATType16)

	// Three directory levels, with the file at the bottom. Only FIRST gets a
	// root entry; the deeper two are reached through their parents' content.
	leaf := makeTimestampedEntry("DEEP", "TXT", 0x20, 30, 11)
	im.writeSubdir([]uint32{9}, 8, leaf) // THIRD, whose parent is SECOND
	third := makeTimestampedEntry("THIRD", "", 0x10, 9, 0)
	im.writeSubdir([]uint32{8}, 7, third) // SECOND, whose parent is FIRST
	second := makeTimestampedEntry("SECOND", "", 0x10, 8, 0)
	im.addSubdir("FIRST", []uint32{7}, second)

	v := im.volume()
	defer v.Close()

	rep, err := v.ReportDeep("image.img")
	if err != nil {
		t.Fatalf("ReportDeep failed: %v", err)
	}

	const wantPath = "/FIRST/SECOND/THIRD/DEEP.TXT"
	row := rowFor(t, rep, wantPath)

	if row.Path != wantPath {
		t.Errorf("Path = %q, want %q", row.Path, wantPath)
	}
	if row.Name != "DEEP.TXT" {
		t.Errorf("Name = %q, want the basename %q", row.Name, "DEEP.TXT")
	}
	if row.Name != path.Base(row.Path) {
		t.Errorf("Name %q is not the basename of Path %q", row.Name, row.Path)
	}
	// Filename is retained as the full path; the two must not drift apart.
	if row.Filename != row.Path {
		t.Errorf("Filename = %q, want it equal to Path %q", row.Filename, row.Path)
	}

	// And the path must address the same entry through the public API.
	f, err := v.OpenPath(row.Path)
	if err != nil {
		t.Fatalf("OpenPath(%q) failed: %v", row.Path, err)
	}
	if got := f.Size(); int64(got) != row.Size {
		t.Errorf("OpenPath returned a %d-byte file, report says %d", got, row.Size)
	}
}

// TestReportPathAndNameOnEveryRow holds the invariant across the whole report,
// including the virtual and volume-label rows that are not ordinary files.
func TestReportPathAndNameOnEveryRow(t *testing.T) {
	im := newTestImage(t, FATType32)
	im.addFile(testFile{base: "ROOT", ext: "BIN", longName: "root-file.bin",
		clusters: []uint32{5}, size: 4, content: patternBytes(4, 2), terminate: true})

	v := im.volumeWithOptions(OpenOptions{IncludeVirtualRootEntries: true})
	defer v.Close()

	rep, err := v.ReportDeep("image.img")
	if err != nil {
		t.Fatalf("ReportDeep failed: %v", err)
	}
	if len(rep.Files) == 0 {
		t.Fatal("report has no rows")
	}
	for _, row := range rep.Files {
		if row.Path == "" {
			t.Errorf("row %q has an empty path", row.Filename)
		}
		if row.Filename != row.Path {
			t.Errorf("row %q: Filename and Path disagree (%q)", row.Path, row.Filename)
		}
		if row.Name == "" {
			t.Errorf("row %q has an empty name", row.Path)
		}
	}
}
