package libfat

import (
	"context"
	"errors"
	"testing"
)

// walkRecord is one callback invocation, kept so that order and arguments can
// be asserted together.
type walkRecord struct {
	path   string
	parent uint32
	entry  DirEntry
}

func collectWalk(t *testing.T, v *Volume, opts WalkOptions) []walkRecord {
	t.Helper()
	var got []walkRecord
	err := v.WalkWithOptions(context.Background(), opts, func(p string, parent uint32, e DirEntry) error {
		got = append(got, walkRecord{path: p, parent: parent, entry: e})
		return nil
	})
	if err != nil {
		t.Fatalf("WalkWithOptions failed: %v", err)
	}
	return got
}

func walkPaths(records []walkRecord) []string {
	paths := make([]string, 0, len(records))
	for _, r := range records {
		paths = append(paths, r.path)
	}
	return paths
}

func containsPath(records []walkRecord, want string) bool {
	for _, r := range records {
		if r.path == want {
			return true
		}
	}
	return false
}

func recordFor(t *testing.T, records []walkRecord, want string) walkRecord {
	t.Helper()
	for _, r := range records {
		if r.path == want {
			return r
		}
	}
	t.Fatalf("path %q not walked; got %v", want, walkPaths(records))
	return walkRecord{}
}

// TestWalkReportsChildrenInPreOrderDiskOrder pins both halves of the ordering
// contract: entries appear in the order their records sit in the directory, and
// a subdirectory's subtree precedes its next sibling.
func TestWalkReportsChildrenInPreOrderDiskOrder(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "AAA", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addSubdir("SUB", []uint32{10, 11}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))
	im.addFile(testFile{base: "ZZZ", ext: "TXT", clusters: []uint32{6}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	got := walkPaths(collectWalk(t, v, WalkOptions{}))
	want := []string{"/AAA.TXT", "/SUB", "/SUB/CHILD.TXT", "/ZZZ.TXT"}
	if len(got) != len(want) {
		t.Fatalf("walked %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walked %v, want %v", got, want)
		}
	}
}

// TestWalkDoesNotReportRoot guards the decision not to synthesise a DirEntry
// for a directory that has no record anywhere on the volume.
func TestWalkDoesNotReportRoot(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "ONLY", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	v := im.volume()
	defer v.Close()

	for _, r := range collectWalk(t, v, WalkOptions{}) {
		if r.path == "/" {
			t.Fatal("the root directory was reported")
		}
	}
}

// TestWalkArgumentsMatchTheEntry asserts the two documented redundancies so
// they cannot drift apart from the entry they duplicate.
func TestWalkArgumentsMatchTheEntry(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "TOP", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addSubdir("SUB", []uint32{10, 11}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))

	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{})
	if len(records) == 0 {
		t.Fatal("nothing was walked")
	}
	for _, r := range records {
		if r.path != r.entry.Path {
			t.Fatalf("path argument %q != entry.Path %q", r.path, r.entry.Path)
		}
		if r.parent != r.entry.ParentFirstCluster {
			t.Fatalf("%s: parent argument %d != entry.ParentFirstCluster %d",
				r.path, r.parent, r.entry.ParentFirstCluster)
		}
	}
}

func TestWalkParentClusterForFixedRootRegionIsZero(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			im.addFile(testFile{base: "ROOTED", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
			v := im.volume()
			defer v.Close()

			r := recordFor(t, collectWalk(t, v, WalkOptions{}), "/ROOTED.TXT")
			if r.parent != FixedRootCluster {
				t.Fatalf("parent = %d, want FixedRootCluster", r.parent)
			}
		})
	}
}

func TestWalkParentClusterForFAT32RootIsRootCluster(t *testing.T) {
	im := newTestImage(t, FATType32)
	im.addFile(testFile{base: "ROOTED", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	v := im.volume()
	defer v.Close()

	r := recordFor(t, collectWalk(t, v, WalkOptions{}), "/ROOTED.TXT")
	if r.parent != v.RootCluster() {
		t.Fatalf("parent = %d, want root cluster %d", r.parent, v.RootCluster())
	}
}

func TestWalkParentClusterForSubdirectoryChildren(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addSubdir("SUB", []uint32{10, 11}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))
	v := im.volume()
	defer v.Close()

	r := recordFor(t, collectWalk(t, v, WalkOptions{}), "/SUB/CHILD.TXT")
	if r.parent != 10 {
		t.Fatalf("parent = %d, want the subdirectory's first cluster 10", r.parent)
	}
}

func TestWalkExcludesDeletedByDefault(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6}, size: 4, deleted: true})
	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{})
	if !containsPath(records, "/LIVE.TXT") {
		t.Fatalf("live entry missing; walked %v", walkPaths(records))
	}
	if containsPath(records, "/GONE.TXT") {
		t.Fatalf("deleted entry reported without IncludeDeleted; walked %v", walkPaths(records))
	}
}

func TestWalkIncludesDeletedWhenRequested(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6}, size: 4, deleted: true})
	v := im.volume()
	defer v.Close()

	// Deletion overwrites the first character of the name with 0xE5, which the
	// parser renders as an underscore placeholder.
	records := collectWalk(t, v, WalkOptions{IncludeDeleted: true})
	r := recordFor(t, records, "/_ONE.TXT")
	if !r.entry.Deleted {
		t.Fatal("entry reported without its Deleted flag")
	}
	if !r.entry.FirstCharRecovered && r.entry.Name[0] != '_' {
		t.Fatalf("unexpected deleted name %q", r.entry.Name)
	}
}

// deletedDirPath is where the deleted directory appears once parsed: deletion
// overwrote the "D" of DELDIR with the 0xE5 marker, which renders as "_".
const deletedDirPath = "/_ELDIR"

// deletedDirImage builds a deleted subdirectory whose first cluster still holds
// its "." and ".." records and one child, with a second child written into the
// run's second cluster. Only the first child is recoverable, because the FAT
// chain that led to the second is gone.
func deletedDirImage(t *testing.T) *testImage {
	t.Helper()
	im := newTestImage(t, FATType16)
	im.addDeletedSubdir("DELDIR", []uint32{20, 21}, true,
		makeTimestampedEntry("FIRST", "TXT", 0x20, 25, 10))

	// A record in the second cluster of the run, reachable only by assuming
	// contiguity or by walking a chain that no longer exists.
	second := make([]byte, dirEntrySize)
	copy(second, makeTimestampedEntry("SECOND", "TXT", 0x20, 26, 10))
	im.writeRawCluster(21, second)
	return im
}

func TestWalkDoesNotDescendDeletedDirectoryByDefault(t *testing.T) {
	im := deletedDirImage(t)
	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{IncludeDeleted: true})
	if !containsPath(records, deletedDirPath) {
		t.Fatalf("deleted directory not reported; walked %v", walkPaths(records))
	}
	if containsPath(records, deletedDirPath+"/FIRST.TXT") {
		t.Fatalf("descended into a deleted directory without being asked; walked %v", walkPaths(records))
	}
}

// TestWalkDescendsDeletedDirectoryOnlyIntoItsFirstCluster is the test that
// proves the descent neither walks the FAT nor assumes contiguity: the record
// in the run's second cluster must not appear.
func TestWalkDescendsDeletedDirectoryOnlyIntoItsFirstCluster(t *testing.T) {
	im := deletedDirImage(t)
	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{IncludeDeleted: true, DescendDeletedDirectories: true})
	if !containsPath(records, deletedDirPath+"/FIRST.TXT") {
		t.Fatalf("first-cluster child not recovered; walked %v", walkPaths(records))
	}
	if containsPath(records, deletedDirPath+"/SECOND.TXT") {
		t.Fatalf("a record beyond the first cluster was attributed to the deleted directory; walked %v",
			walkPaths(records))
	}
}

func TestWalkRefusesDeletedDirectoryWithReallocatedFirstCluster(t *testing.T) {
	im := deletedDirImage(t)
	// The cluster has been handed to a later file, so its contents are no
	// longer this directory's.
	im.linkChain([]uint32{20}, true)

	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{IncludeDeleted: true, DescendDeletedDirectories: true})
	if containsPath(records, deletedDirPath+"/FIRST.TXT") {
		t.Fatalf("descended into a reallocated cluster; walked %v", walkPaths(records))
	}
}

func TestWalkRefusesDeletedDirectoryWithoutDotRecords(t *testing.T) {
	im := deletedDirImage(t)
	// Overwrite the "." and ".." records, which is what the stale-copy guard
	// exists to notice.
	im.writeRawCluster(20, patternBytes(64, 0x11))

	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{IncludeDeleted: true, DescendDeletedDirectories: true})
	if containsPath(records, deletedDirPath+"/FIRST.TXT") {
		t.Fatalf("descended into a cluster with no dot records; walked %v", walkPaths(records))
	}
}

func TestWalkCycleGuardStopsSelfReferentialDirectory(t *testing.T) {
	im := newTestImage(t, FATType16)
	// The subdirectory names itself, which a valid volume cannot produce.
	im.addSubdir("SUB", []uint32{10}, makeTimestampedEntry("LOOP", "", 0x10, 10, 0))

	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{})
	loops := 0
	for _, r := range records {
		if r.entry.Name == "LOOP" {
			loops++
		}
	}
	if loops != 1 {
		t.Fatalf("the self-reference was reported %d times, want exactly 1; walked %v", loops, walkPaths(records))
	}
}

func TestWalkDepthCapStopsDescentWithoutError(t *testing.T) {
	im := newTestImage(t, FATType16)
	// OUTER (cluster 10) holds INNER (cluster 12), which holds DEEP.TXT.
	im.writeSubdir([]uint32{12}, 10, makeTimestampedEntry("DEEP", "TXT", 0x20, 30, 4))
	im.addSubdir("OUTER", []uint32{10}, makeTimestampedEntry("INNER", "", 0x10, 12, 0))

	v := im.volume()
	defer v.Close()

	full := collectWalk(t, v, WalkOptions{})
	if !containsPath(full, "/OUTER/INNER/DEEP.TXT") {
		t.Fatalf("the unbounded walk should reach the deepest entry; walked %v", walkPaths(full))
	}

	// MaxDepth counts levels reported, so 1 is the root's children alone.
	shallow := collectWalk(t, v, WalkOptions{MaxDepth: 1})
	if !containsPath(shallow, "/OUTER") {
		t.Fatalf("the root's children should always be reported; walked %v", walkPaths(shallow))
	}
	if containsPath(shallow, "/OUTER/INNER") {
		t.Fatalf("MaxDepth 1 descended below the root; walked %v", walkPaths(shallow))
	}

	// At 2, the directory sitting on the limit is reported but not read.
	capped := collectWalk(t, v, WalkOptions{MaxDepth: 2})
	if !containsPath(capped, "/OUTER/INNER") {
		t.Fatalf("a directory at the limit should still be reported; walked %v", walkPaths(capped))
	}
	if containsPath(capped, "/OUTER/INNER/DEEP.TXT") {
		t.Fatalf("the depth cap did not stop the descent; walked %v", walkPaths(capped))
	}
}

func TestWalkCallbackErrorAbortsAndIsReturnedUnchanged(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "FIRST", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addFile(testFile{base: "SECOND", ext: "TXT", clusters: []uint32{6}, size: 4, terminate: true})
	im.addFile(testFile{base: "THIRD", ext: "TXT", clusters: []uint32{7}, size: 4, terminate: true})

	v := im.volume()
	defer v.Close()

	sentinel := errors.New("stop here")
	calls := 0
	err := v.Walk(context.Background(), func(string, uint32, DirEntry) error {
		calls++
		if calls == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Walk returned %v, want the callback's own error", err)
	}
	if calls != 2 {
		t.Fatalf("callback ran %d times, want 2", calls)
	}
}

// unreadableDirImage names a subdirectory whose chain runs straight into a bad
// cluster: its record parses, so it is reported, but reading its contents
// fails. A file follows it so that continuation past the failure is observable.
func unreadableDirImage(t *testing.T) *testImage {
	t.Helper()
	im := newTestImage(t, FATType16)
	im.addSubdir("SUB", []uint32{10}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))
	im.setFATEntry(10, im.badCluster())
	im.addFile(testFile{base: "AFTER", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	return im
}

func TestWalkContinuesPastUnreadableDirectory(t *testing.T) {
	im := unreadableDirImage(t)

	v := im.volume()
	defer v.Close()

	var got []walkRecord
	err := v.WalkWithOptions(context.Background(), WalkOptions{}, func(p string, parent uint32, e DirEntry) error {
		got = append(got, walkRecord{path: p, parent: parent, entry: e})
		return nil
	})
	if err != nil {
		t.Fatalf("an unreadable directory should not fail the walk: %v", err)
	}
	if !containsPath(got, "/SUB") {
		t.Fatalf("the directory's own record is evidence and should be reported; walked %v", walkPaths(got))
	}
	if !containsPath(got, "/AFTER.TXT") {
		t.Fatalf("the walk did not continue past the unreadable directory; walked %v", walkPaths(got))
	}
}

func TestWalkStopOnReadErrorReturnsTheError(t *testing.T) {
	im := unreadableDirImage(t)
	v := im.volume()
	defer v.Close()

	err := v.WalkWithOptions(context.Background(), WalkOptions{StopOnReadError: true},
		func(string, uint32, DirEntry) error { return nil })
	if err == nil {
		t.Fatal("StopOnReadError did not surface the underlying error")
	}
}

func TestWalkRootReadFailureIsAlwaysAnError(t *testing.T) {
	im := newTestImage(t, FATType32)
	im.setFATEntry(im.rootCluster, im.badCluster())

	v := im.volume()
	defer v.Close()

	err := v.Walk(context.Background(), func(string, uint32, DirEntry) error { return nil })
	if err == nil {
		t.Fatal("a root that cannot be read leaves nothing to walk and must be an error")
	}
}

func TestWalkRejectsNilContext(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	//lint:ignore SA1012 passing nil is exactly what this test asserts about.
	err := v.Walk(nil, func(string, uint32, DirEntry) error { return nil }) //nolint:staticcheck
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("Walk(nil ctx) = %v, want ErrNilContext", err)
	}
}

func TestWalkRejectsNilCallback(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	if err := v.Walk(context.Background(), nil); !errors.Is(err, ErrNilCallback) {
		t.Fatalf("Walk(nil fn) = %v, want ErrNilCallback", err)
	}
}

func TestWalkOnClosedVolume(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err := v.Walk(context.Background(), func(string, uint32, DirEntry) error { return nil })
	if !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("Walk on a closed volume = %v, want ErrVolumeClosed", err)
	}
}

func TestWalkIncludesOrphansAfterTheTree(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addDeletedSubdir("GONE", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))

	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{IncludeOrphans: true})

	lastReachable, firstOrphan := -1, -1
	for i, r := range records {
		if r.entry.Orphaned {
			if firstOrphan < 0 {
				firstOrphan = i
			}
			continue
		}
		lastReachable = i
	}
	if firstOrphan < 0 {
		t.Fatalf("no orphaned entry was reported; walked %v", walkPaths(records))
	}
	if firstOrphan < lastReachable {
		t.Fatalf("orphans were interleaved with the reachable tree; walked %v", walkPaths(records))
	}
	if got := records[firstOrphan].parent; got != 30 {
		t.Fatalf("orphan parent = %d, want the run's first cluster 30", got)
	}
	if got := records[firstOrphan].path; got != OrphanPath+"/LOST.TXT" {
		t.Fatalf("orphan path = %q, want it rooted at OrphanPath", got)
	}
}

// TestWalkOrphanEntriesIgnoreIncludeDeleted pins that IncludeDeleted governs the
// reachable tree only: filtering an orphan run by the deletion marker would drop
// exactly the records the scan exists to find.
func TestWalkOrphanEntriesIgnoreIncludeDeleted(t *testing.T) {
	im := newTestImage(t, FATType16)
	deleted := makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16)
	deleted[0] = 0xE5
	im.addDeletedSubdir("GONE", []uint32{30, 31}, false, deleted)

	v := im.volume()
	defer v.Close()

	records := collectWalk(t, v, WalkOptions{IncludeOrphans: true})
	found := false
	for _, r := range records {
		if r.entry.Orphaned && r.entry.Deleted {
			found = true
		}
	}
	if !found {
		t.Fatalf("a deleted record inside an orphan run was filtered out; walked %v", walkPaths(records))
	}
}

func TestWalkVirtualEntriesFollowOpenOptions(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "REAL", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})

	plain := im.volume()
	defer plain.Close()
	for _, r := range collectWalk(t, plain, WalkOptions{}) {
		if r.entry.Virtual {
			t.Fatalf("virtual entry %q reported without IncludeVirtualRootEntries", r.path)
		}
	}

	withVirtual := im.volumeWithOptions(OpenOptions{IncludeVirtualRootEntries: true})
	defer withVirtual.Close()

	records := collectWalk(t, withVirtual, WalkOptions{})
	if !containsPath(records, "/$MBR") {
		t.Fatalf("virtual entries were not reported; walked %v", walkPaths(records))
	}
	// $OrphanFiles is a virtual directory and must never be descended into.
	for _, r := range records {
		if r.entry.Virtual && r.parent != FixedRootCluster {
			t.Fatalf("virtual entry %q was reported from inside a directory", r.path)
		}
		if r.path != OrphanPath && len(r.path) > len(OrphanPath) && r.path[:len(OrphanPath)+1] == OrphanPath+"/" {
			t.Fatalf("the walk descended into the virtual %s entry", OrphanPath)
		}
	}
}

func TestWalkAcrossFATTypes(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			im.addFile(testFile{base: "TOP", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
			im.addSubdir("SUB", []uint32{10, 11}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))

			v := im.volume()
			defer v.Close()

			records := collectWalk(t, v, WalkOptions{})
			for _, want := range []string{"/TOP.TXT", "/SUB", "/SUB/CHILD.TXT"} {
				if !containsPath(records, want) {
					t.Fatalf("%s missing; walked %v", want, walkPaths(records))
				}
			}
		})
	}
}
