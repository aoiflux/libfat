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
	second := make([]byte, DirEntrySize)
	copy(second, makeTimestampedEntry("SECOND", "TXT", 0x20, 26, 10))
	im.writeRawCluster(21, second)
	return im
}

// TestWalkDeletedDirectoryDoesNotHideALiveReuse is the regression for a deleted
// directory claiming its first cluster in the cycle guard before - and whether or
// not - it was descended into. OLD was removed and NEW created after it: NEW's
// record lands after OLD's stale one and is handed the freed cluster 20, which is
// the ordinary result of rmdir followed by mkdir. The walk used to reach OLD
// first, mark cluster 20 as walked without reading it - the descent was off, or
// the FAT said the cluster was allocated again - and then take NEW for a revisit,
// losing its whole live subtree from every walk that asked for deleted records.
func TestWalkDeletedDirectoryDoesNotHideALiveReuse(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addDeletedSubdir("OLD", []uint32{20}, true)
	im.addSubdir("NEW", []uint32{20}, makeTimestampedEntry("KEPT", "TXT", 0x20, 25, 10))
	v := im.volume()
	defer v.Close()

	for _, opts := range []WalkOptions{
		{},
		{IncludeDeleted: true},
		{IncludeDeleted: true, DescendDeletedDirectories: true},
	} {
		records := collectWalk(t, v, opts)
		if !containsPath(records, "/NEW/KEPT.TXT") {
			t.Errorf("%+v: /NEW/KEPT.TXT missing; the deleted /_LD hid the live directory reusing its cluster: %v",
				opts, walkPaths(records))
		}
		if opts.IncludeDeleted && !containsPath(records, "/_LD") {
			t.Errorf("%+v: the deleted /_LD itself was not reported: %v", opts, walkPaths(records))
		}
	}
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

// skipRecord is one OnSkip invocation.
type skipRecord struct {
	path   string
	entry  DirEntry
	reason SkipReason
	err    error
}

// collectWalkAndSkips runs a walk whose OnSkip records every call.
func collectWalkAndSkips(t *testing.T, v *Volume, opts WalkOptions) ([]walkRecord, []skipRecord) {
	t.Helper()
	var got []walkRecord
	var skips []skipRecord
	opts.OnSkip = func(p string, e DirEntry, reason SkipReason, err error) error {
		skips = append(skips, skipRecord{path: p, entry: e, reason: reason, err: err})
		return nil
	}
	err := v.WalkWithOptions(context.Background(), opts, func(p string, parent uint32, e DirEntry) error {
		got = append(got, walkRecord{path: p, parent: parent, entry: e})
		return nil
	})
	if err != nil {
		t.Fatalf("WalkWithOptions failed: %v", err)
	}
	return got, skips
}

func skipPaths(skips []skipRecord) []string {
	paths := make([]string, 0, len(skips))
	for _, s := range skips {
		paths = append(paths, string(s.reason)+" "+s.path)
	}
	return paths
}

func skipFor(t *testing.T, skips []skipRecord, want string) skipRecord {
	t.Helper()
	for _, s := range skips {
		if s.path == want {
			return s
		}
	}
	t.Fatalf("no skip reported for %q; skips were %v", want, skipPaths(skips))
	return skipRecord{}
}

// skipImage holds one directory for each live reason a walk declines to
// descend, plus an empty directory that must never be reported as skipped and a
// file after them all so that continuation stays observable.
//
//	/EMPTY             cluster 15, no children at all
//	/BADDIR            cluster 10, chain runs into a bad cluster
//	/FARDIR            first cluster 60000, past the end of the data area
//	/OUTER             cluster 20, holds INNER
//	/OUTER/INNER       cluster 22, holds LOOP
//	/OUTER/INNER/LOOP  names cluster 20 again, its own grandparent
//	/AFTER.TXT
func skipImage(t *testing.T) *testImage {
	t.Helper()
	im := newTestImage(t, FATType16)

	im.addSubdir("EMPTY", []uint32{15})

	im.addSubdir("BADDIR", []uint32{10}, makeTimestampedEntry("CHILD", "TXT", 0x20, 30, 8))
	im.setFATEntry(10, im.badCluster())

	// 60000 is above maxClusterNumber on this image (9888) and below the FAT16
	// end-of-chain and bad-cluster values, so the record parses and is listed.
	im.addRootRaw(makeTimestampedEntry("FARDIR", "", 0x10, 60000, 0))

	im.writeSubdir([]uint32{22}, 20, makeTimestampedEntry("LOOP", "", 0x10, 20, 0))
	im.addSubdir("OUTER", []uint32{20}, makeTimestampedEntry("INNER", "", 0x10, 22, 0))

	im.addFile(testFile{base: "AFTER", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	return im
}

// TestWalkOnSkipReportsEveryUndescendedDirectory is the headline test: each of
// the three live skips is reported once, with its own reason, and the empty
// directory - which is what all three look like without the callback - is not
// reported at all.
func TestWalkOnSkipReportsEveryUndescendedDirectory(t *testing.T) {
	im := skipImage(t)
	v := im.volume()
	defer v.Close()

	records, skips := collectWalkAndSkips(t, v, WalkOptions{})

	if len(skips) != 3 {
		t.Fatalf("got %d skips %v, want exactly 3", len(skips), skipPaths(skips))
	}

	bad := skipFor(t, skips, "/BADDIR")
	if bad.reason != SkipReadError {
		t.Fatalf("/BADDIR reason = %q, want %q", bad.reason, SkipReadError)
	}
	if bad.err == nil {
		t.Fatal("/BADDIR was reported without the underlying read error")
	}

	far := skipFor(t, skips, "/FARDIR")
	if far.reason != SkipInvalidCluster {
		t.Fatalf("/FARDIR reason = %q, want %q", far.reason, SkipInvalidCluster)
	}
	if far.err != nil {
		t.Fatalf("/FARDIR carried an error %v; no read was attempted", far.err)
	}

	loop := skipFor(t, skips, "/OUTER/INNER/LOOP")
	if loop.reason != SkipRevisit {
		t.Fatalf("the cross-link reason = %q, want %q", loop.reason, SkipRevisit)
	}
	if loop.err != nil {
		t.Fatalf("the cross-link carried an error %v; no read was attempted", loop.err)
	}

	for _, s := range skips {
		if s.path != s.entry.Path {
			t.Fatalf("skip path %q != entry.Path %q", s.path, s.entry.Path)
		}
		if !s.entry.IsDirectory {
			t.Fatalf("%s was reported skipped but is not a directory", s.path)
		}
		if s.path == "/EMPTY" {
			t.Fatal("an empty directory was reported as skipped")
		}
	}

	// Every skipped directory was handed to the walk callback first, and the
	// walk carried on to the end.
	for _, want := range []string{"/EMPTY", "/BADDIR", "/FARDIR", "/OUTER/INNER/LOOP", "/AFTER.TXT"} {
		if !containsPath(records, want) {
			t.Fatalf("%s missing from the walk; walked %v", want, walkPaths(records))
		}
	}
}

func TestWalkOnSkipReportsDepthLimit(t *testing.T) {
	im := skipImage(t)
	v := im.volume()
	defer v.Close()

	_, atTwo := collectWalkAndSkips(t, v, WalkOptions{MaxDepth: 2})
	inner := skipFor(t, atTwo, "/OUTER/INNER")
	if inner.reason != SkipDepthLimit {
		t.Fatalf("/OUTER/INNER reason = %q, want %q", inner.reason, SkipDepthLimit)
	}
	if inner.err != nil {
		t.Fatalf("/OUTER/INNER carried an error %v; the depth cap is not a failure", inner.err)
	}

	// The depth check precedes the cluster and revisit checks, so a directory on
	// the limit reports the cap whatever else is wrong with it.
	_, atOne := collectWalkAndSkips(t, v, WalkOptions{MaxDepth: 1})
	outer := skipFor(t, atOne, "/OUTER")
	if outer.reason != SkipDepthLimit {
		t.Fatalf("/OUTER reason = %q, want %q", outer.reason, SkipDepthLimit)
	}
	far := skipFor(t, atOne, "/FARDIR")
	if far.reason != SkipDepthLimit {
		t.Fatalf("/FARDIR at the depth limit reason = %q, want %q", far.reason, SkipDepthLimit)
	}
}

// TestWalkOnSkipReportsDeletedDirectoryRefusedByGuard covers the one skip that
// is the stale-copy guard's own refusal rather than a limit or a failed read.
func TestWalkOnSkipReportsDeletedDirectoryRefusedByGuard(t *testing.T) {
	t.Run("reallocated first cluster", func(t *testing.T) {
		im := deletedDirImage(t)
		im.linkChain([]uint32{20}, true)
		v := im.volume()
		defer v.Close()

		_, skips := collectWalkAndSkips(t, v,
			WalkOptions{IncludeDeleted: true, DescendDeletedDirectories: true})
		if len(skips) != 1 {
			t.Fatalf("got %d skips %v, want exactly 1", len(skips), skipPaths(skips))
		}
		if s := skipFor(t, skips, deletedDirPath); s.reason != SkipDeletedUnrecoverable {
			t.Fatalf("reason = %q, want %q", s.reason, SkipDeletedUnrecoverable)
		}
	})

	t.Run("dot records overwritten", func(t *testing.T) {
		im := deletedDirImage(t)
		im.writeRawCluster(20, patternBytes(64, 0x11))
		v := im.volume()
		defer v.Close()

		_, skips := collectWalkAndSkips(t, v,
			WalkOptions{IncludeDeleted: true, DescendDeletedDirectories: true})
		if s := skipFor(t, skips, deletedDirPath); s.reason != SkipDeletedUnrecoverable {
			t.Fatalf("reason = %q, want %q", s.reason, SkipDeletedUnrecoverable)
		}
	})

	// Not descending because the option is off is the caller's own choice, not
	// something the walk decided, so it is not reported.
	t.Run("descent not requested", func(t *testing.T) {
		im := deletedDirImage(t)
		v := im.volume()
		defer v.Close()

		_, skips := collectWalkAndSkips(t, v, WalkOptions{IncludeDeleted: true})
		if len(skips) != 0 {
			t.Fatalf("got %v, want no skips when DescendDeletedDirectories is off", skipPaths(skips))
		}
	})
}

// TestWalkOnSkipIsSilentForEntriesTheWalkDidNotReport pins the invariant that
// OnSkip only ever names a record the walk callback was handed.
func TestWalkOnSkipIsSilentForEntriesTheWalkDidNotReport(t *testing.T) {
	t.Run("deleted directory left out", func(t *testing.T) {
		im := deletedDirImage(t)
		v := im.volume()
		defer v.Close()

		_, skips := collectWalkAndSkips(t, v, WalkOptions{})
		if len(skips) != 0 {
			t.Fatalf("got %v, want no skips for a record IncludeDeleted excluded", skipPaths(skips))
		}
	})

	t.Run("virtual entries and files", func(t *testing.T) {
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "REAL", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
		v := im.volumeWithOptions(OpenOptions{IncludeVirtualRootEntries: true})
		defer v.Close()

		records, skips := collectWalkAndSkips(t, v, WalkOptions{})
		if !containsPath(records, OrphanPath) {
			t.Fatalf("the virtual %s entry was not reported; walked %v", OrphanPath, walkPaths(records))
		}
		if len(skips) != 0 {
			t.Fatalf("got %v, want no skips: a virtual entry names no directory data", skipPaths(skips))
		}
	})
}

func TestWalkOnSkipErrorStopsTheWalk(t *testing.T) {
	im := unreadableDirImage(t)
	v := im.volume()
	defer v.Close()

	sentinel := errors.New("stop at the first skip")
	calls := 0
	var got []string
	err := v.WalkWithOptions(context.Background(), WalkOptions{
		OnSkip: func(string, DirEntry, SkipReason, error) error {
			calls++
			return sentinel
		},
	}, func(p string, _ uint32, _ DirEntry) error {
		got = append(got, p)
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WalkWithOptions returned %v, want the callback's own error", err)
	}
	if calls != 1 {
		t.Fatalf("OnSkip ran %d times, want 1", calls)
	}
	for _, p := range got {
		if p == "/AFTER.TXT" {
			t.Fatalf("the walk continued past the OnSkip error; walked %v", got)
		}
	}
}

// TestWalkStopOnReadErrorDoesNotCallOnSkip pins the documented interaction: the
// walk aborts, so nothing was skipped.
func TestWalkStopOnReadErrorDoesNotCallOnSkip(t *testing.T) {
	im := unreadableDirImage(t)
	v := im.volume()
	defer v.Close()

	calls := 0
	err := v.WalkWithOptions(context.Background(), WalkOptions{
		StopOnReadError: true,
		OnSkip: func(string, DirEntry, SkipReason, error) error {
			calls++
			return nil
		},
	}, func(string, uint32, DirEntry) error { return nil })
	if err == nil {
		t.Fatal("StopOnReadError did not surface the underlying error")
	}
	if calls != 0 {
		t.Fatalf("OnSkip ran %d times; an aborted walk skipped nothing", calls)
	}
}

// TestWalkWithoutOnSkipIsUnchanged is both the additivity pin and the statement
// of the defect: with no callback the empty directory and the three undescended
// ones are indistinguishable, and the walk still returns nil.
func TestWalkWithoutOnSkipIsUnchanged(t *testing.T) {
	im := skipImage(t)
	v := im.volume()
	defer v.Close()

	got := walkPaths(collectWalk(t, v, WalkOptions{}))
	want := []string{"/EMPTY", "/BADDIR", "/FARDIR", "/OUTER", "/OUTER/INNER", "/OUTER/INNER/LOOP", "/AFTER.TXT"}
	if len(got) != len(want) {
		t.Fatalf("walked %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walked %v, want %v", got, want)
		}
	}
}

// TestWalkOnOrphanScanReportsScanProvenance covers the second signal the walk
// used to throw away: the scan's own account of what it could not cover.
func TestWalkOnOrphanScanReportsScanProvenance(t *testing.T) {
	orphanImage := func(t *testing.T) *testImage {
		t.Helper()
		im := newTestImage(t, FATType16)
		im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
		im.addDeletedSubdir("GONE", []uint32{30, 31}, false,
			makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))
		return im
	}

	t.Run("unbounded scan", func(t *testing.T) {
		v := orphanImage(t).volume()
		defer v.Close()

		calls := 0
		var seen *OrphanScanResult
		err := v.WalkWithOptions(context.Background(), WalkOptions{
			IncludeOrphans: true,
			OnOrphanScan: func(r *OrphanScanResult) error {
				calls++
				seen = r
				return nil
			},
		}, func(string, uint32, DirEntry) error { return nil })
		if err != nil {
			t.Fatalf("WalkWithOptions failed: %v", err)
		}
		if calls != 1 {
			t.Fatalf("OnOrphanScan ran %d times, want exactly 1", calls)
		}
		if seen.Truncated {
			t.Fatal("an unbounded scan reported itself truncated")
		}
		if seen.ReachableWalkFailed {
			t.Fatal("a readable tree reported ReachableWalkFailed")
		}
		if len(seen.Directories) == 0 {
			t.Fatal("the scan result carried no orphan directories")
		}
	})

	t.Run("bounded scan", func(t *testing.T) {
		v := orphanImage(t).volume()
		defer v.Close()

		var seen *OrphanScanResult
		err := v.WalkWithOptions(context.Background(), WalkOptions{
			IncludeOrphans: true,
			OrphanScan:     OrphanScanOptions{MaxClusters: 1},
			OnOrphanScan: func(r *OrphanScanResult) error {
				seen = r
				return nil
			},
		}, func(string, uint32, DirEntry) error { return nil })
		if err != nil {
			t.Fatalf("WalkWithOptions failed: %v", err)
		}
		if seen == nil || !seen.Truncated {
			t.Fatalf("a scan bounded to one cluster did not report Truncated: %+v", seen)
		}
	})

	t.Run("blind reachability pre-pass", func(t *testing.T) {
		im := orphanImage(t)
		im.addSubdir("SUB", []uint32{10}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))
		im.setFATEntry(10, im.badCluster())

		v := im.volume()
		defer v.Close()

		var seen *OrphanScanResult
		err := v.WalkWithOptions(context.Background(), WalkOptions{
			IncludeOrphans: true,
			OnOrphanScan: func(r *OrphanScanResult) error {
				seen = r
				return nil
			},
		}, func(string, uint32, DirEntry) error { return nil })
		if err != nil {
			t.Fatalf("WalkWithOptions failed: %v", err)
		}
		if seen == nil || !seen.ReachableWalkFailed {
			t.Fatalf("an unreadable live subtree did not set ReachableWalkFailed: %+v", seen)
		}
	})

	t.Run("hook error stops the walk", func(t *testing.T) {
		v := orphanImage(t).volume()
		defer v.Close()

		sentinel := errors.New("refuse a blind scan")
		var got []string
		err := v.WalkWithOptions(context.Background(), WalkOptions{
			IncludeOrphans: true,
			OnOrphanScan:   func(*OrphanScanResult) error { return sentinel },
		}, func(p string, _ uint32, _ DirEntry) error {
			got = append(got, p)
			return nil
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("WalkWithOptions returned %v, want the hook's own error", err)
		}
		for _, p := range got {
			if len(p) >= len(OrphanPath) && p[:len(OrphanPath)] == OrphanPath {
				t.Fatalf("an orphan entry was reported after the hook refused the scan; walked %v", got)
			}
		}
		if len(got) == 0 {
			t.Fatal("the reachable tree should have been reported before the scan ran")
		}
	})
}
