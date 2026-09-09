package libfat

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestScanOrphansContextRejectsNilContext(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	defer v.Close()

	//lint:ignore SA1012 passing nil is exactly what this test asserts about.
	_, err := v.ScanOrphansContext(nil, OrphanScanOptions{}) //nolint:staticcheck
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("ScanOrphansContext(nil) = %v, want ErrNilContext", err)
	}
}

// TestScanOrphansContextCancelledBeforeStartScansNothing proves two things at
// once: that the pacing counter is tested before it advances, so an
// already-cancelled context is caught on the very first unit of work, and that
// the reachability pre-pass is paced by the same counter as the sweep.
func TestScanOrphansContextCancelledBeforeStartScansNothing(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addDeletedSubdir("GONE", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))

	v := im.volume()
	defer v.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := v.ScanOrphansContext(ctx, OrphanScanOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled unchanged", err)
	}
	if result == nil {
		t.Fatal("a cancelled scan must still return its partial result")
	}
	if !result.Truncated {
		t.Fatal("a cancelled scan must report Truncated")
	}
	if result.ClustersScanned != 0 {
		t.Fatalf("ClustersScanned = %d, want 0 on an already-cancelled context", result.ClustersScanned)
	}
	if len(result.Directories) != 0 {
		t.Fatalf("found %d directories despite cancellation", len(result.Directories))
	}
}

func TestScanOrphansDelegatesToContextForm(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addDeletedSubdir("GONE", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))

	v := im.volume()
	defer v.Close()

	plain, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	withCtx, err := v.ScanOrphansContext(context.Background(), OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphansContext failed: %v", err)
	}

	if len(plain.Directories) != len(withCtx.Directories) {
		t.Fatalf("directory counts differ: %d vs %d", len(plain.Directories), len(withCtx.Directories))
	}
	for i := range plain.Directories {
		if plain.Directories[i].FirstCluster != withCtx.Directories[i].FirstCluster {
			t.Fatalf("directory %d differs: cluster %d vs %d", i,
				plain.Directories[i].FirstCluster, withCtx.Directories[i].FirstCluster)
		}
	}
}

func TestScanOrphansContextCancelledDuringSweepReturnsPartialResult(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addDeletedSubdir("GONE", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))

	// Far enough in that the reachability pre-pass has finished and the sweep
	// has begun reading clusters.
	v, ctx := im.volumeCancellingAfter(50)
	defer v.Close()

	result, err := v.ScanOrphansContext(ctx, OrphanScanOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled unchanged", err)
	}
	if result == nil {
		t.Fatal("a cancelled scan must still return its partial result")
	}
	if !result.Truncated {
		t.Fatal("a cancelled scan must report Truncated")
	}
	if result.ClustersScanned == 0 {
		t.Fatal("the sweep was expected to have examined clusters before the cancellation landed")
	}
}

// pacedTreeImage builds a tree of many small directories, none of them anywhere
// near cancellationCheckInterval entries on its own, holding more entries in
// total than that interval. It returns the image and the number of entries a
// walk of it reports. That shape is what separates a walk-wide pacing counter
// from one that restarts at every directory.
func pacedTreeImage(t *testing.T) (*testImage, int) {
	t.Helper()
	im := newTestImage(t, FATType16)

	const (
		dirs         = 90
		perDirectory = 12
	)
	for d := range dirs {
		cluster := uint32(100 + d)
		entries := make([][]byte, 0, perDirectory)
		for i := range perDirectory {
			entries = append(entries,
				makeTimestampedEntry(fmt.Sprintf("F%d", i), "TXT", 0x20, uint32(2000+d*perDirectory+i), 4))
		}
		im.addSubdir(fmt.Sprintf("D%d", d), []uint32{cluster}, entries...)
	}

	// Each directory contributes its own record plus its children.
	total := dirs * (perDirectory + 1)
	if total <= cancellationCheckInterval {
		t.Fatalf("the tree holds %d entries, which does not exceed the check interval %d",
			total, cancellationCheckInterval)
	}
	if perDirectory >= cancellationCheckInterval {
		t.Fatalf("a single directory holds %d entries, which reaches the check interval on its own",
			perDirectory)
	}
	return im, total
}

// countingContext records how often the walk consults the context, which is
// what makes the pacing rule observable rather than merely asserted.
type countingContext struct {
	context.Context
	calls *int
}

func (c countingContext) Err() error {
	*c.calls++
	return c.Context.Err()
}

// TestWalkConsultsContextOncePerCheckInterval pins the pacing rule: the counter
// spans the whole walk, so the number of context consultations is proportional
// to the entries walked and not to the number of directories they are spread
// across. A counter that reset at each directory would consult the context once
// per directory - ninety times here rather than twice - which is the drift this
// guards against.
func TestWalkConsultsContextOncePerCheckInterval(t *testing.T) {
	im, total := pacedTreeImage(t)

	v := im.volume()
	defer v.Close()

	calls := 0
	ctx := countingContext{Context: context.Background(), calls: &calls}

	walked := 0
	if err := v.Walk(ctx, func(string, uint32, DirEntry) error {
		walked++
		return nil
	}); err != nil {
		t.Fatalf("Walk failed: %v", err)
	}
	if walked != total {
		t.Fatalf("walked %d entries, want %d", walked, total)
	}

	want := (total + cancellationCheckInterval - 1) / cancellationCheckInterval
	if calls != want {
		t.Fatalf("consulted the context %d times over %d entries, want %d "+
			"(one per %d entries, counted across the whole walk)",
			calls, total, want, cancellationCheckInterval)
	}
}

// TestWalkNoticesCancellationMidWalk complements the pacing test: a context
// cancelled while the walk is running stops it at the next check.
func TestWalkNoticesCancellationMidWalk(t *testing.T) {
	im, _ := pacedTreeImage(t)

	v, ctx := im.volumeCancellingAfter(5)
	defer v.Close()

	err := v.Walk(ctx, func(string, uint32, DirEntry) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Walk = %v, want context.Canceled", err)
	}
}

func TestWalkCancelledContextCallsNothing(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 4, terminate: true})
	im.addSubdir("SUB", []uint32{10}, makeTimestampedEntry("CHILD", "TXT", 0x20, 20, 8))

	v := im.volume()
	defer v.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := v.Walk(ctx, func(string, uint32, DirEntry) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Walk = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("callback ran %d times on an already-cancelled context, want 0", calls)
	}
}

func TestWalkCancellationReachesOrphanPhase(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addDeletedSubdir("GONE", []uint32{30, 31}, false,
		makeTimestampedEntry("LOST", "TXT", 0x20, 40, 16))

	v, ctx := im.volumeCancellingAfter(50)
	defer v.Close()

	err := v.WalkWithOptions(ctx, WalkOptions{IncludeOrphans: true},
		func(string, uint32, DirEntry) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Walk with IncludeOrphans = %v, want context.Canceled", err)
	}
}
