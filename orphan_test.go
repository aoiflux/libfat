package libfat

import (
	"strings"
	"testing"
)

func orphanNames(result *OrphanScanResult) []string {
	var names []string
	for _, e := range result.Entries() {
		names = append(names, e.Name)
	}
	return names
}

func hasName(names []string, want string) bool {
	for _, n := range names {
		if strings.EqualFold(n, want) {
			return true
		}
	}
	return false
}

func TestScanOrphansRecoversChildrenOfDeletedDirectory(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addDeletedSubdir("LOSTDIR", []uint32{400}, true,
		makeTimestampedEntry("CHILDA", "TXT", 0x20, 410, 100),
		makeTimestampedEntry("CHILDB", "TXT", 0x20, 411, 200),
		makeTimestampedEntry("CHILDC", "DAT", 0x20, 412, 300),
	)

	v := im.volume()
	result, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if len(result.Directories) != 1 {
		t.Fatalf("expected 1 orphan directory, got %d", len(result.Directories))
	}

	dir := result.Directories[0]
	if !dir.HasDotEntries {
		t.Fatal("the directory's first cluster carries dot records and should be recognised as such")
	}
	if dir.FirstCluster != 400 {
		t.Fatalf("FirstCluster = %d, want 400", dir.FirstCluster)
	}
	if len(dir.Ranges) != 1 || dir.Ranges[0].StartByte != im.clusterOffset(400) {
		t.Fatalf("ranges = %v, want a single run at cluster 400 (%d)", dir.Ranges, im.clusterOffset(400))
	}

	names := orphanNames(result)
	for _, want := range []string{"CHILDA.TXT", "CHILDB.TXT", "CHILDC.DAT"} {
		if !hasName(names, want) {
			t.Fatalf("orphan %q not recovered; got %v", want, names)
		}
	}

	for _, e := range result.Entries() {
		if !e.Orphaned {
			t.Fatalf("entry %q should be marked Orphaned", e.Name)
		}
		if !strings.HasPrefix(e.Path, OrphanPath+"/") {
			t.Fatalf("entry %q has path %q, want it rooted at %s", e.Name, e.Path, OrphanPath)
		}
	}
}

// TestScanOrphansEntryOffsetsAreAddressable checks that a recovered entry's
// reported image offset actually contains that entry. Without it the recovery
// would be unusable: a caller could not go back and read the structure.
func TestScanOrphansEntryOffsetsAreAddressable(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addDeletedSubdir("LOSTDIR", []uint32{420}, true,
		makeTimestampedEntry("FINDME", "TXT", 0x20, 430, 50),
	)

	v := im.volume()
	result, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}

	entries := result.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 orphan, got %d", len(entries))
	}
	entry := entries[0]

	// "." and ".." occupy the first two records, so FINDME sits at offset 64.
	want := im.clusterOffset(420) + 64
	if entry.EntryAbsoluteOffset != want {
		t.Fatalf("EntryAbsoluteOffset = %d, want %d", entry.EntryAbsoluteOffset, want)
	}

	got := make([]byte, 11)
	if _, err := v.ReadAt(got, entry.EntryAbsoluteOffset); err != nil {
		t.Fatalf("reading the reported offset failed: %v", err)
	}
	if string(got) != "FINDME  TXT" {
		t.Fatalf("bytes at the reported offset = %q, want %q", got, "FINDME  TXT")
	}
}

// TestScanOrphansExcludesReachableEntries is the property that makes the result
// meaningful: entries already visible by ordinary traversal are not orphans.
// Allocation filtering is disabled so that reachability alone does the work.
func TestScanOrphansExcludesReachableEntries(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addSubdir("LIVEDIR", []uint32{440},
		makeTimestampedEntry("VISIBLE", "TXT", 0x20, 450, 100),
	)
	im.addDeletedSubdir("LOSTDIR", []uint32{460}, true,
		makeTimestampedEntry("HIDDEN", "TXT", 0x20, 470, 100),
	)

	v := im.volume()
	result, err := v.ScanOrphans(OrphanScanOptions{ScanAllocatedClusters: true})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if result.ReachableWalkFailed {
		t.Fatal("the live tree should have walked cleanly")
	}

	names := orphanNames(result)
	if hasName(names, "VISIBLE.TXT") {
		t.Fatalf("an entry reachable from the root was reported as an orphan: %v", names)
	}
	if !hasName(names, "HIDDEN.TXT") {
		t.Fatalf("the unreachable entry was not recovered; got %v", names)
	}
}

func TestScanOrphansFollowsContiguousContinuationClusters(t *testing.T) {
	im := newTestImage(t, FATType16)

	// 512-byte clusters hold 16 records; "." and ".." take two, so 14 fillers
	// push DEEPKID into the second cluster of the directory.
	var entries [][]byte
	for i := 0; i < 14; i++ {
		entries = append(entries, makeTimestampedEntry(padName("PAD", i), "TXT", 0x20, 500+uint32(i), 10))
	}
	entries = append(entries, makeTimestampedEntry("DEEPKID", "TXT", 0x20, 520, 42))
	im.addDeletedSubdir("BIGLOST", []uint32{480, 481}, true, entries...)

	v := im.volume()

	result, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if !hasName(orphanNames(result), "DEEPKID.TXT") {
		t.Fatalf("entry in the continuation cluster was not recovered; got %v", orphanNames(result))
	}
	dir := result.Directories[0]
	if TotalLength(dir.Ranges) != 1024 {
		t.Fatalf("ranges cover %d bytes, want both clusters (1024)", TotalLength(dir.Ranges))
	}

	// With continuation disabled, only the first cluster is examined.
	limited, err := v.ScanOrphans(OrphanScanOptions{OnlyFirstCluster: true})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if hasName(orphanNames(limited), "DEEPKID.TXT") {
		t.Fatal("OnlyFirstCluster should not reach the continuation cluster")
	}
	if !hasName(orphanNames(limited), "PADA.TXT") {
		t.Fatal("the first cluster's entries should still be recovered")
	}
}

// TestScanOrphansRejectsStaleDirectoryCopy exercises the "." self-reference
// check. A cluster holding a copy of directory data that belongs elsewhere,
// which happens when a directory is moved or its clusters are reused, must not
// be reported as a directory located here.
func TestScanOrphansRejectsStaleDirectoryCopy(t *testing.T) {
	im := newTestImage(t, FATType16)

	stale := make([]byte, 512)
	// "." claims cluster 999 while the data actually sits in cluster 530.
	copy(stale[0:32], makeShortEntry(".", "", 0x10, 999, 0))
	copy(stale[32:64], makeShortEntry("..", "", 0x10, 0, 0))
	copy(stale[64:96], makeTimestampedEntry("GHOST", "TXT", 0x20, 540, 10))
	im.writeRawCluster(530, stale)

	v := im.volume()
	result, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if hasName(orphanNames(result), "GHOST.TXT") {
		t.Fatal("a cluster whose \".\" record points elsewhere must not be accepted as a directory")
	}

	// It is still reachable through the weaker heuristic, which does not
	// require the dot records to be self-consistent.
	loose, err := v.ScanOrphans(OrphanScanOptions{AllowMissingDotEntries: true})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if !hasName(orphanNames(loose), "GHOST.TXT") {
		t.Fatalf("AllowMissingDotEntries should surface the stale copy; got %v", orphanNames(loose))
	}
}

func TestScanOrphansIgnoresFileData(t *testing.T) {
	im := newTestImage(t, FATType16)

	// Free clusters holding ordinary file content must not be mistaken for
	// directories under either heuristic.
	im.writeRawCluster(550, patternBytes(512, 0x5C))
	im.writeRawCluster(551, []byte(strings.Repeat("Lorem ipsum dolor sit amet. ", 18)))

	v := im.volume()
	for _, opts := range []OrphanScanOptions{
		{},
		{AllowMissingDotEntries: true},
		{AllowMissingDotEntries: true, MinValidEntries: 1},
	} {
		result, err := v.ScanOrphans(opts)
		if err != nil {
			t.Fatalf("ScanOrphans failed: %v", err)
		}
		for _, dir := range result.Directories {
			if dir.FirstCluster == 550 || dir.FirstCluster == 551 {
				t.Fatalf("file data at cluster %d was reported as a directory (opts %+v)",
					dir.FirstCluster, opts)
			}
		}
	}
}

func TestScanOrphansSkipsAllocatedClustersByDefault(t *testing.T) {
	im := newTestImage(t, FATType16)

	// Directory data that survives inside a cluster now allocated to a live
	// file: stale content, reported only when explicitly requested.
	content := make([]byte, 512)
	copy(content[0:32], makeShortEntry(".", "", 0x10, 560, 0))
	copy(content[32:64], makeShortEntry("..", "", 0x10, 0, 0))
	copy(content[64:96], makeTimestampedEntry("STALE", "TXT", 0x20, 570, 10))
	im.writeRawCluster(560, content)
	im.setFATEntry(560, im.eoc()) // marked in use

	v := im.volume()

	result, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if hasName(orphanNames(result), "STALE.TXT") {
		t.Fatal("allocated clusters should be skipped by default")
	}
	if result.ClustersSkipped == 0 {
		t.Fatal("skipped clusters should be counted")
	}

	deep, err := v.ScanOrphans(OrphanScanOptions{ScanAllocatedClusters: true})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if !hasName(orphanNames(deep), "STALE.TXT") {
		t.Fatalf("ScanAllocatedClusters should surface it; got %v", orphanNames(deep))
	}
}

func TestScanOrphansRecordsParentCluster(t *testing.T) {
	im := newTestImage(t, FATType16)

	content := make([]byte, 512)
	copy(content[0:32], makeShortEntry(".", "", 0x10, 580, 0))
	copy(content[32:64], makeShortEntry("..", "", 0x10, 44, 0)) // nested under cluster 44
	copy(content[64:96], makeTimestampedEntry("NESTED", "TXT", 0x20, 590, 10))
	im.writeRawCluster(580, content)

	v := im.volume()
	result, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if len(result.Directories) != 1 {
		t.Fatalf("expected 1 orphan directory, got %d", len(result.Directories))
	}
	if result.Directories[0].ParentCluster != 44 {
		t.Fatalf("ParentCluster = %d, want 44 from the \"..\" record",
			result.Directories[0].ParentCluster)
	}
}

func TestScanOrphansRespectsBounds(t *testing.T) {
	im := newTestImage(t, FATType16)
	for i, cluster := range []uint32{600, 610, 620, 630} {
		im.addDeletedSubdir("LOST", []uint32{cluster}, false,
			makeTimestampedEntry(padName("ORPH", i), "TXT", 0x20, 700+uint32(i), 10),
		)
	}

	v := im.volume()

	all, err := v.ScanOrphans(OrphanScanOptions{})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if len(all.Directories) != 4 {
		t.Fatalf("expected 4 orphan directories, got %d", len(all.Directories))
	}

	capped, err := v.ScanOrphans(OrphanScanOptions{MaxDirectories: 2})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if len(capped.Directories) != 2 {
		t.Fatalf("MaxDirectories=2 returned %d", len(capped.Directories))
	}
	if !capped.Truncated {
		t.Fatal("a bounded scan must report that it stopped early")
	}

	short, err := v.ScanOrphans(OrphanScanOptions{MaxClusters: 10})
	if err != nil {
		t.Fatalf("ScanOrphans failed: %v", err)
	}
	if !short.Truncated {
		t.Fatal("MaxClusters should truncate the scan")
	}
	if short.ClustersScanned > 10 {
		t.Fatalf("scanned %d clusters despite a cap of 10", short.ClustersScanned)
	}
}

func TestScanOrphansAcrossFATTypes(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			im.addDeletedSubdir("LOSTDIR", []uint32{50}, true,
				makeTimestampedEntry("ORPHAN", "TXT", 0x20, 60, 128),
			)

			v := im.volume()
			result, err := v.ScanOrphans(OrphanScanOptions{})
			if err != nil {
				t.Fatalf("ScanOrphans failed: %v", err)
			}
			if !hasName(orphanNames(result), "ORPHAN.TXT") {
				t.Fatalf("orphan not recovered on %s; got %v", fatType, orphanNames(result))
			}
		})
	}
}

func TestScanOrphansOnClosedVolume(t *testing.T) {
	im := newTestImage(t, FATType16)
	v := im.volume()
	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, err := v.ScanOrphans(OrphanScanOptions{}); err != ErrVolumeClosed {
		t.Fatalf("error = %v, want ErrVolumeClosed", err)
	}
}
