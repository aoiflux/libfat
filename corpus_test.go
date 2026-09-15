package libfat

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Tests against the synthetic corpus in testdata/gen_corpus.sh.
//
// The three real images cover one sector size, two FAT types, no deletions and
// no fragmented regular file. Everything else libfat parses is otherwise tested
// only against fixtures this package builds itself, and a fixture is built from
// the same reading of the spec as the parser, so it cannot catch a misreading.
// Each volume here exists because it is the only one that covers something, and
// the `why` field says what.
//
// They are skipped unless the corpus is present. Generate it with:
//
//	wsl.exe -- sh testdata/gen_corpus.sh /mnt/e/dataset/fat_synth
const envCorpus = "LIBFAT_CORPUS"

// corpusVolume pins what a volume is, so that a change in any of it is a
// failure with a name rather than a surprise somewhere downstream.
type corpusVolume struct {
	file string
	// why is what only this volume covers. It is printed on failure, because a
	// corpus entry whose purpose nobody remembers stops being maintained.
	why string

	fatType           string
	bytesPerSector    uint32
	sectorsPerCluster uint32
	clusterCount      uint32

	// label is what the volume is called; bootSectorLabel is the boot sector's
	// copy when it differs, and "" means the two agree.
	label           string
	bootSectorLabel string

	secondFAT      bool
	fsInfo         bool
	usedBackup     bool
	mirrorMismatch bool

	// deletedFiles and orphanFiles are counted over non-directory entries.
	// fragmentedFiles counts live files occupying more than one run.
	deletedFiles    int
	orphanFiles     int
	fragmentedFiles int

	// noOracle records that 7-Zip cannot read this volume at all, so there is
	// nothing to cross-check it against.
	noOracle string
}

var corpusVolumes = []corpusVolume{
	{
		file:    "fat12_b512.img",
		why:     "FAT12 at all: no real image in the dataset is FAT12, and its 12-bit entries straddle byte and sector boundaries",
		fatType: FATType12, bytesPerSector: 512, sectorsPerCluster: 8, clusterCount: 2041,
		label: "FAT12B512", secondFAT: true,
	},
	{
		file:    "fat12_b4096.img",
		why:     "FAT12 with a 4096-byte sector, so the FAT spans a different number of sectors than the 512-byte case",
		fatType: FATType12, bytesPerSector: 4096, sectorsPerCluster: 1, clusterCount: 2041,
		label: "FAT12B4096", secondFAT: true,
	},
	{
		file:    "fat16_b1024.img",
		why:     "a sector size other than 512; every real image is 512, so the sector arithmetic is otherwise tested at one value",
		fatType: FATType16, bytesPerSector: 1024, sectorsPerCluster: 4, clusterCount: 4087,
		label: "FAT16B1024", secondFAT: true,
	},
	{
		file:    "fat16_bigroot.img",
		why:     "a 1024-entry fixed root directory region, twice the usual, so RootDirectoryFragments returns a longer run",
		fatType: FATType16, bytesPerSector: 512, sectorsPerCluster: 4, clusterCount: 8159,
		label: "FAT16BIGRT", secondFAT: true,
	},
	{
		file:    "fat32_onefat.img",
		why:     "a volume with one FAT, the only place Capabilities.SecondFAT is false",
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 73124,
		label: "FAT32ONEFAT", secondFAT: false, fsInfo: true,
	},

	{
		file:    "fat12_deleted.img",
		why:     "deleted records on FAT12, in the fixed root region rather than a cluster",
		fatType: FATType12, bytesPerSector: 512, sectorsPerCluster: 8, clusterCount: 2041,
		label: "FAT12DEL", secondFAT: true, deletedFiles: 3,
	},
	{
		file:    "fat16_deleted.img",
		why:     "deleted records on FAT16, including one whose long-name slots were orphaned by the deletion",
		fatType: FATType16, bytesPerSector: 512, sectorsPerCluster: 4, clusterCount: 8167,
		label: "FAT16DEL", secondFAT: true, deletedFiles: 3,
	},
	{
		file:    "fat32_deleted.img",
		why:     "deleted records on FAT32, whose chains are read back through a 32-bit table",
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 72562,
		label: "FAT32DEL", secondFAT: true, fsInfo: true, deletedFiles: 3,
	},

	{
		file:    "fat16_frag.img",
		why:     "a fragmented regular file: every fragmented entry on the real images is a directory",
		fatType: FATType16, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 32481,
		label: "FAT16FRAG", secondFAT: true, deletedFiles: 14, fragmentedFiles: 1,
	},
	{
		file:    "fat32_frag.img",
		why:     "a fragmented regular file on FAT32",
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 72562,
		label: "FAT32FRAG", secondFAT: true, fsInfo: true, deletedFiles: 33, fragmentedFiles: 1,
	},

	{
		file:    "fat32_relabel.img",
		why:     "a boot sector label that disagrees with the root directory's, which is what Windows leaves behind",
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 72562,
		label: "REALNAME", bootSectorLabel: "NO NAME", secondFAT: true, fsInfo: true,
	},
	{
		file:    "fat32_mirror.img",
		why:     "two FATs that disagree, the only thing FATMirrorMismatches can be exercised against",
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 72562,
		label: "FAT32MIRROR", secondFAT: true, fsInfo: true, mirrorMismatch: true,
	},
	{
		file:    "fat32_nobootsec.img",
		why:     "a destroyed primary boot sector, the only volume opened through the backup copy",
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 72562,
		label: "FAT32BACKUP", secondFAT: true, fsInfo: true, usedBackup: true,
		noOracle: "7-Zip reports \"cannot open the file as archive\"; libfat reads it through the backup boot sector",
	},
	{
		file:    "fat32_orphan.img",
		why:     "a directory whose record is deleted and whose clusters are released while its contents stay intact, which is what ScanOrphans is for",
		fatType: FATType32, bytesPerSector: 512, sectorsPerCluster: 1, clusterCount: 72562,
		label: "FAT32ORPHAN", secondFAT: true, fsInfo: true, orphanFiles: 3,
	},
}

// corpusDir returns the corpus directory, or skips. It defaults to a fat_synth
// subdirectory of the image directory, so the usual case needs one variable
// rather than two.
func corpusDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv(envCorpus); dir != "" {
		return dir
	}
	dir := filepath.Join(imagesDir(t), "fat_synth")
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Skipf("no corpus at %s; generate it with testdata/gen_corpus.sh, or set %s",
			dir, envCorpus)
	}
	return dir
}

func (spec corpusVolume) open(t *testing.T) (*os.File, *Volume) {
	t.Helper()
	path := filepath.Join(corpusDir(t), spec.file)
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("%s: %v", spec.file, err)
	}
	t.Cleanup(func() { _ = f.Close() })

	v, err := Open(f)
	if err != nil {
		t.Fatalf("%s failed to open: %v\n  this volume is the only one covering %s", spec.file, err, spec.why)
	}
	t.Cleanup(func() { _ = v.Close() })
	return f, v
}

// TestCorpusGeometry checks each volume is the one the manifest describes, which
// is also what makes the counts pinned further down meaningful.
func TestCorpusGeometry(t *testing.T) {
	for _, spec := range corpusVolumes {
		t.Run(spec.file, func(t *testing.T) {
			_, v := spec.open(t)
			t.Logf("covers: %s", spec.why)

			if got := v.FATType(); got != spec.fatType {
				t.Errorf("FATType = %q, want %q", got, spec.fatType)
			}
			if got := v.BytesPerSector(); got != spec.bytesPerSector {
				t.Errorf("BytesPerSector = %d, want %d", got, spec.bytesPerSector)
			}
			if got := v.SectorsPerCluster(); got != spec.sectorsPerCluster {
				t.Errorf("SectorsPerCluster = %d, want %d", got, spec.sectorsPerCluster)
			}
			if got := v.ClusterCount(); got != spec.clusterCount {
				t.Errorf("ClusterCount = %d, want %d", got, spec.clusterCount)
			}
			if got := v.UsedBackupBootSector(); got != spec.usedBackup {
				t.Errorf("UsedBackupBootSector = %v, want %v", got, spec.usedBackup)
			}

			caps := v.Capabilities()
			if caps.SecondFAT != spec.secondFAT {
				t.Errorf("SecondFAT = %v, want %v", caps.SecondFAT, spec.secondFAT)
			}
			if caps.FSInfoSector != spec.fsInfo {
				t.Errorf("FSInfoSector = %v, want %v", caps.FSInfoSector, spec.fsInfo)
			}
		})
	}
}

// TestCorpusVolumeLabel covers the label fix against volumes written by a third
// party, including the one where the two copies deliberately disagree.
func TestCorpusVolumeLabel(t *testing.T) {
	for _, spec := range corpusVolumes {
		t.Run(spec.file, func(t *testing.T) {
			_, v := spec.open(t)

			if got := v.VolumeLabel(); got != spec.label {
				t.Errorf("VolumeLabel = %q, want %q", got, spec.label)
			}
			wantBoot := spec.bootSectorLabel
			if wantBoot == "" {
				wantBoot = spec.label
			}
			if got := v.BootSectorVolumeLabel(); got != wantBoot {
				t.Errorf("BootSectorVolumeLabel = %q, want %q", got, wantBoot)
			}
			if got := v.VolumeLabelSource(); got != "root directory" {
				t.Errorf("VolumeLabelSource = %q, want %q; mkfs.fat and mlabel both write the "+
					"root directory record, so every volume here should have one", got, "root directory")
			}
			if spec.bootSectorLabel != "" && v.VolumeLabel() == v.BootSectorVolumeLabel() {
				t.Error("the two label copies agree on the volume built specifically to make them differ")
			}
		})
	}
}

// corpusCounts walks a volume and counts what the manifest pins.
type corpusCounts struct {
	live, deleted, orphan, fragmented int
	maxRuns                           int
}

func countCorpus(t *testing.T, v *Volume) corpusCounts {
	t.Helper()
	rep, err := v.ReportDeep("corpus")
	if err != nil {
		t.Fatalf("ReportDeep failed: %v", err)
	}
	var c corpusCounts
	for _, row := range rep.Files {
		if row.Type != "file" {
			continue
		}
		switch {
		case row.IsOrphaned:
			c.orphan++
		case row.IsDeleted:
			c.deleted++
		default:
			c.live++
			if n := len(row.Fragments); n > 1 {
				c.fragmented++
				if n > c.maxRuns {
					c.maxRuns = n
				}
			}
		}
	}
	return c
}

// TestCorpusDeletedAndOrphanedCounts pins the recovery coverage. Nothing else in
// the suite reaches these paths on a volume libfat did not build: all three real
// images have zero deleted records.
func TestCorpusDeletedAndOrphanedCounts(t *testing.T) {
	for _, spec := range corpusVolumes {
		t.Run(spec.file, func(t *testing.T) {
			_, v := spec.open(t)
			got := countCorpus(t, v)

			if got.deleted != spec.deletedFiles {
				t.Errorf("%d deleted files, want %d (covers: %s)", got.deleted, spec.deletedFiles, spec.why)
			}
			if got.orphan != spec.orphanFiles {
				t.Errorf("%d orphaned files, want %d (covers: %s)", got.orphan, spec.orphanFiles, spec.why)
			}
			if got.fragmented != spec.fragmentedFiles {
				t.Errorf("%d fragmented live files, want %d (covers: %s)",
					got.fragmented, spec.fragmentedFiles, spec.why)
			}
			if spec.fragmentedFiles > 0 && got.maxRuns < 2 {
				t.Errorf("the fragmented file has %d runs; this volume exists to provide more than one", got.maxRuns)
			}
			if got.live == 0 {
				t.Error("no live files; the volume is empty")
			}
		})
	}
}

// TestCorpusFATMirrorMismatch holds the mismatch counter to the one volume whose
// FATs were made to disagree, and to zero everywhere else. A counter that
// reported mismatches on a healthy volume would call every image damaged.
func TestCorpusFATMirrorMismatch(t *testing.T) {
	for _, spec := range corpusVolumes {
		t.Run(spec.file, func(t *testing.T) {
			_, v := spec.open(t)
			if _, err := v.ReportDeep("corpus"); err != nil {
				t.Fatalf("ReportDeep failed: %v", err)
			}
			got := v.FATMirrorMismatches()
			if spec.mirrorMismatch && got == 0 {
				t.Errorf("no mismatches on the volume whose second FAT was deliberately edited")
			}
			if !spec.mirrorMismatch && got != 0 {
				t.Errorf("%d FAT mirror mismatches on a volume whose FATs agree", got)
			}
		})
	}
}

// TestCorpusFragmentedFileReadsBackCorrectly is the assertion the fragmented
// volumes exist for: a multi-run extent map, read straight from the image, must
// reproduce the bytes an outside reader extracted.
//
// A contiguous file cannot fail this test in an interesting way. A fragmented
// one can: it fails whenever the chain walk, the run coalescing or the final
// trim is wrong, and until this corpus existed nothing outside this package's
// own fixtures produced one.
func TestCorpusFragmentedFileReadsBackCorrectly(t *testing.T) {
	for _, spec := range corpusVolumes {
		if spec.fragmentedFiles == 0 {
			continue
		}
		t.Run(spec.file, func(t *testing.T) {
			f, v := spec.open(t)
			oracle := loadOracle(t, filepath.Join(corpusDir(t), spec.file+".fat-oracle.tsv"))

			checked := 0
			err := v.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
				if e.IsDirectory || e.Size == 0 {
					return nil
				}
				file, err := v.OpenEntry(e)
				if err != nil {
					t.Errorf("%s: OpenEntry failed: %v", path, err)
					return nil
				}
				ranges, err := file.Fragments()
				if err != nil {
					t.Errorf("%s: Fragments failed: %v", path, err)
					return nil
				}
				if len(ranges) < 2 {
					return nil
				}

				want, ok := oracle.entries[path]
				if !ok || want.sha256 == "-" {
					t.Errorf("%s is fragmented but the oracle has no hash for it", path)
					return nil
				}
				got, n, err := hashViaExtents(f, ranges)
				if err != nil {
					t.Errorf("%s: reading the image at the reported offsets failed: %v", path, err)
					return nil
				}
				if n != int64(e.Size) {
					t.Errorf("%s: the runs cover %d bytes, the entry records %d", path, n, e.Size)
				}
				if got != want.sha256 {
					t.Errorf("%s: %d runs read back as the wrong bytes\n  7-Zip  %s\n  libfat %s\n  ranges %v",
						path, len(ranges), want.sha256, got, ranges)
				}
				checked++
				return nil
			})
			if err != nil {
				t.Fatalf("Walk failed: %v", err)
			}
			if checked != spec.fragmentedFiles {
				t.Errorf("checked %d fragmented files, the manifest records %d", checked, spec.fragmentedFiles)
			}
		})
	}
}

// TestCorpusOrphanScanRecoversNames goes past counting: the records inside an
// unreferenced directory cluster must still decode, or finding the cluster is
// worth nothing.
func TestCorpusOrphanScanRecoversNames(t *testing.T) {
	for _, spec := range corpusVolumes {
		if spec.orphanFiles == 0 {
			continue
		}
		t.Run(spec.file, func(t *testing.T) {
			_, v := spec.open(t)

			result, err := v.ScanOrphans(OrphanScanOptions{})
			if err != nil {
				t.Fatalf("ScanOrphans failed: %v", err)
			}

			var names []string
			for _, e := range result.Entries() {
				if !e.IsDirectory {
					names = append(names, e.Name)
				}
			}
			if len(names) != spec.orphanFiles {
				t.Fatalf("ScanOrphans found %d orphaned files %v, want %d", len(names), names, spec.orphanFiles)
			}
			// gen_corpus.sh puts these three in the directory it orphans.
			for _, want := range []string{"orphan-one.txt", "orphan-two.bin", "THIRD.TXT"} {
				found := false
				for _, got := range names {
					if strings.EqualFold(got, want) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%q is not among the recovered names %v", want, names)
				}
			}
			for _, e := range result.Entries() {
				if e.Path != "" && !strings.HasPrefix(e.Path, OrphanPath) {
					t.Errorf("%q is not rooted at %q, so it claims a path no directory leads to",
						e.Path, OrphanPath)
				}
			}
		})
	}
}

// TestCorpusDeletedEntriesKeepTheirEvidence checks what survives a deletion,
// which is the part of FAT that forensic work actually rests on.
func TestCorpusDeletedEntriesKeepTheirEvidence(t *testing.T) {
	for _, spec := range corpusVolumes {
		if spec.deletedFiles == 0 {
			continue
		}
		t.Run(spec.file, func(t *testing.T) {
			path := filepath.Join(corpusDir(t), spec.file)
			f, err := os.Open(path)
			if err != nil {
				t.Skipf("%s: %v", spec.file, err)
			}
			defer f.Close()

			// RecoverDeletedLongNames is off by default, so this is also the
			// only place its effect is measured against a real deletion.
			v, err := OpenWithOptions(f, OpenOptions{RecoverDeletedLongNames: true})
			if err != nil {
				t.Fatalf("open failed: %v", err)
			}
			defer v.Close()

			rep, err := v.ReportDeep("corpus")
			if err != nil {
				t.Fatalf("ReportDeep failed: %v", err)
			}

			var deleted []FATFile
			for _, row := range rep.Files {
				if row.IsDeleted && !row.IsOrphaned && row.Type == "file" {
					deleted = append(deleted, row)
				}
			}
			if len(deleted) != spec.deletedFiles {
				t.Fatalf("%d deleted files, want %d", len(deleted), spec.deletedFiles)
			}

			recovered := 0
			for _, row := range deleted {
				if row.Size == 0 {
					continue
				}
				// The size and first cluster survive deletion; that is the
				// whole basis for recovering content from a deleted record.
				if row.FirstCluster == 0 {
					t.Errorf("%s: a deleted non-empty file lost its first cluster", row.Path)
				}
				if row.NameSource == NameSourceRecoveredLFN {
					recovered++
				}
				if row.EntryAbsoluteOffset < 0 {
					t.Errorf("%s: the deleted record could not be located on disk", row.Path)
				}
			}
			t.Logf("%d deleted files, %d with a long name recovered from the orphaned slots",
				len(deleted), recovered)
		})
	}
}

// TestCorpusMatchesOracle cross-checks the live tree of every corpus volume
// against 7-Zip, which is what makes the new geometry - FAT12, a 4096-byte
// sector, a 1024-entry root - evidence rather than assertion.
func TestCorpusMatchesOracle(t *testing.T) {
	for _, spec := range corpusVolumes {
		t.Run(spec.file, func(t *testing.T) {
			if spec.noOracle != "" {
				t.Skipf("no oracle: %s", spec.noOracle)
			}
			_, v := spec.open(t)
			oracle := loadOracle(t, filepath.Join(corpusDir(t), spec.file+".fat-oracle.tsv"))

			if got := strings.ToUpper(oracle.filesystem); got != v.FATType() {
				t.Errorf("7-Zip calls this %s, libfat calls it %s", got, v.FATType())
			}
			if got := int64(v.BytesPerCluster()); oracle.clusterSiz != got {
				t.Errorf("cluster size: 7-Zip %d, libfat %d", oracle.clusterSiz, got)
			}
			if got := int64(v.BytesPerSector()); oracle.sectorSize != got {
				t.Errorf("sector size: 7-Zip %d, libfat %d", oracle.sectorSize, got)
			}

			seen := make(map[string]bool, len(oracle.entries))
			hashed := 0
			err := v.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
				want, ok := oracle.entries[path]
				if !ok {
					t.Errorf("libfat reports %q, which 7-Zip did not find", path)
					return nil
				}
				seen[path] = true
				if want.dir != e.IsDirectory {
					t.Errorf("%s: 7-Zip says directory=%v, libfat says %v", path, want.dir, e.IsDirectory)
					return nil
				}
				if e.IsDirectory {
					return nil
				}
				if want.size != int64(e.Size) {
					t.Errorf("%s: 7-Zip says %d bytes, libfat says %d", path, want.size, e.Size)
				}
				if want.shortName != "" && !strings.EqualFold(want.shortName, e.ShortName) {
					t.Errorf("%s: 7-Zip reads the short name as %q, libfat as %q",
						path, want.shortName, e.ShortName)
				}
				if want.sha256 == "-" || e.Size == 0 {
					return nil
				}
				file, err := v.OpenEntry(e)
				if err != nil {
					t.Errorf("%s: OpenEntry failed: %v", path, err)
					return nil
				}
				got, _, err := hashViaReader(file)
				if err != nil {
					t.Errorf("%s: reading through libfat failed: %v", path, err)
					return nil
				}
				if got != want.sha256 {
					t.Errorf("%s: content differs from what 7-Zip extracted\n  7-Zip  %s\n  libfat %s",
						path, want.sha256, got)
				}
				hashed++
				return nil
			})
			if err != nil {
				t.Fatalf("Walk failed: %v", err)
			}
			for path, want := range oracle.entries {
				if !seen[path] {
					kind := "file"
					if want.dir {
						kind = "directory"
					}
					t.Errorf("7-Zip found the %s %q, which libfat did not report", kind, path)
				}
			}
			if hashed == 0 {
				t.Fatal("no content was compared against the oracle")
			}
			t.Logf("%d entries agreed, %d files hashed identically", len(seen), hashed)
		})
	}
}

// TestCorpusExtentsReadBackContent runs the whole-volume extent sweep the real
// images get, over the geometries they do not have.
func TestCorpusExtentsReadBackContent(t *testing.T) {
	for _, spec := range corpusVolumes {
		t.Run(spec.file, func(t *testing.T) {
			f, v := spec.open(t)
			lo, hi := v.BaseOffset(), v.BaseOffset()+int64(v.VolumeSize())
			cluster := int64(v.BytesPerCluster())
			spent := budget(t)

			rootRuns, err := v.RootDirectoryFragments()
			if err != nil {
				t.Fatalf("RootDirectoryFragments failed: %v", err)
			}
			dirRuns := map[string][]Range{"/": rootRuns}

			var files, placed int
			err = v.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
				if err := verifyEntryRecord(f, e); err != nil {
					t.Errorf("%s: %v", path, err)
				}
				if runs, ok := dirRuns[e.DirectoryPath]; ok && e.EntryAbsoluteOffset >= 0 {
					placed++
					if !holdsOffset(runs, e.EntryAbsoluteOffset) {
						t.Errorf("%s: its record at %d lies outside the runs reported for %q: %v",
							path, e.EntryAbsoluteOffset, e.DirectoryPath, runs)
					}
				}

				ranges, err := v.FragmentOffsets(e)
				if err != nil {
					t.Errorf("%s: FragmentOffsets failed: %v", path, err)
					return nil
				}
				covered := checkRunInvariants(t, path, ranges, lo, hi)

				if e.IsDirectory {
					if cluster > 0 && covered%cluster != 0 {
						t.Errorf("%s: a directory covers %d bytes, not a whole number of %d-byte clusters",
							path, covered, cluster)
					}
					dirRuns[path] = ranges
					return nil
				}
				if e.Size == 0 {
					return nil
				}
				files++
				if covered != int64(e.Size) {
					t.Errorf("%s: runs cover %d bytes, the entry records %d", path, covered, e.Size)
				}

				file, err := v.OpenEntry(e)
				if err != nil {
					t.Errorf("%s: OpenEntry failed: %v", path, err)
					return nil
				}
				inFile, err := file.ReaderAt()
				if err != nil {
					t.Errorf("%s: ReaderAt failed: %v", path, err)
					return nil
				}
				if err := verifyRunsInPlace(f, inFile, ranges, spent.probe); err != nil {
					t.Errorf("%s: %v\n  ranges %v", path, err, ranges)
				}
				return nil
			})
			if err != nil && !errors.Is(err, errBudgetSpent) {
				t.Fatalf("Walk failed: %v", err)
			}
			if files == 0 || placed == 0 {
				t.Fatalf("%d files checked, %d records placed", files, placed)
			}
		})
	}
}

// deletedRecord is one row of the corpus's deleted.tsv: what a deliberate
// deletion removed, and the sha256 of the bytes that were there.
type deletedRecord struct {
	path   string
	size   int64
	sha256 string
}

func loadDeleted(t *testing.T, image string) []deletedRecord {
	t.Helper()
	path := filepath.Join(corpusDir(t), "deleted.tsv")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("no deletion manifest at %s: %v", path, err)
	}
	var out []deletedRecord
	for i, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if line == "" {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) != 4 {
			t.Fatalf("%s:%d: %d columns, want 4", path, i+1, len(cols))
		}
		if cols[0] != image {
			continue
		}
		out = append(out, deletedRecord{
			path:   cols[1],
			size:   mustAtoi(t, path, i+1, cols[2]),
			sha256: cols[3],
		})
	}
	return out
}

// TestCorpusRecoversDeletedContent is the forensic claim this library exists to
// make, checked end to end: a file whose record is marked deleted and whose FAT
// chain has been released can still be read back byte for byte.
//
// Nothing else in the suite can test this. A deleted entry keeps its size and
// first cluster but loses its chain, so the extents have to be reconstructed
// under FragmentOptions.AssumeContiguous - a hypothesis, which is why the result
// is flagged Assumed and why the only way to know the hypothesis was right is to
// compare against the bytes that were actually there. The corpus records those
// because it did the deleting.
func TestCorpusRecoversDeletedContent(t *testing.T) {
	for _, spec := range corpusVolumes {
		if spec.deletedFiles == 0 || !strings.Contains(spec.file, "_deleted") {
			continue
		}
		t.Run(spec.file, func(t *testing.T) {
			want := loadDeleted(t, spec.file)
			if len(want) == 0 {
				t.Skipf("the deletion manifest has no rows for %s", spec.file)
			}

			f, v := spec.open(t)

			// Collect the deleted records. The three sizes are distinct, which
			// is what identifies them: a deleted entry's name is damaged by
			// definition, so matching on it would be matching on the thing
			// under test.
			bySize := make(map[int64]DirEntry)
			err := v.WalkWithOptions(t.Context(), WalkOptions{IncludeDeleted: true},
				func(_ string, _ uint32, e DirEntry) error {
					if e.Deleted && !e.IsDirectory && e.Size > 0 {
						bySize[int64(e.Size)] = e
					}
					return nil
				})
			if err != nil {
				t.Fatalf("WalkWithOptions failed: %v", err)
			}

			for _, rec := range want {
				e, ok := bySize[rec.size]
				if !ok {
					t.Errorf("no deleted record of %d bytes for %s", rec.size, rec.path)
					continue
				}

				// A file spanning more than one cluster cannot be covered
				// without AssumeContiguous: the chain is gone, and libfat does
				// not invent offsets uninvited. A file inside a single cluster
				// is a different case - its first cluster is the whole of it,
				// so no chain is needed and the extents are complete without
				// any assumption. That is correct, not a gap, and it is why
				// this check is conditional rather than universal.
				if rec.size > int64(v.BytesPerCluster()) {
					plain, err := v.FragmentOffsetsWithOptions(e, FragmentOptions{})
					if err == nil && plain != nil && plain.BytesCovered == rec.size {
						t.Errorf("%s: the released chain still covered all %d bytes; "+
							"this volume no longer tests recovery", rec.path, rec.size)
					}
				}

				result, err := v.FragmentOffsetsWithOptions(e, FragmentOptions{AssumeContiguous: true})
				if err != nil {
					t.Errorf("%s: recovery failed: %v", rec.path, err)
					continue
				}
				if !result.Assumed {
					t.Errorf("%s: extents were reconstructed but not flagged Assumed, so a "+
						"caller cannot tell the hypothesis from a fact", rec.path)
				}
				if result.ChainWalked {
					t.Errorf("%s: ChainWalked is true for a record whose chain was released", rec.path)
				}
				if result.FirstClusterReallocated {
					t.Logf("%s: its first cluster has been reused, so the content is not expected back", rec.path)
					continue
				}
				if result.BytesCovered != rec.size {
					t.Errorf("%s: recovered extents cover %d bytes, the record says %d",
						rec.path, result.BytesCovered, rec.size)
					continue
				}

				got, n, err := hashViaExtents(f, result.Ranges)
				if err != nil {
					t.Errorf("%s: reading the recovered extents failed: %v", rec.path, err)
					continue
				}
				if n != rec.size {
					t.Errorf("%s: read %d bytes, want %d", rec.path, n, rec.size)
				}
				if got != rec.sha256 {
					t.Errorf("%s: recovered %d bytes that are not what was deleted\n"+
						"  deleted   %s\n  recovered %s\n  ranges %v",
						rec.path, n, rec.sha256, got, result.Ranges)
				}
			}
		})
	}
}
