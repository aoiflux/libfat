package libfat

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Cross-checks against an oracle produced by an implementation that shares no
// code with this library.
//
// The rest of the integration tests read each image twice through libfat and
// compare the results. That proves self-consistency, which is worth having, but
// it cannot catch a misreading that both paths make: a chain walk that lands on
// the wrong cluster gives the same wrong answer to the extent map and to the
// reader built on it, and they agree. Only an outside opinion closes that gap.
//
// The oracle is a TSV produced by testdata/make_fat_oracle.py, which drives
// 7-Zip's FAT handler. It is generated once and read here, so running these
// tests needs no 7-Zip. When the file is absent the test skips: an oracle is
// extra assurance, not a prerequisite for the rest of the suite.

// oracleEntry is one row of the oracle.
type oracleEntry struct {
	dir       bool
	size      int64
	shortName string
	modified  string
	created   string
	sha256    string
}

type oracleFile struct {
	filesystem string
	label      string
	volumeSize int64
	clusterSiz int64
	sectorSize int64
	entries    map[string]oracleEntry
}

// oraclePath is where the oracle for a volume lives. It sits beside the image,
// following the convention of the other .oracle.tsv files in the dataset.
func (spec realVolume) oraclePath(t *testing.T) string {
	t.Helper()
	name := spec.file + ".fat-oracle.tsv"
	if spec.oracle != "" {
		name = spec.oracle
	}
	return filepath.Join(imagesDir(t), name)
}

func loadOracle(t *testing.T, path string) *oracleFile {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("no oracle at %s: %v\n\tgenerate one with: python3 testdata/make_fat_oracle.py <image>", path, err)
	}
	defer f.Close()

	out := &oracleFile{entries: make(map[string]oracleEntry)}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for line := 1; sc.Scan(); line++ {
		cols := strings.Split(sc.Text(), "\t")
		switch cols[0] {
		case "V":
			if len(cols) < 6 {
				t.Fatalf("%s:%d: volume record has %d columns, want 6", path, line, len(cols))
			}
			out.filesystem, out.label = cols[1], cols[2]
			out.volumeSize = mustAtoi(t, path, line, cols[3])
			out.clusterSiz = mustAtoi(t, path, line, cols[4])
			out.sectorSize = mustAtoi(t, path, line, cols[5])
		case "d":
			out.entries[cols[1]] = oracleEntry{dir: true}
		case "f":
			if len(cols) < 7 {
				t.Fatalf("%s:%d: file record has %d columns, want 7", path, line, len(cols))
			}
			out.entries[cols[1]] = oracleEntry{
				size:      mustAtoi(t, path, line, cols[2]),
				shortName: cols[3],
				modified:  cols[4],
				created:   cols[5],
				sha256:    cols[6],
			}
		default:
			t.Fatalf("%s:%d: unknown record type %q", path, line, cols[0])
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if len(out.entries) == 0 {
		t.Fatalf("%s has no entries", path)
	}
	return out
}

func mustAtoi(t *testing.T, path string, line int, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		t.Fatalf("%s:%d: %q is not a number: %v", path, line, s, err)
	}
	return n
}

// TestRealVolumeMatchesIndependentOracle compares what libfat reads from each
// volume against what 7-Zip read from the same bytes.
//
// The comparison covers the things a second implementation can genuinely
// corroborate: the volume's geometry, the exact set of paths - which is where
// long-name assembly either works or does not - every file's size and 8.3 short
// name, and the content itself, by sha256 of the bytes libfat hands back
// against the sha256 of the bytes 7-Zip extracted.
func TestRealVolumeMatchesIndependentOracle(t *testing.T) {
	for _, spec := range realVolumes {
		t.Run(spec.name, func(t *testing.T) {
			oracle := loadOracle(t, spec.oraclePath(t))
			_, v := spec.open(t)

			if got := strings.ToUpper(oracle.filesystem); got != v.FATType() {
				t.Errorf("7-Zip calls this %s, libfat calls it %s", got, v.FATType())
			}
			if oracle.label != "" && oracle.label != v.VolumeLabel() {
				t.Errorf("label: 7-Zip %q, libfat %q", oracle.label, v.VolumeLabel())
			}
			if got := int64(v.BytesPerCluster()); oracle.clusterSiz != got {
				t.Errorf("cluster size: 7-Zip %d, libfat %d", oracle.clusterSiz, got)
			}
			if got := int64(v.BytesPerSector()); oracle.sectorSize != got {
				t.Errorf("sector size: 7-Zip %d, libfat %d", oracle.sectorSize, got)
			}
			if got := int64(v.VolumeSize()); oracle.volumeSize != got {
				t.Errorf("volume size: 7-Zip %d, libfat %d", oracle.volumeSize, got)
			}

			spent := budget(t)
			seen := make(map[string]bool, len(oracle.entries))
			var hashed, hashedBytes int64
			var mismatched int

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
				if testing.Short() && hashedBytes >= spent.bytes {
					return nil
				}
				file, err := v.OpenEntry(e)
				if err != nil {
					t.Errorf("%s: OpenEntry failed: %v", path, err)
					return nil
				}
				got, n, err := hashViaReader(file)
				if err != nil {
					t.Errorf("%s: reading through libfat failed: %v", path, err)
					return nil
				}
				if got != want.sha256 {
					mismatched++
					if mismatched <= 10 {
						t.Errorf("%s: content differs from what 7-Zip extracted\n  7-Zip  %s\n  libfat %s",
							path, want.sha256, got)
					}
				}
				hashed++
				hashedBytes += n
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
				t.Fatal("no file content was compared against the oracle")
			}
			t.Logf("%d entries agreed; %d files (%d bytes) hashed identically",
				len(seen), hashed, hashedBytes)
		})
	}
}

// TestRealVolumeTimestampsMatchOracle is kept apart from the comparison above
// because a disagreement here means something different.
//
// FAT records local time with no zone beside it. libfat decodes those fields as
// UTC because there is nothing else it could honestly do - see the
// TimezoneOffsets field of Capabilities - so what is compared is the wall-clock
// reading of the stored bytes, not an instant. A mismatch is a decoding
// difference, not a bad extent map, and it does not invalidate anything the
// other integration tests establish.
func TestRealVolumeTimestampsMatchOracle(t *testing.T) {
	const layout = "2006-01-02 15:04:05"

	for _, spec := range realVolumes {
		t.Run(spec.name, func(t *testing.T) {
			oracle := loadOracle(t, spec.oraclePath(t))
			_, v := spec.open(t)

			var compared, differed int
			err := v.Walk(t.Context(), func(path string, _ uint32, e DirEntry) error {
				want, ok := oracle.entries[path]
				if !ok || want.dir || want.modified == "" {
					return nil
				}
				compared++
				if got := e.ModifiedAt.UTC().Format(layout); got != want.modified {
					differed++
					if differed <= 5 {
						t.Errorf("%s: modified time reads %q to 7-Zip and %q to libfat",
							path, want.modified, got)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("Walk failed: %v", err)
			}
			if compared == 0 {
				t.Skip("the oracle carries no timestamps")
			}
			t.Logf("%d modification times compared, %d differed", compared, differed)
		})
	}
}
