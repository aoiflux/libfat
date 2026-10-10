package libfat

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestEncodeDecodeDateTimeRoundTrip(t *testing.T) {
	check := func(want time.Time) {
		t.Helper()
		date, clock, tenths := EncodeDateTime(want)
		if tenths > 199 {
			t.Fatalf("%v: tenths %d out of range", want, tenths)
		}
		if got := DecodeDateTime(date, clock, tenths); !got.Equal(want) {
			t.Fatalf("%v encoded as (%#04x, %#04x, %d) and decoded as %v", want, date, clock, tenths, got)
		}
	}

	// Every 10 ms step across a two-second window, which covers every tenths
	// value against both an even and an odd second.
	start := time.Date(2024, time.February, 29, 23, 59, 58, 0, time.UTC)
	for i := 0; i < 400; i++ {
		check(start.Add(time.Duration(i) * 10 * time.Millisecond))
	}

	// Every day of the first year, a leap year, and the last year.
	for _, year := range []int{1980, 2000, 2107} {
		for d := time.Date(year, 1, 1, 13, 37, 41, 120_000_000, time.UTC); d.Year() == year; d = d.AddDate(0, 0, 1) {
			check(d)
		}
	}

	// A prime stride across the whole range, landing on arbitrary times of day
	// and arbitrary 10 ms offsets.
	const stride = 7919*time.Second + 130*time.Millisecond
	end := time.Date(2107, time.December, 31, 23, 59, 59, 990_000_000, time.UTC)
	for at := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC); !at.After(end); at = at.Add(stride) {
		check(at)
	}

	check(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC))
	check(end)
}

func TestEncodeDateTimeEdges(t *testing.T) {
	if d, c, n := EncodeDateTime(time.Time{}); d != 0 || c != 0 || n != 0 {
		t.Fatalf("zero time encoded as (%d, %d, %d), want all zero", d, c, n)
	}
	if !DecodeDateTime(EncodeDateTime(time.Time{})).IsZero() {
		t.Fatal("zero time did not round-trip to the zero time")
	}

	first := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(2107, 12, 31, 23, 59, 59, 990_000_000, time.UTC)
	for _, tc := range []struct {
		in, want time.Time
	}{
		{time.Date(1979, 12, 31, 23, 59, 59, 0, time.UTC), first},
		{time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC), first},
		{time.Date(2108, 1, 1, 0, 0, 0, 0, time.UTC), last},
		{time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), last},
		// Precision below 10 ms is truncated, not rounded.
		{time.Date(2010, 6, 1, 8, 0, 1, 999_999_999, time.UTC), time.Date(2010, 6, 1, 8, 0, 1, 990_000_000, time.UTC)},
		// A time in another zone encodes as the same instant in UTC.
		{time.Date(2010, 6, 1, 10, 30, 0, 0, time.FixedZone("UTC+2", 2*3600)), time.Date(2010, 6, 1, 8, 30, 0, 0, time.UTC)},
	} {
		if got := DecodeDateTime(EncodeDateTime(tc.in)); !got.Equal(tc.want) {
			t.Errorf("%v: got %v, want %v", tc.in, got, tc.want)
		}
	}

	// The field layout, checked by hand: 2001-09-09 01:46:40 is year offset 21,
	// month 9, day 9, and 01:46:40 is clock 1<<11|46<<5|20.
	d, c, n := EncodeDateTime(time.Date(2001, 9, 9, 1, 46, 41, 50_000_000, time.UTC))
	if d != 21<<9|9<<5|9 || c != 1<<11|46<<5|20 || n != 105 {
		t.Fatalf("got (%#04x, %#04x, %d)", d, c, n)
	}
}

// TestLFNChecksumVectors checks the exported checksum against fixed values.
// They were computed with the algorithm as the FAT specification writes it -
// rotate right by one, then add the next byte - which is a different
// formulation from the one this package implements, and "A          " was
// stepped through by hand. TestRealVolumeCodecsAgreeWithImages checks the
// function against slots that Windows and mkfs.fat wrote, when the real images
// are available.
func TestLFNChecksumVectors(t *testing.T) {
	for _, tc := range []struct {
		name string
		want byte
	}{
		{"README  TXT", 0x73},
		{"PROGRA~1   ", 0x20},
		{"QUARTE~1TXT", 0x6E},
		{"NEWFOL~1   ", 0xDD},
		{"           ", 0xF7},
		{"A          ", 0x80},
	} {
		var raw [11]byte
		copy(raw[:], tc.name)
		if got := LFNChecksum(raw); got != tc.want {
			t.Errorf("LFNChecksum(%q) = %#02x, want %#02x", tc.name, got, tc.want)
		}
		if got := lfnChecksum(raw[:]); got != tc.want {
			t.Errorf("the test builder's checksum disagrees for %q: %#02x", tc.name, got)
		}
	}
}

func TestLFNCharOffsetsMatchParser(t *testing.T) {
	if LFNCharOffsets != lfnCharOffsets {
		t.Fatalf("exported offsets %v differ from the parser's %v", LFNCharOffsets, lfnCharOffsets)
	}
	// Mutating the export must not reach the parser.
	saved := LFNCharOffsets
	LFNCharOffsets[0] = 99
	defer func() { LFNCharOffsets = saved }()
	if lfnCharOffsets[0] != 1 {
		t.Fatal("assigning to LFNCharOffsets changed the parser's offsets")
	}
}

func raw11(s string) [11]byte {
	var r [11]byte
	copy(r[:], s)
	return r
}

func TestParseShortName(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		ntres byte
		want  string
	}{
		{"README  TXT", 0, "README.TXT"},
		{"README  TXT", NTResLowerBase, "readme.TXT"},
		{"README  TXT", NTResLowerExt, "README.txt"},
		{"README  TXT", NTResLowerBase | NTResLowerExt, "readme.txt"},
		{"MAKEFILE   ", 0, "MAKEFILE"},
		{"\x05BC     TXT", 0, "\xe5BC.TXT"},
		{".          ", 0, "."},
		{"..         ", 0, ".."},
	} {
		if got := ParseShortName(raw11(tc.raw), tc.ntres); got != tc.want {
			t.Errorf("ParseShortName(%q, %#02x) = %q, want %q", tc.raw, tc.ntres, got, tc.want)
		}
	}
}

func TestIsValid83Char(t *testing.T) {
	for _, b := range []byte("AZ09!#$%&'()-@^_`{}~ ") {
		if !IsValid83Char(b) {
			t.Errorf("%q rejected", b)
		}
	}
	for _, b := range []byte("\x00\x1f\"*+,./:;<=>?[\\]|") {
		if IsValid83Char(b) {
			t.Errorf("%q accepted", b)
		}
	}
	if !IsValid83Char(0x80) {
		t.Error("an OEM code-page byte was rejected")
	}
}

// TestValidShortNameAgreesWithReadDir is the claim ValidShortName makes: it
// accepts exactly the names ReadDir lists as live records. Each candidate is
// written as a timestamped, attribute-valid record, so the name is the only
// thing that decides whether it is listed.
func TestValidShortNameAgreesWithReadDir(t *testing.T) {
	alphabet := []byte{'A', 'z', '1', '~', ' ', ' ', ' ', '.', '*', '+', 0x05, 0xE5, 0x7F, 0x80, 0x1F}
	rng := rand.New(rand.NewPCG(1, 2))
	candidates := [][11]byte{
		raw11("README  TXT"), raw11("A          "), raw11("        TXT"), raw11("           "),
		raw11(".          "), raw11("..         "), raw11("A.         "), raw11(" A         "),
		raw11("A B     TXT"), raw11("AB      T X"), raw11("\x05BC     TXT"), raw11("ABC     \x80XT"),
	}
	for len(candidates) < 1500 {
		var r [11]byte
		for i := range r {
			r[i] = alphabet[rng.IntN(len(alphabet))]
		}
		if r[0] == 0x00 || r[0] == DeletedMarker {
			// Not a live record at all; ValidShortName rejects these outright.
			if ValidShortName(r) {
				t.Fatalf("%q accepted with a free or deleted first byte", r)
			}
			continue
		}
		candidates = append(candidates, r)
	}

	const perImage = 500 // the FAT16 builder's root holds 512 records
	for start := 0; start < len(candidates); start += perImage {
		batch := candidates[start:min(start+perImage, len(candidates))]
		im := newTestImage(t, FATType16)
		for _, name := range batch {
			rec := makeTimestampedEntry("", "", AttrArchive, 0, 0)
			copy(rec[0:11], name[:])
			im.addRootRaw(rec)
		}
		root, err := im.volume().GetRootDirectory()
		if err != nil {
			t.Fatal(err)
		}
		entries, err := root.ReadDirWithOptions(ReadDirOptions{IncludeDotEntries: true})
		if err != nil {
			t.Fatal(err)
		}
		listed := make(map[int64]bool)
		for _, e := range entries {
			if !e.Deleted {
				listed[e.EntryOffset] = true
			}
		}
		for i, name := range batch {
			if got, want := ValidShortName(name), listed[int64(i*DirEntrySize)]; got != want {
				t.Errorf("%q: ValidShortName = %v, ReadDir lists it = %v", name, got, want)
			}
		}
	}
}

func TestReadDirWithOptions(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LIVE", ext: "TXT", clusters: []uint32{5}, size: 3, content: []byte("abc"), terminate: true})
	im.addFile(testFile{base: "GONE", ext: "TXT", clusters: []uint32{6}, size: 3, content: []byte("xyz"), deleted: true})
	im.addSubdir("SUB", []uint32{10}, makeTimestampedEntry("INNER", "TXT", AttrArchive, 0, 0))
	v := im.volume()

	root, _ := v.GetRootDirectory()
	names := func(entries []DirEntry) []string {
		var out []string
		for _, e := range entries {
			out = append(out, e.Path)
		}
		return out
	}

	all, err := root.ReadDir()
	if err != nil {
		t.Fatal(err)
	}
	if got := names(all); len(got) != 3 {
		t.Fatalf("ReadDir = %v, want LIVE, the deleted entry and SUB", got)
	}

	live, err := root.ReadDirWithOptions(ReadDirOptions{ExcludeDeleted: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := names(live); len(got) != 2 || got[0] != "/LIVE.TXT" || got[1] != "/SUB" {
		t.Fatalf("ExcludeDeleted = %v", got)
	}

	sub, err := v.OpenPath("/SUB")
	if err != nil {
		t.Fatal(err)
	}
	withDots, err := sub.ReadDirWithOptions(ReadDirOptions{IncludeDotEntries: true})
	if err != nil {
		t.Fatal(err)
	}
	got := names(withDots)
	if len(got) != 3 || got[0] != "/SUB/." || got[1] != "/SUB/.." || got[2] != "/SUB/INNER.TXT" {
		t.Fatalf("IncludeDotEntries = %v", got)
	}
	if !withDots[0].IsDirectory || withDots[0].FirstCluster != 10 || withDots[1].FirstCluster != 0 {
		t.Fatalf("dot records misread: %+v %+v", withDots[0], withDots[1])
	}

	plain, _ := sub.ReadDir()
	if got := names(plain); len(got) != 1 {
		t.Fatalf("ReadDir on a subdirectory = %v, want only INNER.TXT", got)
	}
}

func TestGeometry(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			g := im.volume().Geometry()
			want := Geometry{
				BytesPerSector:     im.bytesPerSector,
				SectorsPerCluster:  im.spc,
				BytesPerCluster:    im.bytesPerCluster(),
				ReservedSectors:    im.reserved,
				NumberOfFATs:       im.numFATs,
				FATSizeSectors:     im.fatSectors,
				FirstFATSector:     im.reserved,
				RootDirFirstSector: im.rootDirSector,
				RootDirSectors:     im.rootDirSectors,
				FirstDataSector:    im.firstDataSector,
				RootCluster:        DefaultRootCluster,
				ClusterCount:       im.clusterCount,
				TotalSectors:       im.totalSectors,
			}
			if fatType == FATType32 {
				// The root is cluster 2, which is the first data sector.
				want.RootDirFirstSector = im.firstDataSector
			}
			if g != want {
				t.Fatalf("Geometry =\n%+v\nwant\n%+v", g, want)
			}
			// The documented cluster formula lands where the builder put it.
			sector := g.FirstDataSector + (7-DefaultRootCluster)*g.SectorsPerCluster
			if int64(sector)*int64(g.BytesPerSector) != im.clusterOffset(7) {
				t.Fatal("cluster address from Geometry disagrees with the image")
			}
		})
	}
}
