package libfat

import (
	"bytes"
	"testing"
)

// TestRangeFileOffsetIsCumulative is the F6 acceptance case. FAT has no holes,
// so the invariant that matters is that FileOffset is exactly the sum of the
// preceding runs' lengths and that the runs, read in order, reproduce the file.
func TestRangeFileOffsetIsCumulative(t *testing.T) {
	im := newTestImage(t, FATType16)
	size := int(im.bytesPerCluster()*3 + 17)
	content := patternBytes(size, 0x5A)

	// Deliberately discontiguous, so more than one run is produced.
	im.addFile(testFile{
		base:      "FRAG",
		ext:       "BIN",
		clusters:  []uint32{3, 8, 9, 20},
		size:      uint32(size),
		content:   content,
		terminate: true,
	})

	v := im.volume()
	defer v.Close()

	e := entryNamed(t, v, "FRAG.BIN")
	ranges, err := v.FragmentOffsets(e)
	if err != nil {
		t.Fatalf("FragmentOffsets failed: %v", err)
	}
	if len(ranges) < 2 {
		t.Fatalf("fixture produced %d run(s); the test needs a fragmented file", len(ranges))
	}

	var want int64
	var rebuilt []byte
	for i, r := range ranges {
		if r.FileOffset != want {
			t.Errorf("run %d: FileOffset = %d, want %d (sum of preceding lengths)",
				i, r.FileOffset, want)
		}
		if r.Sparse {
			t.Errorf("run %d is marked sparse; FAT has no sparse allocation", i)
		}
		buf := make([]byte, r.Length)
		if _, rerr := v.ReadAt(buf, r.StartByte); rerr != nil {
			t.Fatalf("run %d: ReadAt(%d) failed: %v", i, r.StartByte, rerr)
		}
		rebuilt = append(rebuilt, buf...)
		want += r.Length
	}

	if want != int64(size) {
		t.Errorf("runs sum to %d bytes, want %d: the slice is not gap-free", want, size)
	}
	if !bytes.Equal(rebuilt, content) {
		t.Fatalf("concatenating the runs did not reproduce the file")
	}
}

// TestSlackRangeFileOffsetIsFileSize pins the one range whose FileOffset is not
// a position inside the file: slack begins where the file's bytes end.
func TestSlackRangeFileOffsetIsFileSize(t *testing.T) {
	im := newTestImage(t, FATType16)
	size := uint32(im.bytesPerCluster() + 5) // ends mid-cluster, so slack exists
	im.addFile(testFile{
		base:      "SLACK",
		ext:       "BIN",
		clusters:  []uint32{4, 5},
		size:      size,
		content:   patternBytes(int(size), 0x33),
		terminate: true,
	})

	v := im.volume()
	defer v.Close()

	e := entryNamed(t, v, "SLACK.BIN")
	slack, ok, err := v.SlackRange(e)
	if err != nil {
		t.Fatalf("SlackRange failed: %v", err)
	}
	if !ok {
		t.Fatal("expected slack for a file that does not end on a cluster boundary")
	}
	if slack.FileOffset != int64(size) {
		t.Fatalf("slack FileOffset = %d, want the file size %d", slack.FileOffset, size)
	}
	// Slack must begin exactly where the last data run ends.
	ranges, err := v.FragmentOffsets(e)
	if err != nil {
		t.Fatalf("FragmentOffsets failed: %v", err)
	}
	if last := ranges[len(ranges)-1]; slack.StartByte != last.EndByte() {
		t.Fatalf("slack starts at %d, want %d", slack.StartByte, last.EndByte())
	}
}

// TestCoalesceRenumbersFileOffsets covers the exported helper's documented
// promise, since a caller assembling ranges from another source relies on it.
func TestCoalesceRenumbersFileOffsets(t *testing.T) {
	in := []Range{
		{StartByte: 100, Length: 10, FileOffset: 999},
		{StartByte: 110, Length: 10, FileOffset: 999}, // adjacent, merges
		{StartByte: 500, Length: 7, FileOffset: 999},
	}
	got := Coalesce(in)
	if len(got) != 2 {
		t.Fatalf("Coalesce produced %d runs, want 2", len(got))
	}
	if got[0].FileOffset != 0 {
		t.Errorf("first run FileOffset = %d, want 0", got[0].FileOffset)
	}
	if got[0].Length != 20 {
		t.Errorf("merged run Length = %d, want 20", got[0].Length)
	}
	if got[1].FileOffset != 20 {
		t.Errorf("second run FileOffset = %d, want 20", got[1].FileOffset)
	}

	single := Coalesce([]Range{{StartByte: 8, Length: 4, FileOffset: 77}})
	if single[0].FileOffset != 0 {
		t.Errorf("single-run FileOffset = %d, want 0", single[0].FileOffset)
	}
}

// TestFileOffsetMatchesReportFragments pins the two representations against
// each other, since the report now reads the value rather than deriving it.
func TestFileOffsetMatchesReportFragments(t *testing.T) {
	im := newTestImage(t, FATType32)
	size := uint32(im.bytesPerCluster()*2 + 3)
	im.addFile(testFile{
		base:      "REP",
		ext:       "BIN",
		clusters:  []uint32{6, 11, 12},
		size:      size,
		content:   patternBytes(int(size), 0x21),
		terminate: true,
	})

	v := im.volume()
	defer v.Close()

	e := entryNamed(t, v, "REP.BIN")
	ranges, err := v.FragmentOffsets(e)
	if err != nil {
		t.Fatalf("FragmentOffsets failed: %v", err)
	}
	rep, err := v.Report("t")
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	row := rowFor(t, rep, e.Path)
	if len(row.Fragments) != len(ranges) {
		t.Fatalf("report has %d fragments, ranges have %d", len(row.Fragments), len(ranges))
	}
	for i := range ranges {
		if row.Fragments[i].FileOffset != ranges[i].FileOffset {
			t.Errorf("fragment %d: report says %d, Range says %d",
				i, row.Fragments[i].FileOffset, ranges[i].FileOffset)
		}
	}
}
