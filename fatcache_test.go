package libfat

import (
	"bytes"
	"io"
	"sync"
	"testing"
)

// countingReaderAt records the offset and length of every read, so a test can
// count the reads that touched a particular region of the image.
type countingReaderAt struct {
	inner io.ReaderAt
	mu    sync.Mutex
	reads [][2]int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	c.reads = append(c.reads, [2]int64{off, int64(len(p))})
	c.mu.Unlock()
	return c.inner.ReadAt(p, off)
}

// readsWithin counts the reads that overlap [start, end).
func (c *countingReaderAt) readsWithin(start, end int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.reads {
		if r[0] < end && r[0]+r[1] > start {
			n++
		}
	}
	return n
}

func (c *countingReaderAt) reset() {
	c.mu.Lock()
	c.reads = nil
	c.mu.Unlock()
}

// fatRegion returns the image byte range holding every FAT copy.
func (im *testImage) fatRegion() (int64, int64) {
	start := int64(im.reserved) * int64(im.bytesPerSector)
	end := start + int64(im.numFATs*im.fatSectors)*int64(im.bytesPerSector)
	return start, end
}

// longChainImage holds a 4000-cluster file split across two runs, which costs
// thousands of FAT lookups to walk.
func longChainImage(t *testing.T) (*testImage, []byte) {
	t.Helper()
	im := newTestImage(t, FATType32)
	var clusters []uint32
	for c := uint32(10); c < 2010; c++ {
		clusters = append(clusters, c)
	}
	for c := uint32(3000); c < 5000; c++ {
		clusters = append(clusters, c)
	}
	content := patternBytes(len(clusters)*int(im.bytesPerCluster()), 0x5A)
	im.addFile(testFile{
		base: "LONG", ext: "BIN", clusters: clusters,
		size: uint32(len(content)), content: content, terminate: true,
	})
	return im, content
}

func TestCacheFATReadsTablesOnce(t *testing.T) {
	im, content := longChainImage(t)
	fatStart, fatEnd := im.fatRegion()

	counter := &countingReaderAt{inner: &mockReaderAt{data: im.data}}
	v, err := OpenWithOptions(counter, OpenOptions{CacheFAT: true})
	if err != nil {
		t.Fatal(err)
	}
	counter.reset()

	f, err := v.OpenPath("/LONG.BIN")
	if err != nil {
		t.Fatal(err)
	}
	data, err := f.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, content) {
		t.Fatal("content differs")
	}
	root, _ := v.GetRootDirectory()
	if _, err := root.ReadDir(); err != nil {
		t.Fatal(err)
	}

	// One read per FAT copy, however long the chain and however many entries
	// were listed.
	if got := counter.readsWithin(fatStart, fatEnd); got > int(im.numFATs) {
		t.Fatalf("%d reads touched the FAT region with CacheFAT, want at most %d", got, im.numFATs)
	}
}

func TestUncachedFATReadsPerEntry(t *testing.T) {
	// The baseline the cache exists to remove, pinned so that the cached test
	// above is known to measure something.
	im, _ := longChainImage(t)
	fatStart, fatEnd := im.fatRegion()

	counter := &countingReaderAt{inner: &mockReaderAt{data: im.data}}
	v, err := Open(counter)
	if err != nil {
		t.Fatal(err)
	}
	counter.reset()
	f, err := v.OpenPath("/LONG.BIN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAll(); err != nil {
		t.Fatal(err)
	}
	if got := counter.readsWithin(fatStart, fatEnd); got < 4000 {
		t.Fatalf("uncached walk made only %d FAT reads; the baseline has changed", got)
	}
}

func TestCacheFATPreservesMirrorAccounting(t *testing.T) {
	im, content := longChainImage(t)

	// Diverge one FAT #2 entry in the middle of the chain. FAT #1 stays
	// authoritative because its value is plausible.
	fat2 := int(im.reserved+im.fatSectors)*int(im.bytesPerSector) + 100*4
	putUint32LE(im.data, fat2, 0x0FFFFFF7)

	run := func(cache bool) (uint64, []byte) {
		v := im.volumeWithOptions(OpenOptions{CacheFAT: cache})
		f, err := v.OpenPath("/LONG.BIN")
		if err != nil {
			t.Fatal(err)
		}
		data, err := f.ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		return v.FATMirrorMismatches(), data
	}

	uncached, d1 := run(false)
	cached, d2 := run(true)
	if uncached == 0 {
		t.Fatal("the diverged entry was never compared")
	}
	if cached != uncached {
		t.Fatalf("mismatches: cached %d, uncached %d", cached, uncached)
	}
	if !bytes.Equal(d1, content) || !bytes.Equal(d2, content) {
		t.Fatal("content differs")
	}
}

func TestCacheFATFallsBackOnShortImage(t *testing.T) {
	// An image cut off inside FAT #2 cannot be cached. The volume must still
	// open and read exactly as it does without the cache.
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "A", ext: "TXT", clusters: []uint32{5}, size: 3, content: []byte("abc"), terminate: true})
	cut := int(im.reserved+im.fatSectors)*int(im.bytesPerSector) + 10
	whole := im.data
	short := append([]byte(nil), whole[:cut]...)

	for _, cache := range []bool{false, true} {
		v, err := OpenWithOptions(&mockReaderAt{data: short}, OpenOptions{CacheFAT: cache})
		if err != nil {
			t.Fatalf("cache=%v: open failed: %v", cache, err)
		}
		if _, err := v.IsClusterAllocated(5); err != nil {
			t.Fatalf("cache=%v: %v", cache, err)
		}
	}
}
