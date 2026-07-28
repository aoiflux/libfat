package libfat

import (
	"strings"
	"testing"
)

// testImage builds a synthetic but structurally valid FAT volume in which every
// file's cluster chain is specified explicitly. Placing files at chosen
// clusters is what makes fragmentation testable: the expected byte ranges can
// be computed by hand from the layout and compared against the library's.
type testImage struct {
	t *testing.T

	data    []byte
	fatType string

	bytesPerSector  uint32
	spc             uint32
	reserved        uint32
	numFATs         uint32
	fatSectors      uint32
	rootEntries     uint32
	totalSectors    uint32
	rootDirSectors  uint32
	firstDataSector uint32
	rootDirSector   uint32
	rootCluster     uint32
	clusterCount    uint32

	rootCursor int
}

func newTestImage(t *testing.T, fatType string) *testImage {
	t.Helper()

	im := &testImage{t: t, fatType: fatType, bytesPerSector: 512, spc: 1, numFATs: 2}
	cfg := bootSectorConfig{
		oem:            "MSWIN4.1",
		bytesPerSector: 512,
		volumeLabel:    "TESTVOL",
		fsTypeHint:     fatType,
	}

	switch fatType {
	case FATType12:
		im.reserved, im.fatSectors, im.rootEntries, im.totalSectors = 1, 9, 224, 2880
		cfg.sectorsPerCluster, cfg.reservedSectors, cfg.numberOfFATs = 1, 1, 2
		cfg.rootEntryCount, cfg.totalSectors16, cfg.fatSize16 = 224, 2880, 9
	case FATType16:
		im.reserved, im.fatSectors, im.rootEntries, im.totalSectors = 1, 40, 512, 10000
		cfg.sectorsPerCluster, cfg.reservedSectors, cfg.numberOfFATs = 1, 1, 2
		cfg.rootEntryCount, cfg.totalSectors16, cfg.fatSize16 = 512, 10000, 40
	case FATType32:
		// FAT32 requires at least 65525 clusters, which fixes the minimum
		// image size at roughly 34 MB with 512-byte single-sector clusters.
		im.reserved, im.fatSectors, im.rootEntries, im.totalSectors = 32, 528, 0, 67632
		cfg.sectorsPerCluster, cfg.reservedSectors, cfg.numberOfFATs = 1, 32, 2
		cfg.rootEntryCount, cfg.totalSectors32, cfg.fatSize32 = 0, 67632, 528
		cfg.rootCluster = 2
		im.rootCluster = 2
	default:
		t.Fatalf("unsupported FAT type %q", fatType)
	}

	im.rootDirSectors = (im.rootEntries*dirEntrySize + im.bytesPerSector - 1) / im.bytesPerSector
	im.rootDirSector = im.reserved + im.numFATs*im.fatSectors
	im.firstDataSector = im.rootDirSector + im.rootDirSectors
	im.clusterCount = (im.totalSectors - im.firstDataSector) / im.spc

	im.data = makeBootSectorImage(cfg)

	// Reserved FAT entries: media descriptor and end-of-chain marker.
	im.setFATEntry(0, 0x0FFFFFF8)
	im.setFATEntry(1, im.eoc())
	if fatType == FATType32 {
		im.setFATEntry(im.rootCluster, im.eoc())
	}
	return im
}

func (im *testImage) bytesPerCluster() uint32 {
	return im.bytesPerSector * im.spc
}

func (im *testImage) eoc() uint32 {
	switch im.fatType {
	case FATType12:
		return 0x0FFF
	case FATType16:
		return 0xFFFF
	default:
		return 0x0FFFFFFF
	}
}

func (im *testImage) badCluster() uint32 {
	switch im.fatType {
	case FATType12:
		return 0x0FF7
	case FATType16:
		return 0xFFF7
	default:
		return 0x0FFFFFF7
	}
}

// clusterOffset returns the absolute image offset of a data cluster, computed
// independently of the library so that tests verify rather than mirror it.
func (im *testImage) clusterOffset(cluster uint32) int64 {
	sector := im.firstDataSector + (cluster-2)*im.spc
	return int64(sector) * int64(im.bytesPerSector)
}

func (im *testImage) setFATEntry(cluster, value uint32) {
	for table := uint32(0); table < im.numFATs; table++ {
		base := int(im.reserved+table*im.fatSectors) * int(im.bytesPerSector)
		switch im.fatType {
		case FATType12:
			off := base + int(cluster+(cluster/2))
			cur := uint16(im.data[off]) | uint16(im.data[off+1])<<8
			if cluster&1 == 0 {
				cur = (cur & 0xF000) | uint16(value&0x0FFF)
			} else {
				cur = (cur & 0x000F) | uint16((value&0x0FFF)<<4)
			}
			putUint16LE(im.data, off, cur)
		case FATType16:
			putUint16LE(im.data, base+int(cluster)*2, uint16(value))
		case FATType32:
			putUint32LE(im.data, base+int(cluster)*4, value&0x0FFFFFFF)
		}
	}
}

// linkChain writes the FAT links for the given clusters. When terminate is
// false the final cluster is left without an end-of-chain marker, which is what
// a freed chain looks like.
func (im *testImage) linkChain(clusters []uint32, terminate bool) {
	for i := 0; i+1 < len(clusters); i++ {
		im.setFATEntry(clusters[i], clusters[i+1])
	}
	if terminate && len(clusters) > 0 {
		im.setFATEntry(clusters[len(clusters)-1], im.eoc())
	}
}

// freeChain zeroes the FAT entries for the given clusters, simulating deletion.
func (im *testImage) freeChain(clusters []uint32) {
	for _, c := range clusters {
		im.setFATEntry(c, 0)
	}
}

func (im *testImage) writeClusterData(clusters []uint32, content []byte) {
	per := int(im.bytesPerCluster())
	for i, c := range clusters {
		start := i * per
		if start >= len(content) {
			return
		}
		end := start + per
		if end > len(content) {
			end = len(content)
		}
		off := im.clusterOffset(c)
		copy(im.data[off:off+int64(end-start)], content[start:end])
	}
}

func (im *testImage) rootDirOffset() int64 {
	if im.fatType == FATType32 {
		return im.clusterOffset(im.rootCluster)
	}
	return int64(im.rootDirSector) * int64(im.bytesPerSector)
}

// addRootRaw appends 32-byte directory entries to the root directory and
// returns the buffer offset of the first one.
func (im *testImage) addRootRaw(entries ...[]byte) int {
	first := im.rootCursor
	base := im.rootDirOffset()
	for _, e := range entries {
		off := base + int64(im.rootCursor)
		copy(im.data[off:off+dirEntrySize], e)
		im.rootCursor += dirEntrySize
	}
	return first
}

// testFile describes one file to place in the image.
type testFile struct {
	base     string
	ext      string
	longName string
	attrs    byte
	clusters []uint32
	size     uint32
	content  []byte
	// deleted marks the directory entries with 0xE5 and, unless keepChain is
	// set, frees the FAT entries the way a real deletion does.
	deleted   bool
	keepChain bool
	// terminate writes an end-of-chain marker on the last cluster.
	terminate bool
}

func (im *testImage) addFile(f testFile) {
	im.t.Helper()

	if f.attrs == 0 {
		f.attrs = 0x20
	}
	if !f.deleted || f.keepChain {
		im.linkChain(f.clusters, f.terminate)
	} else {
		im.linkChain(f.clusters, false)
		im.freeChain(f.clusters)
	}
	if len(f.content) > 0 {
		im.writeClusterData(f.clusters, f.content)
	}

	shortName := paddedASCII(strings.ToUpper(f.base), 8)
	shortName = append(shortName, paddedASCII(strings.ToUpper(f.ext), 3)...)

	var raw [][]byte
	if f.longName != "" {
		for _, e := range makeLFNEntries(f.longName, string(shortName)) {
			if f.deleted {
				e[0] = 0xE5
			}
			raw = append(raw, e)
		}
	}

	cluster := uint32(0)
	if len(f.clusters) > 0 {
		cluster = f.clusters[0]
	}
	short := makeShortEntry(f.base, f.ext, f.attrs, uint16(cluster), f.size)
	putUint16LE(short, 20, uint16(cluster>>16))
	// Give every entry a timestamp so that isValidShortEntry accepts it.
	putUint16LE(short, 24, 0x4A21)
	putUint16LE(short, 22, 0x6000)
	if f.deleted {
		short[0] = 0xE5
	}
	raw = append(raw, short)
	im.addRootRaw(raw...)
}

// addSubdir creates a subdirectory occupying the given clusters and fills it
// with "." and ".." followed by entries. Passing non-adjacent clusters produces
// a fragmented directory, which is what exercises absolute-offset mapping.
func (im *testImage) addSubdir(name string, clusters []uint32, entries ...[]byte) {
	im.t.Helper()

	im.linkChain(clusters, true)

	content := make([]byte, len(clusters)*int(im.bytesPerCluster()))
	dot := makeShortEntry(".", "", 0x10, uint16(clusters[0]), 0)
	dotdot := makeShortEntry("..", "", 0x10, 0, 0)
	copy(content[0:dirEntrySize], dot)
	copy(content[dirEntrySize:2*dirEntrySize], dotdot)
	cursor := 2 * dirEntrySize
	for _, e := range entries {
		copy(content[cursor:cursor+dirEntrySize], e)
		cursor += dirEntrySize
	}
	im.writeClusterData(clusters, content)

	dirEntry := makeShortEntry(name, "", 0x10, uint16(clusters[0]), 0)
	putUint16LE(dirEntry, 20, uint16(clusters[0]>>16))
	putUint16LE(dirEntry, 24, 0x4A21)
	im.addRootRaw(dirEntry)
}

// addDeletedSubdir creates a subdirectory whose entry in the root is marked
// deleted and whose FAT chain has been freed, but whose clusters still hold
// intact directory data. This is what a deleted directory tree actually looks
// like on disk and is the situation orphan recovery exists to handle.
//
// When linkParent is false the root entry is omitted entirely, modelling a
// directory whose parent entry has itself been overwritten.
func (im *testImage) addDeletedSubdir(name string, clusters []uint32, linkParent bool, entries ...[]byte) {
	im.t.Helper()

	content := make([]byte, len(clusters)*int(im.bytesPerCluster()))
	copy(content[0:dirEntrySize], makeShortEntry(".", "", 0x10, uint16(clusters[0]), 0))
	copy(content[dirEntrySize:2*dirEntrySize], makeShortEntry("..", "", 0x10, 0, 0))
	cursor := 2 * dirEntrySize
	for _, e := range entries {
		copy(content[cursor:cursor+dirEntrySize], e)
		cursor += dirEntrySize
	}
	im.writeClusterData(clusters, content)

	// Deletion frees the chain; the cluster contents are left untouched.
	im.freeChain(clusters)

	if linkParent {
		dirEntry := makeShortEntry(name, "", 0x10, uint16(clusters[0]), 0)
		putUint16LE(dirEntry, 20, uint16(clusters[0]>>16))
		putUint16LE(dirEntry, 24, 0x4A21)
		dirEntry[0] = 0xE5
		im.addRootRaw(dirEntry)
	}
}

// writeRawCluster fills a cluster with arbitrary bytes.
func (im *testImage) writeRawCluster(cluster uint32, data []byte) {
	off := im.clusterOffset(cluster)
	copy(im.data[off:off+int64(len(data))], data)
}

// makeTimestampedEntry builds a short entry that passes validation, for use as
// subdirectory content.
func makeTimestampedEntry(base, ext string, attrs byte, cluster uint32, size uint32) []byte {
	e := makeShortEntry(base, ext, attrs, uint16(cluster), size)
	putUint16LE(e, 20, uint16(cluster>>16))
	putUint16LE(e, 24, 0x4A21)
	putUint16LE(e, 22, 0x6000)
	return e
}

func (im *testImage) volume() *Volume {
	im.t.Helper()
	v, err := Open(&mockReaderAt{data: im.data})
	if err != nil {
		im.t.Fatalf("Open failed: %v", err)
	}
	return v
}

func (im *testImage) volumeWithOptions(opts OpenOptions) *Volume {
	im.t.Helper()
	v, err := OpenWithOptions(&mockReaderAt{data: im.data}, opts)
	if err != nil {
		im.t.Fatalf("OpenWithOptions failed: %v", err)
	}
	return v
}

// entryNamed locates a parsed entry by name, including deleted ones.
func entryNamed(t *testing.T, v *Volume, name string) DirEntry {
	t.Helper()
	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}
	entries, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, e := range entries {
		if strings.EqualFold(e.Name, name) || strings.EqualFold(e.ShortName, name) {
			return e
		}
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name)
	}
	t.Fatalf("entry %q not found; directory holds %v", name, got)
	return DirEntry{}
}

// patternBytes produces deterministic, position-dependent content so that a
// read spanning fragments cannot pass by accident.
func patternBytes(n int, seed byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = seed ^ byte(i*7+i/251)
	}
	return out
}
