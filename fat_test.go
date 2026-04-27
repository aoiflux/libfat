package libfat

import (
	"errors"
	"io"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
)

type mockReaderAt struct {
	data []byte
}

func (m *mockReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

type mockFileInfo struct {
	name string
	mode fs.FileMode
}

func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) Size() int64        { return 0 }
func (m mockFileInfo) Mode() fs.FileMode  { return m.mode }
func (m mockFileInfo) ModTime() time.Time { return time.Time{} }
func (m mockFileInfo) IsDir() bool        { return m.mode.IsDir() }
func (m mockFileInfo) Sys() interface{}   { return nil }

type mockReaderAtWithStat struct {
	data []byte
	info fs.FileInfo
}

func (m *mockReaderAtWithStat) ReadAt(p []byte, off int64) (int, error) {
	return (&mockReaderAt{data: m.data}).ReadAt(p, off)
}

func (m *mockReaderAtWithStat) Stat() (fs.FileInfo, error) {
	return m.info, nil
}

func TestOpenRejectsDirectoryInput(t *testing.T) {
	reader := &mockReaderAtWithStat{
		data: make([]byte, BootSectorSize),
		info: mockFileInfo{name: "testdir", mode: fs.ModeDir},
	}

	_, err := Open(reader)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrInputIsDirectory) {
		t.Fatalf("expected ErrInputIsDirectory, got %v", err)
	}
}

func TestOpenParsesFAT12BootSector(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSDOS5.0",
		bytesPerSector:    512,
		sectorsPerCluster: 1,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    224,
		totalSectors16:    2880,
		fatSize16:         9,
		volumeLabel:       "FAT12VOL",
		fsTypeHint:        FATType12,
	})

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if got := v.FATType(); got != FATType12 {
		t.Fatalf("unexpected FAT type: got %s want %s", got, FATType12)
	}
	if got := v.FirstRootDirSector(); got != 19 {
		t.Fatalf("unexpected root sector: got %d want 19", got)
	}
	if got := v.FirstDataSector(); got != 33 {
		t.Fatalf("unexpected first data sector: got %d want 33", got)
	}
	if got := v.VolumeLabel(); got != "FAT12VOL" {
		t.Fatalf("unexpected volume label: got %q", got)
	}
}

func TestOpenParsesFAT16BootSector(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 4,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    512,
		totalSectors16:    32768,
		fatSize16:         32,
		volumeLabel:       "FAT16VOL",
		fsTypeHint:        FATType16,
	})

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if got := v.FATType(); got != FATType16 {
		t.Fatalf("unexpected FAT type: got %s want %s", got, FATType16)
	}
	if got := v.FirstRootDirSector(); got != 65 {
		t.Fatalf("unexpected root sector: got %d want 65", got)
	}
	if got := v.FirstDataSector(); got != 97 {
		t.Fatalf("unexpected first data sector: got %d want 97", got)
	}
	if got := v.ClusterCount(); got != 8167 {
		t.Fatalf("unexpected cluster count: got %d want 8167", got)
	}
}

func TestOpenParsesFAT32BootSector(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       2,
		volumeLabel:       "FAT32VOL",
		fsTypeHint:        FATType32,
	})

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if got := v.FATType(); got != FATType32 {
		t.Fatalf("unexpected FAT type: got %s want %s", got, FATType32)
	}
	if got := v.RootCluster(); got != 2 {
		t.Fatalf("unexpected root cluster: got %d want 2", got)
	}
	if got := v.FirstDataSector(); got != 2080 {
		t.Fatalf("unexpected first data sector: got %d want 2080", got)
	}
	if got := v.FirstRootDirSector(); got != 2080 {
		t.Fatalf("unexpected root sector: got %d want 2080", got)
	}
	if got := v.ClusterCount(); got != 130812 {
		t.Fatalf("unexpected cluster count: got %d want 130812", got)
	}
	if got := v.VolumeLabel(); got != "FAT32VOL" {
		t.Fatalf("unexpected volume label: got %q", got)
	}
	if got, ok := v.FreeClusterCountHint(); !ok || got != 0x00010000 {
		t.Fatalf("unexpected free cluster hint: got (%d,%v)", got, ok)
	}
	if got, ok := v.NextFreeClusterHint(); !ok || got != 0x00000020 {
		t.Fatalf("unexpected next free cluster hint: got (%d,%v)", got, ok)
	}
}

func TestOpenWithFATTypeRejectsUnknownType(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSDOS5.0",
		bytesPerSector:    512,
		sectorsPerCluster: 1,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    224,
		totalSectors16:    2880,
		fatSize16:         9,
		volumeLabel:       "FAT12VOL",
		fsTypeHint:        FATType12,
	})

	_, err := OpenWithFATType(&mockReaderAt{data: img}, "FAT64")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnsupportedFAT) {
		t.Fatalf("expected ErrUnsupportedFAT, got %v", err)
	}
}

func TestOpenWithFATTypeRejectsFAT12WhenClusterCountTooHigh(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 4,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    512,
		totalSectors16:    32768,
		fatSize16:         32,
		volumeLabel:       "FAT16VOL",
		fsTypeHint:        FATType16,
	})

	_, err := OpenWithFATType(&mockReaderAt{data: img}, FATType12)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenWithFATTypeAcceptsMatchingType(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 4,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    512,
		totalSectors16:    32768,
		fatSize16:         32,
		volumeLabel:       "FAT16VOL",
		fsTypeHint:        FATType16,
	})

	v, err := OpenWithFATType(&mockReaderAt{data: img}, FATType16)
	if err != nil {
		t.Fatalf("OpenWithFATType failed: %v", err)
	}
	if got := v.FATType(); got != FATType16 {
		t.Fatalf("unexpected FAT type: got %s want %s", got, FATType16)
	}
}

func TestOpenFAT32WithInvalidFSInfo(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       2,
		volumeLabel:       "FAT32BAD",
		fsTypeHint:        FATType32,
	})

	fsInfo := img[1*BootSectorSize : 2*BootSectorSize]
	putUint32LE(fsInfo, 0, 0x0)

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if v.FSInfo() == nil {
		t.Fatal("expected FSInfo to be present")
	}
	if v.FSInfo().Valid {
		t.Fatal("expected FSInfo to be invalid")
	}
	if _, ok := v.FreeClusterCountHint(); ok {
		t.Fatal("expected free cluster hint to be unavailable")
	}
	if _, ok := v.NextFreeClusterHint(); ok {
		t.Fatal("expected next free cluster hint to be unavailable")
	}
}

func TestOpenUsesBackupBootSectorForFAT32(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       2,
		volumeLabel:       "FAT32BKP",
		fsTypeHint:        FATType32,
	})

	backup := append([]byte(nil), img[:BootSectorSize]...)
	copy(img[6*BootSectorSize:(6+1)*BootSectorSize], backup)
	clear(img[:BootSectorSize])

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if !v.UsedBackupBootSector() {
		t.Fatal("expected backup boot sector to be used")
	}
	if v.GetBootSector().Offset != 6*BootSectorSize {
		t.Fatalf("unexpected boot sector offset: got %d want %d", v.GetBootSector().Offset, 6*BootSectorSize)
	}
	if got := v.VolumeLabel(); got != "FAT32BKP" {
		t.Fatalf("unexpected volume label: got %q", got)
	}
}

func TestOpenRejectsMissingMagic(t *testing.T) {
	img := make([]byte, BootSectorSize)
	img[11] = 0x00
	img[12] = 0x02
	img[13] = 1
	img[14] = 1
	img[16] = 2

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsInvalidSectorSize(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    8192,
		sectorsPerCluster: 1,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    224,
		totalSectors16:    2880,
		fatSize16:         9,
		volumeLabel:       "BADSECT",
		fsTypeHint:        FATType12,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFAT16WithZeroRootEntries(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 4,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    0,
		totalSectors16:    32768,
		fatSize16:         32,
		volumeLabel:       "BAD16",
		fsTypeHint:        FATType16,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFAT32WithNonZeroRootEntries(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		rootEntryCount:    32,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       2,
		volumeLabel:       "BAD32",
		fsTypeHint:        FATType32,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFAT32WithRootClusterBelowMinimum(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       1,
		volumeLabel:       "BADROOT1",
		fsTypeHint:        FATType32,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFAT32WithRootClusterOutOfRange(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		totalSectors32:    1000,
		fatSize32:         32,
		rootCluster:       200,
		volumeLabel:       "BADROOT2",
		fsTypeHint:        FATType32,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFirstDataSectorPastEnd(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 1,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    224,
		totalSectors16:    30,
		fatSize16:         9,
		volumeLabel:       "BADGEO",
		fsTypeHint:        FATType12,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsTooManyFATs(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 4,
		reservedSectors:   1,
		numberOfFATs:      9,
		rootEntryCount:    512,
		totalSectors16:    32768,
		fatSize16:         32,
		volumeLabel:       "BADFAT",
		fsTypeHint:        FATType16,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFirstFATSectorPastEnd(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 1,
		reservedSectors:   64,
		numberOfFATs:      2,
		rootEntryCount:    224,
		totalSectors16:    63,
		fatSize16:         9,
		volumeLabel:       "BADFATSECT",
		fsTypeHint:        FATType12,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFAT32WithNonZeroFATSize16(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      2,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       2,
		volumeLabel:       "BADFATSZ",
		fsTypeHint:        FATType32,
	})
	putUint16LE(img[:BootSectorSize], 22, 1)

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestOpenRejectsFAT16LayoutWithZeroFATSize16(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 4,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    512,
		totalSectors32:    32768,
		fatSize32:         32,
		rootCluster:       2,
		volumeLabel:       "BADFATS2",
		fsTypeHint:        FATType32,
	})

	_, err := Open(&mockReaderAt{data: img})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidBootSector) {
		t.Fatalf("expected ErrInvalidBootSector, got %v", err)
	}
}

func TestReadDirAndOpenPathOnFAT16(t *testing.T) {
	helloContent := strings.Repeat("A", 512) + "world!\n"
	img := makeFAT16DirectoryImage(t)
	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	entries, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name)
	}
	if !reflect.DeepEqual(names, []string{"hello world.txt", "DOCS"}) {
		t.Fatalf("unexpected root entries: got %v", names)
	}

	hello, err := v.OpenPath("/hello world.txt")
	if err != nil {
		t.Fatalf("OpenPath hello failed: %v", err)
	}
	data, err := hello.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll hello failed: %v", err)
	}
	if got := string(data); got != helloContent {
		t.Fatalf("unexpected hello content length: got %d want %d", len(got), len(helloContent))
	}

	buf := make([]byte, 10)
	n, err := hello.ReadAt(buf, 509)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt hello failed: %v", err)
	}
	if got := string(buf[:n]); got != "AAAworld!\n" {
		t.Fatalf("unexpected cross-cluster read: got %q", got)
	}

	note, err := v.OpenPath("/docs/note.txt")
	if err != nil {
		t.Fatalf("OpenPath nested failed: %v", err)
	}
	noteData, err := note.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll nested failed: %v", err)
	}
	if got := string(noteData); got != "note" {
		t.Fatalf("unexpected nested content: got %q", got)
	}
}

func TestReadDirSkipsVolumeLabelByDefault(t *testing.T) {
	img := makeFAT16DirectoryImage(t)
	rootOffset := 41 * 512
	entries := img[rootOffset : rootOffset+1024]
	copy(entries[128:160], makeShortEntry("MYLABEL", "", attrVolumeID, 0, 0))
	entries[160] = 0x00

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	list, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if hasEntryNamed(list, "MYLABEL") {
		t.Fatal("expected volume label entry to be hidden by default")
	}
}

func TestReadDirIncludesVolumeLabelWhenEnabled(t *testing.T) {
	img := makeFAT16DirectoryImage(t)
	rootOffset := 41 * 512
	entries := img[rootOffset : rootOffset+1024]
	copy(entries[128:160], makeShortEntry("MYLABEL", "", attrVolumeID, 0, 0))
	entries[160] = 0x00

	v, err := OpenWithOptions(&mockReaderAt{data: img}, OpenOptions{IncludeVolumeLabelEntries: true})
	if err != nil {
		t.Fatalf("OpenWithOptions failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	list, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if !hasEntryNamed(list, "MYLABEL") {
		t.Fatal("expected volume label entry when option enabled")
	}
}

func TestRootReadDirIncludesVirtualEntriesWhenEnabled(t *testing.T) {
	img := makeFAT16DirectoryImage(t)

	v, err := OpenWithOptions(&mockReaderAt{data: img}, OpenOptions{IncludeVirtualRootEntries: true})
	if err != nil {
		t.Fatalf("OpenWithOptions failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	list, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	for _, name := range []string{"$MBR", "$FAT1", "$FAT2", "$OrphanFiles"} {
		if !hasEntryNamed(list, name) {
			t.Fatalf("expected virtual root entry %s", name)
		}
	}
}

func TestReadDirSkipsInvalidDeletedEntries(t *testing.T) {
	img := makeFAT16DirectoryImage(t)
	rootOffset := 41 * 512
	entries := img[rootOffset : rootOffset+1024]
	entries[128] = 0xE5
	for i := 129; i < 160; i++ {
		entries[i] = 0x00
	}
	entries[160] = 0x00

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	list, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("unexpected entry count with invalid deleted slot: got %d want 2", len(list))
	}
}

func hasEntryNamed(entries []DirEntry, name string) bool {
	for _, entry := range entries {
		if entry.Name == name {
			return true
		}
	}
	return false
}

func TestReadDirRecoversDeletedEntryFromSlack(t *testing.T) {
	img := makeFAT16DirectoryImage(t)
	rootOffset := 41 * 512
	entries := img[rootOffset : rootOffset+1024]

	entries[128] = 0x00
	deleted := makeShortEntry("GHOST", "TXT", 0x20, 5, 4)
	deleted[0] = 0xE5
	copy(entries[160:192], deleted)
	entries[192] = 0x00

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	list, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(list) != 3 {
		t.Fatalf("unexpected entry count: got %d want 3", len(list))
	}

	recovered := list[2]
	if recovered.Name != "_HOST.TXT" {
		t.Fatalf("unexpected recovered name: got %q", recovered.Name)
	}
	if !recovered.Deleted {
		t.Fatal("expected recovered entry to be deleted")
	}
	if !recovered.Recovered {
		t.Fatal("expected recovered entry to be marked as recovered")
	}
	if !recovered.ClusterAllocated {
		t.Fatal("expected recovered entry cluster allocation metadata to be true")
	}
}

func TestReadDirFallsBackToShortNameOnLFNChecksumMismatch(t *testing.T) {
	img := makeFAT16DirectoryImage(t)
	rootOffset := 41 * 512
	entries := img[rootOffset : rootOffset+1024]

	entries[0+13] ^= 0x01

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	list, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(list) == 0 {
		t.Fatal("expected entries")
	}
	if got := list[0].Name; got != "HELLOW~1.TXT" {
		t.Fatalf("expected short-name fallback, got %q", got)
	}
}

func TestReadDirFallsBackToShortNameOnLFNSequenceBreak(t *testing.T) {
	img := makeFAT16DirectoryImage(t)
	rootOffset := 41 * 512
	entries := img[rootOffset : rootOffset+1024]

	entries[32+0] = 0x03

	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	root, err := v.GetRootDirectory()
	if err != nil {
		t.Fatalf("GetRootDirectory failed: %v", err)
	}

	list, err := root.ReadDir()
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(list) == 0 {
		t.Fatal("expected entries")
	}
	if got := list[0].Name; got != "HELLOW~1.TXT" {
		t.Fatalf("expected short-name fallback, got %q", got)
	}
}

func TestReadFATEntryFAT12OddEvenDecoding(t *testing.T) {
	img := make([]byte, 2*BootSectorSize)
	fat := img[BootSectorSize:]

	// Cluster 2 (even) = 0xABC, cluster 3 (odd) = 0xDEF.
	fat[3] = 0xBC
	fat[4] = 0xFA
	fat[5] = 0xDE

	v := &Volume{
		reader:         &mockReaderAt{data: img},
		fatType:        FATType12,
		bytesPerSector: BootSectorSize,
		firstFATSector: 1,
		fatSizeSectors: 1,
		numberOfFATs:   1,
		clusterCount:   32,
	}

	got2, err := v.readFATEntry(2)
	if err != nil {
		t.Fatalf("readFATEntry(2) failed: %v", err)
	}
	if got2 != 0xABC {
		t.Fatalf("unexpected FAT12 even value: got %#x want %#x", got2, 0xABC)
	}

	got3, err := v.readFATEntry(3)
	if err != nil {
		t.Fatalf("readFATEntry(3) failed: %v", err)
	}
	if got3 != 0xDEF {
		t.Fatalf("unexpected FAT12 odd value: got %#x want %#x", got3, 0xDEF)
	}
}

func TestReadFATEntryRejectsOutOfRangeCluster(t *testing.T) {
	v := &Volume{
		reader:         &mockReaderAt{data: make([]byte, 2*BootSectorSize)},
		fatType:        FATType16,
		bytesPerSector: BootSectorSize,
		firstFATSector: 1,
		fatSizeSectors: 1,
		numberOfFATs:   1,
		clusterCount:   8,
	}

	_, err := v.readFATEntry(11)
	if err == nil {
		t.Fatal("expected out-of-range error")
	}
	if !errors.Is(err, ErrCorruptStructure) {
		t.Fatalf("expected ErrCorruptStructure, got %v", err)
	}
}

func TestReadFATEntryRejectsFAT12BoundaryCross(t *testing.T) {
	v := &Volume{
		reader:         &mockReaderAt{data: make([]byte, 2*BootSectorSize)},
		fatType:        FATType12,
		bytesPerSector: BootSectorSize,
		firstFATSector: 1,
		fatSizeSectors: 1,
		numberOfFATs:   1,
		clusterCount:   400,
	}

	// For FAT12, cluster 341 has entryOffset 511 and requires 2 bytes.
	_, err := v.readFATEntry(341)
	if err == nil {
		t.Fatal("expected FAT bounds error")
	}
	if !errors.Is(err, ErrCorruptStructure) {
		t.Fatalf("expected ErrCorruptStructure, got %v", err)
	}
}

func TestReadFATEntryUsesSecondaryOnInvalidPrimary(t *testing.T) {
	img := make([]byte, 3*BootSectorSize)

	// FAT1 (sector 1): cluster 2 -> implausible pointer for this image.
	putUint16LE(img, int(1*BootSectorSize+2*2), 0x1234)
	// FAT2 (sector 2): cluster 2 -> valid pointer to cluster 5.
	putUint16LE(img, int(2*BootSectorSize+2*2), 0x0005)

	v := &Volume{
		reader:         &mockReaderAt{data: img},
		fatType:        FATType16,
		bytesPerSector: BootSectorSize,
		firstFATSector: 1,
		fatSizeSectors: 1,
		numberOfFATs:   2,
		clusterCount:   16,
	}

	got, err := v.readFATEntry(2)
	if err != nil {
		t.Fatalf("readFATEntry failed: %v", err)
	}
	if got != 5 {
		t.Fatalf("expected fallback to secondary FAT value 5, got %d", got)
	}
	if v.FATMirrorMismatches() != 1 {
		t.Fatalf("expected 1 FAT mirror mismatch, got %d", v.FATMirrorMismatches())
	}
}

func TestReadFATEntryMismatchKeepsPrimaryWhenPlausible(t *testing.T) {
	img := make([]byte, 3*BootSectorSize)

	// Both values plausible but different: keep primary, still count mismatch.
	putUint16LE(img, int(1*BootSectorSize+2*2), 0x0006)
	putUint16LE(img, int(2*BootSectorSize+2*2), 0x0007)

	v := &Volume{
		reader:         &mockReaderAt{data: img},
		fatType:        FATType16,
		bytesPerSector: BootSectorSize,
		firstFATSector: 1,
		fatSizeSectors: 1,
		numberOfFATs:   2,
		clusterCount:   16,
	}

	got, err := v.readFATEntry(2)
	if err != nil {
		t.Fatalf("readFATEntry failed: %v", err)
	}
	if got != 6 {
		t.Fatalf("expected primary FAT value 6, got %d", got)
	}
	if v.FATMirrorMismatches() != 1 {
		t.Fatalf("expected 1 FAT mirror mismatch, got %d", v.FATMirrorMismatches())
	}
}

type bootSectorConfig struct {
	oem               string
	bytesPerSector    uint16
	sectorsPerCluster uint8
	reservedSectors   uint16
	numberOfFATs      uint8
	rootEntryCount    uint16
	totalSectors16    uint16
	totalSectors32    uint32
	fatSize16         uint16
	fatSize32         uint32
	rootCluster       uint32
	volumeLabel       string
	fsTypeHint        string
}

func makeBootSectorImage(cfg bootSectorConfig) []byte {
	totalSectors := int(cfg.totalSectors32)
	if totalSectors == 0 {
		totalSectors = int(cfg.totalSectors16)
	}
	img := make([]byte, totalSectors*int(cfg.bytesPerSector))
	bs := img[:BootSectorSize]
	copy(bs[0:3], []byte{0xEB, 0x3C, 0x90})
	copy(bs[3:11], paddedASCII(cfg.oem, 8))
	putUint16LE(bs, 11, cfg.bytesPerSector)
	bs[13] = cfg.sectorsPerCluster
	putUint16LE(bs, 14, cfg.reservedSectors)
	bs[16] = cfg.numberOfFATs
	putUint16LE(bs, 17, cfg.rootEntryCount)
	putUint16LE(bs, 19, cfg.totalSectors16)
	bs[21] = 0xF8
	putUint16LE(bs, 22, cfg.fatSize16)
	putUint16LE(bs, 24, 63)
	putUint16LE(bs, 26, 255)
	putUint32LE(bs, 28, 0)
	putUint32LE(bs, 32, cfg.totalSectors32)

	if cfg.fatSize16 == 0 {
		putUint32LE(bs, 36, cfg.fatSize32)
		putUint16LE(bs, 40, 0)
		putUint16LE(bs, 42, 0)
		if cfg.rootCluster == 0 {
			cfg.rootCluster = 2
		}
		putUint32LE(bs, 44, cfg.rootCluster)
		putUint16LE(bs, 48, 1)
		putUint16LE(bs, 50, 6)
		bs[64] = 0x80
		bs[66] = 0x29
		putUint32LE(bs, 67, 0x12345678)
		copy(bs[71:82], paddedASCII(cfg.volumeLabel, 11))
		copy(bs[82:90], paddedASCII(cfg.fsTypeHint, 8))

		fsInfo := img[1*BootSectorSize : 2*BootSectorSize]
		putUint32LE(fsInfo, 0, 0x41615252)
		putUint32LE(fsInfo, 484, 0x61417272)
		putUint32LE(fsInfo, 488, 0x00010000)
		putUint32LE(fsInfo, 492, 0x00000020)
		putUint32LE(fsInfo, 508, 0xAA550000)
	} else {
		bs[36] = 0x80
		bs[38] = 0x29
		putUint32LE(bs, 39, 0x12345678)
		copy(bs[43:54], paddedASCII(cfg.volumeLabel, 11))
		copy(bs[54:62], paddedASCII(cfg.fsTypeHint, 8))
	}
	putUint16LE(bs, 510, BootSectorMagic)
	return img
}

func makeFAT16DirectoryImage(t *testing.T) []byte {
	t.Helper()
	helloContent := strings.Repeat("A", 512) + "world!\n"
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 1,
		reservedSectors:   1,
		numberOfFATs:      2,
		rootEntryCount:    32,
		totalSectors16:    5000,
		fatSize16:         20,
		volumeLabel:       "TESTVOL",
		fsTypeHint:        FATType16,
	})

	writeFAT16Entry(img, 1, 0, 0xFFF8)
	writeFAT16Entry(img, 1, 1, 0xFFFF)
	writeFAT16Entry(img, 1, 2, 0xFFFF)
	writeFAT16Entry(img, 1, 3, 4)
	writeFAT16Entry(img, 1, 4, 0xFFFF)
	writeFAT16Entry(img, 1, 5, 0xFFFF)
	writeFAT16Entry(img, 21, 0, 0xFFF8)
	writeFAT16Entry(img, 21, 1, 0xFFFF)
	writeFAT16Entry(img, 21, 2, 0xFFFF)
	writeFAT16Entry(img, 21, 3, 4)
	writeFAT16Entry(img, 21, 4, 0xFFFF)
	writeFAT16Entry(img, 21, 5, 0xFFFF)

	rootOffset := 41 * 512
	entries := img[rootOffset : rootOffset+1024]
	lfnEntries := makeLFNEntries("hello world.txt", "HELLOW~1TXT")
	copy(entries[0:32], lfnEntries[0])
	copy(entries[32:64], lfnEntries[1])
	copy(entries[64:96], makeShortEntry("HELLOW~1", "TXT", 0x20, 3, uint32(len(helloContent))))
	copy(entries[96:128], makeShortEntry("DOCS", "", 0x10, 2, 0))
	entries[128] = 0x00

	dataOffset := 43 * 512
	docsDir := img[dataOffset : dataOffset+512]
	copy(docsDir[0:32], makeShortEntry(".", "", 0x10, 2, 0))
	copy(docsDir[32:64], makeShortEntry("..", "", 0x10, 0, 0))
	copy(docsDir[64:96], makeShortEntry("NOTE", "TXT", 0x20, 5, 4))
	docsDir[96] = 0x00

	copy(img[(44*512):(44*512)+512], []byte(helloContent[:512]))
	copy(img[(45*512):(45*512)+512], []byte("world!\n"))
	copy(img[(46*512):(46*512)+512], []byte("note"))

	return img
}

func writeFAT16Entry(img []byte, sector uint32, cluster uint16, value uint16) {
	offset := int(sector*512) + int(cluster*2)
	putUint16LE(img, offset, value)
}

func makeShortEntry(base, ext string, attrs byte, cluster uint16, size uint32) []byte {
	entry := make([]byte, 32)
	copy(entry[0:8], paddedASCII(strings.ToUpper(base), 8))
	copy(entry[8:11], paddedASCII(strings.ToUpper(ext), 3))
	entry[11] = attrs
	putUint16LE(entry, 26, cluster)
	putUint32LE(entry, 28, size)
	return entry
}

func makeLFNEntries(name, shortName string) [][]byte {
	encoded := utf16.Encode([]rune(name))
	checksum := lfnChecksum([]byte(shortName))
	count := (len(encoded) + 12) / 13
	entries := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		partIndex := count - 1 - i
		start := partIndex * 13
		end := start + 13
		if end > len(encoded) {
			end = len(encoded)
		}
		part := encoded[start:end]
		entry := make([]byte, 32)
		seq := byte(partIndex + 1)
		if partIndex == count-1 {
			seq |= 0x40
		}
		entry[0] = seq
		entry[11] = attrLongName
		entry[13] = checksum
		writeLFNPart(entry, part)
		entries = append(entries, entry)
	}
	return entries
}

func writeLFNPart(entry []byte, part []uint16) {
	offsets := []int{1, 3, 5, 7, 9, 14, 16, 18, 20, 22, 24, 28, 30}
	for i, offset := range offsets {
		value := uint16(0xFFFF)
		if i < len(part) {
			value = part[i]
		} else if i == len(part) {
			value = 0x0000
		}
		putUint16LE(entry, offset, value)
	}
}

func lfnChecksum(shortName []byte) byte {
	var sum byte
	for i := 0; i < len(shortName); i++ {
		sum = ((sum & 1) << 7) + (sum >> 1) + shortName[i]
	}
	return sum
}

func paddedASCII(value string, width int) []byte {
	out := make([]byte, width)
	copy(out, []byte(value))
	for i := len(value); i < width; i++ {
		out[i] = ' '
	}
	return out
}

func putUint16LE(buf []byte, offset int, value uint16) {
	buf[offset] = byte(value)
	buf[offset+1] = byte(value >> 8)
}

func putUint32LE(buf []byte, offset int, value uint32) {
	buf[offset] = byte(value)
	buf[offset+1] = byte(value >> 8)
	buf[offset+2] = byte(value >> 16)
	buf[offset+3] = byte(value >> 24)
}
