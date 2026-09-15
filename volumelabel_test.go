package libfat

import (
	"encoding/json"
	"testing"
)

// FAT keeps a volume's label in two places that are free to disagree: the boot
// sector's BS_VolLab field, written once at format time, and a record in the
// root directory carrying the volume-ID attribute. Only the second is
// authoritative. Windows leaves the boot sector reading "NO NAME" whatever the
// volume is called, so preferring it means reporting a name no operating system
// and no other tool will show.
//
// These tests use a fixture whose boot sector says TESTVOL, so a root record is
// always distinguishable from the fallback.

const bootSectorFixtureLabel = "TESTVOL"

func labelEntry(name string) []byte {
	return makeTimestampedEntry(name, "", attrVolumeID, 0, 0)
}

func TestVolumeLabelPrefersRootDirectoryRecord(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			im.addRootRaw(labelEntry("REALNAME"))

			v := im.volume()
			defer v.Close()

			if got := v.VolumeLabel(); got != "REALNAME" {
				t.Errorf("VolumeLabel = %q, want the root directory's %q", got, "REALNAME")
			}
			if got := v.BootSectorVolumeLabel(); got != bootSectorFixtureLabel {
				t.Errorf("BootSectorVolumeLabel = %q, want %q", got, bootSectorFixtureLabel)
			}
			if got := v.VolumeLabelSource(); got != "root directory" {
				t.Errorf("VolumeLabelSource = %q, want %q", got, "root directory")
			}
		})
	}
}

func TestVolumeLabelFallsBackToBootSector(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "A", ext: "TXT", clusters: []uint32{3},
		size: 4, content: patternBytes(4, 1), terminate: true})

	v := im.volume()
	defer v.Close()

	if got := v.VolumeLabel(); got != bootSectorFixtureLabel {
		t.Errorf("VolumeLabel = %q, want the boot sector's %q with no root record",
			got, bootSectorFixtureLabel)
	}
	if got := v.VolumeLabelSource(); got != "boot sector" {
		t.Errorf("VolumeLabelSource = %q, want %q", got, "boot sector")
	}
}

// TestVolumeLabelIgnoresLongNameSlots guards the one way this scan can go
// wrong: a long-name slot has attribute 0x0F, which includes the volume-ID bit,
// so testing that bit without excluding 0x0F first turns the first long-named
// file on the volume into its label.
func TestVolumeLabelIgnoresLongNameSlots(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addFile(testFile{base: "LONGFI", ext: "TXT", longName: "a-long-file-name.txt",
		clusters: []uint32{3}, size: 4, content: patternBytes(4, 1), terminate: true})

	v := im.volume()
	defer v.Close()

	if got := v.VolumeLabel(); got != bootSectorFixtureLabel {
		t.Errorf("VolumeLabel = %q, want %q; a long-name slot was read as a label",
			got, bootSectorFixtureLabel)
	}
}

// TestVolumeLabelIgnoresDeletedRecord: a deleted label is not the volume's name.
func TestVolumeLabelIgnoresDeletedRecord(t *testing.T) {
	im := newTestImage(t, FATType16)
	deleted := labelEntry("OLDNAME")
	deleted[0] = 0xE5
	im.addRootRaw(deleted, labelEntry("NEWNAME"))

	v := im.volume()
	defer v.Close()

	if got := v.VolumeLabel(); got != "NEWNAME" {
		t.Errorf("VolumeLabel = %q, want %q", got, "NEWNAME")
	}
}

// TestVolumeLabelStopsAtEndOfDirectory holds the scan to FAT's own rule. A
// record beginning 0x00 means no record after it has ever been used, so reading
// past one would be reading whatever happened to be in free space.
func TestVolumeLabelStopsAtEndOfDirectory(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addRootRaw(make([]byte, dirEntrySize), labelEntry("PASTEND"))

	v := im.volume()
	defer v.Close()

	if got := v.VolumeLabel(); got == "PASTEND" {
		t.Error("a label past the end-of-directory marker was read as the volume's name")
	}
	if got := v.VolumeLabelSource(); got != "boot sector" {
		t.Errorf("VolumeLabelSource = %q, want %q", got, "boot sector")
	}
}

// TestVolumeLabelOnClosedVolumeDoesNotPanic covers the accessors after Close,
// which read cached values rather than the image.
func TestVolumeLabelOnClosedVolumeDoesNotPanic(t *testing.T) {
	im := newTestImage(t, FATType16)
	im.addRootRaw(labelEntry("REALNAME"))
	v := im.volume()
	if err := v.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if got := v.VolumeLabel(); got != "REALNAME" {
		t.Errorf("VolumeLabel after Close = %q, want %q", got, "REALNAME")
	}
}

// TestReportCarriesVolumeLabelProvenance pins the report's side of this: both
// readings are present, and the report says which one it used.
func TestReportCarriesVolumeLabelProvenance(t *testing.T) {
	im := newTestImage(t, FATType32)
	im.addRootRaw(labelEntry("REALNAME"))

	v := im.volume()
	defer v.Close()

	rep, err := v.Report("image.img")
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if rep.Filesystem.VolumeLabel != "REALNAME" {
		t.Errorf("volume_label = %q, want %q", rep.Filesystem.VolumeLabel, "REALNAME")
	}
	if rep.Filesystem.BootSectorVolumeLabel != bootSectorFixtureLabel {
		t.Errorf("boot_sector_volume_label = %q, want %q",
			rep.Filesystem.BootSectorVolumeLabel, bootSectorFixtureLabel)
	}
	if rep.Filesystem.VolumeLabelSource != "root directory" {
		t.Errorf("volume_label_source = %q, want %q",
			rep.Filesystem.VolumeLabelSource, "root directory")
	}

	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	var back FATReport
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if back.Filesystem.VolumeLabelSource != rep.Filesystem.VolumeLabelSource ||
		back.Filesystem.BootSectorVolumeLabel != rep.Filesystem.BootSectorVolumeLabel {
		t.Error("the label provenance did not survive a JSON round trip")
	}
}
