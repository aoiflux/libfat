package libfat

import (
	"encoding/json"
	"regexp"
	"testing"
)

// formatFixed is what every FAT volume must answer, whatever its variant. A
// value here changing means a claim about the format changed, which is exactly
// the kind of edit that should have to be deliberate.
var formatFixed = map[string]bool{
	"unicode_names":           true,
	"case_sensitive":          false,
	"creation_times":          true,
	"modification_times":      true,
	"access_times":            true,
	"metadata_change_times":   false,
	"sub_second_timestamps":   true,
	"timezone_offsets":        false,
	"posix_permissions":       false,
	"hard_links":              false,
	"symbolic_links":          false,
	"extended_attributes":     false,
	"sparse_files":            false,
	"compression":             false,
	"stable_file_identity":    false,
	"identity_reuse_counter":  false,
	"allocation_bitmap":       false,
	"valid_data_length":       false,
	"declared_contiguity":     false,
	"deleted_entries_survive": true,
	"journal":                 false,
}

func capabilityMap(t *testing.T, c Capabilities) map[string]bool {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("json.Marshal(Capabilities) failed: %v", err)
	}
	var m map[string]bool
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	return m
}

// TestCapabilitiesFormatFixedAcrossVariants asserts the format-level answers are
// identical on every variant libfat supports.
func TestCapabilitiesFormatFixedAcrossVariants(t *testing.T) {
	for _, fatType := range []string{FATType12, FATType16, FATType32} {
		t.Run(fatType, func(t *testing.T) {
			im := newTestImage(t, fatType)
			v := im.volume()
			defer v.Close()

			got := capabilityMap(t, v.Capabilities())
			for field, want := range formatFixed {
				if got[field] != want {
					t.Errorf("%s = %v, want %v", field, got[field], want)
				}
			}
		})
	}
}

// TestCapabilitiesVolumeDependentFields covers the F7 acceptance case: the
// fields that are read from the volume's boot record must actually differ
// between the variants where the formats differ.
func TestCapabilitiesVolumeDependentFields(t *testing.T) {
	fat16 := newTestImage(t, FATType16).volume()
	defer fat16.Close()
	fat32 := newTestImage(t, FATType32).volume()
	defer fat32.Close()

	c16 := fat16.Capabilities()
	c32 := fat32.Capabilities()

	// FSInfo is a FAT32 structure; FAT12 and FAT16 have none at all.
	if c16.FSInfoSector {
		t.Error("FAT16 reports an FSInfo sector, which the variant does not have")
	}
	if !c32.FSInfoSector {
		t.Error("FAT32 fixture reports no FSInfo sector")
	}

	// Likewise the backup boot sector.
	if c16.BackupBootSector {
		t.Error("FAT16 reports a backup boot sector, which the variant does not have")
	}
	if !c32.BackupBootSector {
		t.Error("FAT32 fixture reports no backup boot sector")
	}

	// Both fixtures are built with two FATs, so this one agrees - but it must
	// track the boot record rather than being hardcoded.
	if !c16.SecondFAT || !c32.SecondFAT {
		t.Errorf("SecondFAT = %v/%v, want true for both two-FAT fixtures",
			c16.SecondFAT, c32.SecondFAT)
	}
}

// TestCapabilitiesSecondFATTracksBootRecord pins SecondFAT to the volume rather
// than to the format, since that is what its doc comment promises.
func TestCapabilitiesSecondFATTracksBootRecord(t *testing.T) {
	img := makeBootSectorImage(bootSectorConfig{
		oem:               "MSWIN4.1",
		bytesPerSector:    512,
		sectorsPerCluster: 8,
		reservedSectors:   32,
		numberOfFATs:      1,
		totalSectors32:    1048576,
		fatSize32:         1024,
		rootCluster:       2,
		volumeLabel:       "ONEFAT",
		fsTypeHint:        FATType32,
	})
	v, err := Open(&mockReaderAt{data: img})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer v.Close()

	if v.Capabilities().SecondFAT {
		t.Error("SecondFAT is true on a volume with one FAT")
	}
}

// TestCapabilitiesNilVolume covers the nil-receiver guard, which exists so that
// a caller can ask about a volume that failed to open without a panic.
func TestCapabilitiesNilVolume(t *testing.T) {
	var v *Volume
	if got := v.Capabilities(); got != (Capabilities{}) {
		t.Errorf("nil Volume reported %+v, want the zero value", got)
	}
}

// TestCapabilitiesJSONKeysAreSnakeCase holds capabilities.go to the family's
// tag convention, which libfat already follows everywhere else.
func TestCapabilitiesJSONKeysAreSnakeCase(t *testing.T) {
	camel := regexp.MustCompile(`[a-z][A-Z]`)
	for key := range capabilityMap(t, Capabilities{}) {
		if camel.MatchString(key) {
			t.Errorf("json key %q is camelCase", key)
		}
	}
}

// TestCapabilitiesNoFieldIsOmitted pins the rule that a false capability is an
// answer rather than an absence, so no field may carry omitempty.
func TestCapabilitiesNoFieldIsOmitted(t *testing.T) {
	got := capabilityMap(t, Capabilities{})
	// 21 format-fixed fields plus the three read from the boot record.
	if want := len(formatFixed) + 3; len(got) != want {
		t.Fatalf("zero-value Capabilities marshalled %d keys, want %d; a field "+
			"was added without updating this test, or one carries omitempty",
			len(got), want)
	}
}
