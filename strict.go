package libfat

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// The checks OpenOptions.Strict adds. Each is a separate pass beside the
// tolerant parser rather than a branch inside it, so that a volume opened
// without Strict runs none of this code, and so that a directory which passes
// is parsed by exactly the code every other caller gets: Strict can refuse a
// listing but never alter one.

// strictDirectory checks a live directory's raw records, before they are
// parsed. dirCluster identifies the directory as DirEntry.ParentFirstCluster
// would, and isRoot says whether "." and ".." are forbidden or required.
func (v *Volume) strictDirectory(data []byte, dirCluster uint32, isRoot bool) *StrictError {
	fail := func(check StrictCheck, offset int, format string, args ...any) *StrictError {
		return &StrictError{
			Check:       check,
			DirCluster:  dirCluster,
			EntryOffset: int64(offset),
			Detail:      fmt.Sprintf(format, args...),
		}
	}

	var (
		lfnActive   bool
		lfnExpected byte
		lfnSum      byte
		endSeen     bool
	)
	for offset := 0; offset+DirEntrySize <= len(data); offset += DirEntrySize {
		rec := data[offset : offset+DirEntrySize]
		index := offset / DirEntrySize

		if !isRoot && index < 2 {
			want := [2]string{".", ".."}[index]
			if !isDotRecord(rec, want) {
				return fail(StrictDotEntries, offset, "record %d of a subdirectory is not %q", index, want)
			}
			if index == 0 && entryFirstCluster(rec) != dirCluster {
				return fail(StrictDotEntries, offset, "\".\" names cluster %d, not its own directory's %d",
					entryFirstCluster(rec), dirCluster)
			}
			continue
		}

		switch {
		case rec[0] == 0x00:
			if lfnActive {
				return fail(StrictLFNSequence, offset, "long name not followed by its short entry")
			}
			endSeen = true
			continue
		case endSeen && rec[0] != DeletedMarker:
			return fail(StrictAfterEndMarker, offset, "live record after the end-of-directory marker")
		case rec[0] == DeletedMarker:
			if lfnActive {
				return fail(StrictLFNSequence, offset, "long name interrupted by a deleted record")
			}
			continue
		}

		if rec[11] == AttrLongName {
			if !isValidLFNEntry(rec, false) {
				return fail(StrictLFNSequence, offset, "invalid long-name slot (sequence %#02x)", rec[0])
			}
			ord, sum := rec[0]&0x3F, rec[13]
			if rec[0]&LFNLastFlag != 0 {
				if lfnActive {
					return fail(StrictLFNSequence, offset, "new long name begins before the previous one ended")
				}
				lfnActive, lfnExpected, lfnSum = true, ord, sum
				continue
			}
			if !lfnActive {
				return fail(StrictLFNSequence, offset, "long-name slot %d without the slot carrying the last-part flag", ord)
			}
			if sum != lfnSum {
				return fail(StrictLFNChecksum, offset, "slot checksum %#02x differs from the name's %#02x", sum, lfnSum)
			}
			if ord != lfnExpected-1 {
				return fail(StrictLFNSequence, offset, "slot ordinal %d where %d was expected", ord, lfnExpected-1)
			}
			lfnExpected = ord
			continue
		}

		if !isValidShortEntry(rec) {
			return fail(StrictInvalidEntry, offset, "record does not parse as a short entry")
		}
		if lfnActive {
			if rec[11]&AttrVolumeID != 0 {
				return fail(StrictLFNSequence, offset, "long name followed by a volume label")
			}
			if lfnExpected != 1 {
				return fail(StrictLFNSequence, offset, "long name stops at slot %d, before slot 1", lfnExpected)
			}
			var name [11]byte
			copy(name[:], rec[0:11])
			if got := LFNChecksum(name); got != lfnSum {
				return fail(StrictLFNChecksum, offset, "slots carry checksum %#02x, short name gives %#02x", lfnSum, got)
			}
			lfnActive = false
		}
		if rec[11]&AttrVolumeID != 0 {
			continue
		}
		if name := string(rec[0:11]); name == ".          " || name == "..         " {
			return fail(StrictDotEntries, offset, "\".\" or \"..\" record outside the first two records of a subdirectory")
		}

		cluster := entryFirstCluster(rec)
		if cluster == 1 || (cluster != 0 && cluster > v.maxClusterNumber()) {
			se := fail(StrictChainCorrupt, offset, "first cluster %d is outside the data area", cluster)
			se.Cluster = cluster
			se.Err = ErrCorruptStructure
			return se
		}
		if cluster != 0 {
			// Every live entry's first FAT entry is read anyway, to fill in
			// ClusterAllocated; reading it here as well surfaces a mirror
			// disagreement at the record it belongs to.
			if _, err := v.readFATEntryMode(cluster, true); err != nil {
				var se *StrictError
				if errors.As(err, &se) {
					return se.atEntry(dirCluster, int64(offset))
				}
			}
		}
	}
	if lfnActive {
		return fail(StrictLFNSequence, len(data)-len(data)%DirEntrySize, "long name runs off the end of the directory")
	}
	return nil
}

// strictDuplicates reports the first live entry whose long or short name
// equals another live entry's under case folding. It runs on parsed entries,
// after strictDirectory has passed, so every name in them came from an intact
// structure.
func strictDuplicates(entries []DirEntry, dirCluster uint32) *StrictError {
	owner := make(map[string]int, 2*len(entries))
	for i, e := range entries {
		if e.Deleted || e.Virtual || e.Attributes&AttrVolumeID != 0 || e.Name == "." || e.Name == ".." {
			continue
		}
		for _, name := range []string{e.Name, e.ShortName} {
			if name == "" {
				continue
			}
			key := foldKey(name)
			if j, ok := owner[key]; ok && j != i {
				return &StrictError{
					Check:       StrictDuplicateName,
					DirCluster:  dirCluster,
					EntryOffset: e.EntryOffset,
					Detail:      fmt.Sprintf("%q collides with %q", name, entries[j].Name),
				}
			}
			owner[key] = i
		}
	}
	return nil
}

// foldKey maps a name to a key that two names share exactly when
// strings.EqualFold reports them equal: each rune is replaced by the smallest
// member of its simple case-folding orbit.
func foldKey(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			if f < least {
				least = f
			}
		}
		b.WriteRune(least)
	}
	return b.String()
}

// strictChain checks a live entry's walked chain, given why the walk stopped. A
// file's chain must hold exactly the clusters its size needs; a directory's
// need only exist, which reaching this point already proves.
func (v *Volume) strictChain(entry DirEntry, result *FragmentResult, stop chainStop) *StrictError {
	fail := func(check StrictCheck, cluster uint32, err error, format string, args ...any) *StrictError {
		return &StrictError{
			Check:       check,
			DirCluster:  entry.ParentFirstCluster,
			EntryOffset: entry.EntryOffset,
			Cluster:     cluster,
			Detail:      fmt.Sprintf(format, args...),
			Err:         err,
		}
	}

	switch stop.reason {
	case stopFree:
		return fail(StrictChainFreeCluster, stop.cluster, nil, "chain runs into a free cluster after %d clusters", result.ClustersWalked)
	case stopBad:
		return fail(StrictChainCorrupt, stop.cluster, ErrCorruptStructure, "chain reaches a bad-cluster marker")
	case stopRange:
		return fail(StrictChainCorrupt, stop.cluster, ErrCorruptStructure, "chain leaves the data area")
	case stopLoop:
		return fail(StrictChainCorrupt, stop.cluster, ErrCorruptStructure, "chain loops")
	case stopRead:
		var se *StrictError
		if errors.As(stop.err, &se) {
			return se.atEntry(entry.ParentFirstCluster, entry.EntryOffset)
		}
		return fail(StrictChainCorrupt, stop.cluster, stop.err, "FAT entry unreadable")
	case stopCap:
		// The caller bounded the walk; a chain it chose not to finish is not
		// evidence of anything.
		return nil
	}

	if entry.IsDirectory {
		return nil
	}
	perCluster := uint64(v.bytesPerCluster)
	needed := (entry.Size + perCluster - 1) / perCluster
	if uint64(result.ClustersWalked) != needed {
		var err error
		if uint64(result.ClustersWalked) < needed {
			err = ErrTruncatedChain
		}
		return fail(StrictChainLength, 0, err, "chain holds %d clusters, size %d needs %d",
			result.ClustersWalked, entry.Size, needed)
	}
	return nil
}
