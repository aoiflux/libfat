package libfat

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidBootSector = errors.New("invalid boot sector")
	ErrUnsupportedFAT    = errors.New("unsupported FAT variant")
	ErrVolumeClosed      = errors.New("volume is closed")
	ErrInputIsDirectory  = errors.New("input is a directory")
	ErrInvalidPath       = errors.New("invalid path")
	ErrFileNotFound      = errors.New("file not found")
	ErrNotDirectory      = errors.New("not a directory")
	ErrNotFile           = errors.New("not a file")
	ErrIsDirectory       = errors.New("is a directory")
	ErrCorruptStructure  = errors.New("corrupt filesystem structure")

	// ErrInvalidBaseOffset reports an OpenOptions.BaseOffset that cannot
	// describe a position in an image: a negative value, or one so large that
	// adding the volume's size to it would overflow an int64.
	ErrInvalidBaseOffset = errors.New("invalid base offset")

	// ErrTruncatedChain reports that a cluster chain covered fewer bytes than
	// the directory entry's recorded size. This is the normal outcome for
	// deleted entries whose FAT entries have been freed: the chain walk stops
	// after the first cluster because the FAT no longer records a successor.
	// Callers that tolerate partial data should check with errors.Is and use
	// whatever bytes or ranges were returned alongside the error.
	ErrTruncatedChain = errors.New("cluster chain shorter than entry size")

	// ErrFragmented reports that an operation requiring a single contiguous
	// extent was attempted on a file stored in more than one run.
	ErrFragmented = errors.New("file is fragmented")

	// ErrNoDataClusters reports that an entry has no allocatable first cluster
	// (an empty file, or an entry whose cluster fields were zeroed).
	ErrNoDataClusters = errors.New("entry has no data clusters")
)

// ErrStrict is matched by every *StrictError, so errors.Is(err, ErrStrict)
// tells a strict violation from an I/O failure or an unusable volume.
var ErrStrict = errors.New("strict check failed")

// StrictCheck names the rule a StrictError reports a violation of.
type StrictCheck string

const (
	// StrictChainFreeCluster: a chain reached a cluster whose FAT entry is
	// free (0) before an end-of-chain marker. Without Strict the chain simply
	// ends there.
	StrictChainFreeCluster StrictCheck = "chain-free-cluster"
	// StrictChainCorrupt: a chain loops, reaches a bad-cluster marker, or names
	// a cluster outside the data area, or an entry's first cluster is outside
	// it.
	StrictChainCorrupt StrictCheck = "chain-corrupt"
	// StrictChainLength: a file's chain holds a different number of clusters
	// from the ceil(size/cluster size) its entry's size requires. Without
	// Strict a long chain is trimmed silently and a short one is
	// ErrTruncatedChain alongside the bytes it does hold.
	StrictChainLength StrictCheck = "chain-length"
	// StrictLFNSequence: long-name slots are out of order, lack the slot
	// carrying LFNLastFlag, stop before ordinal 1, are not followed by their
	// short entry, or are not valid slots. Without Strict the entry falls back
	// to its 8.3 name.
	StrictLFNSequence StrictCheck = "lfn-sequence"
	// StrictLFNChecksum: a slot's checksum differs from the other slots' or
	// from LFNChecksum of the short entry it precedes.
	StrictLFNChecksum StrictCheck = "lfn-checksum"
	// StrictDotEntries: a subdirectory does not begin with "." pointing at
	// itself and then ".." pointing at its parent (0 when the parent is the
	// root, on every FAT type), or a "." or ".." record appears anywhere else,
	// including anywhere in the root. A directory opened through one of its
	// own "." or ".." records has no known parent, so its ".." is not checked.
	StrictDotEntries StrictCheck = "dot-entries"
	// StrictDuplicateName: two live entries in one directory have names, long
	// or short, that are equal under case folding.
	StrictDuplicateName StrictCheck = "duplicate-name"
	// StrictAfterEndMarker: a record that is not deleted follows the 0x00
	// record that ends the directory. Without Strict it is ignored.
	StrictAfterEndMarker StrictCheck = "after-end-marker"
	// StrictInvalidEntry: a live record is not a valid short entry - a bad
	// attribute byte, a name with illegal characters, a size with no cluster.
	// Without Strict it is skipped.
	StrictInvalidEntry StrictCheck = "invalid-entry"
	// StrictFATMirror: the copies of the FAT disagree about a cluster.
	// Without Strict the first copy is used, unless only the second holds a
	// plausible value, and the disagreement is counted by
	// Volume.FATMirrorMismatches. OpenOptions.StrictSkipFATMirror turns this
	// one check off.
	StrictFATMirror StrictCheck = "fat-mirror"
)

// StrictError reports a structure that OpenOptions.Strict rejects and the
// default, tolerant reading would have accepted or repaired.
//
// DirCluster and EntryOffset locate the 32-byte record the violation was found
// at, in the same terms as DirEntry.ParentFirstCluster and EntryOffset: the
// first cluster of the directory holding it (FixedRootCluster for the FAT12
// and FAT16 root region) and the record's offset within that directory's data.
// For a chain the record is the entry that owns the chain. EntryOffset is -1
// when no record is to blame, as for the root directory's own chain or a FAT
// mirror disagreement found outside any walk.
type StrictError struct {
	Check       StrictCheck
	DirCluster  uint32
	EntryOffset int64
	// Cluster is the cluster at which a chain or FAT check failed, and 0 for
	// checks that are not about a cluster.
	Cluster uint32
	Detail  string
	// Err is the underlying error where there is one, such as
	// ErrCorruptStructure for a loop or ErrTruncatedChain for a short chain.
	Err error
}

func (e *StrictError) Error() string {
	msg := fmt.Sprintf("strict %s at directory cluster %d, entry offset %d", e.Check, e.DirCluster, e.EntryOffset)
	if e.Cluster != 0 {
		msg += fmt.Sprintf(", cluster %d", e.Cluster)
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns ErrStrict and, when set, Err, so that errors.Is matches
// either.
func (e *StrictError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrStrict}
	}
	return []error{ErrStrict, e.Err}
}

// atEntry places a violation found without knowing which record was to blame
// - a chain walk, a FAT lookup - at the record of the entry that owns it,
// unless it has already been placed.
func (e *StrictError) atEntry(dirCluster uint32, entryOffset int64) *StrictError {
	if e.EntryOffset < 0 && e.DirCluster == 0 {
		e.DirCluster = dirCluster
		e.EntryOffset = entryOffset
	}
	return e
}

type VolumeError struct {
	Op  string
	Err error
}

func (e *VolumeError) Error() string {
	return fmt.Sprintf("volume %s: %v", e.Op, e.Err)
}

func (e *VolumeError) Unwrap() error {
	return e.Err
}

type ParseError struct {
	Structure string
	Offset    int64
	Err       error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse %s at offset %d: %v", e.Structure, e.Offset, e.Err)
}

func (e *ParseError) Unwrap() error {
	return e.Err
}

type PathError struct {
	Op        string
	Path      string
	Component string
	Err       error
}

func (e *PathError) Error() string {
	return fmt.Sprintf("%s %s at %s: %v", e.Op, e.Path, e.Component, e.Err)
}

func (e *PathError) Unwrap() error {
	return e.Err
}

func wrapVolumeError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &VolumeError{Op: op, Err: err}
}

func wrapParseError(structure string, offset int64, err error) error {
	if err == nil {
		return nil
	}
	return &ParseError{Structure: structure, Offset: offset, Err: err}
}

func wrapPathError(op, path, component string, err error) error {
	if err == nil {
		return nil
	}
	return &PathError{Op: op, Path: path, Component: component, Err: err}
}
