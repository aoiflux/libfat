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
