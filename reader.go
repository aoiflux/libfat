package libfat

import (
	"errors"
	"fmt"
	"io"
	"sort"
)

// fragmentReader serves a file's bytes directly from its runs, so that a read
// touches only the clusters it needs. It replaces buffering the whole file for
// every call, which turned repeated reads of a large file into quadratic I/O.
type fragmentReader struct {
	volume *Volume
	ranges []Range
	starts []int64
	size   int64
	pos    int64
}

func newFragmentReader(v *Volume, ranges []Range) *fragmentReader {
	starts := make([]int64, len(ranges))
	var total int64
	for i, r := range ranges {
		starts[i] = total
		total += r.Length
	}
	return &fragmentReader{volume: v, ranges: ranges, starts: starts, size: total}
}

// Size returns the number of bytes the runs account for.
func (r *fragmentReader) Size() int64 {
	return r.size
}

func (r *fragmentReader) ReadAt(p []byte, off int64) (int, error) {
	if r.volume.IsClosed() {
		return 0, ErrVolumeClosed
	}
	if off < 0 {
		return 0, ErrInvalidPath
	}
	if off >= r.size {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	read := 0
	for read < len(p) {
		logical := off + int64(read)
		if logical >= r.size {
			return read, io.EOF
		}
		i := sort.Search(len(r.starts), func(i int) bool { return r.starts[i] > logical }) - 1
		if i < 0 {
			return read, io.EOF
		}
		run := r.ranges[i]
		delta := logical - r.starts[i]
		available := run.Length - delta
		want := int64(len(p) - read)
		if want > available {
			want = available
		}
		n, err := r.volume.ReadAt(p[read:read+int(want)], run.StartByte+delta)
		read += n
		if err != nil && !errors.Is(err, io.EOF) {
			return read, err
		}
		if n < int(want) {
			return read, io.EOF
		}
	}
	return read, nil
}

func (r *fragmentReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *fragmentReader) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = r.pos + offset
	case io.SeekEnd:
		target = r.size + offset
	default:
		return 0, fmt.Errorf("%w: invalid whence %d", ErrInvalidPath, whence)
	}
	if target < 0 {
		return 0, fmt.Errorf("%w: negative seek target %d", ErrInvalidPath, target)
	}
	r.pos = target
	return target, nil
}

// fragments returns the file's runs, tolerating a truncated chain so that the
// recovered prefix of a deleted file remains readable.
func (f *File) fragments() (*fragmentReader, error) {
	if f.reader != nil {
		return f.reader, nil
	}
	if f.volume.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if f.isDir {
		return nil, ErrNotFile
	}
	if f.size == 0 {
		f.reader = newFragmentReader(f.volume, nil)
		return f.reader, nil
	}

	result, err := f.volume.FragmentOffsetsWithOptions(f.entry, f.fragmentOptions)
	if err != nil {
		return nil, err
	}
	f.reader = newFragmentReader(f.volume, result.Ranges)
	f.truncated = result.Truncated
	return f.reader, nil
}

// Reader returns a reader over the file's data that resolves fragments lazily,
// without buffering the whole file. The returned reader shares the file's
// underlying volume and is not safe for concurrent use.
//
// For a deleted file whose chain is truncated, the reader covers only the bytes
// that could be located; use SetFragmentOptions with AssumeContiguous first to
// read a contiguity-based reconstruction instead.
func (f *File) Reader() (io.ReadSeeker, error) {
	r, err := f.fragments()
	if err != nil {
		return nil, err
	}
	return &fragmentReader{volume: r.volume, ranges: r.ranges, starts: r.starts, size: r.size}, nil
}

// ReaderAt returns an io.ReaderAt over the file's data. Unlike Reader it holds
// no cursor, so it is safe to use from multiple goroutines provided the volume
// is not closed concurrently.
func (f *File) ReaderAt() (io.ReaderAt, error) {
	r, err := f.fragments()
	if err != nil {
		return nil, err
	}
	return &fragmentReader{volume: r.volume, ranges: r.ranges, starts: r.starts, size: r.size}, nil
}

// SectionReader returns a section reader over the file's data. It succeeds only
// for a file stored in one contiguous run, where a single image extent covers
// the whole file; fragmented files return ErrFragmented and must use Reader.
func (f *File) SectionReader() (*io.SectionReader, error) {
	r, err := f.fragments()
	if err != nil {
		return nil, err
	}
	if len(r.ranges) != 1 {
		return nil, fmt.Errorf("%w: %s occupies %d runs", ErrFragmented, f.path, len(r.ranges))
	}
	return io.NewSectionReader(f.volume, r.ranges[0].StartByte, r.ranges[0].Length), nil
}

// SetFragmentOptions selects how the file's runs are derived. It must be called
// before the first read; it discards any reader already built.
func (f *File) SetFragmentOptions(opts FragmentOptions) {
	f.fragmentOptions = opts
	f.reader = nil
	f.truncated = false
}

// Fragments returns the absolute byte ranges holding the file's data.
//
// A contiguous file yields exactly one Range; more than one means the file is
// fragmented. For directories the ranges cover whole clusters. When the chain
// accounts for fewer bytes than the entry's size, the located ranges are
// returned with an error wrapping ErrTruncatedChain.
func (f *File) Fragments() ([]Range, error) {
	if f.volume.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if f.isRoot {
		return f.volume.RootDirectoryFragments()
	}
	return f.volume.FragmentOffsets(f.entry)
}

// FragmentsWithOptions returns the file's runs together with provenance flags.
func (f *File) FragmentsWithOptions(opts FragmentOptions) (*FragmentResult, error) {
	if f.volume.IsClosed() {
		return nil, ErrVolumeClosed
	}
	if f.isRoot {
		ranges, err := f.volume.RootDirectoryFragments()
		if err != nil {
			return nil, err
		}
		return &FragmentResult{Ranges: ranges, BytesCovered: TotalLength(ranges)}, nil
	}
	return f.volume.FragmentOffsetsWithOptions(f.entry, opts)
}

// IsFragmented reports whether the file's data is split across more than one
// contiguous run.
func (f *File) IsFragmented() (bool, error) {
	ranges, err := f.Fragments()
	if err != nil && !errors.Is(err, ErrTruncatedChain) {
		return false, err
	}
	return IsFragmented(ranges), nil
}

// IsFragmented reports whether the entry's data is split across more than one
// contiguous run. A truncated chain is not itself an error here: the runs that
// were located still answer the question for the part that is addressable.
func (v *Volume) IsFragmented(entry DirEntry) (bool, error) {
	ranges, err := v.FragmentOffsets(entry)
	if err != nil && !errors.Is(err, ErrTruncatedChain) {
		return false, err
	}
	return IsFragmented(ranges), nil
}

// Slack returns the unused tail of the file's final cluster. See
// Volume.SlackRange for the details.
func (f *File) Slack() (Range, bool, error) {
	if f.isRoot {
		return Range{}, false, nil
	}
	return f.volume.SlackRange(f.entry)
}
