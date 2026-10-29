package libfat

import "time"

// The functions in this file are pure codecs: they turn on-disk field values
// into Go values and back, and touch no volume. They are exported so that
// software writing FAT structures can encode them by the same rules this
// package decodes them by, and so reject what this package would reject.
// They are not a writer; nothing here lays out a volume or a directory.

// fatEpoch and fatEnd bound what a FAT date and time can express: years 1980
// through 2107 (a 7-bit offset from 1980), to 10 ms.
var (
	fatEpoch = time.Date(1980, time.January, 1, 0, 0, 0, 0, time.UTC)
	fatEnd   = time.Date(2107, time.December, 31, 23, 59, 59, 990_000_000, time.UTC)
)

// DecodeDateTime decodes a directory entry's date, time and tenths fields.
//
// clock counts seconds in units of two, and tenths - named for the field, but
// counted in units of 10 ms - supplies the odd second and the sub-second part,
// from 0 to 199. Pass 0 for the fields an entry does not record: a modification
// time has no tenths, and an access time no clock either.
//
// The result is in UTC because FAT stores no time zone; the values were written
// in the recording system's local time. A date of 0, which is how a time that
// was never recorded is stored, and a date whose month or day is out of range
// both decode to the zero time.Time.
func DecodeDateTime(date, clock uint16, tenths byte) time.Time {
	if date == 0 {
		return time.Time{}
	}
	year := int((date>>9)&0x7F) + 1980
	month := time.Month((date >> 5) & 0x0F)
	day := int(date & 0x1F)
	hour := int((clock >> 11) & 0x1F)
	minute := int((clock >> 5) & 0x3F)
	second := int(clock&0x1F) * 2
	nsec := int(tenths) * 10 * 1_000_000
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}
	}
	return time.Date(year, month, day, hour, minute, second, nsec, time.UTC)
}

// EncodeDateTime is the inverse of DecodeDateTime. It returns the date, the
// two-second clock, and tenths: the odd second and the sub-second part in 10 ms
// units, 0 to 199.
//
// t is converted to UTC first, so that DecodeDateTime returns the same instant.
// Precision below 10 ms is truncated. A t outside what FAT can express is
// clamped rather than rejected: anything before 1980-01-01 00:00:00 encodes as
// that moment and anything after 2107-12-31 23:59:59.99 as that one. The zero
// time.Time encodes as all zeros, the "not recorded" value DecodeDateTime maps
// back to the zero time.
//
// For a field that records less - modification times have no tenths, access
// times only a date - discard what the field does not hold.
func EncodeDateTime(t time.Time) (date, clock uint16, tenths byte) {
	if t.IsZero() {
		return 0, 0, 0
	}
	t = t.UTC()
	if t.Before(fatEpoch) {
		t = fatEpoch
	}
	if t.After(fatEnd) {
		t = fatEnd
	}
	date = uint16(t.Year()-1980)<<9 | uint16(t.Month())<<5 | uint16(t.Day())
	clock = uint16(t.Hour())<<11 | uint16(t.Minute())<<5 | uint16(t.Second()/2)
	tenths = byte((t.Second()%2)*100 + t.Nanosecond()/10_000_000)
	return date, clock, tenths
}

// LFNChecksum is the checksum every long-name slot carries over the 11-byte
// short name it belongs to. A long name is attached to a short entry only
// when every slot's checksum matches the entry's name.
func LFNChecksum(short [11]byte) byte {
	return lfnShortNameChecksum(short[:])
}

// IsValid83Char reports whether b may appear in a short name, by the rule this
// package parses with.
//
// Space is valid, because it is how both fields are padded; a writer
// generating a short name strips spaces from the basis first. Bytes from 0x80
// up are valid in both the base and the extension, as OEM code-page
// characters, as Windows accepts them. The dot is not valid: it separates base
// from extension when a name is displayed and is never stored, except in the
// "." and ".." records.
//
// It judges one byte. Whether a whole name is accepted is ValidShortName's
// question, which also asks where spaces fall.
func IsValid83Char(b byte) bool {
	return isValid83Char(b, false)
}

// ParseShortName renders the 11-byte name field of a live entry as ReadDir
// would. A leading KanjiE5 becomes 0xE5, trailing spaces are trimmed from base
// and extension, the two are joined with '.' when the extension is not empty,
// and ntres - the entry's byte 12 - lower-cases the base under NTResLowerBase
// and the extension under NTResLowerExt. Only the ASCII letters A-Z are
// lowered, as Windows does; every other byte is left as stored.
//
// Bytes are not translated from any code page: a byte from 0x80 up comes back
// as that byte. raw is taken to be live, so a leading DeletedMarker is not
// treated as a deletion.
func ParseShortName(raw [11]byte, ntres byte) string {
	if raw[0] == KanjiE5 {
		raw[0] = DeletedMarker
	}
	return formatShortName(raw, ntres)
}

// ValidShortName reports whether raw is a name this package accepts in a live
// record. It is the rule ReadDir applies, so a writer that checks with it
// writes no name a reader built on this package would drop.
//
// It is the reader's rule, which is deliberately tolerant, and not a statement
// of what Windows would generate. raw[0] must be neither 0x00, which ends a
// directory, nor DeletedMarker, which marks a deleted record; KanjiE5 is
// accepted in its place. Each field must be valid characters followed only by
// padding, the base may not be entirely blank unless the extension carries the
// name, and dots are accepted in the first two positions so that the "." and
// ".." records parse.
func ValidShortName(raw [11]byte) bool {
	if raw[0] == 0x00 || raw[0] == DeletedMarker {
		return false
	}
	if !looksLikeShortName(raw[0:8], raw[8:11]) {
		return false
	}
	return formatShortName(raw, 0) != ""
}
