package libfat

// Capabilities reports what a FAT volume can record, as distinct from what it
// happens to record.
//
// It exists so that a consumer can tell "this format does not keep that" from
// "that was absent here". Without it a pass comparing two readings of a volume
// has no way to know that FAT keeps no metadata-change time, and a report
// merging several filesystems would show FAT files as having lost permissions
// they never had. FAT is the format where this matters most bluntly, because it
// records so little: no owner, no permissions, no link count, no extended
// attributes, no holes.
//
// Most of these are properties of the format and identical for every FAT
// volume. They are still fields rather than documentation so that a caller can
// branch on a value instead of on a string naming the filesystem. The few that
// depend on the volume in hand say so.
type Capabilities struct {
	// UnicodeNames reports that names can be stored as UTF-16 code units. FAT
	// can, in long-name slots; the short name beside them is OEM-encoded, which
	// is why DirEntry carries both and NameSource says which one Name came
	// from.
	UnicodeNames bool `json:"unicode_names"`
	// CaseSensitive reports whether name comparison distinguishes case. FAT is
	// case-insensitive and, for long names, case-preserving, so two names
	// differing only in case cannot coexist even though the spelling is
	// recorded faithfully.
	CaseSensitive bool `json:"case_sensitive"`

	// CreationTimes, ModificationTimes and AccessTimes report which timestamps
	// the directory records carry. FAT has all three, at three different
	// resolutions - see SubSecondTimestamps, which is where that matters.
	//
	// The last-access field is an extension rather than part of the original
	// FAT12 and FAT16 layout, and a volume written by a system that does not
	// maintain it leaves it zero. This library reads it on every variant, so a
	// zero access time is absent rather than impossible.
	CreationTimes     bool `json:"creation_times"`
	ModificationTimes bool `json:"modification_times"`
	AccessTimes       bool `json:"access_times"`
	// MetadataChangeTimes reports whether a record carries the moment its own
	// metadata last changed - ctime on a POSIX filesystem. FAT has no such
	// field, so a rename or an attribute change can leave no timestamp behind
	// at all, and a consumer must not read the absence of one as evidence that
	// nothing happened.
	MetadataChangeTimes bool `json:"metadata_change_times"`
	// SubSecondTimestamps reports that some timestamps carry a fraction of a
	// second. FAT's do, but only one of them, and the asymmetry is large enough
	// that a consumer comparing two readings needs it spelled out:
	//
	//   - creation is recorded to 10 milliseconds,
	//   - modification to 2 seconds,
	//   - last access to the day, and is decoded as midnight UTC.
	//
	// So timestamp equality is not proof that nothing changed on this format.
	// Two genuinely different states one second apart carry identical
	// modification times, and two a few hours apart carry identical access
	// times.
	SubSecondTimestamps bool `json:"sub_second_timestamps"`
	// TimezoneOffsets reports that timestamps can be anchored to UTC by an
	// offset stored beside them. FAT stores none: the recorded values are local
	// time on whatever wrote them, and this library decodes them as UTC because
	// there is nothing else it could honestly do. Comparing timestamps across
	// volumes written in different zones compares different clocks.
	TimezoneOffsets bool `json:"timezone_offsets"`

	// POSIXPermissions, HardLinks, SymbolicLinks, ExtendedAttributes,
	// SparseFiles and Compression are all absent from FAT. A file is a name, an
	// attribute byte and a chain of clusters; there is no owner, no link count,
	// no hole and no second stream.
	POSIXPermissions   bool `json:"posix_permissions"`
	HardLinks          bool `json:"hard_links"`
	SymbolicLinks      bool `json:"symbolic_links"`
	ExtendedAttributes bool `json:"extended_attributes"`
	SparseFiles        bool `json:"sparse_files"`
	Compression        bool `json:"compression"`

	// StableFileIdentity reports whether the volume records a file identity
	// that survives being reused - an inode number with a generation count, or
	// anything equivalent. FAT records none, which makes this the most
	// consequential entry in this struct for a consumer diffing two readings of
	// a volume: a FileID names a slot in a directory, and a slot reused after a
	// deletion carries its predecessor's FileID exactly. See FileID for what
	// follows.
	StableFileIdentity bool `json:"stable_file_identity"`
	// IdentityReuseCounter reports whether a reused identity can be recognised
	// as reused. It cannot. It is stated separately from StableFileIdentity so
	// that a consumer checking only for the presence of an identity does not
	// mistake the pair libfat does supply for one that carries a generation.
	IdentityReuseCounter bool `json:"identity_reuse_counter"`

	// AllocationBitmap reports that free space is recorded independently of the
	// FAT. It is not: on FAT the table is both the chain and the allocation
	// record, which is why a deleted entry's chain is gone rather than merely
	// unreferenced, and why FragmentResult.FirstClusterReallocated is the most
	// that can be said about a deleted file's clusters.
	AllocationBitmap bool `json:"allocation_bitmap"`
	// ValidDataLength reports that a record distinguishes the bytes ever
	// written from the bytes allocated. FAT does not, so the tail of a file's
	// last cluster cannot be told apart from content by metadata alone; it is
	// reported as slack, by SlackRange.
	ValidDataLength bool `json:"valid_data_length"`
	// DeclaredContiguity reports that a record can state that its data occupies
	// consecutive clusters, leaving its FAT entries undefined. FAT has no such
	// flag - exFAT's NoFatChain is the sibling format's version - so a deleted
	// file's layout past its first cluster can only ever be assumed. That is
	// what FragmentOptions.AssumeContiguous does, and why the result is flagged
	// Assumed.
	DeclaredContiguity bool `json:"declared_contiguity"`
	// DeletedEntriesSurvive reports that deleting a file leaves its directory
	// record in place, marked rather than erased. FAT overwrites the first byte
	// of the short name with 0xE5 and the sequence byte of every long-name
	// slot, so the size, the timestamps and the first cluster survive, and the
	// name survives in part - see OpenOptions.RecoverDeletedLongNames.
	DeletedEntriesSurvive bool `json:"deleted_entries_survive"`

	// Journal reports whether the volume has a journal this library can read.
	// No FAT volume does. The format records no history of what changed, which
	// is why change detection on FAT rests on comparing two readings rather
	// than on replaying a log.
	Journal bool `json:"journal"`

	// SecondFAT reports whether this volume carries more than one file
	// allocation table. It is a fact about the volume in hand, read from its
	// boot record, rather than about the format. When it is true,
	// FATMirrorMismatches counts the entries whose copies disagreed.
	SecondFAT bool `json:"second_fat"`
	// FSInfoSector reports whether this volume has a usable FAT32 FSInfo
	// sector, read from its boot record. It is false on FAT12 and FAT16, which
	// have no such structure, and on a FAT32 volume whose FSInfo sector could
	// not be parsed. When true, FreeClusterCountHint and NextFreeClusterHint
	// have something to report - hints, which the format does not require to be
	// accurate.
	FSInfoSector bool `json:"fs_info_sector"`
	// BackupBootSector reports whether this volume records a backup boot
	// sector, read from its boot record. Only FAT32 has one. It is what let
	// this volume be opened at all if the primary was unreadable; see
	// UsedBackupBootSector for whether that actually happened.
	BackupBootSector bool `json:"backup_boot_sector"`
}

// Capabilities returns what this volume's format records.
//
// Every field is either fixed by FAT itself or, where the field says so, read
// from this volume's boot record. Nothing here reads the volume's directories,
// so it is cheap and cannot fail.
func (v *Volume) Capabilities() Capabilities {
	if v == nil {
		return Capabilities{}
	}

	caps := Capabilities{
		UnicodeNames:      true,
		CaseSensitive:     false,
		CreationTimes:     true,
		ModificationTimes: true,
		AccessTimes:       true,
		// The false values below are written out rather than left to the zero
		// value, because each one is an answer this type exists to give.
		MetadataChangeTimes: false,
		SubSecondTimestamps: true,
		TimezoneOffsets:     false,

		POSIXPermissions:   false,
		HardLinks:          false,
		SymbolicLinks:      false,
		ExtendedAttributes: false,
		SparseFiles:        false,
		Compression:        false,

		StableFileIdentity:   false,
		IdentityReuseCounter: false,

		AllocationBitmap:      false,
		ValidDataLength:       false,
		DeclaredContiguity:    false,
		DeletedEntriesSurvive: true,

		Journal: false,
	}

	caps.SecondFAT = v.numberOfFATs > 1
	caps.FSInfoSector = v.fsInfo != nil
	caps.BackupBootSector = v.fatType == FATType32 &&
		v.bootSector != nil && v.bootSector.BackupBootSector != 0

	return caps
}
