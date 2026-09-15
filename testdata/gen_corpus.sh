#!/bin/sh
# gen_corpus.sh — build a synthetic FAT corpus for the corpus tests.
#
# The three real images the integration tests use are good evidence, but between
# them they cover one sector size, two FAT types, no deletions and no fragmented
# regular file. Everything else libfat parses is tested only against fixtures
# this package builds itself, and a fixture is built from the same reading of the
# spec as the parser, so it cannot catch a misreading. This corpus fills those
# gaps with volumes written by implementations that share no code or reasoning
# with libfat: dosfstools for the geometry, mtools for the content.
#
# Two things FAT gives us that the sibling libhfs corpus could not have:
#
#   - mtools manipulates a FAT image entirely in user space. No kernel driver,
#     no mount, no root. So these volumes have real trees, real file data and
#     real deletions rather than being empty shells.
#   - Almost everything is deterministic: file content from a seeded PRNG, file
#     timestamps pinned with touch, and the volume serial and the volume label
#     record's own timestamps pinned with mkfs.fat -i --invariant.
#
# Successive runs are still not byte-identical, and it is worth knowing why
# rather than assuming otherwise. mmd stamps every directory record it creates
# with the wall clock, and mtools has no option to override it, so a few dozen
# bytes per volume change on each run. Nothing in the suite pins those, and the
# oracles survive a regeneration because they describe files rather than
# directory timestamps - but do not treat a regenerated image as a byte-for-byte
# reproducible fixture.
#
# Requires nothing preinstalled and no root: dosfstools and mtools are fetched
# and unpacked into $HOME as an ordinary user. Under WSL, invoke from PowerShell
# rather than Git Bash, which rewrites /mnt/... arguments into Windows paths:
#
#   wsl.exe -- sh /mnt/o/research/libfat/testdata/gen_corpus.sh /mnt/e/dataset/fat_synth
#
# Volumes total about 300 MB. Every one is checked with fsck.fat, and the three
# that are damaged on purpose say so in their own section below.

set -u

OUT=${1:-}
if [ -z "$OUT" ]; then
	echo "usage: $0 <output-directory>" >&2
	exit 2
fi

TOOLS=$HOME/.fattools
SBIN=$TOOLS/root/usr/sbin
MBIN=$TOOLS/root/usr/bin

# --- fetch dosfstools and mtools without root ------------------------------
# apt-get download needs no privileges and dpkg-deb -x unpacks anywhere, so the
# tools land in $HOME rather than needing a sudo password WSL will not give.
if [ ! -x "$SBIN/mkfs.fat" ] || [ ! -x "$MBIN/mtools" ]; then
	echo "fetching dosfstools and mtools into $TOOLS"
	mkdir -p "$TOOLS/pkg" "$TOOLS/root" || exit 1
	( cd "$TOOLS/pkg" && apt-get download dosfstools mtools >/dev/null 2>&1 ) || {
		echo "could not download dosfstools and mtools; is apt available?" >&2
		exit 1
	}
	for d in "$TOOLS"/pkg/*.deb; do
		dpkg-deb -x "$d" "$TOOLS/root" || exit 1
	done
fi
if [ ! -x "$SBIN/mkfs.fat" ] || [ ! -x "$MBIN/mtools" ]; then
	echo "tools not usable under $TOOLS/root" >&2
	exit 1
fi

MKFS=$SBIN/mkfs.fat
FSCK=$SBIN/fsck.fat
MCOPY=$MBIN/mcopy
MDEL=$MBIN/mdel
MMD=$MBIN/mmd
MLABEL=$MBIN/mlabel

# mtools refuses geometries it does not recognise unless told not to check, and
# several volumes here exist precisely because their geometry is unusual.
export MTOOLSRC=$TOOLS/mtoolsrc
printf 'MTOOLS_SKIP_CHECK=1\n' > "$MTOOLSRC"

mkdir -p "$OUT" || exit 1

# What the deletions removed, so that recovered bytes can be checked against the
# bytes that were there.
DELETED=$OUT/deleted.tsv
: > "$DELETED"

# --- deterministic source material -----------------------------------------
SRC=$OUT/.src
rm -rf "$SRC"; mkdir -p "$SRC" || exit 1

python3 - "$SRC" <<'PY' || exit 1
import os, random, sys
d = sys.argv[1]
r = random.Random(20260916)          # fixed so the corpus is reproducible
def w(name, n):
    with open(os.path.join(d, name), "wb") as f:
        f.write(r.randbytes(n))
open(os.path.join(d, "tiny.txt"), "wb").write(b"hello")
open(os.path.join(d, "empty.txt"), "wb").close()
w("exact.bin", 4096)
w("small.bin", 1000)
w("chunk.bin", 524288)
w("frag.bin", 2100000)
w("big.bin", 300000)
PY

# Pinned so the directory records carry the same timestamps on every run.
touch -d '2026-01-02 03:04:05' "$SRC"/*

fail=0
report() {
	if [ "$1" -eq 0 ]; then
		echo "  ok"
	else
		echo "  FAILED"
		fail=$((fail + 1))
	fi
}

# check IMG — fsck.fat must find nothing to repair. The three deliberately
# damaged volumes do not use this.
check() {
	if "$FSCK" -n "$1" >/dev/null 2>&1; then
		return 0
	fi
	echo "  fsck.fat is unhappy with $1"
	return 1
}

blank() { dd if=/dev/zero of="$1" bs=1M count="$2" status=none; }

# populate IMG — a small tree with long names, present on every content volume
# so the name decoder meets a third-party writer everywhere.
populate() {
	img=$1
	"$MMD" -i "$img" ::/Docs ::/Docs/Reports ::/Media || return 1
	"$MCOPY" -i "$img" "$SRC/tiny.txt"  ::/Docs/tiny.txt || return 1
	"$MCOPY" -i "$img" "$SRC/empty.txt" ::/Docs/empty.txt || return 1
	"$MCOPY" -i "$img" "$SRC/exact.bin" ::/Docs/Reports/exact.bin || return 1
	"$MCOPY" -i "$img" "$SRC/small.bin" ::/Docs/Reports/a-name-long-enough-to-need-several-slots.bin || return 1
	"$MCOPY" -i "$img" "$SRC/tiny.txt"  ::/Media/UPPER.TXT || return 1
	"$MCOPY" -i "$img" "$SRC/tiny.txt"  ::/Media/MixedCase.Txt || return 1
	return 0
}

echo "=== geometry ==================================================="

# fat12_b512: FAT12 at all. No real image in the dataset is FAT12, and it is the
# variant with the awkward on-disk form - entries are 12 bits, so every other
# one straddles a byte boundary and, with 512-byte sectors, pairs straddle
# sector boundaries too.
echo "fat12_b512.img"
IMG=$OUT/fat12_b512.img
blank "$IMG" 8
"$MKFS" --invariant -F 12 -s 8 -n FAT12B512 -i 12005120 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && check "$IMG"
report $?

# fat12_b4096: the same variant with a 4096-byte sector, so the FAT spans a
# different number of sectors and the straddle arithmetic differs.
echo "fat12_b4096.img"
IMG=$OUT/fat12_b4096.img
blank "$IMG" 8
"$MKFS" --invariant -F 12 -S 4096 -s 1 -n FAT12B4096 -i 12040960 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && check "$IMG"
report $?

# fat16_b1024: a sector that is not 512 bytes. Every real image in the dataset
# is 512, so all of libfat's sector arithmetic is exercised at one value only.
echo "fat16_b1024.img"
IMG=$OUT/fat16_b1024.img
blank "$IMG" 16
"$MKFS" --invariant -F 16 -S 1024 -s 4 -n FAT16B1024 -i 16010240 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && check "$IMG"
report $?

# fat16_bigroot: a fixed root directory region of 1024 entries rather than the
# usual 512, so the region spans twice as many sectors and RootDirectoryFragments
# returns a longer run.
echo "fat16_bigroot.img"
IMG=$OUT/fat16_bigroot.img
blank "$IMG" 16
"$MKFS" --invariant -F 16 -r 1024 -s 4 -n FAT16BIGRT -i 16102400 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && check "$IMG"
report $?

# fat32_onefat: one FAT rather than two. Capabilities.SecondFAT is false here and
# true everywhere else, and there is no mirror for FATMirrorMismatches to count.
echo "fat32_onefat.img"
IMG=$OUT/fat32_onefat.img
blank "$IMG" 36
"$MKFS" --invariant -F 32 -f 1 -s 1 -n FAT32ONEFAT -i 32000001 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && check "$IMG"
report $?

echo "=== deletions =================================================="

# delete_set IMG — write files, then delete a known set of them, including one
# with a long name so the long-name slots are orphaned the way a real deletion
# orphans them.
#
# Each removal is recorded in deleted.tsv with the sha256 of what was there.
# Nothing else in the corpus can say what a deleted file *was*, and recovering
# content from a record whose chain has been released is the thing forensic
# callers actually need FAT parsing for.
delete_set() {
	img=$1
	"$MMD" -i "$img" ::/Trash || return 1
	"$MCOPY" -i "$img" "$SRC/small.bin" ::/Trash/gone-short.bin || return 1
	"$MCOPY" -i "$img" "$SRC/exact.bin" ::/Trash/a-deleted-file-with-a-long-name.bin || return 1
	"$MCOPY" -i "$img" "$SRC/tiny.txt"  ::/Trash/KEPT.TXT || return 1
	"$MCOPY" -i "$img" "$SRC/big.bin"   ::/Trash/gone-big.bin || return 1
	"$MDEL" -i "$img" ::/Trash/gone-short.bin || return 1
	"$MDEL" -i "$img" ::/Trash/a-deleted-file-with-a-long-name.bin || return 1
	"$MDEL" -i "$img" ::/Trash/gone-big.bin || return 1

	base=$(basename "$img")
	record_deleted "$base" /Trash/gone-short.bin "$SRC/small.bin"
	record_deleted "$base" /Trash/a-deleted-file-with-a-long-name.bin "$SRC/exact.bin"
	record_deleted "$base" /Trash/gone-big.bin "$SRC/big.bin"
	return 0
}

# record_deleted IMAGE PATH SOURCE — one row of deleted.tsv.
record_deleted() {
	printf '%s	%s	%s	%s
' "$1" "$2" 		"$(wc -c < "$3")" "$(sha256sum "$3" | cut -d" " -f1)" >> "$DELETED"
}

# One per FAT type: a deleted record is read back differently on each, because
# the chain it used to point at lives in a 12-, 16- or 32-bit table, and on FAT12
# and FAT16 a record in the root sits in the fixed region rather than a cluster.
echo "fat12_deleted.img"
IMG=$OUT/fat12_deleted.img
blank "$IMG" 8
"$MKFS" --invariant -F 12 -s 8 -n FAT12DEL -i 12000099 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && delete_set "$IMG" && check "$IMG"
report $?

echo "fat16_deleted.img"
IMG=$OUT/fat16_deleted.img
blank "$IMG" 16
"$MKFS" --invariant -F 16 -s 4 -n FAT16DEL -i 16000099 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && delete_set "$IMG" && check "$IMG"
report $?

echo "fat32_deleted.img"
IMG=$OUT/fat32_deleted.img
blank "$IMG" 36
"$MKFS" --invariant -F 32 -s 1 -n FAT32DEL -i 32000099 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" && delete_set "$IMG" && check "$IMG"
report $?

echo "=== fragmentation =============================================="

# frag IMG N — write N chunks, punch every other one out, then write a file big
# enough that it has to reuse the holes. Every fragmented entry on the three real
# images is a directory; this is the only place a fragmented *regular file*
# exists, which is the shape FragmentOffsets is really for.
#
# N has to nearly fill the volume. Leaving plenty of free space past the chunks
# does not work on FAT32: mtools follows the FSInfo next-free-cluster hint, which
# points beyond everything already written, so the final file lands in clean
# space and comes back contiguous. Filling the volume first leaves the holes as
# the only place it can go.
frag() {
	img=$1; n=$2
	i=0
	while [ $i -lt "$n" ]; do
		"$MCOPY" -i "$img" "$SRC/chunk.bin" "::/c$i.bin" || return 1
		i=$((i + 1))
	done
	i=0
	while [ $i -lt "$n" ]; do
		"$MDEL" -i "$img" "::/c$i.bin" || return 1
		i=$((i + 2))
	done
	"$MCOPY" -i "$img" "$SRC/frag.bin" ::/frag.bin || return 1
	return 0
}

echo "fat16_frag.img"
IMG=$OUT/fat16_frag.img
blank "$IMG" 16
"$MKFS" --invariant -F 16 -s 1 -n FAT16FRAG -i 16000077 "$IMG" >/dev/null 2>&1 &&
	frag "$IMG" 30 && check "$IMG"
report $?

echo "fat32_frag.img"
IMG=$OUT/fat32_frag.img
blank "$IMG" 36
"$MKFS" --invariant -F 32 -s 1 -n FAT32FRAG -i 32000077 "$IMG" >/dev/null 2>&1 &&
	frag "$IMG" 68 && check "$IMG"
report $?

echo "=== damaged on purpose ========================================="
# The three below are modified after formatting, by the Python at the end of each
# section. They are NOT fsck-clean, and that is the point: each reproduces a
# state a real volume reaches that a formatter will never write.

# fat32_relabel: the boot sector's label and the root directory's label disagree.
# mlabel writes both, so the boot sector copy is put back to NO NAME afterwards -
# which is exactly what Windows leaves behind, and what made libfat report the
# wrong name until the root directory record was preferred.
echo "fat32_relabel.img"
IMG=$OUT/fat32_relabel.img
blank "$IMG" 36
"$MKFS" --invariant -F 32 -s 1 -n SCRATCH -i 32000055 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" &&
	"$MLABEL" -i "$IMG" ::REALNAME &&
	python3 - "$IMG" <<'PY'
import sys
# BS_VolLab lives at offset 71 of a FAT32 boot sector. Only this field is
# touched; the root directory's volume-ID record keeps what mlabel wrote.
with open(sys.argv[1], "r+b") as f:
    f.seek(71)
    f.write(b"NO NAME    ")
PY
report $?

# fat32_mirror: the two FATs disagree. A formatter writes them identical and
# fsck.fat will not leave them otherwise, so the second copy is edited directly.
# FATMirrorMismatches has no other way to be exercised on a real volume.
echo "fat32_mirror.img"
IMG=$OUT/fat32_mirror.img
blank "$IMG" 36
"$MKFS" --invariant -F 32 -s 1 -n FAT32MIRROR -i 32000066 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" &&
	python3 - "$IMG" <<'PY'
import sys
with open(sys.argv[1], "r+b") as f:
    b = f.read(512)
    g = lambda o, n: int.from_bytes(b[o:o + n], "little")
    bps, res, fatsz = g(11, 2), g(14, 2), g(36, 4)
    second = (res + fatsz) * bps            # start of FAT #2
    # Corrupt four entries well past the ones the tree actually uses, so the
    # volume still reads correctly through FAT #1 and only the mirror disagrees.
    f.seek(second + 4 * 900)
    f.write(bytes([0xAD, 0xDE, 0x00, 0x00]) * 4)
PY
report $?

# fat32_nobootsec: the primary boot sector is destroyed, so the volume can only
# be opened through the backup copy at sector 6. UsedBackupBootSector is false on
# every other volume anywhere in the test corpus.
echo "fat32_nobootsec.img"
IMG=$OUT/fat32_nobootsec.img
blank "$IMG" 36
"$MKFS" --invariant -F 32 -s 1 -n FAT32BACKUP -i 32000088 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" &&
	python3 - "$IMG" <<'PY'
import sys
with open(sys.argv[1], "r+b") as f:
    f.seek(0)
    f.write(b"\x00" * 512)
PY
report $?

# fat32_orphan: a directory whose own record is deleted and whose clusters are
# released, while the records inside it stay exactly as they were. That is what
# ScanOrphans exists to find, and no sequence of mtools commands produces it -
# mdeltree deletes the children too, which leaves nothing to recover.
echo "fat32_orphan.img"
IMG=$OUT/fat32_orphan.img
blank "$IMG" 36
"$MKFS" --invariant -F 32 -s 1 -n FAT32ORPHAN -i 32000044 "$IMG" >/dev/null 2>&1 &&
	populate "$IMG" &&
	"$MMD" -i "$IMG" ::/Lost &&
	"$MCOPY" -i "$IMG" "$SRC/tiny.txt" ::/Lost/orphan-one.txt &&
	"$MCOPY" -i "$IMG" "$SRC/small.bin" ::/Lost/orphan-two.bin &&
	"$MCOPY" -i "$IMG" "$SRC/tiny.txt" ::/Lost/THIRD.TXT &&
	python3 - "$IMG" <<'PY'
import sys
path = sys.argv[1]
with open(path, "r+b") as f:
    b = f.read(512)
    g = lambda o, n: int.from_bytes(b[o:o + n], "little")
    bps, spc, res = g(11, 2), g(13, 1), g(14, 2)
    nfat, fatsz, root = g(16, 1), g(36, 4), g(44, 4)
    first_data = res + nfat * fatsz

    def cluster_offset(c):
        return (first_data + (c - 2) * spc) * bps

    # Find LOST's record in the root and read the cluster it points at.
    f.seek(cluster_offset(root))
    rootdir = bytearray(f.read(bps * spc))
    target = None
    for i in range(0, len(rootdir), 32):
        rec = rootdir[i:i + 32]
        if rec[0] == 0x00:
            break
        if rec[11] == 0x0F or rec[0] == 0xE5:
            continue
        if rec[0:11].rstrip() == b"LOST" and rec[11] & 0x10:
            target = (i, int.from_bytes(rec[26:28], "little") |
                      int.from_bytes(rec[20:22], "little") << 16)
            break
    if target is None:
        sys.exit("LOST not found in the root directory")
    slot, cluster = target

    # Mark the directory's own record deleted, and the long-name slots that
    # precede it, exactly as deleting it would.
    j = slot
    rootdir[j] = 0xE5
    j -= 32
    while j >= 0 and rootdir[j + 11] == 0x0F:
        rootdir[j] = 0xE5
        j -= 32
    f.seek(cluster_offset(root))
    f.write(rootdir)

    # Release its chain in both FATs, so nothing reachable claims the cluster
    # and the records inside it are unreferenced rather than merely hidden.
    for n in range(nfat):
        f.seek((res + n * fatsz) * bps + cluster * 4)
        f.write(b"\x00\x00\x00\x00")
PY
report $?

echo
for f in "$OUT"/*.img; do
	printf '%-24s %9s bytes  oem=%s\n' "$(basename "$f")" \
		"$(wc -c < "$f")" \
		"$(dd if="$f" bs=1 skip=3 count=8 status=none | tr -d '\0')"
done

rm -rf "$SRC"

echo
if [ "$fail" -ne 0 ]; then
	echo "$fail volume(s) failed"
	exit 1
fi
echo "all volumes written to $OUT"
