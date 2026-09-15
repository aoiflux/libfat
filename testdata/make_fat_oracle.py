#!/usr/bin/env python3
"""Generate an independent oracle for a FAT image, for integration_test.go.

The integration tests read every image twice through libfat and compare the
results, which proves the library is self-consistent but not that it is right:
both readings derive their offsets from the same code. This script produces the
outside opinion that closes that gap, by listing and extracting the image with
7-Zip, whose FAT handler shares no code with libfat.

Usage:
    python3 make_fat_oracle.py IMAGE [-o OUTPUT] [--no-content] [--7z PATH]

The output is written next to the image as IMAGE.fat-oracle.tsv unless -o says
otherwise, following the convention of the other .oracle.tsv files in the
dataset. Records are tab separated:

    V   <filesystem> <label> <physical size> <cluster size> <sector size>
    d   <path>
    f   <path> <size> <short name> <modified> <created> <sha256 or ->

Paths are absolute and slash separated, matching libfat's DirEntry.Path. The
sha256 column is '-' with --no-content, which skips extraction: listing alone
still checks name assembly, sizes and the 8.3 short names, and is much faster.
"""

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile

VOLUME_KEYS = {
    "File System": "filesystem",
    "Label": "label",
    "Physical Size": "physical_size",
    "Cluster Size": "cluster_size",
    "Sector Size": "sector_size",
}


def run_7z(sevenzip, *args):
    out = subprocess.run(
        [sevenzip, *args], capture_output=True, text=True, errors="replace"
    )
    if out.returncode not in (0, 1):  # 1 is "warnings", which FAT tails produce
        sys.exit(f"{sevenzip} {' '.join(args)} failed ({out.returncode}):\n{out.stderr}")
    return out.stdout


def parse_listing(text):
    """Split `7z l -slt` output into the volume header and the entry records."""
    volume, entries = {}, []
    blocks = text.split("\n\n")
    body = False
    for block in blocks:
        fields = {}
        for line in block.splitlines():
            m = re.match(r"^([A-Za-z0-9 ]+) = (.*)$", line)
            if m:
                fields[m.group(1)] = m.group(2)
        if not fields:
            continue
        if not body:
            for key, name in VOLUME_KEYS.items():
                if key in fields:
                    volume[name] = fields[key]
            if "Path" in fields and "Size" in fields and "Attributes" in fields:
                body = True
            else:
                continue
        if "Path" in fields and "Attributes" in fields:
            entries.append(fields)
    return volume, entries


def as_path(raw):
    return "/" + raw.replace("\\", "/").strip("/")


def hash_tree(sevenzip, image, entries):
    """Extract once and hash, keyed by the path 7-Zip reports."""
    tmp = tempfile.mkdtemp(prefix="fat-oracle-")
    try:
        run_7z(sevenzip, "x", "-y", f"-o{tmp}", "--", image)
        digests = {}
        for e in entries:
            if "D" in e.get("Attributes", ""):
                continue
            local = os.path.join(tmp, e["Path"].replace("/", os.sep))
            if not os.path.isfile(local):
                continue
            h = hashlib.sha256()
            with open(local, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
            digests[e["Path"]] = h.hexdigest()
        return digests
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("image")
    ap.add_argument("-o", "--output")
    ap.add_argument("--no-content", action="store_true")
    ap.add_argument("--7z", dest="sevenzip", default="7z")
    args = ap.parse_args()

    if shutil.which(args.sevenzip) is None:
        sys.exit(f"{args.sevenzip} is not on PATH")

    volume, entries = parse_listing(run_7z(args.sevenzip, "l", "-slt", "--", args.image))
    if not entries:
        sys.exit(f"7-Zip listed no entries in {args.image}; is it a FAT image?")

    digests = {} if args.no_content else hash_tree(args.sevenzip, args.image, entries)

    out = args.output or args.image + ".fat-oracle.tsv"
    with open(out, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\t".join([
            "V",
            volume.get("filesystem", ""),
            volume.get("label", ""),
            volume.get("physical_size", ""),
            volume.get("cluster_size", ""),
            volume.get("sector_size", ""),
        ]) + "\n")
        for e in sorted(entries, key=lambda e: e["Path"]):
            path = as_path(e["Path"])
            if "D" in e.get("Attributes", ""):
                fh.write(f"d\t{path}\n")
                continue
            fh.write("\t".join([
                "f",
                path,
                e.get("Size", ""),
                e.get("Short Name", ""),
                e.get("Modified", ""),
                e.get("Created", ""),
                digests.get(e["Path"], "-"),
            ]) + "\n")

    files = sum(1 for e in entries if "D" not in e.get("Attributes", ""))
    print(f"{out}: {files} files, {len(entries) - files} directories, "
          f"{len(digests)} hashed")


if __name__ == "__main__":
    main()
