# libfat v0.3.5

This release fixes one bug: walks that include deleted records could lose live
subtrees. It also corrects two pieces of documentation. Upgrade if you use
`WalkOptions.IncludeDeleted` or `ReportDeep`.

The Go API is unchanged. libfat is still pure Go with zero dependencies, and
Go 1.25 remains the minimum.

```
go get github.com/aoiflux/libfat@v0.3.5
```

## Fixed

- **A walk that included deleted records could lose a live directory's whole
  subtree.** The cause was the walk's cycle guard. A deleted directory claimed
  its first cluster there before any attempt to read it, even when the walk
  never descended into it, either because `DescendDeletedDirectories` was off
  or because the cluster had been reallocated. A live directory that later took
  over that cluster was then treated as already walked, and everything under it
  went missing.

  This is the normal result of deleting a directory and creating another. The
  new record is written after the stale one and gets the freed cluster.
  `ReportDeep` lost the same subtrees.

  A deleted directory now claims its cluster only once the walk actually
  descends into it, so cycles among deleted records are still stopped. Walks
  without `IncludeDeleted` were never affected. The same defect was fixed in
  libxfat (XFAT-U2).
- **v0.3.4 reports its version as 0.3.5.** The release was renumbered after
  its commit was made, so the `Version` constant in the v0.3.4 tag reads
  `0.3.5`. Every report a v0.3.4 build writes carries `library_version`
  `"0.3.5"`, and a published tag cannot be corrected.
- **The documentation said `SkipRevisit` cannot happen on a valid volume.**
  That is true for a live directory, where it means a cycle or a cross-link,
  but not for a deleted one. When a live directory reuses a deleted
  directory's cluster and its record comes first, the live one is walked
  first. The deleted one is then reported `SkipRevisit`, whether or not
  `DescendDeletedDirectories` is set. The `SkipRevisit`, `OnSkip` and package
  documentation now describe both cases. Behaviour is unchanged, and a test
  now pins it.

## Upgrade notes

- **Deep walks and reports may gain entries.** On volumes where a deleted
  directory's cluster was reused, a walk with `IncludeDeleted` now reports
  the live subtree it used to omit, and so does `ReportDeep`. Anything that
  diffs walks or reports across versions will see the extra rows, even though
  the volume has not changed.
- **`library_version` `"0.3.5"` doesn't identify the build.** Both v0.3.4 and
  v0.3.5 write it, and nothing in the report tells them apart. If provenance
  matters, upgrade to v0.3.5 and record the module version from your own
  build, for example with `go version -m <binary>`.
- `ReportSchemaVersion` is unchanged.

See [CHANGELOG.md](CHANGELOG.md) for the full entry.
