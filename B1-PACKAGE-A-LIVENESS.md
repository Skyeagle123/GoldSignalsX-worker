# Package A — confirmation and continuing-publication progress

Reviewed start: `b57ceb5980bcd01afd68a3ba23344237c8009494`.
Scope: PA-LH-REVIEW-01 and PA-LH-REVIEW-02 only. No trading source,
measurement policy, capability boundary or cloud activation changes.

## Complete bounded discovery confirmation

`discoverArchiveRoot()` no longer has a root-count-based confirmation shortcut.
LIST, LINEAGE and CONFIRM use the same bounded driver and authenticated private
archive CAS continuation. At most three phase turns and 512 proof-input reads
occur per invocation; progress I/O remains separately bounded/accounted.
Small packages can still finish in one invocation. Small root count does not
grant an exemption from the proof budget.

The confirmation frontier is the remaining root-key queue, listing cursor,
selected latest root identity/generation/epoch, listing fingerprint/boundary,
phase and count of completely verified packages. A root's content address binds
its package, receipt and predecessor; complete package verification authenticates
every record/chunk and definition dependency. A package is an atomic verification
unit: up to 272 package reads plus root/checkpoint/receipt and one transition
read (276 total), strictly less than the 512-read invocation budget. If a unit
exhausts the *remaining* budget, its partial work is discarded and its exact
root stays at the frontier. The next invocation has enough budget for that unit.
No partially checked package/record contributes to confirmation. There is no
per-record cursor to forge, skip or accumulate.

Namespace, discovery operation/fallback policy, selected root and phase/frontier
are held in the existing private progress authority, with payload digest,
nonce, ETag CAS, one-token replay and expiry. Listing changes fail confirmation;
tampering or changed content-addressed bytes fail verification. Restart and
duplicate continuation delivery retain the last complete unit. Existing
cross-epoch edge validation is unchanged. Both databases may be unavailable.

The accepted immutable-content contract is historical authentication at read
boundaries, not unqualified return-time physical object availability. An archive
writer cannot change an authenticated object to different valid content at the
same SHA-256 key. Corruption observed by a later read fails closed. Confirmation
re-reads earlier listing packages; final latest-package readback remains required.
Deletion/corruption *after* an object's final read is an availability incident,
not a new historical identity or ongoing D1 write permission. Protected writes
retain their fresh SQL fences and their own required availability checks.

## Append-aware retirement obligations

Additive schema 0011 creates `pa_archive_reference_work`, an indexed set of the
distinct **currently registered** archive root keys. It is not an edge/history
log. SQL triggers maintain it from meta, scopes, publications, restores,
restore-lineage authority and format work. Removal of the last durable reference
removes its registry row immediately. The view of authoritative references guards
against removing a still-registered root. The existing source-table bounds imply
at most 4,353 keys; an explicit trigger enforces that maximum.

Every newly registered key receives a never-reused AUTOINCREMENT sequence.
Duplicate current references share one identity; removing/re-registering a key
gets a fresh sequence, even when no old rows remain. `pa_meta.reference_sequence`
is a monotonic high-water mark with a no-decrease SQL guard. Initial migration
backfills current references and clears pre-0011 cursor formats. Old evidence,
restore authority, epochs and original record clocks are preserved.

Retirement proof v2 binds candidate identity/revision/state/ownership, reservation
identity, target epoch, archive namespace and version. Its complete payload has
a progress digest and the existing D1 CAS generation. It retains:

- the completed reference sequence and exact last root key;
- an in-progress registered root identity and predecessor/child frontier;
- at most eight fully proven immutable root tips;
- checked-root count and the observed final reference/catalogue boundary.

Each tip certifies absence of this exact candidate through that immutable root's
authenticated predecessor closure. A new descendant is verified only back to an
exact known tip, including any authorized epoch transition. Generation alone
never permits skipping an edge. New independent references are new work and
must have their own closure checked. The bounded tip cache is an optimization:
cache misses walk the unknown closure completely; there is no lifetime depth
limit. Descending-generation tips are retained preferentially so registering an
older root does not evict the current canonical lineage tip.

Unlike proof v1, unrelated reference/catalogue changes do not clear the historical
prefix. The registry's sorted sequence cursor exposes later current registrations
as additional obligations. Work already selected continues safely if that
registration is replaced; removed references need not remain as tombstones.
Saved prefixes are historical absence facts, so their CAS save need not freeze
registration. Candidate/epoch/ownership/reservation changes invalidate reuse.

At most 512 archive reads are used per maintenance pass. Compaction still runs
after expired-reservation recovery and before ordinary jobs. For a finite prefix
and bounded continuing workload below proof service capacity, it advances through
the prefix, catches up with newly registered work, and retires the candidate if
it remains eligible. New obligations arriving faster than service remain explicit
`PENDING`/`reference_proof_backlog`, with bounded `referenceReads`, `pending` and
current `referenceBacklog`. They do not reset authenticated history. Package B
must qualify actual cloud arrival/service/CPU rates; no unlimited-throughput
guarantee is made. An actual candidate reference or unavailable/corrupt dependency
correctly blocks retirement.

## Fresh deletion and accounting

Proof completion is not deletion authority. The final transaction repeats the
entire prior eligibility predicate, exact candidate/owner/lease/revisions and CAS,
the completed proof bytes, epoch, observed reference revision, catalogue generation
and monotonic sequence high-water mark. A registration during awaited proof/audit
work rejects that deletion, while preserving the historical prefix for the next
pass. Restore/correction/pin/job/incident/format/manifest requirements are freshly
checked. Newly published superseding-package dependencies are also retained.
Reservation token/owner/generation/pool/bytes checks and COMMITTED-to-SETTLED
settlement remain in the deletion transaction. No early proof step settles it.
Repeated/concurrent maintenance cannot settle twice or resurrect publication.

Registry insertion atomically admits and charges 81,920 model bytes
(`estimateWrite(1024,4)`: payload amplification plus row/index/temp allowances).
Hard-ceiling and archive recovery-pool checks include existing RESERVED liability.
This is conservative extra accounting, not cloud capacity evidence. Deleting
registry rows or publication/progress metadata credits **zero** physical/pool
bytes. Qualified physical observation/reconciliation remains separate. Fixed
archive slots (64 x 60,000 bytes), publication cursors (64 x 8,192 bytes), eight
tips per cursor, and current-reference rows are all bounded. Registry rows have
no generation history; obsolete publication cursors clear in bounded batches.

## Compatibility and validation

Migrations 0001–0010 remain byte-identical. Runtime and version-rollback floor
is schema 11. No request creates schema. Fresh chain and populated 0010→0011,
foreign keys, preserved values, uniqueness, capacity, immutable identities,
monotonic sequence/ABA and zero-credit cleanup are local acceptance gates.

New tests use actual published large packages, archive/root verification,
authenticated continuations, restore, publishArchive, real SQLite bindings,
scheduled Worker maintenance, reference-registration triggers and reservation
settlement. Historical repeated roots retain actual publisher-produced package,
definition, catalogue and receipt formats; fixtures do not mock verification.
The exact 8 x 90 reproduction, larger/two-root packages, continuation races,
129/640-root continuing publication, overload, new dependencies, final-fence
races and 72 invalidations with continuing publication are represented.

All prior PA-FINAL/PA-REMED safety fixes, `VALIDATED_AT_REVISION`, target epoch
freshness and trading parity remain required. No cloud migration, provisioning,
Production access, push, PR, merge, deployment, Package B/C or Daybreak is
authorized by this local successor.

Final local acceptance: **730 passed, 0 failed, 0 skipped**. There are 33 new
liveness cases, including the final SQL-budget supplement; 32 passed in the
focused run. Prior 697 accepted tests are preserved. Exact 8 x 90 discovery
completes in five invocations; larger packages in thirteen, and two large roots
in four. With a successor between every scheduled pass, the 129-root retirement
finishes on pass 3 and 640 roots on pass 12. Full evidence, normal auxiliary
gates and local timing **6/6/6** are under `evidence/package-a-liveness/`.
