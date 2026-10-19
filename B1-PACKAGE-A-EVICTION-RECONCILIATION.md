# Narrow runtime successor: older-publication eviction

Parent: `1640c3a9a20fe561dbf190cdf397faf41610179f`. This successor resolves
PB-TOOLING-BLOCK-01 without changing publication identity, immutable archives,
trading, M1, M2 or M3. It does not activate or execute Package B.

## Operation

`reconcilePublicationEvictionAuthority(db, archive, publicationId,
{now, owner, continuation})` operates only on an existing parity-verified
`EVICTION_ELIGIBLE` publication. Use a stable owner and return the supplied
continuation on each subsequent invocation. `PENDING` is incomplete proof;
`READY` is a verified historical proof, **never continuing write authority**.
The maintenance facade exposes `reconcileEviction()` under the existing
`evict` authorization and default-OFF `B1_ARCHIVE_EVICTION_ENABLED` gate.
Reporting does not expose it.

Pass the READY continuation as `reconciliation` to `evictPublication()` or
the maintenance facade's `evict()`. Without it, the original exact-current-root
fence remains unchanged and rejects the original failing schedule.

## Verification and bounds

The replaceable publication cursor binds namespace, publication/root/package/
receipt identity, snapshot digest, source/archive revisions, publisher identity,
current epoch/root/catalogue generation and archive-reference revision. A digest
protects cursor integrity; durable generation, nonce, owner and SQL-clock lease
checks protect CAS/ABA and overlapping workers. Epoch replacement advances the
reference boundary even if an old epoch string is subsequently restored.

Verification phases are LINEAGE, ANCESTRY, MEMBERSHIP, READY:

* LINEAGE uses the accepted bounded, resumable full-lineage verifier, including
  its confirmation pass and authenticated restore epoch transitions.
* ANCESTRY proves actual predecessor edges from the pinned current root to the
  exact older publication root. Merely having two individually valid roots or
  catalogue membership is insufficient.
* MEMBERSHIP confirms exact canonical and semantic catalogue entries, original
  clocks/metadata, package/receipt identity and dependency identities. Market
  sample aliases are checked four per turn, preserving conflict lookup after
  eviction. Each segment handles one member (or four samples), not a permanent
  row per record/edge.

Each reconciliation invocation uses at most 1,024 proof reads, with a maximum
16 completed ancestry roots. Full-lineage verification retains its accepted
512-read budget and bounded private progress slots. Two maximum accepted root
packages plus six bounded catalogue paths fit the membership segment budget.
The cursor/receipt is at most 8,192 bytes on each of the existing maximum 64
publication rows. Leases last 60 seconds, checked against MAX(caller, SQL clock).
No unbounded cursor or edge history is retained.

A lost progress response can be recovered after lease expiry, reusing the saved
frontier if its exact boundary remains compatible. New owner claims rotate the
nonce and generation. Old continuations fail closed. A root/catalogue/reference
or epoch change invalidates that boundary's proof; after expiry a new invocation
can restart against the new boundary. No completion guarantee is claimed if the
boundary changes continually before a bounded proof can finish. PENDING never
counts as successful eviction or physical capacity reclamation.

### Unusable lineage continuation recovery (PA-EVICT-REVIEW-01)

A saved LINEAGE token may outlive its one-hour archive progress slot, or another
verifier of the same root may advance that shared slot beyond its single replay
generation. Only the exact `pa_archive_progress_expired` and
`pa_archive_progress_token` errors from loading an already saved lineage token
permit recovery. Reconciliation then clears only that token and returns
`PENDING`, reason `lineage_progress_restarted`; it does not verify additional
roots or credit incomplete work in that invocation. The next invocation starts
the accepted full authenticated lineage verifier again. The archive protocol,
replay window, confirmation pass, integrity checks and work bounds are unchanged.

The reset is committed only under the existing exact progress bytes, generation,
nonce, owner and SQL-clock lease fences, publication identity, epoch, pinned
root/catalogue/reference boundary, plus current source/archive revisions,
dependency closure/pending/correction/recovery state, parity and absence of an
active restore. Its new generation invalidates old continuations. If a competing
transition wins, the loser re-reads the publication generation and returns
`PENDING`, reason `eviction_reconciliation_raced`, without a continuation or
authority. Poll without the old continuation, respecting the current lease;
boundary/eligibility failures otherwise remain errors. This bounded conflict
response never means READY or deletion.

A crash before reset commit leaves the old token; after commit the durable null
frontier survives response loss. A restart can reacquire after lease expiry.
Missing/corrupt archive bytes, progress integrity failures, invalid predecessor
edges/transitions and membership failures are never converted into successful
proof or caught as stale progress. Expiry can be detected before archive bytes
are read; in that case subsequent fresh verification must still reject damaged
evidence. No new schema or accounting allocation is required, and final eviction
fences and atomic completion receipts remain unchanged.

## Final transaction and archive integrity

Before deletion, both the older package/root and pinned descendant package/root
are read back again. All canonical and semantic membership paths are rechecked.
At most 128 members times two paths times 32 nodes, plus two packages (550 reads),
requires at most 8,742 reads, below the accepted 9,000-operation archive budget.
The archive contract is private/content-addressed immutable evidence. Sample and
lineage proof progress uses that same accepted immutable-history assumption;
it is not an assertion that R2 and D1 share a transaction. An outage or corruption
observed during required verification rejects deletion. No response claims
unqualified return-time availability.

The final D1 batch freshly checks READY cursor generation/nonce/owner/lease and
exact bytes, epoch, current root/catalogue/reference boundary, publication
identity/snapshot/publisher identity, current source/archive revisions, pending
ingress/corrections, dependency revisions/quarantine/conflict, pins, jobs,
recovery/lifecycle/availability/hold state, complete binding set and absence of
an active restore. All existing deletion statements and fences are retained.
If any check fails, the entire protected batch rolls back. No prior READY
response or archive progress token can bypass this batch.

Format migration of an unrelated newer package works if the older exact
package/receipt remains retained. Replaced, superseded or conflicting older
membership fails closed; this operation does not reinterpret supersession or
invent equivalent archive identities. It never rewinds or transplants roots.

## Retry, cleanup and accounting

The final deletion and EVICTED receipt are atomic in the same publication row.
Response-loss retry with the consumed READY token returns the original result
with `replayed: true`, without deleting new evidence or rereleasing liability.
Concurrent consumers cannot both perform the protected batch.

The receipt lasts for the publication's existing metadata lifetime. Once normal
bounded compaction safely removes that publication, the old operation returns
`pa_eviction_not_verified`; it cannot resurrect authority or falsely report a
new deletion. Archive evidence remains independently discoverable.

Migration 0012 adds bounded columns and triggers only. The initial cursor charges
196,608 conservative bytes (16x cursor plus four row/index/temp allowances) to
the archive pool and physical admission model, including outstanding RESERVED
liability in both ceilings. Further cursor replacement is covered by that fixed
allocation. Cleanup grants zero physical or pool credit. No reservation is
released by reconciliation or cursor cleanup. Publication removal removes its
cursor without separate progress tables or unbounded cleanup queues.

New reconciliation requires schema 12. The successor can still run legacy
operations against schema 11; it never initializes schema on requests. After
applying 0012, runtime rollback requires a schema-12-compatible artifact: the
accepted schema-11 runtime rejects that database. Migrations 0001–0011 remain
byte-identical. Do not treat the older tooling's schema-11 rollback plan as
authorization to deploy it against schema 12. Cloud migration/deployment and
Package-B baseline adoption require separate Owner authorization.

## Local evidence

The focused test family exercises actual archive/publication/eviction functions,
the original Queue/scheduled Worker schedule, native SQLite transactions,
correction admission, restore, format migration, multi-instance adapters,
interleaved workers, failure hooks, cursor bounds and resource admission. Full
acceptance evidence is recorded separately with the final candidate.
