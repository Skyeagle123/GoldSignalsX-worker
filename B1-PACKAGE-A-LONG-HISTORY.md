# Package A — bounded long-history proof progress

Reviewed start: `ddb9e705b2860faa7aec9c81bae0a9a6300f245f`.
This successor addresses only PA-REMED-01-R1 and PA-REMED-02-R1. It preserves
the prior cross-epoch authorization, target D1 epoch, write fences, semantic
indexes, correction/reporting contracts, resource ceilings and trading source.

## Archive lineage verification

`verifyArchiveLineage()` now returns `PENDING` plus an opaque continuation when
its invocation budget is exhausted. Callers resume with that continuation and
the same root key. No partial result is a verified root. `hydrateArchive()`
returns `restore_lineage_pending` before creating target restore/index/authority
state; the maintenance restore capability forwards `lineageContinuation`.

Each invocation checks at most 16 roots per traversal segment by default, with
at most 512 proof-input reads. Root count may be configured only within 1–64;
512 is not increased. Small histories retain single-call completion. Resumption
checks the pinned latest root header/commit checkpoint without spending the
whole budget reloading its package before advancing. Each complete predecessor
package is still verified. Strict generation descent and the unchanged
`verifyArchiveEdge()` authenticate every same-epoch and authorized cross-epoch
edge. No lifetime depth limit or growing visited array remains.

A second complete, segmented confirmation walk rechecks earlier verified
segments. Missing/corrupt predecessor evidence, including beyond root 64, is
never converted to completion. The result authenticates immutable historical
content at the verification read boundaries; it is not a claim of unqualified
return-time object availability or fresh D1 write authority. A corruption seen
in confirmation fails closed; repair requires verified original content and a
new/resumed verification as appropriate. Final latest-package readback remains
required. D1 mutations independently repeat their fresh transactional fences.

## Archive-only continuation authority and discovery

The private archive adapter stores progress in exactly 64 replaceable keys:
`<namespace>progress/00` through `63`. These are operational CAS slots, separate
from immutable roots, receipts, transitions and evidence objects. No edge log,
append-only proof generation or live D1 is required. Both source and target D1
may be unavailable. Recovery needs the existing private archive read authority
and conditional-put authority for this operational prefix.

Each slot binds verification format/version, archive namespace, operation scope,
latest root identity/generation/epoch, predecessor frontier/child edge, phase,
bounded count/fingerprint and an exact payload digest. The returned slot/nonce/
digest must match the persisted private record; a caller-supplied frontier is
never trusted. Writes use the storage ETag compare-and-swap and full readback.
Two Workers cannot overwrite the same generation. A racing request may retry
its token; one previous token is retained for idempotent delivery/crash recovery.
Older tokens fail closed and may restart; they never acquire write authority.
This uses the existing trusted-private-writer archive authority model, not a
claim that an unkeyed hash alone authenticates arbitrary caller input.

Slot records are at most 60,000 bytes, contain at most one 64-key listing page,
and expire after one hour without progress. Completed or expired slots can be
replaced under CAS. Colliding active scopes return explicit
`pa_archive_progress_capacity`; expired/replaced continuations restart. Generic
orphan deletion cannot delete progress slots. Their fixed key count bounds
obsolete state; no completed cursor history accumulates. Adapter operation and
byte counters include progress I/O as well as evidence I/O.

`discoverArchiveRoot()` uses authenticated LIST → LINEAGE → CONFIRM phases.
It checks complete paginated root listings, the selected latest root's complete
predecessor chain, and a second complete listing/package readback. Fixed-size
count plus domain-separated SHA-256 multiset fingerprints compare the listing
and strictly descending, distinct predecessor set. Canonical root-key paths
prevent hidden predecessor objects outside that listing. An unrelated branch,
missing predecessor, changed listing, altered continuation or missing epoch
transition fails closed. Raw listing cursors cannot bypass authenticated
progress. The result is `VALIDATED_ARCHIVE_LISTING` for the pinned observed
catalogue, not a promise that no new root can appear across an async boundary.
Corrupt-root fallback still requires explicit `VERIFIED_PREDECESSOR`; it cannot
bypass a missing/invalid transition on the selected lineage.

## Stale-publication reference proof

Schema 0010 adds one bounded JSON cursor and CAS generation to each existing
publication row. There are at most 64 publication rows, never one row per edge.
The cursor is at most 8,192 bytes and records candidate identity, owner/revision
identity, target epoch, reservation identity digest, namespace, registered-root
reference revision, catalogue generation, sorted root-list frontier, current
predecessor and checked-root count. It survives Worker/process restart.

Every legitimate registered-root addition/removal/replacement increments a
durable `archive_reference_revision`, including root ABA, across meta, scopes,
publications, restores, lineage and format work. A candidate/context change
restarts proof instead of reusing old absence authority. Registered roots are
read by a sorted bounded cursor, without the old 128-root/256-visited lifetime
limits. At most 512 reference reads occur per maintenance pass. Only fully
verified roots advance the durable cursor; interrupted package reads may repeat
that one package. A complete package fits within this segment budget. Generation
descent prevents cycles without growing per-edge state.

Saving progress uses exact candidate/owner/nonce/revision checks and the current
reference/catalogue boundary plus CAS generation. The final settlement/deletion
transaction repeats all existing irrecoverability, expiry, correction,
dependency, restore, pin/job/incident, format/manifest and reservation fences.
It also matches the completed proof and exact current reference boundary.
New references, replacement owners or reservation identities cannot borrow old
proof. Proof completion alone does not settle anything. Immutable candidate audit
precedes deletion; cursor fields are excluded from its retry-stable identity.
COMMITTED liability settles once with deletion, with zero physical/pool credit.

Obsolete cursors clear in bounded batches when state/epoch/reference/catalogue
changes; deletion removes the cursor with its publication. Referenced candidates
discard their absence proof. A durable rotating candidate cursor prevents a
prefix of retained candidates from starving later work, while unfinished proofs
receive priority. The existing publication reservation's conservative row/temp
allowance covers the additional bounded cursor; no unreserved per-edge rows are
introduced. Physical allocation still requires the separately qualified model.

Actual scheduled composition runs expired-reservation recovery, then bounded
compaction/proof progress, then ordinary recovery jobs within the existing
statement/archive budgets. Thus job load cannot consume the entire invocation
before proof gets its turn. Schema readiness is checked before scheduled writes.
Reporting remains unable to invoke any of these maintenance operations.

## Schema, tests and acceptance

Only additive `0010_package_a_proof_progress.sql` is added. Migrations 0001–0009
are byte-identical to the reviewed start. Fresh chain, populated 0009→0010,
preserved row values/FKs, JSON/size constraints, reference ABA, stale ownership,
and schema-10 runtime/rollback floor are tested locally. No request initializes
schema, and no cloud migration is executed.

`test-b1-package-a-long-history.mjs` exercises real archive/package verifiers,
real restore and publication, authorized cross-epoch transitions within long
history, archive-only discovery, corruption/fallback, CAS/replay, scheduled
maintenance, >1,024-read reference proofs, 72 invalidations, fresh final fences,
multi-instance completion and bounded state cleanup. Long historical root
fixtures retain actual publisher-produced evidence/packages/definitions and
exact committed-root/receipt formats; target publications and restore use actual
candidate functions. Complete test output and local resource/timing evidence
are retained under `evidence/package-a-long-history/`.

No cloud qualification, Production access, push, PR, merge, deployment, Package
B/C, trading/Exposure policy change or Daybreak is part of this successor.
Independent Astra acceptance remains required.
