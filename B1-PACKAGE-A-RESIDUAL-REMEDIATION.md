# Package A — final residual recovery remediation

Starting candidate: `a36aee651a94c4340858207a5273e331db28713d`.
Only PA-REMED-01 and PA-REMED-02 and their required validation are changed. The six previously fixed PA-FINAL findings remain protected by the complete acceptance suite. Deployment and all new feature gates remain OFF.

## PA-REMED-01 — archive lineage after restore

A fresh target retains its own `pa_meta.epoch`. No source D1 revision epoch is copied. The additive schema-9 `pa_archive_lineage` row records the exact source root/epoch/generation, target epoch, restore identity and content-addressed transition wire/key. The row is immutable, unique per target epoch, and references the completed restore through a restrictive foreign key. Restore installs it in the same atomic batch as verified root authority, under the current lease nonce/generation, epoch, target root/catalogue boundary, authorization expiry and reservation fences. A stale/partial restore cannot install it. The associated completed restore cannot be compacted away.

The first publication (or archive-format migration) crossing epochs materializes that exact durable authorization in private R2. This is historical restore evidence, not continuing D1 write authority. The root includes the transition key; the existing content-addressed SQL-authorized root checkpoint binds the exact root bytes. Its final D1 registration still repeats the fresh publication/closure/root/catalogue fences after archive I/O. Later same-epoch publications use ordinary predecessor links.

Archive-only discovery verifies content hashes, root commit checkpoints, package receipts, every predecessor edge and every epoch transition. A cross-epoch edge must name the exact verified source root and source generation, correct source/target epochs and authorized restore identity. Missing, altered or corrupt transition evidence fails closed even with predecessor fallback. Arbitrary cross-epoch roots are rejected. Corrupt root objects can fall back only under explicit VERIFIED_PREDECESSOR policy. Source and target D1 are not consulted during discovery. Direct restore also verifies predecessor lineage before installing source authority. Transition objects remain retained by orphan eligibility.

Proof traversal is bounded: lineage depth 64; retirement reference roots 128, visited predecessors 256, reads 512 per pass. Exhausted proof budgets fail closed and do not authorize retirement or claim a verified complete lineage. These local bounds are not Production capacity qualification.

## PA-REMED-02 — safe terminal metadata retirement

Correction to the earlier review's diagnosis: the existing runtime operational compactor already had a STALE-publication branch. Its eligibility was insufficiently restrictive. This change strengthens that real path rather than adding a test-only substitute.

A candidate must be expired, rootless, STALE, in the current epoch, and irrecoverable because its pinned source/dependency revision changed or disappeared. An unchanged resumable snapshot is retained. Eligibility excludes active leases, pending/recovery/conflict/quarantine scope state, unresolved corrections (including other scopes depending on the snapshot), active pins/jobs, open incident references, active restore/format work, retained manifest references and shared reservations.

All registered archive roots and their predecessor chains are checked for publication/manifest/receipt references. Unavailable or over-budget proof retains the metadata. Registered roots used by lineage/restore remain protected. No R2 object is deleted. Before deletion an immutable operational audit captures the exact candidate identity/state; this is a retirement-candidate observation, not a fabricated retirement success.

The final atomic batch repeats eligibility and exact publication owner, generation, nonce, lease, source/snapshot, manifest, receipt and reservation identity. It rejects additions to the registered-root set after asynchronous proof. Reservation settlement checks its current owner, generation, token and epoch; only the publication's own archive reservation can settle. Deletion and COMMITTED-to-SETTLED liability reconciliation occur once together. No physical/pool allocation is credited. Qualified physical reconciliation remains separately gated.

Actual compactIndependentState/scheduled Worker composition invokes this bounded path (at most 16 publication candidates per runtime pass), with statement/read budget checks and bounded scanned/retired/blocked observations. Restart, competing maintenance and stale completion converge without double settlement or resurrected authority. A stale publisher's error path now also checks nonce and expected state.

## Schema and validation

Only `0009_package_a_archive_lineage.sql` is added. Migrations 0001–0008 are byte-identical to the reviewed start. The runtime and version-rollback floor are schema 9. Fresh chain, populated 0008-to-0009 upgrade, uniqueness, restrictive FK and immutable-trigger checks execute only in local SQLite fixtures. Request handlers never initialize/migrate schema.

`test-b1-package-a-residual-remediation.mjs` covers the real source publish/empty-target restore/two target publications/archive-only discovery, fresh target epoch, absent/tampered transitions, corruption/fallback, format migration, stale restore/publisher races, restored market conflicts, 72 real publication invalidations across the 64-row limit, runtime maintenance, retained references, exact-once liability, crash/restart and competing Workers.

Final counts, raw complete-acceptance output, all auxiliary gates, qualification plan and local resource/timing observations are saved in `evidence/package-a-residual/`. Historical evidence and recovery archives are preserved. No cloud/Production, push, PR, merge, deploy, Package B/C, trading-policy change or Daybreak is performed. Independent Astra acceptance is still required.
