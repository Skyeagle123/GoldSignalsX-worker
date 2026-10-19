# Package-A Official processing ownership

Narrow successor of `687ee3d5bfb4355bc26b50762b73f7d984ab5cef` for
PA-SAMPLE-ALIAS-EXEC-01. No measurement migration is added. Schema 11/12
compatibility follows the existing runtime; migrations 0001–0012 are unchanged.

## Ownership and dispatch

Evaluations own decision/census identities; signals own Official creation,
confirmation and lifecycle facts, lifecycle projection and Official outcome.
Candidate processing remains evaluation-owned. No lifecycle facts or projections
are copied into Official evaluation scopes.

`resolveProcessingSubject()` validates durable pa_bindings and decision bytes,
including digest, cohort and the originating Official-creation envelope. A signal
with several bindings is resolved through its creation evidence, never an arbitrary
first binding. Missing decision/binding/creation returns PENDING with an explicit
dependency reason. Contradictory evidence fails closed. Existing quarantine and
lifecycle integrity checks remain in force.

Official evaluation dispatch rebuilds the signal lifecycle, attempts fresh
acknowledgement of existing canonical work, or invokes signal processing with a
separate signal job lease. Busy/stale/incomplete work cannot discharge the evaluation.
The actual signal work atomically commits its result, bounded processing checkpoint,
reservation settlement and its own job state. No evaluation lease is borrowed.

## Fresh evaluation completion

The bounded pa_processing payload includes versioned `processingAuthority`:
- exact binding context, epoch, evaluation/signal IDs and cohorts;
- source revisions, scope archive pointers, durable availability and dependency flags;
- complete relevant binding membership and live decision/creation identities;
- cohort market revision/pending/conflict and active-restore absence;
- decision and creation identities/digests;
- signal projection revision/digest.

The completion helper re-reads the exact durable checkpoint, derives and verifies
its immutable RESULT identity/digest, and requires completed captured-snapshot
processing. Its final atomic SQL batch rechecks the entire context, clean linked
scopes, current COMPLETE signal projection, current processing and market revisions,
exact result/checkpoint bytes, durable availability, no pending/running canonical
job, plus the evaluation lease ID/owner/generation/nonce/epoch and SQL-clock expiry.
Only then does it mark the evaluation job DONE. A returned success is not authority.

A canonical DONE job may already have been compacted: the exact current durable
checkpoint/result is the receipt, and absence of pending/running canonical work
remains freshly fenced. A response loss after completion is reconciled by durable
job state; stale workers fail their lease fence.

Archive membership revision includes RESULT insertion, so it is not a source
revision fence for processing its own result. Non-RESULT source revisions, exact
live decision/creation identities and scope archive pointers are fenced instead.
Unrelated global catalogue appends do not invalidate immutable processing inputs.
No change is made to archive or eviction authority.

## Invalidation and boundedness

Existing migration-0007 linked revision propagation remains unchanged. Ingress
completion also enqueues linked evaluation obligations for signal events. Correction
admission enqueues both linked roles atomically. Market admission already enqueues
both roles. Changed revisions/projection authority restart the bounded Official
market scan rather than carrying forward an incompatible fold.

Existing limits remain: 80 bindings, 256 scope facts, 4,032 processing rows,
8,192 jobs, 60,000-byte row/payload and invocation SQL budgets. Context membership
queries use limit-plus-one and byte checks; no history or acknowledgement table is
introduced. Checkpoint size is included in the existing processing reservation.
Logical cleanup grants zero physical-allocation credit.

All pending classifications stay pending. Only confirmed result completion or
freshly fenced census-only completion can complete a processing job. Genuine
pending work continues to block eviction; dependency closure and final eviction
code are unchanged.

## Compatibility and validation

Census identities and PAGE_ONLY reporting, historical availability/asOf, corrections,
content addressing, restore, lifecycle reducer and trading/M1/M2/M3 semantics are
unchanged. A signal projection being COMPLETE does not establish market coverage.

Tests use native SQLite atomic batches, accepted producer/Engine functions, actual
Measurement Queue/scheduled composition, independent adapters, fault hooks and a
source-D1-unavailable restore. The preserved sample-alias and Package-B worktrees
are not implementation inputs to this candidate. A disposable combined-draft test
may validate interoperability; it does not accept or commit the sample-alias draft.

No cloud qualification, publication, deployment or production access is performed.
