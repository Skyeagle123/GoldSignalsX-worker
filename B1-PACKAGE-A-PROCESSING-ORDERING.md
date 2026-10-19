# Consolidated processing remediation (unpublished candidate work)

Parent: `e29a1f1907d98536c8ef18800f8f9da31366c076`.
Accepted published predecessor: `687ee3d5bfb4355bc26b50762b73f7d984ab5cef`.
This work addresses PA-PROCESSING-REVIEW-01, PA-PROCESSING-EXEC-02 and
PA-PROCESSING-EXEC-03. Earlier blocked-checkpoint documents/evidence remain
historical records, not acceptance claims for this work.

## Four authorities

| Class | Durable owner / version | Proves | Does not prove | Restart and fencing |
|---|---|---|---|---|
| Archive verification | Private archive slots; `pa-package-verification-v1` | Complete supported immutable package checks, or an explicit incomplete frontier | Current membership, subject ownership, lifecycle, result, job or eviction authority | Exact namespace/package/receipt/definition/membership context; ETag CAS, nonce, generation, one-hour expiry; obsolete state restarts without proof credit |
| Processing lookup | Requesting job/scope, private archive slots; `pa-processing-lookup-v2` | Bounded enumeration of required creation/lifecycle evidence | A current canonical subject or a completed processing obligation | Target epoch, root/catalogue/reference boundary, scoped revisions and binding set; independent job lease checked before/after progress persistence and before use |
| Sample checkpoint | Canonical signal's bounded `pa_processing` payload and immutable RESULT receipt; `pa-sample-selection-v2` / `pa-sample-fold-v2` | Exact progress through one finite captured input | Permanent event-time watermark or current completion authority | Epoch, separate hot/archive pins, subject/binding/lifecycle identity, predecessor checkpoint CAS and exact receipt; incompatible old algorithm state restarts |
| Completion | Existing signal/evaluation transactions | Exact current durable canonical completion and separately acknowledged evaluation obligation | Continuing authority from an earlier returned COMPLETE | Full current market/dependency/context, projection, result, job owner/generation/nonce/SQL-clock lease and epoch fences |

A completed record-order (`measurement-result-v1`) checkpoint cannot acknowledge
a new-format evaluation obligation. Under the evaluation's current lease and
exact binding/context/checkpoint fence, an idle DONE canonical signal job may be
requeued for authenticated finite-snapshot recomputation. Historical RESULT
records remain unchanged. Active signal leases are never replaced or borrowed.

Signal owns Official lifecycle/outcome; evaluation owns decision/census. Neither
job borrows the other's lease. PENDING never falls through to DONE. Verification
progress has no result/job-completion/eviction operation.

## Finite sample selection

The captured input pins epoch, subject/binding/decision/creation/lifecycle,
hot admission revision and asOf separately from archive root/catalogue/reference
and correction context. It does not reinterpret hot revisions as archive
membership revisions. Enumeration order is container pagination only.

Versioned sample traversal uses receipt time for ticks / interval end for bars,
source/session commitments, type, authenticated sequence and semantic identity.
Provider occurrence intervals, receipt/observation and durable availability stay
separate. Hash/lexical pagination never establishes market first-hit chronology.

Every selected block is at most 64 descriptors AND 16 KiB, and is folded only
after a complete enumeration sweep proves no smaller eligible sample omitted.
Same semantic identity/digest reduces once; contradictory content fails closed.
The adapter retains cross-page continuity, delayed quality finalization and
bounded resumable first-hit dominance verification. Equal-time groups have no
fixed witness-count truncation. Bars preserve interval ambiguity.

Later finite admissions are reconciled separately. Compatible suffixes extend
reusable progress; duplicates deduplicate; earlier samples replay a finite
captured input. Corrections, ownership/lifecycle changes and epoch changes
invalidate incompatible progress. There is no assumed maximum lateness or
permanent watermark. Catch-up is conditional on actual service exceeding
arrival plus verification/replay demand.

## Bounded verification and lookup

Package verification reconstructs authenticated headers/definitions/membership
under fixed accepted manifest bounds, then validates record units through the
existing codec and raw-envelope validators. It persists counts and a record
frontier; no full decoded package is persisted. Actual read bytes, not descriptor
claims, enforce cumulative package accounting. Receipt-bound root verification
also checks the root checkpoint and exact receipt relationships.

Direct historical container reads preserve the original pointer. If current
catalogue membership names a different equivalent container, both the current
authenticated entry and the original full package are checked. A package-only
proof explicitly binds a null receipt and cannot satisfy a receipt-bound root
proof. This preserves the previous direct-package verifier's obligations; it is
not alias migration or rebinding.

The same private 64 slots are used. Exact-context acquisition probes at most 64
slots, validates every read, finds a current matching context before selecting
an empty/expired/complete replaceable slot, and never overwrites active
incompatible progress. ETag CAS remains the race authority. Token load retains
the existing current/one-generation replay rule. Reacquisition without a token
does not make an obsolete token valid. Full active capacity returns bounded
backpressure. Integrity failures are not obsolete-token recovery.

Lookup persists lossless compact fact tuples and creation identity summaries,
not a second copy of each fact's identity/availability metadata. Lifecycle
projection still calls the unchanged `projectLifecycle` reducer. Its original
fact identity, clocks, conflicts, confirmations and persistence outcomes remain
represented. Progress reuse is always followed by fresh processing fences.

## One invocation meter

The outer `processCapturedOutcome` creates one meter. Nested signal work,
pre-resolution archive reads, lifecycle reads and the scanner share it. Adapter
views retain the original registered statement owner and parent SQL accounting.

| Ceiling | Limit |
|---|---:|
| Market record decode plus semantic validation units | 8 |
| Total record decodes | 128 |
| Archive operations including progress | 1,024 |
| Archive bytes read including progress | 32,000,000 |
| Catalogue object reads, included above | 64 |
| SQL statements, also constrained by parent Worker adapter | 900 |
| SQL reserve before checkpoint | 128 |
| Archive operation reserve before checkpoint | 8 |
| Progress/checkpoint envelope | 60,000 bytes |
| Selection | 64 descriptors AND 16 KiB |

Additional conservative limits are 16 MB decoded record bytes per opportunity,
a 4 MB in-invocation decoded-record cache and at most 64 authenticated catalogue
nodes. Cache reuse requires the exact record descriptor/object identity. It
cannot skip verification of a different physical witness. A validated market
packet travels with its decoded record within the invocation, avoiding a second
unaccounted semantic validation. These are execution/storage bounds, not a
deployed Worker peak-memory measurement.

Optional work reserves before beginning. Package units and lookup continuations
yield before consuming checkpoint capacity. The sample scanner checkpoints safe
partial work on budget yield and cannot claim COMPLETE from a yielded DONE
checkpoint. Trace evidence reports the entire outer opportunity and pre-commit
reserve snapshots. No phase receives a fresh eight-record allowance.

## Call graph audit

* Processing wrapper acquires the requesting job lease before archive lookup.
* Subject resolver uses processing-specific creation enumeration and metered
  decision reads, with fresh boundary/binding checks.
* Official evaluation delegates only through a separately leased signal call,
  passing the same meter and reader.
* Processing lifecycle rebuilding enumerates metered hot/archive facts through
  its own continuation and calls the unchanged lifecycle reducer.
* Sample root verification and record/catalogue reads use the same package
  verifier. The draft's separate `processingRootStep` package protocol is removed.
* General publication/report/restore readers remain unchanged. Their execution
  is not smuggled into a processing opportunity under a separate allowance.

## Isolation and validation

No migration or producer/trading/eviction change is permitted. Separate preserved
sample-alias and Package-B worktrees are excluded. Final acceptance requires one
complete aggregate invocation after focused families pass, plus fresh parity,
policy/security checks and exact frozen-source verification. Development logs
are not final acceptance evidence. No push/deployment/cloud qualification occurs.

## Recovery / version compatibility

The checkpoint/result formats are explicitly versioned. Do not run an older
record-order processor against new-format checkpoints as a rollback strategy.
Recovery requires format-aware recomputation of derived work, preserving original
evidence and historical result versions. No schema migration or destructive
reset is part of this change. Publication/deployment remains a separate gate.

The reconstructed archive control is valid synthetic evidence with the reported
59 records / 12 market records / 81 objects / 2,225,978-byte geometry. It contains
a supported quote-basis annotation and an expired operational hold; it is not a
claim of byte identity with the earlier unexported in-memory archive bucket.
