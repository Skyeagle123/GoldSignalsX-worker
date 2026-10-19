# PA-PROCESSING-FINAL-01 continuity witness remediation

Reviewed parent: `8e898c8cdabb500c19560b0c44a483bcfaeda0de`.
This successor changes only the sample fold's continuity adapter and the narrowly required checkpoint/completion compatibility checks. Processing ownership, finite-snapshot selection/replay, archive verification, budgets, final completion fences and eviction are preserved.

The old sample adapter discarded ticks before entry before retaining their continuity relationship. The accepted `foldPostEntry()` instead considers continuity first, then excludes pre-entry ticks from analytical observations. The exact same-session, consecutive-sequence pair at entry minus/plus one second therefore incorrectly lost entry coverage and TP1 SUCCESS.

## Witness and observation

`sampleObservations()` may return a tick as a continuity-only input with empty observation/witness lists. A pre-entry price contributes no MFE, MAE, barrier, first-hit candidate, quality extrema or observation count. An entry-straddling provider/receipt interval retains the accepted uncertainty behavior while still participating in continuity, as in the accepted reducer.

Only one predecessor is retained in `lastTick`. It contains the validated semantic sample ID/digest, provider/source, session, sequence, provider/occurrence time, receipt/observation time, selected durable availability and selection key. It does not store the predecessor price or an accumulating history. The identity comes from authenticated selected evidence and remains protected by the existing immutable result receipt, state/fold digest and transactional checkpoint CAS.

Coverage requires the accepted same nonempty session, source, integer consecutive sequence, monotonic receipt/provider times, and at most 20 seconds between each clock's observations. Coverage begins at the later of entry and predecessor receipt time. A pre-entry-to-pre-entry pair establishes no post-entry coverage. Different provenance, missing sequence, excessive cadence, or contradictory timing cannot manufacture coverage. Bars keep their existing interval eligibility and ambiguity semantics.

## Checkpoint compatibility

The existing algorithm versions remain `pa-sample-selection-v2`, `pa-sample-fold-v2` and `measurement-result-samples-v2`. Fold state now explicitly includes `continuityVersion = pa-continuity-witness-v1`.

A valid older checkpoint lacking this continuity version is not reused: the existing scanner restarts from its authenticated finite input boundary after verifying the saved checkpoint's integrity. A canonical result without this version cannot acknowledge an evaluation obligation. The existing narrowly fenced legacy requeue helper also recognizes pre-continuity sample results, so an idle DONE signal job can be recomputed under its own lease. Historical results are not rewritten. Invalid/tampered checkpoint evidence still fails closed.

The predecessor is reusable scan evidence, never final completion authority. Current market, source, binding, lifecycle, epoch, job ownership and completion fencing are unchanged. No migration or additional state table is used.

## Regression coverage

`test-b1-package-a-processing-continuity.mjs` adds 25 regressions covering:

- Exact Astra pair: COMPLETE entry coverage and TP1 SUCCESS.
- Extreme pre-entry high/low price exclusion from all post-entry analytics.
- Packet grouping/order, selection page size and invocation partition invariance.
- Serialized predecessor identity and one bounded predecessor across 130 pre-entry observations.
- Source/session mismatch, cadence/sequence/provider-time failures, exact-entry sequence and bar/tick interval behavior.
- Actual processing checkpoints followed by scheduled recovery, separate linked evaluation completion, crashes before commit and lost responses after commit.
- Both hot-to-archive arrangements, using authenticated archive reads and the supported freshly fenced reconciliation/eviction path to establish the storage fixture.
- Old checkpoint safe restart, pre-fix canonical acknowledgement rejection, and tampered predecessor integrity rejection.

No test directly marks a processing obligation DONE. Test-only SQL modifications are explicitly invalid historical-format/tampering fixtures. Archive fixture eviction uses accepted runtime exports; no fence is bypassed.

Final evidence is under `evidence/package-a-processing-continuity/`. The final acceptance summary distinguishes the 980 prior tests from these 25 new tests and records the aggregate run against frozen source, policy checks and isolation verification.

## Historical archive control

The exact 59-record / 12-market / 81-object / 2,225,978-byte control now loads `test-fixtures/b1-processing-archive-parent-control.json.gz`. It was generated once by the reviewed parent's unchanged fixture using the independently reconstructed `8e898c8...` source. It includes that parent's immutable archive objects and historical RESULT bytes, with no mutable progress slots. The fixture's compressed SHA-256 is checked before loading, and the accepted verifier independently verifies package integrity. Existing geometry assertions and invocation-wide resource assertions remain intact.

This narrow test-fixture change is necessary because the new continuity witness/version legitimately changes newly generated RESULT payload bytes. It prevents future checkpoint metadata from silently changing the historical control. Other bounded-archive regression fixtures are still produced by the current runtime; no budget or integrity assertion was relaxed.
