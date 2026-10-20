# Deterministic processing/binding producer schedule

Runtime baseline: `800965af73d9398ad5f754b6e8fe62129bfb0ef0`.

Automatic push run `39394745085`, attempt 1, failed at the genuine producer test's fixed 14-event assertion. The test seeded its real synthetic Engine/producer inputs from `Date.now()`. Its daily candles are filtered through the accepted market calendar and then evaluated with the accepted candle-age gate. Consequently, Monday evening produced five confirmation links while Tuesday morning legitimately produced six. The extra event was the `1d` BUY confirmation, not a duplicate or a processing-derived event. The reviewed parent reproduces the same behavior.

The exact five-link regression now starts at `2026-10-19T21:40:00Z`. At this fixed time the synthetic daily candle's age is 337,200,000 ms, greater than the accepted 259,200,000 ms maximum. All seven evaluation census identities are still captured, including the rejected daily evaluation. The 30-minute Official owns its lifecycle/outcome and the original five confirmations.

Before Queue delivery the test checks all 14 producer event types, semantic identities, unique event IDs and count. After actual scheduled recovery it checks the same semantic set and exact event IDs in committed ingress. All previous processing, census, result/checkpoint, reporting and completion assertions remain enabled; one Official creation is also checked explicitly.

An additional fixed-date control at `2026-10-20T08:40:00Z` exercises the legitimate daily confirmation. The daily candle is only 117,600,000 ms old. All 15 producer events, including `native-alias-schedule:1:30m:buy:confirmation:native-alias-schedule:1:1d:buy`, pass through the unchanged Queue entry point and are retained as committed events. No producer output is filtered to force five confirmations.

Only the test file and this documentation/validation evidence change. Runtime, producer, Engine, calendar, continuity, archive/progress, eviction, migrations and workflow configuration remain byte-identical to the baseline. The earlier failed CI remains historical evidence; this task does not rerun it or publish the successor.

Validation order: the two fixed-date controls, the complete processing/binding family, one complete aggregate using the workflow's `node --test test-b1-*.mjs` command, then News, Reliability, Risk Budget, Risk Sizing and Signals Security. Final results and source-identity verification are recorded under `evidence/package-a-ci-fixture/`.
