# Package A — scoped final-review remediation

Reviewed predecessor: `19a872aea1f9b172e0880491fd1d06f9d180cfa9`.
Branch: `phase/b1-independent-measurement`.

This successor implements only PA-FINAL-01 through PA-FINAL-08. It preserves the approved independent architecture and `VALIDATED_AT_REVISION` response contract. It remains deploy-disabled and Production-inactive. Independent final review is still required.

| Finding | Remediation | Regression evidence |
| --- | --- | --- |
| PA-FINAL-01 | Historical duplicate reconciliation repeats ingress/semantic/event state, replay, expiry, epoch, affected revisions and exact job authority in one atomic batch. Failed fences preserve newer state and return historical durability with STALE_PRESERVED; recovery does not finish the newer job. | DLQ/ABANDONED, replay change, expiry, ownership change, quarantine/revision change, unchanged duplicate; entire durable-state comparison proves zero stale effects. |
| PA-FINAL-02 | SQL retires expired rootless CATALOGUED slots only when their pinned source/closure changed. Valid unchanged publications resume. Continuation retains its own atomic claim token, including delayed responses with reused owner names. | Source change before root, two adapters, live-owner preservation, delayed stale root writer, unchanged restart, root-registration crash, same-name ownership replacement. |
| PA-FINAL-03 | Restore validates market envelopes and rebuilds sample identities/digests, original clocks and cohort state. Completion installs verified archive authority against persisted target root/generation and lease. Index overhead is reserved, including populated pre-remediation restores. | Empty target/source unavailable, multiple samples, identical replay, changed-price rejection, outage, unsupported format, partial restart, root change, stale worker, same-name claim, populated upgrade/capacity. |
| PA-FINAL-04 | Live coverage obeys cohort/definition/availability/revision bounds. Archived coverage uses the pinned root and immutable coverage index. Supported older roots use bounded scan continuation. Resolved coverage is authenticated in the cursor; known gaps take precedence. | Exact asOf 100/revision 5 versus proof availability 200/revision 20 race; equality; revision-only race; known gaps; mixed storage; later catalogue exclusion; pinned pagination; older root without alias. |
| PA-FINAL-05 | Every page labels census metrics PAGE_ONLY; queryTotals=null. Continuations require canonical base64 and scoped HMAC. No final-page relabeling occurs. | Three records/two pages; skipped/rejected evaluations; replay/tampering; live/mixed/archive deduplication; correction/result versions; eviction with explicit unavailability. |
| PA-FINAL-06 | Admission INSERT derives classification and first durable admission from one SQL clock value. Staging/final completion repeat the +120-day clock fence. Crossing transitions pending work to UNREDUCIBLE only under current lease/state authority. | Before/exact/after +90; commit crosses +90; duplicate clock preservation; exact/after +120; preparation/archive/final-SQL delays; stale lease and restart. |
| PA-FINAL-07 | Runtime runRecovery, used by scheduled Worker composition, scans at most 16 expired RESERVED rows per pass. Release checks expiry and owner/generation/token/epoch; counts actual transitions. | Scheduled Worker entry, live reservation preservation, repeated maintenance, multiple adapters, bounded 17-row backlog, late commit rejection, reused identity race and nonnegative accounting. |
| PA-FINAL-08 | Plan/runner require workload counts in outage and catch-up including retries/multipart/corrections/archive/reporting. Observer validates recovery booleans and decimal-byte occupancy/reservation dimensions with bounded pressure ratios. | Missing categories reject; named-but-zero categories reject; all-category fake-clock outage/catch-up; incomplete corrections miss target; missing/inconsistent/over-ceiling resources reject. |

## Schema and compatibility

Only 0008_package_a_restore_authority.sql is added. It preserves 0001–0007 and evidence rows, persists target root/generation and semantic-index version, and advances pa_meta.schema_version to 8. The independent runtime and version rollback floor require schema 8 with supported archive formats. No public handler migrates or repairs schema. Migrations executed here only in local SQLite fixtures.

Restore refuses a different installed root and cannot replace authority changed during its operation. Pre-remediation missing indexes are rebuilt under lease/capacity fences. Original occurrence/observation/availability remain unchanged; restoration time is separate.

Reports do not provide query-level census totals. Clients may aggregate returned semantic records after complete traversal. Coverage scans/evicted snapshots produce explicit incomplete/unavailable responses, never an empty complete cohort.

## Local acceptance

Final results are in evidence/package-a-remediation/acceptance-summary.json with raw aggregate and auxiliary logs. The suite includes accepted M1/M2/M3, Engine/full-cycle differential, persistence/stored-record parity, trading timing, revision/ABA/multi-instance, migrations/upgrades/FK, archive/restore, resources and scoped regressions.

Resource observations are local Node/SQLite/fake-R2 evidence only. Fake-clock qualification validates a future driver contract; real-cloud throughput, occupancy, latency and cost remain Package B.

Exactly one local successor commit follows successful validation. SHA/tree/parent and complete-history bundle identity are recorded outside the commit in the recovery ZIP manifest. The original reviewed recovery ZIP remains preserved. No push, PR, merge, deploy, cloud provisioning, Production access, Package B/C execution or Daybreak occurred.
