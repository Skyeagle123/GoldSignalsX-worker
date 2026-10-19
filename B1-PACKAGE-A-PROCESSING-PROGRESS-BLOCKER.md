# PA-PROCESSING-REVIEW-01: preserved, uncommitted remediation draft

Implementation stopped under the Owner's architecture-stop rule. No candidate,
commit, push, migration, cloud access or deployment was made.

The draft separates versioned scan-prefix identity from fresh completion authority.
Only the market revision is masked for prefix compatibility. Source/ownership,
lifecycle, epoch, pending/conflict flags and versions remain exact. Admission
revisions detect same-clock backfills; immutable ready rows/chunks, deletion
and pinned archive boundaries protect the historical prefix. A RESULT digest
binds the checkpoint, fold, cursor and rolling prefix digest. Current market
revision remains in every original completion fence. The existing schema is used.
Corrections, deletion, archive-boundary, lifecycle and ownership changes restart
progress. This is a draft contract, not a reviewed or validated release.

Intermediate direct and scheduled continuing-arrival cases pass. The exact
12+1 schedule advances 8,13,14,15,16,17; the actual scheduled Worker advances
8,16,24,32,40,48,56,64,72,80,88,91 with eleven continuing arrivals. Development
failures and counts are preserved explicitly in the evidence; no complete
acceptance is claimed for the final stopped worktree.

## New blocker: PA-PROCESSING-EXEC-02 (HIGH)

Accepted prepareMarketEvidence emits sixteen valid records for 2,400 ticks. Every
record retains the same export `from`. Accepted processing orders records by
created_at (=from), then hash-derived order_key. That is not chronological sample
ordering. In the reproduction, the earliest sample's packet sorts thirteenth.
The unchanged incremental reducer skips ticks earlier than processedThrough.
The reviewed processing function reports COMPLETE with MFE 5742 rather than 5900.
The draft's conservative observation-order guard prevents that reuse, but alternates
between eight-record progress and reordering PENDING under a stable input boundary.
Removing the guard would silently lose observations; repeated restart is not a
liveness solution. No general sample-order/replay strategy was implemented.

An Owner/Astra decision is required on a bounded sample-safe processing frontier
or overlap/replay mechanism for differently ordered export containers. This is
beyond changing checkpoint compatibility. Preserve all final completion, binding,
epoch, correction and eviction fences; do not modify the lifecycle/outcome reducer
without an explicit decision. Do not treat the preserved draft as accepted code.

See evidence/package-a-processing-progress-blocked/ordering-blocker.log for the
actual producer, reviewed-function control and draft control outputs. The preserved
sample-alias and Package-B worktrees remain untouched.
