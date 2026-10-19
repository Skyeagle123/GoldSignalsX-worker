# Preserved processing-ordering draft — not an accepted candidate

Implementation resumed exactly from the verified PA-PROCESSING-EXEC-02 checkpoint.
No commit or push was created. The separate sample-alias and Package-B worktrees
remain unchanged. Migrations, producer, trading and eviction code are unchanged.

## Intermediate implementation

The draft adds pa-sample-selection-v2 / pa-sample-fold-v2. It pins a finite hot
admission boundary separately from the archive root, selects bounded semantic
sample blocks only after a complete enumeration sweep, deduplicates semantic
identities/digests, carries cross-page continuity, delays quality finalization,
and verifies first-hit candidates using accepted interval/sequence comparisons.
The selector is bounded by 64 descriptors AND 16 KiB; the existing 60KB durable
checkpoint and row limits remain. Later finite deltas extend compatible work;
new earlier samples replay the captured snapshot. Current completion authority
and evaluation acknowledgement retain the full fresh market revision fences.

The draft also contains an intermediate bounded archive-root verifier for the
sample-processing stage. It retains root checkpoint/receipt, definitions,
package closure/counts, object/byte budgets, record digest and semantic checks.
It does NOT establish the required whole-invocation eight-record bound before
canonical binding resolution. It is unfinished, unaccepted work.

## New blocker: PA-PROCESSING-EXEC-03 (HIGH)

A genuine verified publication containing 59 records, including 12 MARKET_EXPORT
records, has 81 package objects and 2,225,978 package bytes. It fits the accepted
archive contract. After archive-only restore, processCapturedOutcome first calls
resolveProcessingSubject. Its archived decision/creation/scope reads use the
accepted archive reader, which verifies the entire package. The probe observed
all twelve market records read/decoded before PROCESSING_BINDING_RESOLVED and
before any canonical pa_processing checkpoint existed.

The sample scanner's eight-record budget starts later and cannot bound those
reads. Installing a post-resolution incremental root proof does not repair that
pre-resolution path. Treating its local counter as a whole-invocation bound
would be false. Skipping accepted root/package checks would weaken verification.

A narrow architecture decision is required for a verification-only archive
priming phase BEFORE canonical subject resolution: its durable progress owner,
namespace, epoch/root/package identity, CAS/lease rules, bounded state placement,
and consumption by binding/lifecycle reads. Such progress must never resolve an
Official binding, validate lifecycle or authorize completion on its own. No new
primer ownership protocol, schema, relaxed budget, or weakened reader was added
after this finding. Implementation stopped under the explicit architecture rule.

## Validation limits

Intermediate: prior binding 60/60, draft progress 33/33, ordering 35/35. The
2400-tick control reproduces old MFE5742 and new MFE5900. Actual scheduled recovery
incorporated 71 continuing arrivals and completed 151 samples. After the bounded
sample-stage archive extension, three archive/restore tests passed. These runs
precede the final stopped worktree acceptance. Full runtime/parity/policy/timing
acceptance was NOT run after the architecture stop. No complete PASS is claimed.

See evidence/package-a-processing-ordering-blocked/ for exact logs, failures,
intermediate successful runs and the pre-dispatch reproduction. The original
processing-progress checkpoint ZIP and all previous recovery artifacts remain.
