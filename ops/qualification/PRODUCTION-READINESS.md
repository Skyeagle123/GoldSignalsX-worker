# Production controller remediation and offline readiness probe

Baseline main: `891da6050bf46718d6b6d2982afece239163e8e9`, tree
`f1dc81773feb5dbafaf8e8df4dea98459207f242`. Accepted live read-only qualification
`40306936416` remains closed. This successor does not diagnose a historical run
or authorize credential migration, hosted qualification, publication or deployment.

## Controller changes

The manifest and authenticated run, approval/environment and Prepare/Deploy job
records require positive exact Python integers for numeric identities. Boolean,
float, string, missing, null and malformed identities fail closed. Runner context
IDs arrive as strings and must have canonical positive decimal syntax before
conversion; API JSON identity values must be integers without conversion.

Both job IDs must exist and all enumerated IDs must be unique. Existing name,
run, attempt, source, status, conclusion and current check-run bindings remain.
After approvals and jobs, the complete authenticated run is fetched and validated
again, using the original shared deadline. Missing conclusion is a refusal.
This reduces a cancellation race; it cannot make GitHub and Cloudflare atomic.

The returned artifact ID must equal the same-run artifact ID supplied by the
pinned Prepare job output. The manifest format is unchanged: it binds artifact,
policy and toolchain bytes, run and source, while the artifact ID is supplied by
the existing workflow output. Returned artifact/run numeric types and an exact
boolean `expired=false` are mandatory. No API response can substitute a missing ID.

## Offline probe and boundaries

Run `python3 -I ops/qualification/deployment_readiness.py` only in a credential-free
offline session. It refuses credential-bearing environment names without reading
their values, executes existing and new tests against the actual validators and
controller, mocks provider/API responses, and replaces the execution boundary.
Socket creation, urllib network access, subprocess launch and OS execution are
blocked around the test suite. Synthetic credentials exist only inside fixtures;
no actual credential store is accessed. Unexpected test output stays in memory;
the probe emits fixed nonsecret status/count messages. A passing probe is not a
production authorization or proof of live GitHub/Cloudflare compatibility.

The original tests remain enabled. The only original test-fixture change adds the
required valid artifact response ID. No workflow, runtime, toolchain, request gate
or read-only qualification-harness byte changes are included.

## Canonicalization and timing contract

Use exactly the installed Python function, without cross-language substitution:

```python
json.dumps(value, ensure_ascii=False, sort_keys=True,
           separators=(',', ':'), allow_nan=False).encode()
```

SHA-256 applies to those UTF-8 bytes. Input parsing rules matter: the manifest and
GitHub API parser reject floats; Cloudflare record parsing allows finite floats.
`1.0`, negative zero, exponent formatting, Unicode key order and normalization
are covered. Redacted historical records cannot reconstruct a full commitment.
Fresh complete Cloudflare records must be parsed and canonicalized in protected
memory; only permitted nonsecret identities and hashes may leave that process.

Four distinct times must not be conflated:

1. Authorization expires at authenticated `run.created_at + 900s`, including build
   and approval wait. It is not a new 15-minute lifetime starting at approval.
2. Final baseline, rollback and GitHub checks share one 10-second monotonic budget.
   The second run read cannot reset or extend it. Equality with a cutoff refuses.
3. Python verifies the limits immediately before its single CLI exec boundary.
4. Node startup, discovery and body preparation consume the same absolute budget.
   The inherited authority pipe carries the original final-check start and deadline,
   run expiry and migration cutoff. The gate checks these immediately before each
   mutation request is created and again before request.end. Every mutation must
   begin within the unchanged final ten-second budget and run/migration authority.

The controller creates a bounded anonymous pipe after full authenticated validation.
Only the read end is inherited across exec; the writer is closed. Its environment
variable is a descriptor locator, not timing authority. The gate requires a FIFO,
reads it once, closes it, removes the locator and freezes validated timing state.
It binds the current PID (unchanged by exec), verified executable path, full manifest
and manifest digest, run/attempt/source, artifact ID/digests, check-run ID and actual
policy bytes. It rejects malformed, extended, contradictory or expired evidence.
The capability's trust comes from the verified controller and clean exec environment,
not from a self-signature. Arbitrary code execution in the trusted runner, or a
same-UID attacker capable of manufacturing the entire launch, is outside this IPC
boundary; no local self-supplied JSON can prove GitHub approval independently.

Timing uses Linux CLOCK_MONOTONIC: Python time.monotonic and pinned Node/libuv hrtime
share that clock across exec. Decimal nanosecond strings avoid JSON integer precision
loss; deadlines are rounded down. Unix wall milliseconds independently check run
expiry, migration cutoff and <=2-second wall/monotonic divergence. Backward clocks,
future origins, deadline equality and incompatible epochs refuse. No deadline is
reset at Node startup. Runtime compatibility must retain the pinned Linux platform.
System suspension and scheduler stalls cannot provide a provider-acceptance deadline:
checks enforce freshness at local pre-initiation/pre-end boundaries, not atomically
at kernel transmission or provider acceptance. A scheduler pause after a check
remains a race. Cancellation, network
flight and provider acceptance are not atomic. A request already sent may commit
after expiry or cancellation; reconcile read-only and never retry automatically.

Tests exercise actual gate code with injected clocks/transport, real controller pipe
handoff, and actual pinned CLI over isolated Unix sockets. The earlier Python-only
startup model remains as historical boundary coverage, not current gate evidence.
No globally persistent exactly-once guarantee is claimed. Existing serialized operation
IDs, sticky failure and no automatic retry remain. A separate process/campaign cannot
be safely restarted after uncertainty without independent provider reconciliation.

## Credential migration: prerequisites, abort and cleanup

All following actions are planning only. Before T0, obtain explicit Owner authority
for the exact transition AND cleanup on success, failure, cancellation or partial
execution. That authority must identify old/new token identities, secret names,
allowed removals/revocations and independent nonmutating verification. Do not
enter an overlap window with cleanup permission still undecided.

Before T0, inventory and drain legacy consumers on historical refs/runs and external
automation; unknown consumers block. Establish serving executable provenance,
D1/DO bidirectional state compatibility, exact Python baseline digests and rollback
authority. Verify environment Owner approval/main-only/no administrator bypass and
secret scope/absence at repository or organization level. No old credential fallback.

The new policy is Editor for the existing individual `goldsignalsx-worker` only,
without account-wide Workers, token administration, zone routes or direct datastore
grants. Effective pinned-Wrangler discovery remains a separate read-only check.
Runtime application secrets and the qualification token remain separate.

T0 is the approved new credential's not-before time. Hard expiry is at or before
T0+60 minutes. No new deployment invocation at or after T0+40 minutes; the run TTL
and final-check budget still apply independently. Do not set steady/isolation-ready
while legacy authority remains. No automatic lifetime extension or second window.

On any failure, drift, missing evidence or uncertain result:

1. Stop new mutations and runs; cancel/establish termination under campaign authority.
   Disable activation flags and independently read them back. Preserve uncertainty.
2. Reconcile actual provider versions/traffic/settings/cron using read-only access.
   A failed/lost response does not prove that no upload or activation occurred.
3. If cutover is confirmed AND rollback readiness is proven, use the preauthorized
   Owner cleanup to remove both legacy repository secrets and revoke the exact old
   provider token by minute 60; independently verify removal and revocation status.
4. If cutover is absent, remove/revoke the NEW authority under that same preapproved
   abort scope or rely on its verified hard expiry. The old path may remain only
   as an explicit temporary incomplete-isolation state, not as a credential fallback.
5. If cutover is uncertain, do not restore an old path or attempt rollback blindly.
   Establish state, then use separately exact recovery authority. New-token expiry
   does not prove legacy revocation. Escalate `SECURITY CLEANUP FAILURE` if overlap,
   credential cleanup, termination or serving state cannot be verified by deadline.
6. Record nonsecret token IDs/status/timestamps, secret-name absence, drained runs,
   flags, unchanged protections and serving identity. Never test revocation with
   an unauthorized write. No automatic credential renewal, retry or reset of T0.

Worker rollback does not restore D1/DO data. Current metadata/ETag is not executable
artifact provenance. These remain separate mandatory activation gates; BLS,
Sample-Alias, Package-B and Measurement work are outside this correction.
