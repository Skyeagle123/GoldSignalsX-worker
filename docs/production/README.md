# Production isolation and mutation safety — review candidate

This is offline infrastructure preparation, not authorization to deploy, migrate
credentials, activate environments, publish Sample-Alias or resume Package-B.
Baseline: `5820a9c7fb10f6d1e6c9c944a3490edc36da9c48`, tree
`6e18bee7090849efbf25b64867723a74a6acc92c`. Only the deployment and BLS
workflows are changed among existing tracked files. Worker, Engine, bindings,
configuration, SQL, publication infrastructure, generator and smoke bytes remain
unchanged. No real Cloudflare credential is used in offline validation.

## Workflow and four trust boundaries

1. Reviewed protected main and repository variables constrain initial dispatch.
   Repository ID 1085006449, Owner ID 125151458, exact source and workflow SHA,
   main ref, manual event and attempt 1 are required. A branch name alone is
   insufficient. New source revisions, including BLS merges, require renewed
   review and explicit source-pin authorization.
2. Prepare has no Production environment or credential references. It installs
   the integrity-locked graph with `npm ci --ignore-scripts`, checks exact
   Node/npm identities, applies the exact-byte toolchain patch and captures
   upstream packaging metadata/modules by a secret-free dry run. Its temporary
   capture spy is removed before the deployment artifact is made.
3. Deploy requests `goldsignalsx-production` with normal deployment history.
   Its two credential references exclusively use `GSX_PRODUCTION_CF_API_TOKEN`
   and `GSX_PRODUCTION_CF_ACCOUNT_ID`. Old repository secrets are never a
   fallback. The trusted process maps those supplied values to Wrangler's
   internal environment names in a freshly constructed environment; this is
   not a read of either legacy Repository Secret. No publication App is used.
4. Authentication of a run-bound GitHub deployment review remains distinct from
   artifact integrity and from Cloudflare's eventual mutation outcome.
   A digest is not an independent signature. The authenticated reviewer record
   supplies identity; the digest binds exactly what that reviewer authorized.

`GSX_PRODUCTION_ENABLED` must remain unset/false until independent acceptance
and live setup. Repository-scoped initial constraints are
`GSX_PRODUCTION_REVIEWED_SHA`, `GSX_PRODUCTION_ENVIRONMENT_ID`, and
`GSX_PRODUCTION_ACCOUNT`. They are not freshly reread authorization variables.
The same-run artifact ID is a trusted job output, checked against the Actions
artifact metadata. Archive bytes, inventory, policy and every executable are
verified before use. Extraction rejects links, duplicate paths, traversal and
oversized content. There is no installation/download/repair in the credential-
bearing verifier or CLI; the preceding pinned artifact action transfers only
that same-run prepared artifact.

## Authorization manifest and approval

`gsx-production-authorization-v1` has an exact field set (see `authorization.py`).
Canonical JSON is UTF-8 with lexically sorted keys and no insignificant spaces;
duplicates, unknown fields, floats/non-finite constants and inconsistent types
are rejected. Bindings include repository/Owner/workflow/source identities,
run/event/attempt, environment numeric identity, account/Worker, all source,
configuration, artifact, policy and toolchain digests, exact Production baseline,
rollback version, run creation and deadline. The manifest does not contain its
own digest. Owner must inspect the complete same-run manifest and comment:

`GSX-PRODUCTION-APPROVE v1 sha256:<64-lowercase-hex-digest>`

Exactly one relevant authenticated approved review must match Owner login/ID,
environment name/ID and comment. Rejections, duplicate relevant reviews,
ambiguous jobs, inaccessible API results or incomplete pagination fail closed.
Run and unique attempt-specific Prepare/Deploy jobs are checked; numeric REST
job ID and `job.check_run_id` are cross-checked via the check-run URL. APIs use
only contents/actions read from the normal GITHUB_TOKEN. No cached API fallback.

Expiry is fixed at authenticated run.created_at + 15 minutes, including build
and approval time. UTC/monotonic consistency detects clock discontinuities.
Final baseline and GitHub approval/run/job/main/tree checks occur in the same
trusted process. The entire final check segment must finish within 10 seconds;
individual requests have bounded timeouts. It then replaces itself with the
verified Node/CLI, once. No mutable external command handoff follows approval.

GitHub review, cancellation and Cloudflare deployment are NOT atomic. Cancellation
signals block future mutations and produce a non-success exit, but cannot retract
an accepted request. Owner withdrawal means cancel the run, establish termination
and inspect actual Cloudflare state. Approval history does not provide reliable
instantaneous revocation ordering/timestamps; no such guarantee is claimed.
No new run/deployment is permitted after an uncertain result until separate
Owner-authorized inspection and recovery. No automatic retry or rollback.

## Pinned toolchain and precise patch

Wrangler 3.114.17, source `f21ee75d09f24e655574e9bae920585e1a31b15e`.
Exact package integrity, original/patched CLI hashes, official Linux x64 Node
24.19.0 archive/binary hashes and bundled npm 11.17.0 identity are recorded in
`ops/production/toolchain/identities.json`. The initial Work npm 11.9.0 is not the
qualified build npm; validation/build uses the official Node distribution pair.
Lockfile v3 fixes all dependency/platform package versions and SRI values.
No lifecycle installation scripts run. Actual CLI tests establish that the
included Linux x64 esbuild platform binary works without an installer fallback.
A clean installation is required before applying the patch; wrong or already
patched upstream bytes are rejected. No fuzzy patching or version substitution.

The patch changes two compiled function bodies only:
- `performApiFetch` delegates to the trusted request gate.
- `retryOnAPIFailure` invokes its action once.

The gate is preloaded before the upstream CLI. It serializes complete request/
response validation, tracks operation identities, refuses duplicates and seals
itself permanently on any failure. It uses Node's core HTTPS client with
`agent:false`, no redirects, proxy routing, retry agents or OAuth fallback.
It verifies upload metadata and every module's name/MIME/digest against the
captured artifact. Raw multipart boundaries may differ; semantic bytes do not.
A cancellation/failed-gate exit guard prevents upstream's SIGTERM exit-0 behavior
from reporting success. Queued trigger updates cannot escape a failed gate.

Approved mutations for the exact account and `goldsignalsx-worker`:
- version POST, then activation POST and script-settings PATCH; OR legacy PUT;
- optional Workers.dev POST;
- cron PUT.

Each operation can be sent once. Legacy query parameters are exact. Response
status, JSON envelope and required operation result identities/types are checked
before the gate opens for another mutation. Requests cannot switch upload modes.
Routes, domains, resource provisioning, assets, Queues, Workflows, dispatch and
service-environment writes are NOT supported by this narrow profile; fail closed
if the source/configuration would require them. They must not be silently added.

The current Production profile requires the already-applied `v1-gold-feed`
Durable Object migration baseline. Unknown or pending migration state stops
Prepare; it does not repair or reapply a migration. Legacy upload is nevertheless
qualified offline with the unchanged configuration's initial migration metadata.
No live migration or rollback compatibility has been qualified here.

## Offline HTTP qualification

Compile `tests/production/no-network.c` to `/tmp/gsx-no-network` on Linux x64.
The child inherits seccomp forbidding IPv4/IPv6 socket creation and io_uring
setup. Only deliberately passed local socket-pair descriptors and stdio are
inherited. This Work environment forbids Unix listeners/network namespaces, so
the fake server reads actual HTTP request bytes over preconnected local AF_UNIX
socket pairs. There is no external test egress. Independent server parsing
counts method/path/body receipt; synthetic credentials only.

The upstream control retains the exact upstream CLI except for a transport-only
adapter at the same API boundary. Retry/helper/payload/deployment logic remains
upstream. The patched artifact is exercised as the actual CLI, not a reimplemented
helper. Successful metadata, module bytes/MIME, bindings, compatibility,
observability, migration metadata, Workers.dev and cron semantics are compared.
Upstream trigger arrival order is concurrent; comparison matches operation
identities, while patched serialization/order is asserted separately.

Injection covers 429/5xx, malformed/duplicate JSON, redirects, disconnect after
body receipt, simulated commitment with response loss, timeout, cancellation,
duplicate operations, unsupported fallback and pre-send payload substitution.
For every failed mutation there is one received request at most and no later
mutation, including a queued trigger. This proves local client behavior only.
It cannot prove Cloudflare executed a request exactly once or guarantee live
configuration/schema/permission compatibility.

## BLS continuity and protection readiness

Original generator and official-source logic remain byte-identical. Six-hour
cron/manual generation and no-change detection remain. Generation is read-only;
only a separate PR writer has contents/PR write. Only the snapshot path can enter
the generated tree. Fixed automation branch, ordinary fast-forward updates,
no automatic approval/merge, no force and no broad bypass. Diverged automation
history stops visibly until Owner resolves the existing PR/branch; it is not
silently overwritten. This is bounded backpressure, not silent loss of updates.
Enable Actions PR creation in repository settings separately. GitHub may require
Owner approval before Actions-created PR CI runs; do not use a PAT/App workaround.

CODEOWNERS readiness rules cover workflows, trusted scripts, locks, config,
smoke and CODEOWNERS. BEFORE protection activation add an eligible independent
human reviewer with write access to these rules. Owner cannot approve an
Owner-authored PR; the present Owner-only entries cannot satisfy that independent
review requirement. Require PR/code-owner review, dismiss stale approvals,
conversation resolution, no admin/App bypass, no force/deletion. This task does
not configure any setting. Use always-running `Production security offline`
check, not a path-filtered check that can remain pending. Existing Test Worker
behavior is unchanged. BLS PRs also need their normal applicable CI/review gates.

## Owner-only migration and activation gates

1. Independent Astra code/artifact acceptance and separately authorized merge.
2. Configure/verify effective main/publication protections, independent reviewer,
   environment reviewer Skyeagle123 (ID 125151458), main-only/no tags, no admin
   bypass. Document single-Owner deployment approval and allowed self-review.
3. Verify publication/Production environment separation, secret scopes, GITHUB_TOKEN
   approval-history/job access, action compatibility and actual API schemas.
4. Obtain exact current baseline and compatible rollback version through separately
   authorized read-only qualification. Check live Worker, migration tag, bindings,
   routes/triggers/settings, token minimum permissions and latency/deadline margin.
5. Separately authorize credential migration. Drain/inventory legacy workflows
   before T0. Owner securely enters replacement token/account ONLY in the Production
   environment, with token not_before T0 and expires_on <= T0+60 minutes. No token
   administration permission in the deployment credential.
6. Non-deploying qualification first; a Production deployment needs separate Owner
   authorization and the exact digest approval. No new deployment starts after
   minute 40. There is no automatic extension or retry.
7. After confirmed success and rollback readiness, remove both old repository
   secrets and revoke the old token within the 60-minute window. Verify removal,
   revocation and drained legacy runs. Only then may Owner separately extend the
   replacement token lifetime. If qualification fails, expiry approaches or an
   outcome is uncertain: stop mutations, inspect state, revoke/remove the NEW
   credential or allow its hard expiry, retain the old path only if not cut over.
   Isolation is then still BLOCKED; do not start another overlap window automatically.

Repository-level legacy secrets remain accessible to historical workflows until
actually removed. Environment naming alone does not complete isolation. No secret
values may be printed, committed, passed as command arguments or placed in artifacts.
The existing publication infrastructure revision pin must be re-reviewed and
updated through Owner setup after a new main revision; never update it automatically.
This package does not register an App or alter the publication workflow.

## Failure and rollback

A nonzero CLI result does not prove nothing changed. Version upload may succeed
before activation/settings/cron fails. No automatic redeploy/rollback follows.
Record run and operation evidence, terminate the run, inspect current versions,
traffic, settings and schedules without mutation, then obtain separate exact
recovery authorization. Rollback may be incompatible with DO lifecycle/resource
changes and must be proven against the live baseline, never assumed.

## Current validation blocker (not hidden by this package)

The unchanged `test-reliability.mjs:86` currently expects `/health` to be healthy
using fixtures at 2026-09-15, but the route uses the current wall clock. Both this
candidate and an independently extracted exact parent fail with actual
`degraded`, expected `healthy`. The preceding direct helper calls explicitly
supply fixture time and pass. The test is not changed, skipped, marked expected
failure, globally clock-faked or retried to obtain acceptance. All other provided
policy suites must still pass. Full-regression acceptance remains BLOCKED until
separate authorization resolves this baseline fixture issue and a fresh normal
aggregate passes. The Draft PR must remain unmerged and the publisher/deployer
inactive. This blocker is separate from the local retry-safety qualification.

The proposed qualification workflow also runs all five unchanged policy suites
as independent, always-created matrix jobs with fail-fast disabled. Configure
these fixed checks after the baseline fixture blocker is resolved; Reliability
is currently expected to fail normally, not be omitted. Do not activate required
checks or merge while that blocker remains. CI also rebuilds the secret-free
artifact twice and must match the committed golden digest without repair.

Baseline input representation is four lowercase SHA-256 digests of canonical
Cloudflare `result` records (`deployments`, `settings`, `subdomain`, `schedules`)
plus the exact migration tag. Owner must review the underlying records as well
as these digests during live qualification. Rollback binds both version UUID and
canonical record digest and is freshly reread before invocation. The authorization
manifest itself remains integer-only canonical JSON. API response parsing also
supports finite fractional metrics (for example startup_time_ms), while rejecting
duplicate keys, non-finite values and malformed JSON. This avoids rejecting a
successful upload merely because legitimate timing metrics are fractional.

Credential mode is an explicit initial constraint, then bound into the run manifest:
`GSX_PRODUCTION_CREDENTIAL_MODE` must be `migration` or `steady`. Migration mode
requires `GSX_PRODUCTION_MIGRATION_T0` (UTC), rejects dispatch outside the first
40 minutes, and rechecks the T0+40-minute invocation cutoff with final approval.
The separate run-created+15-minute expiry still applies; neither limit extends
the other. Actual replacement-token not_before/expiry and old-token revocation
remain Owner-only live qualification duties, never inferred from these variables.
Steady mode requires the Owner's initial `GSX_PRODUCTION_ISOLATION_READY=true`
attestation and no migration T0, only after independently verified removal of
legacy authority. These are initial constraints, not fresh authorization reads.
No mode/window is extended automatically. Owner must verify the new secret names
are absent at repository/organization scope as well as present in the protected
environment; GitHub same-name secret resolution is not a scope attestation.

Source-derived capture stays outside the reusable toolchain archive, in the
separately approved policy. Thus ordinary reviewed BLS snapshot/application
updates do not change the toolchain's golden digest. Deployment still requires
a fresh exact-source pin and run approval after any new main revision.
The BLS writer leaves an identical pending snapshot proposal untouched rather
than creating redundant commits or resetting review.
