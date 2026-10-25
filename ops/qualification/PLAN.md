# Read-only qualification candidate

The qualification-only workflow is disabled unless GSX_QUALIFICATION_ENABLED=true.
It requires exact repository/Owner/main/attempt identity and separately protected
Production and Publication jobs. It never imports a candidate, installs packages,
invokes Wrangler, writes Git objects/refs, or imports deploy/publication scripts.

Owner setup (not performed): bind GSX_QUALIFICATION_REVIEWED_SHA and
GSX_QUALIFICATION_SOURCE_DIGEST to independently accepted installed source.
The digest is SHA-256 of compact sorted JSON mapping every tracked path to its
SHA-256. The trusted source artifact is verified before API calls. No arbitrary
archive/download input is accepted. This does not qualify live transfer of a
future deployment artifact; that remains a separate activation gate.

Production receives only GSX_QUALIFICATION_CF_READ_TOKEN, a NEW environment-only
metadata-read credential. Never supply the deployment token under this name.
Owner must independently inspect its policy before setting
GSX_QUALIFICATION_READ_POLICY_VERIFIED=true. That flag is an attestation, not
provider evidence of actual permissions. The client can issue only GET to exact
settings, schedules and subdomain endpoints for the fixed account/Worker.
It cannot access D1/KV data APIs. Publication receives only read-only GITHUB_TOKEN,
not the App PEM. Source/run/main/approval identities are checked. Redirects,
unknown paths, duplicate JSON keys, oversize replies and cancelled runs fail.
Responses and credential values never enter logs or artifacts.

The protected Production job retains normal GitHub deployment-history recording;
this metadata record is distinct from a Cloudflare deployment. Publication uses
deployment:false. Live approval/rejection/cancellation and API compatibility are
NOT proven by offline tests. Neither job can authorize real publication/deployment.

App token issuance/revocation cannot be tested by this read-only workflow: those
are credential mutations requiring separate explicit authorization and a separately
reviewed harness. Cloudflare deployment-token write authority, complete live
baseline, rollback compatibility, artifact transfer and freshness latency likewise
remain separate qualification gates. Never dispatch deploy/publish as a probe.

Pagination uses per_page=100, at most 10 pages/1000 records per sweep, 1 MiB per
page (20 MiB aggregate across two sweeps), two identical complete sweeps and no
retries. The existing shared final 10-second deadline applies to every final
request and final authorization. Slow scans fail closed; do not extend the budget.
Two matching reads detect drift but cannot make a provider snapshot atomic.
The normalized baseline digest changes; obtain new reviewed baseline evidence.

The cron gate now requires result.schedules. Its new hash is bound in identities;
the NEW golden artifact is recorded separately from immutable historical evidence.
Two independent builds must match; the full CLI suite uses fake HTTP only.

## Diagnostic remediation candidate (offline only)

Original run 40218686732 remains unexplained. These changes do not prove its cause.
STOP output contains only fixed code and stage values: CONTEXT, SOURCE,
RUN_IDENTITY, MAIN_IDENTITY, APPROVAL, JOB_IDENTITY, HTTP_RESPONSE, METADATA,
RUN_STATE, INTERNAL. Exception text, response bodies, URLs and headers are omitted.

Current-job binding uses GITHUB_JOB (the workflow key), explicit unique job names
in this reviewed workflow, the attempt-specific jobs endpoint, and a separate
GET of the selected numeric job. Require a complete two-job inventory without
pagination, unique numeric IDs, the exact two names, exact run/SHA/branch/workflow
name, an in-progress current job, and consistent runner identity. RUNNER_NAME is
only corroborating evidence; it is NOT assumed unique. The numeric ID is discovered
from authenticated metadata, never inferred from GITHUB_JOB or runner name.
No matrix or reusable workflow is supported by this fixed workflow design.

GitHub documentation consulted:
https://docs.github.com/en/actions/reference/workflows-and-actions/variables
https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-jobs
https://docs.github.com/en/rest/actions/workflow-jobs

The attempt endpoint binds the listing to attempt 1. Detail is cross-checked
against that listing. No undocumented attempt field in job responses is required.
The run ID and integer attempt are checked in authenticated run metadata.
Run state is checked again after approvals. All reads remain bounded GET requests,
with no retry, redirect or pagination. Separate reads cannot provide an atomic
provider snapshot; cancellation after the last check remains a timing limitation.
HTTP failures report both HTTP_RESPONSE and the fixed enclosing stage.

Offline fixtures cannot prove hosted API visibility/timing or the original cause.
Independent review and a separately authorized publication/qualification campaign
remain required. A successor commit needs a newly reviewed SHA and source digest;
none of the repository/environment variables are changed by this preparation.

## Strict identity correction (offline only)

Both authenticated run responses use the same full validator, including installed
workflow ID 396101973, exact path, repository and Owner IDs, run ID, integer
attempt 1, main branch, source SHA, event and nonterminal state. Workflow ID is
bound to the previously retrieved installed workflow metadata; no new permission
is required. JSON identity numbers must be positive integers, never booleans,
floats or numeric strings. Approval environment IDs are strict integers and
duplicate environment IDs within a record are refused. Existing job ID, run ID
and runner ID checks remain strict. No check-run ID is used as authority.
The shared validator rejects missing or contradictory final identity evidence.
Original run failure remains unproven; cancellation after the final check remains
a race. All prior controls and tests remain enabled.
