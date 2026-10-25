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
