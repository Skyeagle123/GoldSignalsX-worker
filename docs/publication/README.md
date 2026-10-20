# Exact Git publication infrastructure — Package 1

Status: PREPARATION ONLY. Not installed on main, not enabled, no App or environment
created, no recovery bundle uploaded, no publication attempted. Independent Astra
security review, Owner configuration and live qualification are mandatory gates.

## Purpose and boundaries

The manual `Publish accepted Git bundle` workflow imports an already accepted
complete-history Git bundle and pushes its existing successor commit to
`refs/heads/phase/b1-package-a-processing-binding`. It never creates, signs,
cherry-picks, merges, rebases or replaces an application commit.

The existing application, processing baseline, Sample-Alias checkpoint and
Package-B worktree are outside this infrastructure change. No deployment command,
Cloudflare credential, PAT, workflow dispatch API, CI rerun API, ref-update API or
branch-protection bypass exists in the publisher.

## Three jobs, separate authority

| Job | Inputs and authority | Proves | Does not prove |
| --- | --- | --- | --- |
| Validate | Immutable manifest and staging objects; read-only GitHub token | Bundle bytes, Git object integrity, exact successor, reviewed workflow diff, current infrastructure and expected remote | Approval, publication or CI success |
| Publish | Fresh validation, actual Owner environment approval, exact App installation token | One normal Git fast-forward attempt and post-attempt remote observation | Deployment, unconditional completion after response loss, or CI success |
| Observe | Read-only repository and Actions access | Exact candidate remote and one successful attempt-1 push run of Test Worker with successful required jobs/steps | Permission to push again or resume Package-B by itself |

Each job uses a fresh GitHub-hosted runner, independently reads pinned inputs and
checks trusted infrastructure. No executable output passes between jobs. Actions
checkout is pinned to v4.2.2 commit `11bd71901bbe5b1630ceea73d27597364c9af683`,
checks out only `github.sha`, and does not persist credentials. The candidate is
imported into a fresh **bare** repository and never checked out. Hooks, external
diff/textconv, global/system Git configuration, replace objects, attribute files,
submodule execution and ambient credentials are excluded. Git children have CPU,
address-space and file-size limits. Git must remain patched: parsing untrusted Git
objects still carries a native-parser risk even without script execution.

## Workflow registration and trust

GitHub requires the dispatchable workflow to be present on default branch `main`.
Installation is a separate approved infrastructure PR merge; this package does
not install it. Application publication never moves `main`.

The source must be `refs/heads/main`, the repository numeric ID must be
`1085006449`, and both initial and triggering actor must be Owner `Skyeagle123`
(numeric ID `125151458`). First attempt only. The workflow file SHA, checkout SHA,
manifest infrastructure SHA, Owner-configured infrastructure SHA and fresh remote
main head must all agree. A scheduled BLS update advancing main during an
approval wait safely rejects this run; re-review the new infrastructure revision
and authorize a new run rather than retrying the old one.

All publications share one concurrency group with cancellation disabled. GitHub
concurrency is serialization, not a durable FIFO queue: GitHub may replace a
pending run. Do not submit multiple outstanding requests. A replaced pending run
has no continuing publication authorization.

## Manifest v1 and separate digest

The executable schema is `read_manifest()` in `scripts/publication/publication.py`.
It rejects unknown/missing keys, duplicate JSON keys, invalid types, inconsistent
lengths, paths, ordering, identity or unapproved digest. Maximum JSON input is
16,384 bytes. The workflow has two inputs: `manifest_json` and `manifest_sha256`.
Both are passed as environment data, never interpolated into shell commands.

Required fields:

| Field | Meaning |
| --- | --- |
| format | `gsx-exact-publication-v1` |
| repository / repository_id | Exact approved target identity |
| destination | Exact allowlisted full branch ref |
| candidate_sha / tree_sha / parent_sha | Accepted existing Git objects; exactly one parent |
| expected_remote_head | Must equal the accepted parent |
| bundle_size / bundle_sha256 / bundle_ref | Exact complete v2 bundle and its single advertised branch ref |
| staging_commit | Immutable **transport** commit containing the chunks |
| chunks | Ordered `{index,path,blob_sha,size,sha256}` descriptors |
| infrastructure_sha | Independently reviewed installed main revision |
| workflow_review | Owner numeric ID, evidence reference, exact binary diff digest and approved paths |
| ci | Exactly Test Worker / `.github/workflows/test.yml` / required job `test` |
| manifest_sha256 | SHA-256 of canonical manifest excluding this field |

Canonicalization is Python `json.dumps(object, sort_keys=True,
separators=(',', ':'), ensure_ascii=True).encode()`. The digest must match all
three: embedded digest, dispatch input and Owner-configured
`GSX_APPROVED_MANIFEST_SHA256`. A hash asserts identity, not independent review;
the Owner must approve the actual manifest and workflow diff before configuring it.

The manifest is supplied separately from the staging commit. Embedding its own
staging commit ID inside that commit would create a circular identity dependency.
The infrastructure revision is filled **after installation**, not hardcoded to
the preparation commit. No actual Sample-Alias bundle is part of this package.

Workflow changes are limited to the explicitly reviewed `test.yml` path or no
workflow changes. The verifier recomputes the full binary workflow diff from
parent to candidate and requires its exact SHA-256. New/deleted/other workflow
paths fail closed, as do non-regular file modes. Changing this policy requires a
new infrastructure review, not a manifest workaround. Owner approval of a digest
alone cannot substitute for reading the workflow changes and downstream triggers.

## GitHub-native transport

Later, separately authorized staging can use the GitHub connector's base64 blob
API to transfer bundle chunks, then a tree/commit on a dedicated data-only branch.
Each chunk is at most 131,072 bytes; maximum 256 chunks and 32,000,000 bundle bytes.
Paths are exactly `publication-bundles/<bundle-sha256>/chunk-0000`, etc. The staging
commit must keep the blobs reachable. Do not rely on unreferenced-blob retention.

The runner reads pinned contents membership and exact blob objects, checks raw Git
blob IDs plus chunk SHA-256/length, and checks the final bundle SHA-256/length.
No arbitrary host, URL, redirect, ZIP extraction or remote script is supported.
Every HTTP request has a 30-second timeout, an 8 MB response ceiling, no retry and
no redirect. There are at most 513 package retrieval requests at the current bound
(one commit plus two per chunk); job timeouts impose an additional execution cap.
These are publication-infrastructure limits, unrelated to Measurement's meters.

This repository is public. Authenticated reads do not make the staged bundle
private. Before staging a complete-history bundle, inspect reachable history for
secrets/private material and explicitly authorize public disclosure. Publicly
staged bytes may remain retrievable after branch deletion. Do not promise erasure.

A private staging repository is an architectural option, **not implemented in
v1**. It requires a separately reviewed read-only transfer identity and explicit
repository binding; do not extend the publication App installation or use an
arbitrary URL/PAT. Until separately implemented, confidential transport is blocked.

## One-time Owner setup — not performed by Package 1

1. Independently approve and install this infrastructure through a PR targeting
   main. Protect all main workflow/script changes with required review, no force
   pushes/deletions, and no App/admin bypass. Protect other sensitive refs too.
2. Create a dedicated Owner-owned GitHub App. Repository permissions must be
   exactly Contents write, Workflows write and implicit Metadata read. No account,
   organization, administration, secrets, deployments, PR or Actions-write grants.
   Install only on this repository using selected-repository installation.
3. Create environment `goldsignalsx-publication`. Only Owner is a required reviewer;
   disable administrator bypass; allow only branch main, no tags. With Owner both
   dispatching and approving, leave prevent-self-review OFF; this is single-Owner
   approval, not two-person separation. Two-person policy needs a reviewed actor
   policy change. `deployment: false` avoids a deployment object.
4. Add environment-only secret `GSX_PUBLICATION_APP_PRIVATE_KEY`. Do not use a
   repository/organization secret, PAT or broad connection token. No key value
   belongs in chat, logs, evidence or source. This App key is long-lived and must
   be rotated/revoked under the Owner's credential policy.
5. Configure the nonsecret repository variables below. Numeric IDs must be obtained
   from actual Owner configuration. Leave enablement unset until qualification.
6. Independently verify Production isolation. Move/gate deployment credentials
   through separately authorized Production environment configuration where needed.
   Publication and candidate CI must have no access to Cloudflare/Production
   secrets. Audit all push/workflow_run triggers, GitHub integrations and other
   workflows for deployment side effects. No Production setting is changed here.
7. Protect the destination against force push/deletion without granting App bypass.
   The approved target policy must permit the exact commit fast-forward followed by
   automatic CI. If it mandates PR merge or pre-existing checks, stop rather than
   disable/bypass it. The script checks `protected == true`; detailed protection and
   credential-isolation verification remains an Owner/independent settings gate.
8. Qualify transfer limits, environment approval-history visibility with
   `deployment: false`, effective token permissions, revocation, protection refusal,
   and automatic push CI on separately authorized synthetic infrastructure. API
   unavailability or missing approval history fails closed; it is not auto-approval.
9. After those gates, separately authorize real bundle staging and exact publication.

| Variable | Required value |
| --- | --- |
| GSX_REVIEWED_INFRASTRUCTURE_SHA | Exact installed and reviewed current main SHA |
| GSX_APPROVED_MANIFEST_SHA256 | Owner-approved canonical request digest |
| GSX_PUBLICATION_ENVIRONMENT_ID | Actual protected environment numeric ID |
| GSX_PUBLICATION_APP_ID | Actual dedicated App numeric ID |
| GSX_PUBLICATION_INSTALLATION_ID | Actual selected-repository installation numeric ID |
| GSX_PROTECTIONS_VERIFIED | `true` only after independent settings verification |
| GSX_PRODUCTION_ISOLATION_VERIFIED | `true` only after independent isolation verification |
| GSX_PUBLICATION_ENABLED | `true` only after review/setup/qualification/authorization |

Flags are explicit setup attestations, **not** proof that server settings cannot
later change. Revalidate settings before publication and after any policy change.
The script independently checks the live branch protection flag and actual Owner
approval history, but lacks administration authority to inspect every setting.

## App and credential handling

The Publish job receives the App key only after environment approval. It checks
actual review history for this run, exact environment ID/name, Owner ID and approved
state; no input boolean can manufacture approval. It verifies the App owner and
exact permissions, selected installation context, and issues an installation token
restricted to repository ID 1085006449 with Contents/Workflows write. It validates
the returned scope/expiry and revokes even a usable wrong-scope/expired response.
The API must return the expected repository/scope fields or publication stops.

The token is minted only after repeated independent bundle validation. JWT lifetime
is 5 minutes with 60-second clock allowance; installation token must have more than
5 minutes and no more than 61 minutes remaining. The token is masked with the
runner's masking command, not emitted as a normal log/artifact/output. It is passed
only to the Git/askpass subprocess environment; the helper contains no token.
Git URLs/arguments/configs contain no credential. Cleanup deletes the helper and
revokes the token on success/failure. A hard runner kill may prevent cleanup;
server expiry remains the fallback. Revocation failure fails the job and requires
Owner investigation. There is no automatic issuance/push/revocation retry.

Rotate the App key using an approved replacement, validate its fingerprint and
configuration, then revoke the old key. Suspected compromise requires immediate
App/key/token revocation and branch/workflow audit. No organization-wide credential
or Production token is part of this protocol.

## Single push and uncertain results

Fresh context, approval and expected remote checks run before the sole push. Git
transport also compares the exact advertised head. The command is fixed HTTPS
remote plus `candidate_sha:destination`; never `--force`, `--force-with-lease`,
mirror, wildcard, deletion, tags or fallback. Git receive rejects a racing update.
Force/deletion prohibitions are required so another writer cannot rewind the
branch between observations. There is at most one push call per authorized run.

After any push return/exception, read remote state and revoke; never push again.
If remote equals candidate but the response failed, report uncertainty, observe CI
read-only, and keep the overall workflow failed for investigation. If remote still
equals parent, require fresh Owner authorization before another run. Any different
head stops processing. Duplicate publication never mints a token or pushes; a
validation job finding the candidate already remote currently stops for explicit
read-only inspection rather than accepting earlier CI as a new publication.

The Observe job can run after a failed Publish job, and performs bounded read
polling only (120 polls, 15 seconds, within a 32-minute job limit). It requires
exact remote SHA, Test Worker workflow ID/path/name, event push, attempt 1,
candidate/branch/repository, creation after this publication workflow began,
one unambiguous matching run, success of every job, required job `test`, and
successful required steps. Manual runs, reruns, skipped jobs/steps and stale runs
cannot satisfy the gate. A pending/missing run remains incomplete; no CI rerun,
workflow dispatch or publication retry is issued.

## First accepted application tuple

Candidate `3ff53ca955366b88ebce0ca0278c3a884312020a`;
tree `6451b596079eee51c10532981d1f63fa49506552`;
sole parent `7118403761e084ea9f6e0c623766ce14168a7854`.
Bundle is 1,791,286 bytes, SHA-256
`7d66ba9a56b4fd249a0af56bbab64343a7800a6a810acbece239d49552f74912`.
Recovery ZIP remains separate: SHA-256
`a0e2916662448312d9cab77bc66d6a1dc4d9d50641c75fdb05ee7db9df5b0dc9`.
This package contains no bytes from that bundle or ZIP.

## Offline validation and remaining gates

Run `python3 -B -m unittest discover -s tests/publication -p 'test_*.py' -v`.
Tests require Python 3.12+, Git, OpenSSL, Linux resource limits and PyYAML for the
workflow structure assertion. The publisher itself has no third-party Python
dependency. Tests use synthetic history in temporary directories and mocked HTTP,
tokens and push calls; no actual publication/dispatch/credential creation occurs.

Tests cover manifest and Git provenance, chunk integrity/membership, workflow-diff
commitment, inert malicious candidate scripts/attributes/submodules, approval,
context/revision changes, exact token scope/revocation, single push/response loss,
credential isolation and automatic CI identity/failure gates. These tests do not
prove live GitHub protection enforcement or an App-authenticated automatic CI run.
Those require Packages 2–3 and separate execution authorization.

Implementation is reviewable infrastructure, not approval to install, stage the
real recovery bundle, publish Sample-Alias, deploy or resume Package-B.

References: GitHub documentation for workflow_dispatch default-branch registration,
Git blobs, GitHub App Git access/Workflows permissions, installation token scope,
environment approval/history, deployment:false, and GITHUB_TOKEN push suppression;
Git git-bundle documentation for complete-history import and verification.
