#!/usr/bin/env python3
"""Exact-object publisher. Standard library only; never execute imported code.

Only CLI 'publish' can mint/revoke an App token or invoke one Git push.
All other network operations are bounded, read-only GitHub API calls.
"""
import base64
import contextlib
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

REPOSITORY = "Skyeagle123/GoldSignalsX-worker"
REPOSITORY_ID = 1085006449
OWNER_ID = 125151458
DESTINATIONS = {"refs/heads/phase/b1-package-a-processing-binding"}
ENVIRONMENT = "goldsignalsx-publication"
WORKFLOW = ".github/workflows/publish-accepted-bundle.yml"
CI_PATH = ".github/workflows/test.yml"
FORMAT = "gsx-exact-publication-v1"
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
MAX_MANIFEST = 16384
MAX_BUNDLE = 32_000_000
MAX_CHUNK = 131072
MAX_CHUNKS = 256
MAX_API = 8_000_000
PERMISSIONS = {"contents": "write", "workflows": "write", "metadata": "read"}


class Refused(Exception):
    """Messages are fixed codes; do not attach API bodies, Git output or secrets."""


def require(condition, code):
    if not condition:
        raise Refused(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def integer(value, low, high):
    return type(value) is int and low <= value <= high


def exact_keys(value, keys):
    require(type(value) is dict and set(value) == set(keys.split()), "unexpected_fields")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def parse_json(raw):
    try:
        return json.loads(raw, object_pairs_hook=unique_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(Refused("invalid_json")))
    except (ValueError, TypeError, RecursionError):
        raise Refused("invalid_json") from None


def manifest_digest(manifest):
    return sha256(canonical({k: v for k, v in manifest.items() if k != "manifest_sha256"}))


def read_manifest(raw, supplied_digest, approved_digest):
    require(type(raw) is str and len(raw.encode()) <= MAX_MANIFEST, "manifest_size")
    m = parse_json(raw)
    exact_keys(m, "format repository repository_id destination candidate_sha tree_sha parent_sha "
               "expected_remote_head bundle_size bundle_sha256 bundle_ref staging_commit "
               "chunks infrastructure_sha workflow_review ci manifest_sha256")
    require(m["format"] == FORMAT, "manifest_version")
    require(m["repository"] == REPOSITORY and m["repository_id"] == REPOSITORY_ID,
            "repository_mismatch")
    require(m["destination"] in DESTINATIONS, "destination_not_allowed")
    for key in ("candidate_sha", "tree_sha", "parent_sha", "expected_remote_head",
                "staging_commit", "infrastructure_sha"):
        require(type(m[key]) is str and SHA.fullmatch(m[key]), "invalid_git_identity")
    require(m["parent_sha"] == m["expected_remote_head"] != m["candidate_sha"], "parent_boundary")
    require(integer(m["bundle_size"], 1, MAX_BUNDLE), "bundle_size")
    require(type(m["bundle_sha256"]) is str and DIGEST.fullmatch(m["bundle_sha256"]), "bundle_digest")
    require(type(m["bundle_ref"]) is str and re.fullmatch(r"refs/heads/[A-Za-z0-9_/-]{1,150}",
                                                       m["bundle_ref"]), "bundle_ref")
    chunks = m["chunks"]
    require(type(chunks) is list and 1 <= len(chunks) <= MAX_CHUNKS, "chunk_count")
    total = 0
    for index, chunk in enumerate(chunks):
        exact_keys(chunk, "index path blob_sha size sha256")
        require(chunk["index"] == index and type(chunk["index"]) is int, "chunk_order")
        require(chunk["path"] == f"publication-bundles/{m['bundle_sha256']}/chunk-{index:04d}",
                "chunk_path")
        require(type(chunk["blob_sha"]) is str and SHA.fullmatch(chunk["blob_sha"]), "chunk_blob")
        require(type(chunk["sha256"]) is str and DIGEST.fullmatch(chunk["sha256"]), "chunk_digest")
        require(integer(chunk["size"], 1, MAX_CHUNK), "chunk_size")
        total += chunk["size"]
    require(total == m["bundle_size"], "chunk_total")
    review = m["workflow_review"]
    exact_keys(review, "reviewer_id reference diff_sha256 allowed_paths")
    require(review["reviewer_id"] == OWNER_ID and type(review["reviewer_id"]) is int,
            "workflow_review_owner")
    require(type(review["reference"]) is str and
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 /_.:#-]{0,159}", review["reference"]),
            "workflow_review_reference")
    require(type(review["diff_sha256"]) is str and DIGEST.fullmatch(review["diff_sha256"]),
            "workflow_review_digest")
    require(type(review["allowed_paths"]) is list and
            review["allowed_paths"] in ([], [CI_PATH]), "workflow_paths_not_allowed")
    require(m["ci"] == {"name": "Test Worker", "path": CI_PATH, "required_jobs": ["test"]},
            "ci_contract")
    digest = manifest_digest(m)
    require(type(supplied_digest) is str and DIGEST.fullmatch(supplied_digest), "manifest_digest")
    require(digest == m["manifest_sha256"] == supplied_digest == approved_digest,
            "manifest_not_owner_pinned")
    return m


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise Refused("http_redirect_refused")


class GitHub:
    def __init__(self, token):
        require(type(token) is str and 10 <= len(token) <= 8192 and not re.search(r"\s", token),
                "missing_or_invalid_read_credential")
        self.token = token
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def call(self, path, method="GET", body=None, token=None):
        require(path.startswith("/") and not path.startswith("//") and
                "\r" not in path and "\n" not in path and "#" not in path, "api_path")
        # Only credential issuance and revocation are non-GET operations.
        require(method == "GET" or
                (method == "POST" and re.fullmatch(r"/app/installations/[0-9]+/access_tokens", path)) or
                (method == "DELETE" and path == "/installation/token"), "api_write_not_allowed")
        data = canonical(body) if body is not None else None
        request = urllib.request.Request("https://api.github.com" + path, data=data, method=method,
            headers={"Authorization": "Bearer " + (token or self.token),
                     "Accept": "application/vnd.github+json", "Content-Type": "application/json",
                     "X-GitHub-Api-Version": "2022-11-28", "User-Agent": FORMAT})
        try:
            with self.opener.open(request, timeout=30) as response:
                raw = response.read(MAX_API + 1)
                require(len(raw) <= MAX_API, "api_response_size")
                return parse_json(raw) if raw else None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise Refused("github_request_failed") from None

    def repo(self, suffix):
        return self.call(f"/repos/{REPOSITORY}/{suffix}")


def head(api, branch):
    # An exact Git ref GET, never a branch-name search or first result.
    require(branch == "main" or "refs/heads/" + branch in DESTINATIONS, "head_ref")
    result = api.repo("git/ref/heads/" + urllib.parse.quote(branch, safe="/"))
    require(result.get("ref") == "refs/heads/" + branch and
            result.get("object", {}).get("type") == "commit" and
            SHA.fullmatch(result["object"]["sha"]), "remote_ref_invalid")
    return result["object"]["sha"]


def context(api, m, env):
    expected = {"GITHUB_REPOSITORY": REPOSITORY, "GITHUB_REPOSITORY_ID": str(REPOSITORY_ID),
                "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch",
                "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ACTOR_ID": str(OWNER_ID),
                "GITHUB_TRIGGERING_ACTOR": "Skyeagle123", "GSX_PUBLICATION_ENABLED": "true",
                "GSX_PROTECTIONS_VERIFIED": "true", "GSX_PRODUCTION_ISOLATION_VERIFIED": "true"}
    for key, value in expected.items():
        require(env.get(key) == value, "untrusted_execution_context")
    require(env.get("GITHUB_SHA") == m["infrastructure_sha"] ==
            env.get("GSX_REVIEWED_INFRASTRUCTURE_SHA"), "stale_infrastructure")
    require(env.get("GITHUB_WORKFLOW_REF") ==
            f"{REPOSITORY}/{WORKFLOW}@refs/heads/main", "workflow_source")
    require(env.get("GITHUB_WORKFLOW_SHA") == m["infrastructure_sha"], "workflow_revision")
    require(head(api, "main") == m["infrastructure_sha"], "main_advanced")
    run_id = env.get("GITHUB_RUN_ID", "")
    require(re.fullmatch(r"[1-9][0-9]{0,19}", run_id), "run_identity")
    run = api.repo(f"actions/runs/{run_id}")
    require(run.get("head_sha") == m["infrastructure_sha"] and run.get("head_branch") == "main" and
            run.get("event") == "workflow_dispatch" and run.get("run_attempt") == 1 and
            run.get("actor", {}).get("id") == OWNER_ID and
            run.get("triggering_actor", {}).get("id") == OWNER_ID and
            run.get("path") == WORKFLOW and run.get("repository", {}).get("id") == REPOSITORY_ID,
            "run_context_mismatch")
    return run


def approval(api, env):
    eid = env.get("GSX_PUBLICATION_ENVIRONMENT_ID", "")
    require(re.fullmatch(r"[1-9][0-9]{0,19}", eid), "environment_not_configured")
    reviews = api.repo(f"actions/runs/{env['GITHUB_RUN_ID']}/approvals")
    require(type(reviews) is list and len(reviews) <= 100, "approval_response")
    relevant = [r for r in reviews if any(e.get("id") == int(eid) and e.get("name") == ENVIRONMENT
                                        for e in r.get("environments", []))]
    require(relevant and all(r.get("state") == "approved" and
                            r.get("user", {}).get("id") == OWNER_ID for r in relevant),
            "owner_approval_missing")


def remote_boundary(api, m, allow_published=False):
    branch = m["destination"].removeprefix("refs/heads/")
    current = head(api, branch)
    if allow_published and current == m["candidate_sha"]:
        return "already_published"
    require(current == m["expected_remote_head"], "unexpected_remote_head")
    protection = api.repo("branches/" + urllib.parse.quote(branch, safe=""))
    require(protection.get("protected") is True, "target_not_protected")
    return "ready"


def retrieve_bundle(api, m, path):
    # Pinned commit -> exact path -> blob identity, independently for each bounded chunk.
    commit = api.repo("git/commits/" + m["staging_commit"])
    require(commit.get("sha") == m["staging_commit"], "staging_commit")
    total = 0
    digest = hashlib.sha256()
    with path.open("xb") as output:
        for chunk in m["chunks"]:
            entry = api.repo("contents/" + chunk["path"] + "?ref=" + m["staging_commit"])
            require(entry.get("type") == "file" and entry.get("path") == chunk["path"] and
                    entry.get("sha") == chunk["blob_sha"] and entry.get("size") == chunk["size"],
                    "chunk_membership")
            blob = api.repo("git/blobs/" + chunk["blob_sha"])
            require(blob.get("sha") == chunk["blob_sha"] and blob.get("encoding") == "base64" and
                    blob.get("size") == chunk["size"] and type(blob.get("content")) is str,
                    "chunk_object")
            require(len(blob["content"]) <= MAX_CHUNK * 2, "chunk_encoded_size")
            try:
                data = base64.b64decode(blob["content"].replace("\n", ""), validate=True)
            except ValueError:
                raise Refused("chunk_encoding") from None
            git_digest = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
            require(len(data) == chunk["size"] and sha256(data) == chunk["sha256"] and
                    git_digest == chunk["blob_sha"], "chunk_integrity")
            total += len(data)
            digest.update(data)
            output.write(data)
    require(total == m["bundle_size"] and digest.hexdigest() == m["bundle_sha256"], "bundle_integrity")


class IsolatedGit:
    def __init__(self, root):
        self.root = Path(root)
        self.repo = self.root / "objects.git"
        self.env = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C",
                    "HOME": str(self.root), "GIT_CONFIG_NOSYSTEM": "1",
                    "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_NO_REPLACE_OBJECTS": "1",
                    "GIT_TERMINAL_PROMPT": "0", "GIT_ATTR_NOSYSTEM": "1"}
        self.options = ["git", "-c", "core.hooksPath=/dev/null", "-c", "protocol.allow=never",
                        "-c", "credential.helper=", "-c", "core.attributesFile=/dev/null"]
        self.run(["init", "--bare", "--template=", str(self.repo)], cwd=self.root)

    def run(self, args, cwd=None, extra_env=None, https=False, file=False, check=True, timeout=120):
        cmd = self.options + (["-c", "protocol.https.allow=always"] if https else [])
        cmd += (["-c", "protocol.file.allow=always"] if file else []) + args
        try:
            result = subprocess.run(cmd, cwd=cwd or self.repo, env=self.env | (extra_env or {}),
                                    capture_output=True, timeout=timeout, check=False,
                                    preexec_fn=git_limits)
        except (OSError, subprocess.TimeoutExpired):
            raise Refused("git_execution_failed") from None
        require(not check or result.returncode == 0, "git_verification_failed")
        return result

    def text(self, args):
        return self.run(args).stdout.decode("utf-8").strip()

    def verify(self, bundle, m):
        # Complete v2 bundles only. No prerequisite graph or user-controlled ref installation.
        with bundle.open("rb") as stream:
            header = bytearray()
            while not header.endswith(b"\n\n") and len(header) < 4096:
                byte = stream.read(1)
                require(byte, "bundle_header")
                header.extend(byte)
        expected = f"# v2 git bundle\n{m['candidate_sha']} {m['bundle_ref']}\n\n".encode()
        require(bytes(header) == expected, "bundle_header_or_prerequisites")
        self.run(["bundle", "verify", str(bundle)])
        self.run(["fetch", "--no-tags", "--no-write-fetch-head", str(bundle),
                  m["bundle_ref"] + ":refs/validation/candidate"], file=True)
        self.run(["fsck", "--full", "--strict", "--no-reflogs"])
        packs = list((self.repo / "objects/pack").glob("*.idx"))
        require(len(packs) <= 8, "pack_count")
        for pack in packs:
            self.run(["verify-pack", str(pack)])
        require(self.text(["rev-parse", "refs/validation/candidate"]) == m["candidate_sha"],
                "candidate_mismatch")
        require(self.text(["cat-file", "-t", m["candidate_sha"]]) == "commit", "candidate_type")
        require(self.text(["show", "-s", "--format=%T", m["candidate_sha"]]) == m["tree_sha"],
                "tree_mismatch")
        require(self.text(["show", "-s", "--format=%P", m["candidate_sha"]]) == m["parent_sha"],
                "sole_parent_mismatch")
        self.run(["merge-base", "--is-ancestor", m["parent_sha"], m["candidate_sha"]])
        require(self.text(["rev-list", "--count", m["parent_sha"] + ".." + m["candidate_sha"]]) == "1",
                "successor_count")
        diff_args = ["diff", "--no-ext-diff", "--no-textconv", "--binary", "--no-renames",
                     m["parent_sha"], m["candidate_sha"], "--", ".github/workflows/"]
        delta = self.run(diff_args).stdout
        names = self.run(["diff", "--name-only", "-z", "--no-renames", m["parent_sha"],
                          m["candidate_sha"], "--", ".github/workflows/"]).stdout
        paths = sorted(n.decode() for n in names.split(b"\0") if n)
        require(paths == m["workflow_review"]["allowed_paths"] and
                sha256(delta) == m["workflow_review"]["diff_sha256"], "unreviewed_workflow_diff")
        for path in paths:
            entry = self.text(["ls-tree", m["candidate_sha"], "--", path])
            require(entry.startswith("100644 blob "), "workflow_removed_or_unsafe_mode")

    def push_once(self, m, token):
        # Token is inherited only by Git/askpass; never written to config or command arguments.
        helper = self.root / "askpass.py"
        helper.write_text("#!/usr/bin/python3\nimport os,sys\n"
                          "print('x-access-token' if 'Username' in sys.argv[1] "
                          "else os.environ['GSX_EPHEMERAL_GIT_PASSWORD'])\n")
        helper.chmod(0o700)
        auth = {"GIT_ASKPASS": str(helper), "GSX_EPHEMERAL_GIT_PASSWORD": token}
        remote = "https://github.com/" + REPOSITORY + ".git"
        try:
            # Transport-level preflight also authenticates the Git credential before the sole push.
            advertised = self.run(["ls-remote", "--refs", remote, m["destination"]],
                                  extra_env=auth, https=True).stdout
            require(advertised == f"{m['expected_remote_head']}\t{m['destination']}\n".encode(),
                    "git_remote_head_changed")
            # No force, mirror, tags, deletion, fallback, or retry. Git's receive transaction
            # rejects a racing update; force/deletion prohibitions are an installation prerequisite.
            result = self.run(["push", "--porcelain", "--no-verify", remote,
                               m["candidate_sha"] + ":" + m["destination"]],
                              extra_env=auth, https=True, check=False, timeout=90)
            return result.returncode == 0
        finally:
            helper.unlink(missing_ok=True)


def git_limits():
    # Git parses untrusted object data, even without executing checked-in code.
    # Bound each child on the Linux hosted runner; failure cannot authorize a push.
    resource.setrlimit(resource.RLIMIT_AS, (1_500_000_000, 1_500_000_000))
    resource.setrlimit(resource.RLIMIT_CPU, (90, 90))
    resource.setrlimit(resource.RLIMIT_FSIZE, (256_000_000, 256_000_000))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def parse_time(value):
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, AttributeError):
        raise Refused("invalid_time") from None


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def create_jwt(app_id, private_key):
    require(type(private_key) is str and 100 <= len(private_key) <= 16384 and
            "PRIVATE KEY" in private_key, "app_key_missing_or_invalid")
    now = int(time.time())
    signing = b".".join([b64url(canonical({"alg": "RS256", "typ": "JWT"})),
                         b64url(canonical({"iat": now - 60, "exp": now + 300, "iss": app_id}))])
    with tempfile.TemporaryDirectory(prefix="gsx-app-key-") as temp:
        key = Path(temp) / "key.pem"
        key.touch(mode=0o600)
        key.write_text(private_key)
        try:
            result = subprocess.run(["/usr/bin/openssl", "dgst", "-sha256", "-sign", str(key)],
                input=signing, capture_output=True, timeout=10,
                env={"PATH": "/usr/bin:/bin", "HOME": temp})
            require(result.returncode == 0, "app_signing_failed")
            return (signing + b"." + b64url(result.stdout)).decode()
        except (OSError, subprocess.TimeoutExpired):
            raise Refused("app_signing_failed") from None


@contextlib.contextmanager
def installation_token(api, env, jwt_factory=None):
    jwt_factory = jwt_factory or create_jwt
    app_id = env.get("GSX_PUBLICATION_APP_ID", "")
    installation_id = env.get("GSX_PUBLICATION_INSTALLATION_ID", "")
    require(re.fullmatch(r"[1-9][0-9]{0,19}", app_id) and
            re.fullmatch(r"[1-9][0-9]{0,19}", installation_id), "app_not_configured")
    jwt = jwt_factory(app_id, env.get("GSX_PUBLICATION_APP_PRIVATE_KEY", ""))
    app = api.call("/app", token=jwt)
    require(app.get("id") == int(app_id) and app.get("owner", {}).get("id") == OWNER_ID and
            app.get("permissions") == PERMISSIONS, "app_permissions_or_owner")
    installation = api.call("/app/installations/" + installation_id, token=jwt)
    require(installation.get("app_id") == int(app_id) and
            installation.get("account", {}).get("id") == OWNER_ID and
            installation.get("repository_selection") == "selected" and
            installation.get("permissions") == PERMISSIONS and
            installation.get("suspended_at") is None, "app_installation_scope")
    response = api.call(f"/app/installations/{installation_id}/access_tokens", "POST",
                        {"repository_ids": [REPOSITORY_ID],
                         "permissions": {"contents": "write", "workflows": "write"}}, token=jwt)
    token = response.get("token")
    require(type(token) is str and 10 <= len(token) <= 8192 and not re.search(r"\s", token),
            "installation_credential_invalid")
    try:
        require(response.get("permissions") == PERMISSIONS and
                [r.get("id") for r in response.get("repositories", [])] == [REPOSITORY_ID],
                "installation_token_scope")
        require(300 < parse_time(response.get("expires_at")) - time.time() <= 3660,
                "installation_token_expiry")
        # Documented masking command only; token never enters normal messages/artifacts.
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print("::add-mask::" + token)
        yield token
    finally:
        api.call("/installation/token", "DELETE", token=token)


def publish(api, m, env, git, token_provider=installation_token):
    context(api, m, env)
    approval(api, env)
    if remote_boundary(api, m, allow_published=True) == "already_published":
        return "already_published_no_push"
    # Work is already authenticated before token issuance. Never retry token or push mutations.
    with token_provider(api, env) as token:
        context(api, m, env)
        approval(api, env)
        remote_boundary(api, m)
        success = False
        try:
            success = git.push_once(m, token)
        finally:
            # Even a lost/failed push response must be reconciled by a read, never another write.
            current = head(api, m["destination"].removeprefix("refs/heads/"))
        require(success and current == m["candidate_sha"], "publication_failed_or_uncertain_inspect_remote")
    return "published"


def inspect_ci(api, m, since):
    current = head(api, m["destination"].removeprefix("refs/heads/"))
    require(current == m["candidate_sha"], "candidate_not_remote")
    branch = m["destination"].removeprefix("refs/heads/")
    workflow = api.repo("actions/workflows/test.yml")
    require(workflow.get("name") == "Test Worker" and workflow.get("path") == CI_PATH and
            workflow.get("state") == "active" and integer(workflow.get("id"), 1, 2**63),
            "ci_workflow_identity")
    query = urllib.parse.urlencode({"event": "push", "head_sha": m["candidate_sha"],
                                   "branch": branch, "per_page": 100})
    response = api.repo("actions/runs?" + query)
    require(integer(response.get("total_count"), 0, 100) and
            len(response.get("workflow_runs", [])) == response["total_count"], "ci_run_enumeration")
    runs = [r for r in response["workflow_runs"] if r.get("workflow_id") == workflow["id"]]
    if not runs:
        return {"state": "pending_no_push_run"}
    require(len(runs) == 1, "multiple_ci_runs_inspect")
    run = api.repo(f"actions/runs/{runs[0]['id']}")
    require(run.get("workflow_id") == workflow["id"] and run.get("name") == "Test Worker" and
            run.get("path") == CI_PATH and run.get("event") == "push" and
            run.get("run_attempt") == 1 and run.get("head_sha") == m["candidate_sha"] and
            run.get("head_branch") == branch and run.get("repository", {}).get("id") == REPOSITORY_ID
            and parse_time(run.get("created_at")) >= since, "ci_run_mismatch")
    if run.get("status") != "completed":
        require(run.get("status") in ("queued", "in_progress", "waiting", "pending", "requested"),
                "unexpected_ci_status")
        return {"state": "pending", "run_id": run["id"]}
    require(run.get("conclusion") == "success", "ci_not_success")
    jobs = api.repo(f"actions/runs/{run['id']}/attempts/1/jobs?per_page=100")
    require(integer(jobs.get("total_count"), 1, 100) and
            len(jobs.get("jobs", [])) == jobs["total_count"], "ci_jobs_enumeration")
    require(all(j.get("status") == "completed" and j.get("conclusion") == "success"
                for j in jobs["jobs"]), "ci_jobs_not_success")
    for name in m["ci"]["required_jobs"]:
        matched = [j for j in jobs["jobs"] if j.get("name") == name]
        require(len(matched) == 1 and matched[0].get("steps") and
                all(s.get("status") == "completed" and s.get("conclusion") == "success"
                    for s in matched[0]["steps"]), "required_ci_job_or_step_missing")
    require(head(api, branch) == m["candidate_sha"], "remote_advanced_during_observation")
    return {"state": "success", "run_id": run["id"],
            "url": f"https://github.com/{REPOSITORY}/actions/runs/{run['id']}"}


def summary(value):
    text = json.dumps(value, sort_keys=True)
    print(text)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as stream:
            stream.write("```json\n" + text + "\n```\n")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    require(argv in (["validate"], ["publish"], ["observe"]), "invalid_operation")
    env = dict(os.environ)
    # Never let child commands inherit credentials or arbitrary runner Git configuration.
    os.environ.pop("GSX_PUBLICATION_APP_PRIVATE_KEY", None)
    os.environ.pop("GH_READ_TOKEN", None)
    api = GitHub(env.get("GH_READ_TOKEN", ""))
    m = read_manifest(env.get("INPUT_MANIFEST_JSON", ""), env.get("INPUT_MANIFEST_SHA256", ""),
                      env.get("GSX_APPROVED_MANIFEST_SHA256", ""))
    run = context(api, m, env)
    operation = argv[0]
    if operation == "observe":
        # Read polling only. No retry of publication, dispatch, or CI mutations.
        for _ in range(120):
            result = inspect_ci(api, m, parse_time(run["created_at"]))
            if result["state"] == "success":
                summary(result)
                return
            time.sleep(15)
        summary(result)
        raise Refused("ci_observation_incomplete_no_rerun")
    if operation == "publish":
        approval(api, env)
    status = remote_boundary(api, m, allow_published=operation == "publish")
    if status == "already_published":
        summary({"state": "already_published_no_push", "candidate": m["candidate_sha"]})
        return
    with tempfile.TemporaryDirectory(prefix="gsx-publication-") as temp:
        bundle = Path(temp) / "input.bundle"
        retrieve_bundle(api, m, bundle)
        git = IsolatedGit(temp)
        git.verify(bundle, m)
        # Confirm trusted checkout really is the declared infrastructure commit.
        trusted = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, check=False)
        require(trusted.returncode == 0 and trusted.stdout.decode().strip() == m["infrastructure_sha"],
                "infrastructure_checkout_mismatch")
        context(api, m, env)
        state = "validated"
        if operation == "publish":
            state = publish(api, m, env, git)
        summary({"state": state, "repository": REPOSITORY, "destination": m["destination"],
                 "candidate": m["candidate_sha"], "tree": m["tree_sha"], "parent": m["parent_sha"],
                 "manifest_sha256": m["manifest_sha256"], "bundle_sha256": m["bundle_sha256"],
                 "infrastructure": m["infrastructure_sha"], "workflow_review": m["workflow_review"]})


if __name__ == "__main__":
    try:
        main()
    except Refused as error:
        print("Publication stopped: " + str(error), file=sys.stderr)
        sys.exit(1)
    except Exception:
        # Do not serialize unexpected exceptions: they can contain HTTP headers or credentials.
        print("Publication stopped: unexpected_error_inspect_read_only", file=sys.stderr)
        sys.exit(1)
