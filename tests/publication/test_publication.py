"""Offline security qualification. Synthetic bundles only; all HTTP/pushes mocked."""
import base64
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("publication", ROOT / "scripts/publication/publication.py")
p = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(p)


def seal(m):
    m["manifest_sha256"] = p.manifest_digest(m)
    return m


class Synthetic:
    def __init__(self, root):
        self.root = Path(root)
        source = self.root / "source"
        source.mkdir()
        self.source = source
        self.env = {"PATH": "/usr/bin:/bin", "HOME": str(self.root),
                    "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
                    "GIT_AUTHOR_NAME": "Offline fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                    "GIT_COMMITTER_NAME": "Offline fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
                    "GIT_AUTHOR_DATE": "2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE": "2026-01-01T00:00:00Z"}
        self.git("init", "--template=", "-b", "fixture")
        (source / ".github/workflows").mkdir(parents=True)
        (source / ".github/workflows/test.yml").write_text("name: Test Worker\non: push\n")
        (source / "application.txt").write_text("parent\n")
        self.git("add", ".")
        self.git("commit", "-qm", "synthetic parent")
        self.parent = self.git("rev-parse", "HEAD").strip()
        self.marker = self.root / "CANDIDATE_WAS_EXECUTED"
        # Executable, hooks, attributes/filter driver request, and submodule metadata must stay inert.
        evil = source / "evil.sh"
        evil.write_text("#!/bin/sh\ntouch " + str(self.marker) + "\n")
        evil.chmod(0o755)
        (source / ".gitattributes").write_text("* filter=evil\n")
        (source / ".gitmodules").write_text('[submodule "evil"]\n path = evil\n url = https://invalid.example/x\n')
        (source / ".github/workflows/test.yml").write_text("name: Test Worker\non: [push, workflow_dispatch]\n")
        self.git("add", ".")
        self.git("commit", "-qm", "synthetic accepted successor")
        self.candidate = self.git("rev-parse", "HEAD").strip()
        self.tree = self.git("rev-parse", "HEAD^{tree}").strip()
        self.bundle = self.root / "synthetic.bundle"
        self.git("bundle", "create", str(self.bundle), "refs/heads/fixture")
        self.data = self.bundle.read_bytes()
        chunks = []
        self.blobs = {}
        for index, start in enumerate(range(0, len(self.data), 256)):
            data = self.data[start:start + 256]
            oid = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
            chunks.append({"index": index, "path": f"publication-bundles/{p.sha256(self.data)}/chunk-{index:04d}",
                           "blob_sha": oid, "size": len(data), "sha256": p.sha256(data)})
            self.blobs[oid] = {"sha": oid, "size": len(data), "encoding": "base64",
                               "content": base64.b64encode(data).decode()}
        self.m = seal({"format": p.FORMAT, "repository": p.REPOSITORY, "repository_id": p.REPOSITORY_ID,
                       "destination": next(iter(p.DESTINATIONS)), "candidate_sha": self.candidate,
                       "tree_sha": self.tree, "parent_sha": self.parent, "expected_remote_head": self.parent,
                       "bundle_size": len(self.data), "bundle_sha256": p.sha256(self.data),
                       "bundle_ref": "refs/heads/fixture", "staging_commit": "a" * 40,
                       "infrastructure_sha": "b" * 40, "chunks": chunks,
                       "workflow_review": {"reviewer_id": p.OWNER_ID, "reference": "Offline fixture review",
                           "diff_sha256": p.sha256(self.git_bytes("diff", "--no-ext-diff", "--no-textconv",
                               "--binary", "--no-renames", self.parent, self.candidate, "--", ".github/workflows/")),
                           "allowed_paths": [p.CI_PATH]},
                       "ci": {"name": "Test Worker", "path": p.CI_PATH, "required_jobs": ["test"]}})

    def git_bytes(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.source, env=self.env, stderr=subprocess.DEVNULL)

    def git(self, *args):
        return self.git_bytes(*args).decode()


class API:
    def __init__(self, fixture):
        self.fixture = fixture
        self.m = copy.deepcopy(fixture.m)
        self.target = self.m["parent_sha"]
        self.main = self.m["infrastructure_sha"]
        self.protected = True
        self.calls = []
        self.reviews = [{"state": "approved", "user": {"id": p.OWNER_ID},
                         "environments": [{"id": 77, "name": p.ENVIRONMENT}]}]
        self.run = {"id": 99, "head_sha": self.main, "head_branch": "main", "event": "workflow_dispatch",
                    "run_attempt": 1, "actor": {"id": p.OWNER_ID}, "triggering_actor": {"id": p.OWNER_ID},
                    "path": p.WORKFLOW, "repository": {"id": p.REPOSITORY_ID}, "created_at": "2026-01-01T00:00:00Z"}
        self.ci = {"id": 123, "workflow_id": 456, "name": "Test Worker", "path": p.CI_PATH,
                   "head_sha": self.m["candidate_sha"], "head_branch": self.m["destination"][11:],
                   "event": "push", "run_attempt": 1, "repository": {"id": p.REPOSITORY_ID},
                   "created_at": "2026-01-01T00:01:00Z", "status": "completed", "conclusion": "success"}
        self.ci_runs = [self.ci]
        self.jobs = [{"name": "test", "status": "completed", "conclusion": "success",
                      "steps": [{"name": "Run aggregate", "status": "completed", "conclusion": "success"}]}]
        self.token_response = {"token": "offline-installation-secret", "permissions": p.PERMISSIONS.copy(),
                               "repositories": [{"id": p.REPOSITORY_ID}],
                               "expires_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 3500))}
        self.app_permissions = p.PERMISSIONS.copy()
        self.installation_selection = "selected"

    def call(self, path, method="GET", body=None, token=None):
        self.calls.append((path, method, body))
        if path == "/app":
            return {"id": 5, "owner": {"id": p.OWNER_ID}, "permissions": self.app_permissions}
        if path == "/app/installations/6":
            return {"app_id": 5, "account": {"id": p.OWNER_ID}, "permissions": self.app_permissions,
                    "repository_selection": self.installation_selection, "suspended_at": None}
        if path == "/app/installations/6/access_tokens":
            return self.token_response
        if path == "/installation/token":
            return None
        raise AssertionError(path)

    def repo(self, suffix):
        self.calls.append((suffix, "GET", None))
        if suffix.startswith("git/ref/heads/"):
            branch = suffix.removeprefix("git/ref/heads/")
            return {"ref": "refs/heads/" + branch,
                    "object": {"sha": self.main if branch == "main" else self.target, "type": "commit"}}
        if suffix.startswith("branches/"):
            return {"protected": self.protected}
        if suffix == "actions/runs/99":
            return self.run
        if suffix == "actions/runs/99/approvals":
            return self.reviews
        if suffix == "git/commits/" + self.m["staging_commit"]:
            return {"sha": self.m["staging_commit"]}
        if suffix.startswith("contents/"):
            path = suffix.removeprefix("contents/").split("?")[0]
            c = next(c for c in self.m["chunks"] if c["path"] == path)
            return {"path": path, "type": "file", "sha": c["blob_sha"], "size": c["size"]}
        if suffix.startswith("git/blobs/"):
            return self.fixture.blobs[suffix.removeprefix("git/blobs/")]
        if suffix == "actions/workflows/test.yml":
            return {"id": 456, "name": "Test Worker", "path": p.CI_PATH, "state": "active"}
        if suffix.startswith("actions/runs?"):
            return {"total_count": len(self.ci_runs), "workflow_runs": self.ci_runs}
        if suffix == "actions/runs/123":
            return self.ci
        if suffix == "actions/runs/123/attempts/1/jobs?per_page=100":
            return {"total_count": len(self.jobs), "jobs": self.jobs}
        raise AssertionError(suffix)


def environment(m):
    return {"GITHUB_REPOSITORY": p.REPOSITORY, "GITHUB_REPOSITORY_ID": str(p.REPOSITORY_ID),
            "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_RUN_ATTEMPT": "1", "GITHUB_ACTOR_ID": str(p.OWNER_ID),
            "GITHUB_TRIGGERING_ACTOR": "Skyeagle123", "GSX_PUBLICATION_ENABLED": "true",
            "GSX_PROTECTIONS_VERIFIED": "true", "GSX_PRODUCTION_ISOLATION_VERIFIED": "true",
            "GITHUB_SHA": m["infrastructure_sha"], "GSX_REVIEWED_INFRASTRUCTURE_SHA": m["infrastructure_sha"],
            "GITHUB_WORKFLOW_SHA": m["infrastructure_sha"],
            "GITHUB_WORKFLOW_REF": f"{p.REPOSITORY}/{p.WORKFLOW}@refs/heads/main",
            "GITHUB_RUN_ID": "99", "GSX_PUBLICATION_ENVIRONMENT_ID": "77",
            "GSX_PUBLICATION_APP_ID": "5", "GSX_PUBLICATION_INSTALLATION_ID": "6"}


class PublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="gsx-offline-tests-")
        cls.fixture = Synthetic(cls.temp.name)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.m = copy.deepcopy(self.fixture.m)
        self.api = API(self.fixture)
        self.env = environment(self.m)

    def parse(self, m=None):
        m = self.m if m is None else m
        return p.read_manifest(json.dumps(m), m["manifest_sha256"], m["manifest_sha256"])

    def mutate(self, key, value):
        self.m[key] = value
        seal(self.m)
        with self.assertRaises(p.Refused):
            self.parse()

    def verify(self, m=None):
        with tempfile.TemporaryDirectory(prefix="gsx-verify-test-") as folder:
            git = p.IsolatedGit(folder)
            git.verify(self.fixture.bundle, m or self.m)

    def test_correct_manifest(self):
        self.assertEqual(self.parse(), self.m)

    def test_manifest_tamper(self):
        self.m["bundle_size"] += 1
        with self.assertRaises(p.Refused): self.parse()

    def test_unapproved_digest(self):
        with self.assertRaises(p.Refused):
            p.read_manifest(json.dumps(self.m), self.m["manifest_sha256"], "0" * 64)

    def test_missing_field(self):
        del self.m["tree_sha"]
        with self.assertRaises(p.Refused): self.parse()

    def test_unknown_field(self): self.mutate("execute", "evil.sh")
    def test_wrong_repository(self): self.mutate("repository", "other/repo")
    def test_wrong_repository_id(self): self.mutate("repository_id", 1)
    def test_wrong_destination(self): self.mutate("destination", "refs/heads/main")
    def test_deletion_destination(self): self.mutate("destination", ":refs/heads/main")
    def test_force_destination(self): self.mutate("destination", "+refs/heads/main")
    def test_wrong_parent_boundary(self): self.mutate("expected_remote_head", "0" * 40)
    def test_old_format(self): self.mutate("format", "unknown-v0")
    def test_bad_sha(self): self.mutate("candidate_sha", "HEAD; touch /tmp/evil")
    def test_bundle_limit(self): self.mutate("bundle_size", p.MAX_BUNDLE + 1)
    def test_bundle_ref_option_injection(self): self.mutate("bundle_ref", "--upload-pack=evil")

    def test_duplicate_json_key(self):
        with self.assertRaises(p.Refused):
            self.parse_raw('{"format":"x","format":"y"}')

    def parse_raw(self, value):
        return p.read_manifest(value, "0" * 64, "0" * 64)

    def test_manifest_size(self):
        with self.assertRaises(p.Refused): self.parse_raw(" " * (p.MAX_MANIFEST + 1))

    def test_chunk_order(self):
        self.m["chunks"][0]["index"] = 1
        seal(self.m)
        with self.assertRaises(p.Refused): self.parse()

    def test_chunk_path_escape(self):
        self.m["chunks"][0]["path"] = "../../evil"
        seal(self.m)
        with self.assertRaises(p.Refused): self.parse()

    def test_unreviewed_workflow_path(self):
        self.m["workflow_review"]["allowed_paths"] = [p.WORKFLOW]
        seal(self.m)
        with self.assertRaises(p.Refused): self.parse()

    def test_review_wrong_owner(self):
        self.m["workflow_review"]["reviewer_id"] = 1
        seal(self.m)
        with self.assertRaises(p.Refused): self.parse()

    def test_correct_bundle_and_no_execution(self):
        self.verify()
        self.assertFalse(self.fixture.marker.exists())

    def test_wrong_candidate(self):
        self.m["candidate_sha"] = "0" * 40
        with self.assertRaises(p.Refused): self.verify()

    def test_wrong_tree(self):
        self.m["tree_sha"] = "0" * 40
        with self.assertRaises(p.Refused): self.verify()

    def test_wrong_parent(self):
        self.m["parent_sha"] = "0" * 40
        with self.assertRaises(p.Refused): self.verify()

    def test_workflow_diff_commitment(self):
        self.m["workflow_review"]["diff_sha256"] = "0" * 64
        with self.assertRaises(p.Refused): self.verify()

    def test_workflow_diff_requires_explicit_path_review(self):
        self.m["workflow_review"]["allowed_paths"] = []
        with self.assertRaises(p.Refused): self.verify()

    def test_bundle_prerequisites_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "partial.bundle"
            path.write_bytes(self.fixture.data.replace(b"# v2 git bundle\n", b"# v2 git bundle\n-" + b"0" * 40 + b" prerequisite\n", 1))
            with self.assertRaises(p.Refused): p.IsolatedGit(folder).verify(path, self.m)

    def test_bundle_pack_corruption(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bad.bundle"
            path.write_bytes(self.fixture.data[:-32] + b"x" * 32)
            with self.assertRaises(p.Refused): p.IsolatedGit(folder).verify(path, self.m)

    def test_secure_retrieval(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "input.bundle"
            p.retrieve_bundle(self.api, self.m, path)
            self.assertEqual(path.read_bytes(), self.fixture.data)
            self.assertTrue(all(call[1] == "GET" for call in self.api.calls))

    def test_chunk_tamper(self):
        oid = self.m["chunks"][0]["blob_sha"]
        with patch.dict(self.fixture.blobs, {oid: self.fixture.blobs[oid] | {"content": "eA=="}}):
            with tempfile.TemporaryDirectory() as folder, self.assertRaises(p.Refused):
                p.retrieve_bundle(self.api, self.m, Path(folder) / "bad.bundle")

    def test_staging_membership_mismatch(self):
        self.api.m["chunks"][0]["size"] += 1
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(p.Refused):
            p.retrieve_bundle(self.api, self.m, Path(folder) / "bad.bundle")

    def test_complete_bundle_digest(self):
        self.m["bundle_sha256"] = "0" * 64
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(p.Refused):
            p.retrieve_bundle(self.api, self.m, Path(folder) / "bad.bundle")

    def test_trusted_context(self): self.assertEqual(p.context(self.api, self.m, self.env)["id"], 99)

    def test_rerun_refused(self):
        self.env["GITHUB_RUN_ATTEMPT"] = "2"
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_default_disabled(self):
        self.env.pop("GSX_PUBLICATION_ENABLED")
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_wrong_actor(self):
        self.env["GITHUB_ACTOR_ID"] = "1"
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_candidate_branch_execution_refused(self):
        self.env["GITHUB_REF"] = self.m["destination"]
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_stale_infrastructure(self):
        self.env["GSX_REVIEWED_INFRASTRUCTURE_SHA"] = "0" * 40
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_main_advanced(self):
        self.api.main = "0" * 40
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_workflow_revision_mismatch(self):
        self.env["GITHUB_WORKFLOW_SHA"] = "0" * 40
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_server_run_context_mismatch(self):
        self.api.run["event"] = "push"
        with self.assertRaises(p.Refused): p.context(self.api, self.m, self.env)

    def test_owner_approval(self): p.approval(self.api, self.env)

    def test_missing_approval(self):
        self.api.reviews = []
        with self.assertRaises(p.Refused): p.approval(self.api, self.env)

    def test_wrong_approval_owner(self):
        self.api.reviews[0]["user"]["id"] = 1
        with self.assertRaises(p.Refused): p.approval(self.api, self.env)

    def test_wrong_approval_environment(self):
        self.api.reviews[0]["environments"][0]["name"] = "production"
        with self.assertRaises(p.Refused): p.approval(self.api, self.env)

    def test_rejected_approval(self):
        self.api.reviews[0]["state"] = "rejected"
        with self.assertRaises(p.Refused): p.approval(self.api, self.env)

    def test_unprotected_target(self):
        self.api.protected = False
        with self.assertRaises(p.Refused): p.remote_boundary(self.api, self.m)

    def test_wrong_remote_head(self):
        self.api.target = "0" * 40
        with self.assertRaises(p.Refused): p.remote_boundary(self.api, self.m)

    def token_provider(self):
        @contextlib.contextmanager
        def provider(api, env):
            self.minted += 1
            try: yield "offline-installation-secret"
            finally: self.revoked += 1
        return provider

    def run_publish(self, effect, result=True):
        self.minted = self.revoked = 0
        class Git:
            calls = 0
            def push_once(self, manifest, token):
                self.calls += 1
                effect()
                return result
        git = Git()
        return git, lambda: p.publish(self.api, self.m, self.env, git, self.token_provider())

    def test_one_successful_push(self):
        git, run = self.run_publish(lambda: setattr(self.api, "target", self.m["candidate_sha"]))
        self.assertEqual(run(), "published")
        self.assertEqual((git.calls, self.minted, self.revoked), (1, 1, 1))

    def test_failed_push_no_retry(self):
        git, run = self.run_publish(lambda: None, False)
        with self.assertRaises(p.Refused): run()
        self.assertEqual((git.calls, self.revoked), (1, 1))

    def test_lost_push_response_no_retry(self):
        def effect():
            self.api.target = self.m["candidate_sha"]
            raise p.Refused("git_execution_failed")
        git, run = self.run_publish(effect)
        with self.assertRaises(p.Refused): run()
        self.assertEqual((git.calls, self.revoked), (1, 1))
        self.assertTrue(any(c[0].startswith("git/ref/heads/phase/") for c in self.api.calls))

    def test_nonzero_push_with_changed_remote_stays_uncertain(self):
        git, run = self.run_publish(lambda: setattr(self.api, "target", self.m["candidate_sha"]), False)
        with self.assertRaises(p.Refused): run()
        self.assertEqual(git.calls, 1)

    def test_duplicate_publication_no_token_or_push(self):
        self.api.target = self.m["candidate_sha"]
        git, run = self.run_publish(lambda: self.fail("must not push"))
        self.assertEqual(run(), "already_published_no_push")
        self.assertEqual((git.calls, self.minted), (0, 0))

    def test_missing_approval_never_mints(self):
        self.api.reviews = []
        git, run = self.run_publish(lambda: self.fail("must not push"))
        with self.assertRaises(p.Refused): run()
        self.assertEqual((git.calls, self.minted), (0, 0))

    def test_binding_of_context_rechecked_after_token(self):
        @contextlib.contextmanager
        def provider(api, env):
            api.main = "0" * 40
            yield "offline-installation-secret"
        with self.assertRaises(p.Refused): p.publish(self.api, self.m, self.env, None, provider)

    def test_app_token_exact_scope_and_revocation(self):
        with p.installation_token(self.api, self.env, lambda *_: "offline-jwt") as token:
            self.assertEqual(token, self.api.token_response["token"])
        self.assertEqual([c[1] for c in self.api.calls], ["GET", "GET", "POST", "DELETE"])
        self.assertEqual(self.api.calls[2][2], {"repository_ids": [p.REPOSITORY_ID],
                          "permissions": {"contents": "write", "workflows": "write"}})

    def test_broad_app_refused(self):
        self.api.app_permissions = p.PERMISSIONS | {"administration": "write"}
        with self.assertRaises(p.Refused), p.installation_token(self.api, self.env, lambda *_: "jwt"):
            self.fail("must not mint")
        self.assertFalse(any(c[1] == "POST" for c in self.api.calls))

    def test_install_all_repositories_refused(self):
        self.api.installation_selection = "all"
        with self.assertRaises(p.Refused), p.installation_token(self.api, self.env, lambda *_: "jwt"):
            self.fail("must not mint")

    def test_wrong_token_scope_revoked(self):
        self.api.token_response["repositories"] = [{"id": 1}]
        with self.assertRaises(p.Refused), p.installation_token(self.api, self.env, lambda *_: "jwt"):
            self.fail("must not use token")
        self.assertEqual(self.api.calls[-1][1], "DELETE")

    def test_expired_token_revoked(self):
        self.api.token_response["expires_at"] = "2020-01-01T00:00:00Z"
        with self.assertRaises(p.Refused), p.installation_token(self.api, self.env, lambda *_: "jwt"):
            self.fail("must not use token")
        self.assertEqual(self.api.calls[-1][1], "DELETE")

    def test_missing_private_key(self):
        with self.assertRaises(p.Refused): p.create_jwt("5", "")

    def test_invalid_private_key_no_leak(self):
        secret = "-----BEGIN PRIVATE KEY-----\n" + "secret" * 30
        with self.assertRaises(p.Refused) as caught: p.create_jwt("5", secret)
        self.assertNotIn("secret", str(caught.exception))

    def test_missing_read_token(self):
        with self.assertRaises(p.Refused): p.GitHub("")

    def test_api_redirect_refused(self):
        with self.assertRaises(p.Refused):
            p.NoRedirect().redirect_request(None, None, 302, "", {}, "https://evil.invalid")

    def test_api_forbids_dispatch_rerun_and_ref_mutations(self):
        api = p.GitHub("offline-read-secret")
        for path in ("/repos/x/y/actions/workflows/a/dispatches", "/repos/x/y/actions/runs/1/rerun",
                     "/repos/x/y/git/refs/heads/main"):
            with self.subTest(path=path), self.assertRaises(p.Refused): api.call(path, "POST", {})

    def ci(self):
        self.api.target = self.m["candidate_sha"]
        return p.inspect_ci(self.api, self.m, p.parse_time(self.api.run["created_at"]))

    def test_exact_push_ci_success(self): self.assertEqual(self.ci()["state"], "success")

    def test_missing_ci_pending(self):
        self.api.ci_runs = []
        self.assertEqual(self.ci()["state"], "pending_no_push_run")

    def test_running_ci_pending(self):
        self.api.ci["status"] = "in_progress"
        self.assertEqual(self.ci()["state"], "pending")

    def test_failed_ci(self):
        self.api.ci["conclusion"] = "failure"
        with self.assertRaises(p.Refused): self.ci()

    def test_manual_ci_not_substitute(self):
        self.api.ci["event"] = "workflow_dispatch"
        with self.assertRaises(p.Refused): self.ci()

    def test_rerun_ci_not_substitute(self):
        self.api.ci["run_attempt"] = 2
        with self.assertRaises(p.Refused): self.ci()

    def test_wrong_ci_head(self):
        self.api.ci["head_sha"] = "0" * 40
        with self.assertRaises(p.Refused): self.ci()

    def test_wrong_ci_path(self):
        self.api.ci["path"] = ".github/workflows/other.yml"
        with self.assertRaises(p.Refused): self.ci()

    def test_old_ci_not_current_publication(self):
        self.api.ci["created_at"] = "2025-01-01T00:00:00Z"
        with self.assertRaises(p.Refused): self.ci()

    def test_duplicate_ci_runs_require_inspection(self):
        self.api.ci_runs.append(copy.deepcopy(self.api.ci))
        with self.assertRaises(p.Refused): self.ci()

    def test_skipped_required_job(self):
        self.api.jobs[0]["conclusion"] = "skipped"
        with self.assertRaises(p.Refused): self.ci()

    def test_skipped_required_step(self):
        self.api.jobs[0]["steps"][0]["conclusion"] = "skipped"
        with self.assertRaises(p.Refused): self.ci()

    def test_missing_required_job(self):
        self.api.jobs[0]["name"] = "other"
        with self.assertRaises(p.Refused): self.ci()

    def test_source_workflow_security_contract(self):
        # Parse YAML 1.2-compatible BaseLoader when available in local qualification.
        import yaml
        data = yaml.load((ROOT / p.WORKFLOW).read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(list(data["on"]), ["workflow_dispatch"])
        self.assertEqual(data["permissions"], {"contents": "read", "actions": "read"})
        self.assertEqual(data["concurrency"]["cancel-in-progress"], "false")
        self.assertEqual(set(data["jobs"]), {"validate", "publish", "observe"})
        for name, job in data["jobs"].items():
            checkout = job["steps"][0]
            self.assertRegex(checkout["uses"], r"actions/checkout@[0-9a-f]{40}$")
            self.assertEqual(checkout["with"]["ref"], "${{ github.sha }}")
            self.assertEqual(checkout["with"]["persist-credentials"], "false")
            self.assertNotIn("${{ inputs.", job["steps"][1]["run"])
            if name != "publish": self.assertNotIn("secrets.", json.dumps(job))
        self.assertEqual(data["jobs"]["publish"]["environment"],
                         {"name": p.ENVIRONMENT, "deployment": "false"})

    def test_git_push_command_and_credential_isolation(self):
        with tempfile.TemporaryDirectory() as folder:
            git = p.IsolatedGit(folder)
            observed = []
            def run(args, **kwargs):
                observed.append((args, kwargs))
                if args[0] == "ls-remote":
                    data = f"{self.m['parent_sha']}\t{self.m['destination']}\n".encode()
                else: data = b""
                return subprocess.CompletedProcess(args, 0, data, b"")
            with patch.object(git, "run", side_effect=run):
                self.assertTrue(git.push_once(self.m, "offline-installation-secret"))
            self.assertEqual([x[0][0] for x in observed], ["ls-remote", "push"])
            args = observed[-1][0]
            self.assertEqual(args, ["push", "--porcelain", "--no-verify",
                "https://github.com/" + p.REPOSITORY + ".git",
                self.m["candidate_sha"] + ":" + self.m["destination"]])
            self.assertNotIn("offline-installation-secret", repr(args))
            self.assertFalse((Path(folder) / "askpass.py").exists())
            self.assertNotIn("GSX_EPHEMERAL_GIT_PASSWORD", git.env)
            self.assertNotIn("GSX_PUBLICATION_APP_PRIVATE_KEY", git.env)

    def test_git_racing_remote_prevents_push(self):
        with tempfile.TemporaryDirectory() as folder:
            git = p.IsolatedGit(folder)
            with patch.object(git, "run", return_value=subprocess.CompletedProcess([], 0, b"wrong\n", b"")) as run:
                with self.assertRaises(p.Refused): git.push_once(self.m, "offline-installation-secret")
                self.assertEqual(run.call_count, 1)
            self.assertFalse((Path(folder) / "askpass.py").exists())

    def test_git_uncertain_response_removes_credentials(self):
        with tempfile.TemporaryDirectory() as folder:
            git = p.IsolatedGit(folder)
            advertised = subprocess.CompletedProcess([], 0,
                f"{self.m['parent_sha']}\t{self.m['destination']}\n".encode(), b"")
            with patch.object(git, "run", side_effect=[advertised, p.Refused("git_execution_failed")]) as run:
                with self.assertRaises(p.Refused): git.push_once(self.m, "offline-installation-secret")
                self.assertEqual(run.call_count, 2)
            self.assertFalse((Path(folder) / "askpass.py").exists())

    def test_revocation_on_protected_operation_failure(self):
        with self.assertRaises(p.Refused):
            with p.installation_token(self.api, self.env, lambda *_: "jwt"):
                raise p.Refused("synthetic_failure")
        self.assertEqual(self.api.calls[-1][:2], ("/installation/token", "DELETE"))

    def test_invalid_credentials_cannot_issue_token(self):
        self.env["GSX_PUBLICATION_APP_ID"] = "invalid"
        with self.assertRaises(p.Refused), p.installation_token(self.api, self.env, lambda *_: "jwt"):
            self.fail("must not mint")
        self.assertEqual(self.api.calls, [])

    def test_http_failure_does_not_leak_credentials(self):
        import urllib.error
        api = p.GitHub("offline-read-secret")
        with patch.object(api.opener, "open", side_effect=urllib.error.URLError("offline-read-secret")):
            with self.assertRaises(p.Refused) as caught: api.repo("git/ref/heads/main")
        self.assertNotIn("offline-read-secret", str(caught.exception))

    def test_ci_remote_advance_before_gate(self):
        self.api.target = self.m["candidate_sha"]
        original = self.api.repo
        def repo(path):
            result = original(path)
            if path.endswith("/jobs?per_page=100"): self.api.target = "0" * 40
            return result
        with patch.object(self.api, "repo", side_effect=repo), self.assertRaises(p.Refused):
            p.inspect_ci(self.api, self.m, p.parse_time(self.api.run["created_at"]))

    def cli_environment(self):
        return self.env | {"INPUT_MANIFEST_JSON": json.dumps(self.m),
            "INPUT_MANIFEST_SHA256": self.m["manifest_sha256"],
            "GSX_APPROVED_MANIFEST_SHA256": self.m["manifest_sha256"],
            "GH_READ_TOKEN": "offline-read-secret"}

    def test_cli_validate_complete_composition(self):
        original = subprocess.run
        def run(args, **kwargs):
            if args == ["git", "rev-parse", "HEAD"]:
                return subprocess.CompletedProcess(args, 0, (self.m["infrastructure_sha"] + "\n").encode(), b"")
            return original(args, **kwargs)
        output = io.StringIO()
        with patch.dict(os.environ, self.cli_environment(), clear=True), \
             patch.object(p, "GitHub", return_value=self.api), \
             patch.object(p.subprocess, "run", side_effect=run), contextlib.redirect_stdout(output):
            p.main(["validate"])
        self.assertEqual(json.loads(output.getvalue())["state"], "validated")
        self.assertFalse(self.fixture.marker.exists())
        self.assertTrue(all(c[1] == "GET" for c in self.api.calls))
        self.assertNotIn("offline-read-secret", output.getvalue())

    def test_cli_publish_complete_composition_without_real_writes(self):
        original = subprocess.run
        def run(args, **kwargs):
            if args == ["git", "rev-parse", "HEAD"]:
                return subprocess.CompletedProcess(args, 0, (self.m["infrastructure_sha"] + "\n").encode(), b"")
            return original(args, **kwargs)
        def push(manifest, token):
            self.assertEqual(token, "offline-installation-secret")
            self.api.target = manifest["candidate_sha"]
            return True
        output = io.StringIO()
        with patch.dict(os.environ, self.cli_environment(), clear=True), \
             patch.object(p, "GitHub", return_value=self.api), \
             patch.object(p, "create_jwt", return_value="offline-jwt"), \
             patch.object(p.subprocess, "run", side_effect=run), \
             patch.object(p.IsolatedGit, "push_once", side_effect=push) as attempt, \
             contextlib.redirect_stdout(output):
            p.main(["publish"])
        self.assertEqual(json.loads(output.getvalue())["state"], "published")
        self.assertEqual(attempt.call_count, 1)
        self.assertEqual([c[1] for c in self.api.calls if c[1] != "GET"], ["POST", "DELETE"])
        self.assertFalse(self.fixture.marker.exists())
        self.assertNotIn("offline-installation-secret", output.getvalue())
        self.assertNotIn("offline-jwt", output.getvalue())

    def test_cli_observe_read_only_composition(self):
        self.api.target = self.m["candidate_sha"]
        output = io.StringIO()
        with patch.dict(os.environ, self.cli_environment(), clear=True), \
             patch.object(p, "GitHub", return_value=self.api), contextlib.redirect_stdout(output):
            p.main(["observe"])
        self.assertEqual(json.loads(output.getvalue())["state"], "success")
        self.assertTrue(all(c[1] == "GET" for c in self.api.calls))


if __name__ == "__main__":
    unittest.main(verbosity=2)
