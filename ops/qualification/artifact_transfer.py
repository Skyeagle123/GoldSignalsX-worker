"""Draft credential-free transfer qualification. Never invokes deployment.

Pinned recovered files are copied as data. Only unpack's AST is loaded from deploy.py.
"""
import ast
import datetime as dt
import hashlib
import shutil
import io
import zipfile
import json
import os
import pathlib
import re
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request

REPO = 'Skyeagle123/GoldSignalsX-worker'
REPO_ID = 1085006449
OWNER_ID = 125151458
SOURCE_SHA = '892da18f2f3935d2bea669051f101156d11c26ae'
SOURCE_TREE = 'cfdf7e1560bcb96652e4b37db3338c15192c0f0e'
SOURCE_DIGEST = '136fc036d89819f9dbae0590811c4e610a37a62a49c607c0ae2cb8e76c034496'
WORKFLOW = '.github/workflows/artifact-transfer-qualification.yml'
HELPER = 'ops/qualification/artifact_transfer.py'
FORMAT = 'gsx-artifact-qualification-v1'
FILES = {'authorization.json', 'policy.json', 'toolchain.tar.gz'}
MAX_ZIP = 120_000_000
HEX = re.compile(r'[0-9a-f]{64}')

class Refused(Exception):
    pass

def need(ok, reason):
    if not ok:
        raise Refused(reason)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode()

def pairs(items):
    result = {}
    for key, value in items:
        need(key not in result, 'duplicate JSON key')
        result[key] = value
    return result

def parse(data):
    need(len(data) <= 1_048_576, 'JSON bounds')
    def reject(_):
        raise Refused('noninteger JSON number')
    return json.loads(data, object_pairs_hook=pairs,
                      parse_float=reject, parse_constant=reject)

def positive(value):
    return type(value) is int and value > 0

def number(value):
    need(type(value) is str and re.fullmatch('[1-9][0-9]*', value), 'numeric output')
    return int(value)

def stamp(text):
    need(type(text) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', text), 'UTC timestamp')
    return dt.datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp()

def git(root, *args):
    return subprocess.check_output(['git', *args], cwd=root, text=True).strip()

def source_identity(root):
    need(git(root, 'rev-parse', 'HEAD') == SOURCE_SHA, 'source SHA')
    need(git(root, 'rev-parse', 'HEAD^{tree}') == SOURCE_TREE, 'source tree')
    paths = subprocess.check_output(['git', 'ls-files', '-z'], cwd=root).decode().split('\0')[:-1]
    actual = sha(canonical({p: sha((root / p).read_bytes()) for p in paths}))
    need(actual == SOURCE_DIGEST, 'source bytes')

def context():
    need(os.environ.get('GITHUB_REPOSITORY') == REPO, 'repository')
    need(os.environ.get('GITHUB_REPOSITORY_ID') == str(REPO_ID), 'repository ID')
    need(os.environ.get('GITHUB_ACTOR') == 'Skyeagle123' and
         os.environ.get('GITHUB_ACTOR_ID') == str(OWNER_ID), 'Owner')
    need(os.environ.get('GITHUB_REF') == 'refs/heads/main' and
         os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch' and
         os.environ.get('GITHUB_RUN_ATTEMPT') == '1', 'event/ref/attempt')
    control_sha = os.environ.get('REVIEWED_CONTROL_SHA', '')
    need(re.fullmatch('[0-9a-f]{40}', control_sha) and
         os.environ.get('GITHUB_SHA') == control_sha and
         os.environ.get('GITHUB_WORKFLOW_SHA') == control_sha, 'controller revision')
    need(os.environ.get('GITHUB_WORKFLOW_REF') == REPO + '/' + WORKFLOW + '@refs/heads/main', 'workflow')
    need(os.environ.get('GSX_PRODUCTION_ENABLED') == 'false', 'Production disabled')
    for key in os.environ:
        need(not key.startswith(('CLOUDFLARE_', 'CF_', 'GSX_PRODUCTION_CF_',
                                 'GSX_QUALIFICATION_CF_', 'ACTIONS_ID_TOKEN_REQUEST_')),
             'forbidden credential or OIDC environment')
    return {'run_id': number(os.environ['GITHUB_RUN_ID']), 'attempt': 1,
            'controller_sha': control_sha, 'source_sha': SOURCE_SHA,
            'source_tree': SOURCE_TREE, 'source_digest': SOURCE_DIGEST}

def control_identity(root, ctx):
    need(git(root, 'rev-parse', 'HEAD') == ctx['controller_sha'], 'controller checkout')
    need(not git(root, 'status', '--porcelain', '--untracked-files=no'), 'controller dirty')
    return {'workflow_digest': sha((root / WORKFLOW).read_bytes()),
            'helper_digest': sha((root / HELPER).read_bytes())}

def selected_functions(root):
    # Compile just these accepted pure helper definitions; never import deploy.
    space = {'pathlib': pathlib, 'tarfile': tarfile, 'need': need, 'digest': sha}
    for file, name in [('ops/production/prepare.py', 'inventory'),
                       ('ops/production/deploy.py', 'unpack')]:
        tree = ast.parse((root / file).read_text())
        functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
        need(len(functions) == 1 and not functions[0].decorator_list, 'helper definition')
        exec(compile(ast.Module(body=functions, type_ignores=[]), file, 'exec'), space)
    return space['inventory'], space['unpack']

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

def get(path):
    need(path.startswith('/repos/' + REPO + '/actions/'), 'GitHub GET allowlist')
    token = os.environ.get('QUALIFICATION_GH_TOKEN', '')
    need(token, 'read-only GitHub token')
    req = urllib.request.Request('https://api.github.com' + path, headers={
        'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28', 'Cache-Control': 'no-cache'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(req, timeout=10) as response:
        need(response.status == 200 and not response.headers.get('Link'), 'complete GitHub GET')
        return parse(response.read(1_048_577))

def run_metadata(ctx, completed=False):
    run = get(f'/repos/{REPO}/actions/runs/{ctx["run_id"]}')
    need(type(run) is dict, 'run schema')
    for key, expected in [('id', ctx['run_id']), ('run_attempt', 1)]:
        need(positive(run.get(key)) and run[key] == expected, 'run numeric identity')
    need(run.get('head_sha') == ctx['controller_sha'] and run.get('head_branch') == 'main'
         and run.get('path') == WORKFLOW and run.get('event') == 'workflow_dispatch'
         and 'conclusion' in run and
         ((completed and run.get('status') == 'completed' and run['conclusion'] == 'success') or
          (not completed and run.get('status') == 'in_progress' and run['conclusion'] is None)), 'run identity/state')
    for key in ('actor', 'triggering_actor'):
        actor = run.get(key, {})
        need(positive(actor.get('id')) and actor['id'] == OWNER_ID and actor.get('login') == 'Skyeagle123', 'run Owner')
    repo = run.get('repository', {})
    need(positive(repo.get('id')) and repo['id'] == REPO_ID and repo.get('full_name') == REPO, 'run repository')
    stamp(run['created_at'])
    return run

def build_payload(source, control, output, run, ctx):
    need(not output.exists(), 'fresh artifact directory')
    output.mkdir(parents=True)
    dest = output / 'toolchain'
    dest.mkdir()
    inputs = pathlib.Path(os.environ['QUALIFICATION_INPUT_DIR']).resolve()
    need({p.name for p in inputs.iterdir()} == {'toolchain.tar.gz', 'policy.json'}, 'seed exact files')
    for name in ('toolchain.tar.gz', 'policy.json'):
        need((inputs / name).is_file() and not (inputs / name).is_symlink(), 'seed regular file')
    policy_raw = (inputs / 'policy.json').read_bytes()
    need(sha(policy_raw) == '5542f2658bc7c5bb044c557bc373e04d6b29db39caed5cdf174f45b927234acf', 'historical policy pin')
    policy = parse(policy_raw)
    shutil.copyfile(inputs / 'toolchain.tar.gz', output / 'toolchain.tar.gz')
    need((output / 'toolchain.tar.gz').stat().st_size == 97903133 and
         sha((output / 'toolchain.tar.gz').read_bytes()) == 'dd367b1b11e10681d866b560dc6da0c3341389ea2adef1a350ec7a393fe948f7', 'seed archive pin')
    inventory, unpack = selected_functions(source)
    unpack(output / 'toolchain.tar.gz', dest)
    actual = inventory(dest)
    actual.pop('inventory.json')
    need(actual == parse((dest / 'inventory.json').read_bytes()), 'seed complete inventory')
    golden = parse((source / 'evidence/deadline-remediation/toolchain-build.json').read_bytes())['builds'][0]
    need((output / 'toolchain.tar.gz').stat().st_size == golden['artifact_bytes'] and
         sha((output / 'toolchain.tar.gz').read_bytes()) == golden['artifact_sha256'], 'golden tar')
    need(sha((dest / 'inventory.json').read_bytes()) == golden['inventory_sha256'], 'golden inventory')
    (output / 'policy.json').write_bytes(policy_raw)
    shutil.rmtree(dest)
    expires = dt.datetime.fromtimestamp(stamp(run['created_at']) + 900, dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    manifest = dict(ctx, **control_identity(control, ctx), format=FORMAT, repository=REPO,
                    created_at=run['created_at'], expires_at=expires,
                    artifact_name=f'gsx-artifact-qualification-{ctx["run_id"]}-1',
                    files={f: {'bytes': (output / f).stat().st_size,
                               'sha256': sha((output / f).read_bytes())}
                           for f in ('policy.json', 'toolchain.tar.gz')},
                    inventory_digest=golden['inventory_sha256'])
    need(dt.datetime.now(dt.timezone.utc).timestamp() < stamp(expires), 'qualification expired')
    (output / 'authorization.json').write_bytes(canonical(manifest))
    return sha(canonical(manifest))

def check_metadata(art, ctx, expected_id, expected_digest, now):
    need(type(art) is dict and positive(art.get('id')) and art['id'] == expected_id, 'artifact ID')
    need(type(expected_digest) is str and HEX.fullmatch(expected_digest), 'upload digest')
    need(art.get('name') == f'gsx-artifact-qualification-{ctx["run_id"]}-1', 'artifact name')
    need(type(art.get('expired')) is bool and art['expired'] is False and stamp(art['expires_at']) > now, 'artifact expiry')
    need(positive(art.get('size_in_bytes')) and art['size_in_bytes'] <= MAX_ZIP, 'artifact ZIP size')
    need(art.get('digest') == 'sha256:' + expected_digest, 'provider ZIP digest')
    run = art.get('workflow_run', {})
    for key, expected in [('id', ctx['run_id']), ('repository_id', REPO_ID), ('head_repository_id', REPO_ID)]:
        need(positive(run.get(key)) and run[key] == expected, 'artifact run/repository')
    need(run.get('head_sha') == ctx['controller_sha'] and run.get('head_branch') == 'main', 'artifact workflow SHA')
    return {k: art[k] for k in ('id', 'name', 'size_in_bytes', 'digest', 'expired', 'expires_at', 'workflow_run')}

def hash_remote_zip(artifact_id, expected, retain=False):
    # Authentication goes ONLY to api.github.com; never follow with the token.
    token = os.environ['QUALIFICATION_GH_TOKEN']
    req = urllib.request.Request(f'https://api.github.com/repos/{REPO}/actions/artifacts/{artifact_id}/zip',
                                 headers={'Authorization': 'Bearer ' + token})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        opener.open(req, timeout=10)
        raise Refused('expected signed redirect')
    except urllib.error.HTTPError as error:
        need(error.code == 302, 'artifact redirect status')
        target = error.headers.get('Location', '')
    parsed = urllib.parse.urlsplit(target)
    need(parsed.scheme == 'https' and parsed.hostname and
         parsed.hostname.endswith('.blob.core.windows.net') and
         parsed.username is None and parsed.password is None and
         parsed.port in (None, 443), 'signed-download origin')
    retained = bytearray()
    count = 0
    hashed = hashlib.sha256()
    # No bearer, redirects, logging of signed URL, persistence or retries.
    with opener.open(urllib.request.Request(target), timeout=30) as response:
        need(response.status == 200, 'artifact ZIP status')
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            count += len(chunk)
            need(count <= MAX_ZIP and count <= expected['size_in_bytes'], 'ZIP stream bounds')
            hashed.update(chunk)
            if retain:
                need(count <= 1_048_576, 'receipt ZIP bound')
                retained.extend(chunk)
    need(count == expected['size_in_bytes'] and 'sha256:' + hashed.hexdigest() == expected['digest'], 'ZIP bytes/digest')
    return bytes(retained) if retain else None

def verify_payload(source, control, output, ctx, manifest_digest, now):
    need(HEX.fullmatch(manifest_digest), 'manifest output digest')
    need({p.name for p in output.iterdir()} == FILES and
         all(p.is_file() and not p.is_symlink() for p in output.iterdir()), 'artifact exact file set')
    raw = (output / 'authorization.json').read_bytes()
    need(sha(raw) == manifest_digest, 'manifest bytes')
    manifest = parse(raw)
    keys = set(ctx) | {'workflow_digest', 'helper_digest', 'format', 'repository', 'created_at',
                       'expires_at', 'artifact_name', 'files', 'inventory_digest'}
    need(set(manifest) == keys and manifest['format'] == FORMAT and manifest['repository'] == REPO, 'manifest domain/schema')
    for key, expected in dict(ctx, **control_identity(control, ctx)).items():
        need(type(manifest[key]) is type(expected) and manifest[key] == expected, 'manifest identity')
    need(manifest['artifact_name'] == f'gsx-artifact-qualification-{ctx["run_id"]}-1', 'manifest artifact name')
    need(stamp(manifest['expires_at']) == stamp(manifest['created_at']) + 900 and
         stamp(manifest['created_at']) <= now < stamp(manifest['expires_at']), 'manifest expiry')
    need(set(manifest['files']) == {'policy.json', 'toolchain.tar.gz'}, 'payload manifest files')
    for file, record in manifest['files'].items():
        need(set(record) == {'bytes', 'sha256'} and positive(record['bytes']) and HEX.fullmatch(record['sha256']), 'file schema')
        need((output / file).stat().st_size == record['bytes'] and
             sha((output / file).read_bytes()) == record['sha256'], 'payload file bytes')
    golden = parse((source / 'evidence/deadline-remediation/toolchain-build.json').read_bytes())['builds'][0]
    need(manifest['files']['toolchain.tar.gz'] == {'bytes': golden['artifact_bytes'], 'sha256': golden['artifact_sha256']}
         and manifest['inventory_digest'] == golden['inventory_sha256'], 'accepted golden identity')
    inventory, unpack = selected_functions(source)
    with tempfile.TemporaryDirectory() as tmp:
        dest = pathlib.Path(tmp)
        unpack(output / 'toolchain.tar.gz', dest)
        invraw = (dest / 'inventory.json').read_bytes()
        need(sha(invraw) == manifest['inventory_digest'], 'inventory digest')
        actual = inventory(dest)
        actual.pop('inventory.json')
        need(actual == parse(invraw), 'complete unpacked inventory')
    # Nothing from the downloaded archive is imported or executed.
    return manifest

def completed_jobs(ctx):
    # Single bounded page: reject incomplete or paginated evidence rather than guess.
    data = get(f'/repos/{REPO}/actions/runs/{ctx["run_id"]}/attempts/1/jobs?per_page=100')
    need(type(data) is dict and type(data.get('total_count')) is int and
         data['total_count'] == 2 and type(data.get('jobs')) is list and len(data['jobs']) == 2, 'complete job coverage')
    names, ids = set(), set()
    for job in data['jobs']:
        need(type(job) is dict and positive(job.get('id')) and job['id'] not in ids, 'job ID')
        ids.add(job['id'])
        need(job.get('name') in {'prepare_transfer', 'verify_transfer'} and job['name'] not in names, 'job set')
        names.add(job['name'])
        need(type(job.get('run_id')) is int and job['run_id'] == ctx['run_id'] and
             type(job.get('run_attempt')) is int and job['run_attempt'] == 1 and
             job.get('head_sha') == ctx['controller_sha'] and job.get('status') == 'completed' and
             job.get('conclusion') == 'success', 'completed job identity/state')
        need(stamp(job['started_at']) <= stamp(job['completed_at']), 'job time')
        steps = job.get('steps')
        need(type(steps) is list and steps, 'job steps')
        expected_steps = {'prepare_transfer': ['control_checkout', 'source_checkout', 'seed_download', 'package', 'payload_upload'], 'verify_transfer': ['control_checkout', 'source_checkout', 'zip_verify', 'payload_download', 'payload_verify', 'receipt_upload']}
        required = ['gsx:' + value for value in expected_steps[job['name']]]
        actual = [step.get('name') for step in steps if type(step) is dict]
        # API lacks YAML step.id: bind stable ID markers, order and numeric position
        # to exact reviewed workflow bytes. Only explicit provider lifecycle steps allowed.
        allowed = {'Set up job', 'Complete job', 'Post job cleanup'} | {'Post ' + n for n in required if 'checkout' in n}
        need(all(n in required or n in allowed for n in actual), 'unexpected step')
        need([n for n in actual if n in required] == required, 'complete ordered stable steps')
        need(all(next(step['number'] for step in steps if step['name'] == name) == index + 2
                 for index, name in enumerate(required)), 'stable step numeric position')
        need([step.get('number') for step in steps] == sorted(step.get('number') for step in steps), 'step order')
        need(actual.count('Set up job') == 1 and steps[0]['number'] == 1 and actual[0] == 'Set up job', 'setup identity')
        numbers = set()
        for step in steps:
            need(type(step) is dict and positive(step.get('number')) and step['number'] not in numbers and
                 type(step.get('name')) is str and step['name'] and step.get('status') == 'completed' and
                 step.get('conclusion') == 'success', 'step evidence')
            numbers.add(step['number'])
    return data

def receipt_body(ctx, control, manifest, selected):
    return dict(format='gsx-transfer-receipt-v1', stage='active-transfer-verified',
                context=ctx, control=control, manifest_digest=sha(canonical(manifest)), artifact=selected)

def retrieve_receipt(ctx):
    artifact_id = number(os.environ['RECEIPT_ARTIFACT_ID'])
    art = get(f'/repos/{REPO}/actions/artifacts/{artifact_id}')
    need(type(art) is dict and art.get('name') == f'gsx-artifact-receipt-{ctx["run_id"]}-1', 'receipt artifact name')
    need(type(art.get('digest')) is str and art['digest'].startswith('sha256:'), 'receipt digest')
    transformed = dict(art, name=f'gsx-artifact-qualification-{ctx["run_id"]}-1')
    selected = check_metadata(transformed, ctx, artifact_id, art['digest'][7:], dt.datetime.now(dt.timezone.utc).timestamp())
    need(selected['size_in_bytes'] <= 1_048_576, 'receipt size')
    raw = hash_remote_zip(artifact_id, selected, retain=True)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        need(len(members) == 1 and members[0].filename == 'qualification-receipt.json' and
             members[0].file_size <= 1_048_576 and not members[0].is_dir(), 'receipt inventory')
        data = archive.read(members[0])
    document = parse(data)
    need(type(document) is dict and set(document) == {'body', 'sha256'} and
         document['sha256'] == sha(canonical(document['body'])) and data == canonical(document), 'receipt integrity')
    return document['body']

def main():
    mode, source_arg, control_arg, output_arg = sys.argv[1:]
    source, control, output = map(lambda p: pathlib.Path(p).resolve(), (source_arg, control_arg, output_arg))
    ctx = context()
    source_identity(source)
    control_identity(control, ctx)
    if mode == 'build':
        run = run_metadata(ctx)
        value = build_payload(source, control, output, run, ctx)
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write('manifest_digest=' + value + '\n')
    elif mode == 'zip':
        run_metadata(ctx)
        artifact_id = number(os.environ['ARTIFACT_ID'])
        raw = get(f'/repos/{REPO}/actions/artifacts/{artifact_id}')
        now = dt.datetime.now(dt.timezone.utc).timestamp()
        selected = check_metadata(raw, ctx, artifact_id, os.environ['ARTIFACT_DIGEST'], now)
        hash_remote_zip(artifact_id, selected)
        output.mkdir(parents=True)
        (output / 'metadata.json').write_bytes(canonical(selected))
    elif mode in ('verify', 'completed'):
        manifest = verify_payload(source, control, output, ctx, os.environ['MANIFEST_DIGEST'], dt.datetime.now(dt.timezone.utc).timestamp())
        # Re-read identity/expiry after the action's download and local verification.
        run = run_metadata(ctx, completed=mode == 'completed')
        if mode == 'completed':
            completed_jobs(ctx)
        need(run['created_at'] == manifest['created_at'], 'run time binding')
        artifact_id = number(os.environ['ARTIFACT_ID'])
        art = get(f'/repos/{REPO}/actions/artifacts/{artifact_id}')
        selected = check_metadata(art, ctx, artifact_id, os.environ['ARTIFACT_DIGEST'], dt.datetime.now(dt.timezone.utc).timestamp())
        if mode == 'completed':
            hash_remote_zip(artifact_id, selected)
            need(canonical(retrieve_receipt(ctx)) == canonical(receipt_body(ctx, control_identity(control, ctx), manifest, selected)), 'receipt binding/replay')
        else:
            before = parse((output.parent / 'receipt/metadata.json').read_bytes())
            need(selected == before, 'artifact metadata changed')
            body = receipt_body(ctx, control_identity(control, ctx), manifest, selected)
            document = dict(body=body, sha256=sha(canonical(body)))
            (output.parent / 'receipt/qualification-receipt.json').write_bytes(canonical(document))
        need(dt.datetime.now(dt.timezone.utc).timestamp() < stamp(manifest['expires_at']), 'final expiry')
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
            f.write(('HOSTED ARTIFACT QUALIFICATION PASS' if mode == 'completed' else 'TRANSFER VERIFIED — FINAL RUN VERIFICATION PENDING') + '\n\nManifest SHA-256: ' + sha(canonical(manifest)) + '\n\nArtifact ID: ' + str(artifact_id) + '\n\nZIP digest: ' + selected['digest'] + '\n\nZIP bytes: ' + str(selected['size_in_bytes']) + '\n')
    else:
        raise Refused('mode')

if __name__ == '__main__':
    try:
        main()
    except Exception:
        # No tokens, signed URL, response body or exception detail in logs.
        print('ARTIFACT QUALIFICATION STOP', file=sys.stderr)
        raise SystemExit(1)
