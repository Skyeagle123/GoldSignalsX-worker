"""Offline-only probe of actual Production validators and blocked boundaries.

Not a workflow, deployment entrypoint, credential verifier or live readiness
attestation. Tests supply synthetic credentials in private fixtures only.
"""
import contextlib,io,os,pathlib,socket,subprocess,sys,unittest,urllib.request
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[2]
MODULES=('test_authorization','test_deploy','test_history','test_controller_validation')

def forbidden(*args,**kwargs):
 raise AssertionError('OFFLINE_PROBE_FORBIDDEN_OPERATION')

def main():
 # Inspect names only, never retrieve an existing credential value. Refuse to
 # start in a credential-bearing session. The probe does not clear/store/revoke
 # real credentials. Fixture-local patch.dict creates only synthetic values.
 if any(k.startswith(('GSX_PRODUCTION_CF_','CLOUDFLARE_')) or k in
        ('GH_TOKEN','GITHUB_TOKEN','GH_READ_TOKEN','GSX_QUALIFICATION_CF_READ_TOKEN',
         'GSX_PUBLICATION_APP_PRIVATE_KEY','PUBLICATION_PRIVATE_KEY') for k in os.environ):
  print('OFFLINE PROBE STOP: credential-bearing environment');return 1
 sys.dont_write_bytecode=True
 sys.path[:0]=[str(ROOT/'tests/production'),str(ROOT/'ops/production')]
 output=io.StringIO()
 try:
  with contextlib.ExitStack() as stack:
   for obj,name in [(socket,'socket'),(socket,'create_connection'),(urllib.request,'urlopen'),
                    (urllib.request.OpenerDirector,'open'),(subprocess,'Popen'),(os,'system'),
                    (os,'execve'),(os,'execv'),(os,'execvp'),(os,'execvpe'),(os,'posix_spawn'),(os,'posix_spawnp')]:
    stack.enter_context(patch.object(obj,name,forbidden))
   # Probe stdout/stderr are fixed summaries; fixture exceptions/API bodies are
   # retained in memory only. No actual network, CLI or artifact execution.
   stack.enter_context(contextlib.redirect_stdout(output));stack.enter_context(contextlib.redirect_stderr(output))
   suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(m) for m in MODULES)
   result=unittest.TextTestRunner(stream=output,verbosity=0).run(suite)
  if not result.wasSuccessful():
   print('OFFLINE PROBE FAIL: validator regression');return 1
 except Exception:
  print('OFFLINE PROBE FAIL: isolation or harness error');return 1
 print('OFFLINE PROBE PASS tests='+str(result.testsRun)+'; mocks only; deployment NOT authorized')
 print('TIMING: run-created TTL=900s; final pre-exec budget=10s; gate deadline enforced separately; actual gate regressions required')
 return 0

if __name__=='__main__':raise SystemExit(main())
