"""Real inherited-pipe handoff plus actual gate with mocked HTTP transport."""
import json,os,pathlib,stat,subprocess,sys,time,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'ops/production'))
import authorization as a
import deploy as d
from test_authorization import sample
import test_deploy as fixture
Invoked=fixture.Invoked

class ExecutionAuthority(unittest.TestCase):
 def test_pipe_identity_and_absolute_deadline(self):
  m=sample();clock=a.Clock();start=time.monotonic();deadline=start+9
  with d.execution_authority(m,7,11,clock,deadline,'/verified/node',start) as fd:
   fd=int(fd);self.assertTrue(stat.S_ISFIFO(os.fstat(fd).st_mode));self.assertTrue(os.get_inheritable(fd))
   q=json.loads(os.read(fd,4097));self.assertEqual(q['manifest'],m);self.assertEqual(q['manifest_digest'],a.manifest(m));self.assertEqual(q['pid'],os.getpid());self.assertEqual(q['artifact_id'],7);self.assertEqual(q['check_run_id'],11)
   self.assertLessEqual(int(q['deadline_mono_ns']),int(deadline*1e9));self.assertEqual(os.read(fd,1),b'')
  with self.assertRaises(OSError):os.fstat(fd)
 def test_invalid_deadlines(self):
  for value in [None,True,'10',float('nan'),float('inf'),time.monotonic()-1,time.monotonic()+11]:
   with self.subTest(value=str(value)),self.assertRaises(a.Rejected):
    c=a.Clock()
    with d.execution_authority(sample(),1,11,c,value,'node',time.monotonic()):self.fail('invalid handoff')
 def test_invalid_identity(self):
  for value in [True,1.0,'1',None,0]:
   with self.subTest(value=value),self.assertRaises(a.Rejected):
    c=a.Clock();start=time.monotonic()
    with d.execution_authority(sample(),value,11,c,start+10,'node',start):self.fail('invalid identity')
 def test_pinned_linux_shared_monotonic_epoch(self):
  before=time.monotonic_ns()
  p=subprocess.run(['/tmp/gsx-no-network','node','-p','process.hrtime.bigint().toString()'],env={'PATH':os.environ['PATH']},capture_output=True,text=True,check=True)
  after=time.monotonic_ns();observed=int(p.stdout)
  self.assertLessEqual(before,observed);self.assertLessEqual(observed,after)
 def test_gate_boundaries(self):
  p=subprocess.run(['/tmp/gsx-no-network','node',str(ROOT/'tests/production/test_request_deadline.cjs')],cwd=ROOT,env={'PATH':os.environ['PATH']},capture_output=True,text=True)
  self.assertEqual(p.returncode,0,p.stdout+p.stderr);self.assertEqual(json.loads(p.stdout)['tests'],51)

class ControllerHandoff(unittest.TestCase):
 setUp=fixture.DeploymentVerifier.setUp
 save=fixture.DeploymentVerifier.save
 call=fixture.DeploymentVerifier.call
 def test_real_controller_mints_execution_bound_pipe(self):
  seen={};exe=self.call()
  def inspect(path,args,env):
   fd=int(env['GSX_EXECUTION_AUTHORITY_FD']);seen['fd']=fd;q=json.loads(os.read(fd,4097))
   self.assertEqual(q['manifest'],self.m);self.assertEqual(q['artifact_id'],1);self.assertEqual(q['check_run_id'],11);self.assertEqual(q['pid'],os.getpid());self.assertEqual(q['executable'],path)
   self.assertLessEqual(int(q['deadline_mono_ns'])-int(q['final_start_ns']),10_000_000_000)
   raise Invoked()
  exe.side_effect=inspect
  with self.assertRaises(Invoked):d.deploy(self.build)
  self.assertEqual(exe.call_count,1)
  with self.assertRaises(OSError):os.fstat(seen['fd'])
