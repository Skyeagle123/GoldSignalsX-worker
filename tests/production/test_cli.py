import copy,hashlib,json,pathlib,subprocess,sys,unittest
from email.parser import BytesParser
from email.policy import default
from cli_http import scenario,prepare_control,ROOT
class CLIQualification(unittest.TestCase):
 @classmethod
 def setUpClass(cls):prepare_control()
 def test_invalid_schedule_contract_single_request(self):
  for fault in ('old-schedules','wrong-schedules'):
   with self.subTest(fault=fault):
    code,calls,out=scenario(fault=fault,at='schedules');self.assertNotEqual(code,0,out);self.assertEqual(sum(c['kind']=='schedules' for c in calls),1);self.assertEqual(calls[-1]['kind'],'schedules')
 def test_kernel_egress_denied(self):
  p=subprocess.run(['/tmp/gsx-no-network',sys.executable,'-c','import socket; socket.socket(socket.AF_INET)'],capture_output=True);self.assertNotEqual(p.returncode,0);self.assertIn(b'Operation not permitted',p.stderr)
 def test_kernel_ipv6_denied(self):
  p=subprocess.run(['/tmp/gsx-no-network',sys.executable,'-c','import socket; socket.socket(socket.AF_INET6)'],capture_output=True);self.assertNotEqual(p.returncode,0)
 def test_upstream_retry_control(self):
  code,calls,out=scenario(control=True,fault='500-malformed');self.assertNotEqual(code,0,out);self.assertEqual([c['kind'] for c in calls],['versions']*3)
 def test_successful_fractional_api_metrics(self):
  code,calls,out=scenario(floats=True);self.assertEqual(code,0,out);self.assertEqual([c['kind']for c in calls],['versions','deployments','script-settings','subdomain','schedules'])
 def test_legacy_success(self):
  code,calls,out=scenario(legacy=True);self.assertEqual(code,0,out);self.assertEqual([c['kind'] for c in calls],['legacy','subdomain','schedules'])
 def test_versioned_success(self):
  code,calls,out=scenario();self.assertEqual(code,0,out);self.assertEqual([c['kind'] for c in calls],['versions','deployments','script-settings','subdomain','schedules'])
 def test_successful_upstream_parity(self):
  for legacy in (False,True):
   with self.subTest(legacy=legacy):
    a,ca,oa=scenario(legacy=legacy);b,cb,ob=scenario(legacy=legacy,control=True);self.assertEqual(a,0,oa);self.assertEqual(b,0,ob)
    # Upstream trigger updates are concurrent; arrival order is not semantic.
    # The patched order is separately asserted in the two success tests.
    ka={(c['method'],c['path']):c for c in ca};kb={(c['method'],c['path']):c for c in cb}
    self.assertEqual(len(ka),len(ca));self.assertEqual(len(kb),len(cb));self.assertEqual(set(ka),set(kb))
    for key in ka:
     x,y=ka[key],kb[key]
     def semantics(c):
      if 'multipart/form-data'not in c['content_type']:return json.loads(c['body'])
      msg=BytesParser(policy=default).parsebytes(('Content-Type: '+c['content_type']+'\r\nMIME-Version: 1.0\r\n\r\n').encode()+c['body']);r={}
      for part in msg.iter_parts():
       name=part.get_param('name',header='Content-Disposition');b=part.get_payload(decode=True);r[name]=json.loads(b) if name=='metadata' else {'sha256':hashlib.sha256(b).hexdigest(),'type':part.get_content_type(),'filename':part.get_filename()}
      return r
     self.assertEqual(semantics(x),semantics(y))
 def test_duplicate_mutation_blocked(self):
  code,calls,out=scenario(attack='duplicate');self.assertNotEqual(code,0,out);self.assertEqual([c['kind']for c in calls],['versions','deployments','script-settings','subdomain'])
 def test_fallback_route_blocked(self):
  code,calls,out=scenario(attack='fallback');self.assertNotEqual(code,0,out);self.assertEqual(calls,[])
 def test_failure_before_mutation_send(self):
  code,calls,out=scenario(attack='tamper');self.assertNotEqual(code,0,out);self.assertEqual(calls,[])
 def test_cancellation_before_mutation(self):
  code,calls,out=scenario(fault='cancel-before');self.assertNotEqual(code,0,out);self.assertEqual(calls,[])
 def test_cancellation_with_queued_trigger(self):
  code,calls,out=scenario(fault='cancel',at='subdomain');self.assertNotEqual(code,0,out);self.assertEqual([c['kind']for c in calls],['versions','deployments','script-settings','subdomain'])
 def test_invalid_version_uuid_stops_before_activation(self):
  code,calls,out=scenario(fault='bad-id',at='versions');self.assertNotEqual(code,0,out);self.assertEqual([c['kind']for c in calls],['versions'])
 def test_missing_upload_witness_stops_before_activation(self):
  code,calls,out=scenario(fault='missing-resources',at='versions');self.assertNotEqual(code,0,out);self.assertEqual([c['kind']for c in calls],['versions'])
 def test_invalid_activation_uuid_stops_settings(self):
  code,calls,out=scenario(fault='bad-id',at='deployments');self.assertNotEqual(code,0,out);self.assertEqual([c['kind']for c in calls],['versions','deployments'])
 def test_invalid_legacy_id_stops_triggers(self):
  code,calls,out=scenario(fault='bad-id',at='legacy',legacy=True);self.assertNotEqual(code,0,out);self.assertEqual([c['kind']for c in calls],['legacy'])
 def test_timeout_blocks_queued_trigger(self):
  code,calls,out=scenario(fault='timeout',at='subdomain',timeout=18);self.assertNotEqual(code,0);self.assertEqual([c['kind']for c in calls],['versions','deployments','script-settings','subdomain'])

def case(at,fault,legacy=False):
 def test(self):
  code,calls,out=scenario(fault=fault,at=at,legacy=legacy);self.assertNotEqual(code,0,out)
  kinds=[c['kind']for c in calls];self.assertEqual(kinds.count(at),1,(kinds,out));self.assertEqual(kinds[-1],at);self.assertFalse(any(c['after_failure']for c in calls));self.assertFalse(any(c.get('trailing_bytes',0) for c in calls));self.assertEqual(calls[-1]['server_committed'],fault=='committed-loss')
 return test
for at in ['versions','deployments','script-settings','subdomain','schedules','legacy']:
 for fault in ['429','500','524','500-malformed','malformed','duplicate-json','307','disconnect','committed-loss']:
  setattr(CLIQualification,'test_'+at.replace('-','_')+'_'+fault.replace('-','_'),case(at,fault,at=='legacy'))
