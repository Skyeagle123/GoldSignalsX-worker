"""Offline golden-artifact qualification, no Production variables/credentials."""
import json,pathlib,sys,tempfile,tarfile
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from prepare import build,ROOT,digest,need
expected=json.loads((ROOT/'evidence/deadline-remediation/toolchain-build.json').read_text())['builds'][0]
with tempfile.TemporaryDirectory()as td:
 results=[]
 for n in ('one','two'):
  parent=pathlib.Path(td)/n;parent.mkdir();out=parent/'toolchain';out.mkdir();build(out);b=(parent/'toolchain.tar.gz').read_bytes();results.append(digest(b))
  with tarfile.open(parent/'toolchain.tar.gz')as archive:need('capture.json'not in archive.getnames() and 'goldsignalsx-worker.js'not in archive.getnames(),'source leaked into reusable toolchain')
  need(len(b)==expected['artifact_bytes'] and digest(b)==expected['artifact_sha256'],'golden artifact mismatch')
 need(results[0]==results[1],'non-reproducible artifact');print('REPRODUCIBLE GOLDEN ARTIFACT PASS '+results[0])
