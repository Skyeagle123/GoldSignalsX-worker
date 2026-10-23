import hashlib,json,os,pathlib,subprocess
root=pathlib.Path(__file__).resolve().parents[2]
changed=subprocess.check_output(['git','diff','--name-only'],cwd=root,text=True).splitlines()
if not changed:
 with open(os.environ['GITHUB_OUTPUT'],'a')as f:f.write('changed=false\n')
else:
 if changed!=['bls-official-snapshot.js']:raise SystemExit('unexpected generated scope')
 out=root/'.bls-artifact';out.mkdir();b=(root/changed[0]).read_bytes();(out/changed[0]).write_bytes(b)
 (out/'manifest.json').write_text(json.dumps({'changed':changed,'run_id':os.environ['GITHUB_RUN_ID'],'source_sha':os.environ['GITHUB_SHA'],'sha256':hashlib.sha256(b).hexdigest()}))
 with open(os.environ['GITHUB_OUTPUT'],'a')as f:f.write('changed=true\n')
