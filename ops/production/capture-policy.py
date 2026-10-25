"""Secret-free dry-run capture using an exact, temporary upstream spy.
The spy CLI is never included in the deploy artifact."""
import hashlib,json,os,pathlib,subprocess,sys,tempfile
ROOT=pathlib.Path(__file__).resolve().parent
CLI=ROOT/'toolchain/node_modules/wrangler/wrangler-dist/cli.js'
def capture(source,output,launcher=()):
 s=CLI.read_text();needle='  formData.set("metadata", JSON.stringify(metadata));'
 if s.count(needle)!=1:raise ValueError('capture patch mismatch')
 code=needle+'''
  require("node:fs").writeFileSync(process.env.GSX_CAPTURE_PATH, JSON.stringify({metadata,
    modules:Object.fromEntries([main2].concat(modules || [], sourceMaps || []).map(m=>[m.name,{sha256:require("node:crypto").createHash("sha256").update(m.content).digest("hex"),type:m.type?toMimeType(m.type):"application/source-map",name:m.name}]))}));'''
 spy=CLI.with_name('gsx-prepare-capture.js')
 if spy.exists():raise ValueError('capture exists')
 try:
  spy.write_text(s.replace(needle,code))
  env={k:v for k,v in os.environ.items() if k in ('PATH','HOME','TMPDIR')}
  env.update({'WRANGLER_SEND_METRICS':'false','CI':'true','GSX_CAPTURE_PATH':str(pathlib.Path(output).resolve())})
  subprocess.run([*launcher,'node',str(spy),'deploy','--dry-run','--config',str(pathlib.Path(source)/'wrangler.toml')],env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
 finally:spy.unlink(missing_ok=True)
 return json.loads(pathlib.Path(output).read_text())
if __name__=='__main__':capture(sys.argv[1],sys.argv[2])
