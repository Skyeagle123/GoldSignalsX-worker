"""Deterministic exact-byte patch. Never run candidate-controlled code."""
import hashlib,json,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parent
UPSTREAM='3ddc7ba0e400b3ad75e49a4bb18dc9b6b6727873920efe4797940b67bdbb790a'
def patch(path):
 b=path.read_bytes()
 if hashlib.sha256(b).hexdigest()!=UPSTREAM:raise ValueError('unexpected upstream CLI bytes')
 s=b.decode()
 start=s.index('async function performApiFetch(');end=s.index('__name(performApiFetch,',start)
 replacement='''async function performApiFetch(resource, init2 = {}, queryParams, abortSignal) {
  return await require("../../../../request-gate.cjs").fetch(resource, init2, queryParams, abortSignal, import_undici3.Response);
}
'''
 original=s[start:end]
 if s.count(original)!=1:raise ValueError('patch cardinality')
 s=s[:start]+replacement+s[end:]
 start=s.index('async function retryOnAPIFailure(');end=s.index('__name(retryOnAPIFailure,',start)
 s=s[:start]+'''async function retryOnAPIFailure(action, backoff = 0, attempts = MAX_ATTEMPTS) {
  return await action();
}
'''+s[end:]
 path.write_bytes(s.encode())
 return {'upstream_cli_sha256':UPSTREAM,'patched_cli_sha256':hashlib.sha256(s.encode()).hexdigest(),'patcher_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()}
if __name__=='__main__':print(json.dumps(patch(ROOT/'toolchain/node_modules/wrangler/wrangler-dist/cli.js'),sort_keys=True))
