"""Bounded complete history; two identical sweeps or refusal. No transport retries."""
import math,re,datetime as dt
from authorization import need,canonical,stamp
UUID=re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z')
MAX_PAGES=10
def complete_history(read):
 def sweep():
  rows=[];seen=set();total=None;pages=None;last=None
  for page in range(1,MAX_PAGES+1):
   v=read(page);info=v.get('result_info',{})
   need(v.get('success') is True and v.get('errors')==[],'history envelope')
   for k in ('page','per_page','total_count','total_pages','count'):
    need(type(info.get(k)) is int,'history pagination schema')
   need(info['page']==page and info['per_page']==100 and not info.get('cursor'),'history page identity')
   n=info['total_count'];p=info['total_pages']
   need(0<n<=1000 and p==math.ceil(n/100) and p<=MAX_PAGES,'history bounds')
   if total is None:total,pages=n,p
   need((n,p)==(total,pages),'history pagination drift')
   batch=v.get('result',{}).get('deployments')
   need(type(batch) is list and len(batch)==info['count']==min(100,total-len(rows)),'history incomplete page')
   for row in batch:
    need(type(row) is dict and type(row.get('id')) is str and UUID.fullmatch(row['id']) and row['id'] not in seen,'history duplicate/identity')
    seen.add(row['id']);date=row.get('created_on');need(type(date) is str and re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z',date),'history timestamp');created=dt.datetime.fromisoformat(date.replace('Z','+00:00')).timestamp()
    need(last is None or created<=last,'history ordering');last=created
    versions=row.get('versions');need(type(versions) is list and bool(versions),'history versions')
    ids=set();traffic=0
    for version in versions:
     vid=version.get('version_id');percent=version.get('percentage')
     need(type(vid) is str and UUID.fullmatch(vid) and vid not in ids,'history version identity');ids.add(vid)
     need(type(percent) in (int,float) and 0<=percent<=100,'history traffic');traffic+=percent
    need(traffic==100,'history traffic total');rows.append(row)
   if page==pages:
    need(len(rows)==total,'history total');return {'deployments':rows}
  raise ValueError('history page bound')
 first=sweep();second=sweep()
 need(canonical(first)==canonical(second),'history snapshot changed')
 return first
