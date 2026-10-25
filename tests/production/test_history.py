import copy,pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'ops/production'))
from history import complete_history
from authorization import Rejected
def fixture(n=97):
 rows=[{'id':f'{i:08x}-1111-2222-3333-444444444444','created_on':'2026-10-22T20:35:19.384411Z','versions':[{'version_id':'11111111-1111-2222-3333-444444444444','percentage':100}]} for i in range(n)]
 return [{'success':True,'errors':[],'result':{'deployments':rows[i:i+100]},'result_info':{'page':i//100+1,'per_page':100,'total_count':n,'total_pages':(n+99)//100,'count':len(rows[i:i+100])}}for i in range(0,n,100)]
class History(unittest.TestCase):
 def test_97(self):
  pages=fixture();calls=[]
  result=complete_history(lambda p:(calls.append(p),copy.deepcopy(pages[p-1]))[1])
  self.assertEqual(len(result['deployments']),97);self.assertEqual(calls,[1,1])
 def test_ten_pages(self):
  pages=fixture(1000);calls=[]
  self.assertEqual(len(complete_history(lambda p:(calls.append(p),copy.deepcopy(pages[p-1]))[1])['deployments']),1000)
  self.assertEqual(calls,list(range(1,11))*2)
 def test_invalid(self):
  mutations=[lambda p:p[0].update(success=False),lambda p:p[0].update(errors=[{}]),lambda p:p[0]['result_info'].update(page=2),lambda p:p[0]['result_info'].update(count=1),lambda p:p[0]['result_info'].update(total_pages=11),lambda p:p[0]['result_info'].update(total_count=0),lambda p:p[0]['result']['deployments'].pop(),lambda p:p[0]['result']['deployments'].__setitem__(1,p[0]['result']['deployments'][0]),lambda p:p[0]['result']['deployments'][0]['versions'][0].update(percentage=99),lambda p:p[0]['result']['deployments'][0].update(created_on='wrong')]
  for mutate in mutations:
   with self.subTest(mutate=mutate):
    pages=fixture();mutate(pages)
    with self.assertRaises((Rejected,ValueError)):complete_history(lambda p:copy.deepcopy(pages[p-1]))
 def test_second_sweep_drift(self):
  pages=fixture();calls=[]
  def read(p):
   calls.append(p);v=copy.deepcopy(pages[p-1])
   if len(calls)>1:v['result']['deployments'][0]['versions'][0]['version_id']='22222222-1111-2222-3333-444444444444'
   return v
  with self.assertRaises(Rejected):complete_history(read)
 def test_missing_page_and_response_loss_not_retried(self):
  calls=[]
  def read(p):calls.append(p);raise OSError('lost response')
  with self.assertRaises(OSError):complete_history(read)
  self.assertEqual(calls,[1])
