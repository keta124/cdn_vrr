from es_query import *
from connect_possgres import *
from datetime import datetime

VEGA_DC =['vdc01','fpt01','vt02','vt01']

class Table_datacenter(object):
    def __init__(self,table):
        self.table =table
    def load_state_dc(self,column):
        psql= Possgres_sql('cdnvrr','sontn','localhost','Son@1123')
        query = """SELECT %s FROM %s WHERE id= (SELECT id FROM %s ORDER BY id DESC limit 1);""" %(column,self.table,self.table)
        return psql.query_db(query)

class Vega_routing(object):
    def __init__(self):
        self.timenow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    def load_eable_state(self):
        # query db load current state
        psql= Possgres_sql('cdnvrr','sontn','localhost','Son@1123')
        query = """SELECT * FROM state_vrr WHERE state='False'"""
        return psql.query_db(query)
    def load_es_state(self):
        es =Es_query()
        load_es = es.parse_network()
        return load_es
    def merge_table_dc(self):
        # phase 1 : find network (load_es) in current state
        result_es = self.load_es_state()
        #print result_es
        for dc in VEGA_DC:
            rows=[]
            table_ = "dc_"+dc
            table_dc = Table_datacenter(table_)
            try:
                id_current = table_dc.load_state_dc('id')[0][0]+1
            except:
                id_current =1
            for re in result_es:
                if re["datacenter"] in dc.replace('0',''):
                    re['timestamp']=self.timenow
                    re['id']=id_current
                    re['state']='True'
            rows.append(re)
            cdn_vrr_dc = Query_cdn_vrr('cdnvrr','sontn','localhost','Son@1123')
            cdn_vrr_dc.update_state_table(table_)
            cdn_vrr_dc.insert_table_dc(table_,rows)
            # phase 2 : compare history ??? weight of network
    def merge_table_state(self):
        #phase 1 load new state
        # SELECT * FROM dc_vdc01 WHERE state='True' ORDER BY weight DESC;
        psql_dc = Possgres_sql('cdnvrr','sontn','localhost','Son@1123')
        rows=[]
        for dc in VEGA_DC:
            table_ = "dc_"+dc
            query = "SELECT (timestamp,network,weight,state,customer_isp) FROM %s WHERE state='True' ORDER BY weight DESC;" %table_
            result = psql_dc.query_db(query)
            for res in result:
                rows.append(res[0])
        print rows
        #phase 2 new route
        # ???
        # vt1 ==> vt2 ==> vdc =>fpt ==>vt1
        # occur ring => blacklist => not routing
            # select with weight min & blacklist 
            # sub_job : clear black_list everyday

        #phase 2 load current state 

        #phase compare old and new state
        #cdn_vrr_.update_state_table('state_vrr')

if __name__ == '__main__':
  vrr =Vega_routing()
  vrr.merge_table_state()
  '''
  print vrr.merge_state()
  table ="dc_vdc01"
  query = """SELECT * FROM %s WHERE id= (SELECT id FROM %s ORDER BY id DESC limit 1);""" %(table,table)
  print query
  '''