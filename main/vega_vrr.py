import sys
import  os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from src.es_query import *
from src.connect_possgres import *
from config.config_vrr import Config_vrr
from datetime import datetime

## Load config
VEGA_DC =Config_vrr().DATECENTER
PSQL_CONFIG = Config_vrr().PSQL_CONFIG

class Table_datacenter(object):
    def __init__(self,table):
        self.table =table
    def load_state_dc(self,column):
        psql= Possgres_sql(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
        query = """SELECT %s FROM %s WHERE id= (SELECT id FROM %s ORDER BY id DESC limit 1);""" %(column,self.table,self.table)
        return psql.query_db(query)

class Vega_routing(object):
    def __init__(self):
        self.timenow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    def load_eable_state(self):
        psql= Possgres_sql(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
        query = """SELECT network,datacenter,weight,state,timestamp,customer_isp FROM state_vrr WHERE state='True'"""
        rows= psql.query_db(query)
        rows =list(map(lambda x: {
                                'network':x[0],
                                'datacenter':x[1],
                                'weight':x[2],
                                'state':x[3],
                                'timestamp':x[4],
                                'customer_isp':x[5]},rows))
        return rows
    def load_es_state(self):
        es =Es_query()
        load_es = es.parse_network()
        return load_es
    def merge_table_dc(self):
        result_es = self.load_es_state()
        cdn_vrr_dc = Query_cdn_vrr(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
        for dc in VEGA_DC:
            rows=[]
            table_ = "dc_"+dc
            table_dc = Table_datacenter(table_)
            try:
                id_current = table_dc.load_state_dc('id')[0][0]+1
            except:
                id_current =1
            result_es_ = list(filter(lambda x: (x["datacenter"] in dc.replace('0','')),result_es))
            rows = list(map(lambda x:{
                                    'network':x['network'],
                                    'datacenter':x['datacenter'],
                                    'id':id_current,
                                    'state':'True',
                                    'timestamp':self.timenow,
                                    'weight':x['weight'],
                                    'customer_isp':x['customer_isp']
                                    },result_es_))
            cdn_vrr_dc.update_state_table(table_)
            cdn_vrr_dc.insert_table_dc(table_,rows)

    def merge_table_state(self):
        #phase 1 load new state
        # SELECT * FROM dc_vdc01 WHERE state='True' ORDER BY weight DESC;
        psql = Possgres_sql(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
        rows_dc=[]
        for dc in VEGA_DC:
            table_ = "dc_"+dc
            query = "SELECT * FROM %s WHERE state='True' ORDER BY weight DESC;" %table_
            result = psql.query_db(query)
            result_ = list(map(lambda x: {
                                        'timestamp':x[1],
                                        'network':x[2],
                                        'weight':x[3],
                                        'state':x[4],
                                        'customer_isp':x[5],
                                        'datacenter':table_.replace('dc_','')},result))
            rows_dc = rows_dc+result_
        # map dict with key network
        rows_dc_ = dict(map(lambda x:(x['network'],x),rows_dc ))
        print rows_dc_
        # Phase 2: Modify route
        # OK
        # vt1 => vt2 =>fpt =>vdc
        # Phase 2 :load current state 
        rows_state =self.load_eable_state()
        rows_state_ = dict(map(lambda x:(x['network'],x),rows_state ))
        rows_state_.update(rows_dc_)
        #print rows_state_
        # phase 3: merge
        # 3.1 find key
class Merge_row(object):
    def __init__(self):
        pass


if __name__ == '__main__':
    vrr =Vega_routing()
    vrr.merge_table_dc()
