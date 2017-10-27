import sys
import  os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from src.es_query import *
from src.connect_possgres import *
from config.config_vrr import Config_vrr
from datetime import datetime
from src.new_routing import *
from src.file_rw import File_RW
import copy
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
        self.psql_ = Query_cdn_vrr(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
        self.psql = Possgres_sql(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
    def load_eable_state(self):
        query = """SELECT network,datacenter,weight,state,timestamp,customer_isp FROM state_vrr WHERE state='True'"""
        rows= self.psql.query_db(query)
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
            cdn_vrr_dc.update_state_column(table_,'')
            cdn_vrr_dc.insert_table_dc(table_,rows)
    def merge_table_state(self):
        #phase 1 load new state
        rows_dc=[]
        for dc in VEGA_DC:
            table_ = "dc_"+dc
            query = "SELECT * FROM %s WHERE state='True' ORDER BY weight DESC;" %table_
            result = self.psql.query_db(query)
            result_ = list(map(lambda x: {
                                        'timestamp':x[1],
                                        'network':x[2],
                                        'weight':x[3],
                                        'state':x[4],
                                        'customer_isp':x[5],
                                        'datacenter':table_.replace('dc_','')},result))
            rows_dc = rows_dc+result_
        rows_dc_copy = copy.deepcopy(rows_dc)
        rows_state =self.load_eable_state()
        #
        rows_dc_origin = dict(map(lambda x:(x['network'],x),rows_dc ))
        # map new route
        rows_dc_list = list(map(Routing().map_new_route,rows_dc_copy))
        rows_dc_map = dict(map(lambda x:(x['network'],x),rows_dc_list ))
        #
        #
        rows_state_origin = dict(map(lambda x:(x['network'],x),rows_state ))
        rows_state_ = copy.deepcopy(rows_state_origin)
        #
        rows_state_.update(rows_dc_map)
        rows_state_origin.update(rows_dc_origin)
        #
        rows_state_list = rows_state_.items()
        rows_state_origin_list = rows_state_origin.items()
        # update rows_state_origin table state_vrr
        rows_state_map = list(map(lambda x:x[1],rows_state_list))
        rows_state_origin_map = list(map(lambda x:x[1],rows_state_origin_list))
        #
        self.psql_.update_state_column('state_vrr','')
        self.psql_.insert_state_vrr('state_vrr',rows_state_origin_map)
        # update rows_state_origin table new_route_vrr
        self.psql_.update_state_column('new_route_vrr','')
        self.psql_.insert_state_vrr('new_route_vrr',rows_state_map)

    def recomment_vrr(self,table):
        query = "SELECT network,datacenter FROM " +table+" WHERE state ='True'"
        result = self.psql.query_db(query)
        return result

if __name__ == '__main__':
    vrr =Vega_routing()
    vrr.merge_table_dc()
    vrr.merge_table_state()
    list_write = vrr.recomment_vrr('new_route_vrr')
    list_write_map = list(map(lambda x: ''+x[0]+' => '+x[1],list_write))
    File_RW('vrr_info',list_write_map).writefile()