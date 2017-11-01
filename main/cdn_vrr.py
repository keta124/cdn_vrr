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

class Vega_routing(object):
    def __init__(self):
        self.timenow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.psql = Query_cdn_vrr(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])

    def load_es_state(self):
        es =Es_query()
        load_es = es.parse_network()
        return load_es

    def load_vrr_info(self,table):
        query = "SELECT network,datacenter,weight,state,timestamp,customer_isp FROM " +table+" WHERE state ='True'"
        rows = self.psql.query_db(query)
        rows_map =list(map(lambda x: {
                                'network':x[0],
                                'datacenter':x[1],
                                'weight':x[2],
                                'state':x[3],
                                'timestamp':x[4],
                                'customer_isp':x[5]},rows))
        return dict(map(lambda x:(x['network'],x),rows_map ))

    def merge_table_state(self):
        result_es = self.load_es_state()
        rows = list(map(lambda x:{
                                    'network':x['network'],
                                    'datacenter':x['datacenter'],
                                    'state':'True',
                                    'timestamp':self.timenow,
                                    'weight':x['weight'],
                                    'customer_isp':x['customer_isp']
                                    },result_es))
        self.psql.update_state_column('state_vrr','')
        self.psql.insert_state_vrr('state_vrr',rows)
        # map new_route & filter
        rows_map_new_route = list(map(Routing().map_new_route,rows))
        rows_map_filter = list(filter(lambda x:x['weight']>50,rows_map_new_route))
        rows_for_add = dict(map(lambda x:(x['network'],x),rows_map_filter ))
        # load old vrr_info
        rows_enabled =  self.load_vrr_info('vrr_info')
        #update
        rows_enabled.update(rows_for_add)
        # get key,values
        rows_enabled_ = rows_enabled.items()
        rows_write = list(map(lambda x:x[1],rows_enabled_))
        # 
        self.psql.update_state_column('vrr_info','')
        self.psql.insert_state_vrr('vrr_info',rows_write)

    def recomment_vrr(self,table):
        query = "SELECT network,datacenter FROM " +table+" WHERE state ='True'"
        result = self.psql.query_db(query)
        return result

if __name__ == '__main__':
    vrr =Vega_routing()
    vrr.merge_table_state()
    list_write = vrr.recomment_vrr('vrr_info')
    list_write_map = list(map(lambda x: ''+str(x[0])+' => '+str(x[1]),list_write))
    list_write_map_mode = list(map(lambda x: ''+str(x[0])+','+str(x[1]).replace('01','-01').replace('02','-02'),list_write))
    File_RW('vrr_info_origin.txt',list_write_map).writefile()
    File_RW('vrr_info.txt',list_write_map_mode).writefile()