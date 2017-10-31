from datetime import datetime
from datetime import timedelta
from config.config_vrr import Config_vrr
from src.connect_possgres import *
VEGA_DC =Config_vrr().DATECENTER
PSQL_CONFIG = Config_vrr().PSQL_CONFIG

class Routing(object):
    def __init__(self):
        self.psql =Query_cdn_vrr(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
    def new_route(self,datacenter):
        if "vdc01" in datacenter:
            return 'vt01'
        elif "vt01" in datacenter:
            return'vt02'
        elif "vt02" in datacenter:
            return 'fpt01'
        elif "fpt01" in datacenter:
            return'vdc01'
    def map_new_default(self,row):
        row['datacenter'] =self.new_route(row['datacenter'])
        return row
    def avg_weight(self,row,no_hour):
        network = row['network']
        datacenter = row['datacenter']
        query = "SELECT weight FROM state_vrr WHERE (datacenter ='%s' AND network = '%s' AND timestamp >= '%s'::timestamp)" %(datacenter,network,datetime.now()-timedelta(hours=no_hour)) 
        rows_query = self.psql.query_db(query)
        weight_map = map(lambda x:x[0],rows_query)
        try:
            weight_reduce = reduce(lambda x,y:x+y,weight_map)
        except:
            weight_reduce =0
        return [weight_reduce,len(weight_map)]

    def map_new_route(self,row):
        weight_2h = self.avg_weight(row,2)
        avg_weight_2h = (row['weight']*2 + weight_2h[0]*weight_2h[1] ) / (weight_2h[1]+2)
        #
        weight_1d = self.avg_weight(row,24)
        avg_weight_1d = (row['weight']*2 + weight_1d[0]*weight_1d[1] ) / (weight_2h[1]+2)
        #
        if (row['weight'] >50 and (avg_weight_2h >30 or avg_weight_1d >20) and weight_1d[1]>3 and weight_2h[1]>2 ):
            # check ring
            network = row['network']
            query = "SELECT datacenter FROM vrr_log WHERE (network = '%s' AND timestamp >= '%s'::timestamp)" %(network,datetime.now()-timedelta(hours=24)) 
            rows_query = self.psql.query_db(query)
            rows_query_map = list(map(lambda x: x[0],rows_query))
            if not set(VEGA_DC).issubset(set(rows_query_map)):
                self.psql.insert_vrr_log('vrr_log',row)
                return self.map_new_default(row)
        else :
            row['weight'] =0
            return row