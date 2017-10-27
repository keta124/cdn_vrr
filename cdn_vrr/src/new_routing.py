from datetime import datetime
from datetime import timedelta
from config.config_vrr import Config_vrr
from src.connect_possgres import *
VEGA_DC =Config_vrr().DATECENTER
PSQL_CONFIG = Config_vrr().PSQL_CONFIG

class Routing(object):
    def __init__(self):
        pass
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
    def map_new_route(self,row):
        network = row['network']
        psql =Possgres_sql(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
        query = "SELECT datacenter,weight FROM state_vrr WHERE (network = '%s' AND timestamp >= '%s'::timestamp)" %(network,datetime.now()-timedelta(days=1)) 
        rows_query = psql.query_db(query)
        rows_query_ = dict(map(lambda x: (x[0],x[1]),rows_query))
        rows_network = str(map(lambda x: x[0], rows_query))
        if not set(VEGA_DC).issubset(set(rows_network)):
            return self.map_new_default(row)
        else :
            result =[row['datacenter'],100]
            for element in rows_query_:
                if int(element.values()) < int(result[1]):
                    result=[element.keys,int(element.values)]
            row['datacenter'] = result[0]
            row['weight'] = result[1]
            return row