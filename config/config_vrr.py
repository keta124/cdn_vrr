TABLE_STATE ='state_vrr'
DATECENTER =['vdc01','fpt01','vt01','vt02']
PSQL_CONFIG =['cdnvrr','vega','localhost','Hethong@2017Veg@']
ES_HOST ='192.168.142.101'
ES_PORT =9200
WHITE_LIST_HOST = ['103.216.120.0/22','125.212.193.0/24']
class Config_vrr(object):
    def __init__(self):
        self.TABLE_STATE =TABLE_STATE
        self.DATECENTER=DATECENTER
        self.PSQL_CONFIG=PSQL_CONFIG
        self.ES_HOST=ES_HOST
        self.ES_PORT=ES_PORT
        self.WHITE_LIST_HOST=WHITE_LIST_HOST