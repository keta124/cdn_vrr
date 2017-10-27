TABLE_STATE ='state_vrr'
DATECENTER =['vdc01','fpt01','vt01','vt02']
PSQL_CONFIG =['cdnvrr','sontn','localhost','Son@1123']
ES_HOST ='192.168.142.105'
ES_PORT =9200

class Config_vrr(object):
    def __init__(self):
        self.TABLE_STATE =TABLE_STATE
        self.DATECENTER=DATECENTER
        self.PSQL_CONFIG=PSQL_CONFIG
        self.ES_HOST=ES_HOST
        self.ES_PORT=ES_PORT
