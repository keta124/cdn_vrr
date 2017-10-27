import sys
import  os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from src.network_subnet import *
from config.config_vrr import Config_vrr
from src.connect_possgres import *
from datetime import datetime

class File_origin(object):
	def __init__(self,path):
		self.path =path
	def remove_char(self,origin):
		char =[' ','\t','=','>',',',']']
		for i in char:
			origin =origin.replace(i,'')
		return origin
	def read_file(self):
		f= open(self.path, "r")
		lines = f.read().splitlines()
		result =[]
		for line in lines:
			if line and(line[0] is not '#'):
				line_ = self.remove_char(line)
				result.append(line_.split('['))
		return result

def load_file_origin():
	file = File_origin('VegaGeoISP')
	network = Network()
	rows = file.read_file()
	result =[]
	for row in rows :
		nw = network.devide_subnet(row[0],24)
		# map
		for nw_ in nw:
			result.append([nw_,row[1]])
	result_ =[]
	result_nw =[]
	for i in result:
		if i[0] not in result_nw:
			result_.append(i)
			result_nw.append(i[0])
	return result_

def load_first_time():
  table = Config_vrr().TABLE_STATE
  PSQL_CONFIG = Config_vrr().PSQL_CONFIG
  timenow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  load_file = load_file_origin()
  rows =list(map(lambda x: {
                          'network':x[0],
                          'customer_isp':'VEGA',
                          'weight':0,
                          'state':'True',
                          'timestamp':timenow,
                          'datacenter':x[1]},load_file))
  psg = Query_cdn_vrr(PSQL_CONFIG[0],PSQL_CONFIG[1],PSQL_CONFIG[2],PSQL_CONFIG[3])
  psg.update_state_table(table)
  psg.insert_state_vrr(table,rows)
if __name__ == '__main__':
	load_first_time()