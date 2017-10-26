#from config.config_vrr import Config_vrr
from collections import namedtuple

class Routing(object):
	"""docstring for Routing"""
	def __init__(self):
		pass

class Map_route(object):
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
            return'vt02'
    def map_new_route(self,row):
    	#fields = ('timestamp','network','requesttime','weight','state','customer_isp','datacenter')
        #Table_dc = namedtuple('Table_dc',fields)
        row['datacenter'] =Map_route().new_route(row['datacenter'])
        return row

if __name__ == '__main__':
	y = [{'a':{'a':1,'b':2,'datacenter':'vdc01'}},{'b':{'a':1,'b':2,'datacenter':'vt01'}}]
	print x[0].values()
	y = dict(map(lambda x: {x.keys():x.values().Map_route().map_new_route()},y))
	#y = list(map(lambda x: x,y))
	print y