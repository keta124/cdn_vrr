from elasticsearch import Elasticsearch
from config.config_vrr import Config_vrr
class Es_query(object):
	def __init__(self):
		self.host = Config_vrr().ES_HOST
		self.port = Config_vrr().ES_PORT

	def query_es(self,index_str, body_dict):
	    try:
	      	es = Elasticsearch([{'host': self.host, 'port': self.port}])
	      	response = es.search(index=index_str, body=body_dict)
	      	return response
	    except:
	      	print 'ES QUERY'
	      	return ""

  	def query_subnet(self,query):
  		try:
  			body_ = {
			  "size": 0,
			  "query": {
			    "filtered": {
			      "query": {
			        "query_string": {
			          "query": query,
			          "analyze_wildcard": True
			        }
			      },
			      "filter": {
			        "range": {
			          "timecheck": {
			          	"gte": "now-30m"
			          }
			        }
			      }
			    }
			  },
			  "aggs": {
			    "subnet": {
			      "terms": {
			        "field": "subnet.raw",
			        "size": 5000,
			        "order": {
			          "1": "desc"
			        }
			      },
			      "aggs": {
			        "1": {
			          "max": {
			            "field": "total_request"
			          }
			        },
			        "datacenter": {
			          "terms": {
			            "field": "datacenter.raw",
			            "size": 50,
			            "order": {
			              "1": "desc"
			            }
			          },
			          "aggs": {
			            "1": {
			              "max": {
			                "field": "total_request"
			              }
			            },
			            "isp_client": {
			              "terms": {
			                "field": "isp_client.raw",
			                "size": 50,
			                "order": {
			                  "1": "desc"
			                }
			              },
			              "aggs": {
			                "1": {
			                  "max": {
			                    "field": "total_request"
			                  }
			                },
			               "percent_low": {
                  			  "avg": {
                    			"field": "percent_low"
                  			  }
                			},
                			"percent_mid": {
                  			  "avg": {
                    			"field": "percent_mid"
                  			  }
                			} 
			              }
			            }
			          }
			        }
			      }
			    }
			  }
			}
			index_ = 'statbw_customerisp-2017*'
			response=self.query_es(index_, body_)
			return response["aggregations"]["subnet"]["buckets"]
		except:
			print 'EXCEPT ES DEST QUERY'
			return []

	def map_datacenter(self,dc):
		if 'vdc1' in dc:
			return 'vdc01'
		elif 'vt1' in dc:
			return 'vt01'
		elif 'vt2' in dc:
			return 'vt02'
		elif 'fpt1' in dc:
			return 'fpt01'
		else:
			return dc		
	def parse_network(self):
		query="*"
		response = self.query_subnet(query)
		parsre_es =[]
		for i in response :
			try:
				parsre_dict ={}
				parsre_dict["network"] = i["key"]
				min_weight_stt =100
				for j in range (0,len(i["datacenter"]["buckets"])):
					percent_low = i["datacenter"]["buckets"][j]["isp_client"]["buckets"][0]["percent_low"]["value"]
					percent_mid = i["datacenter"]["buckets"][j]["isp_client"]["buckets"][0]["percent_mid"]["value"]
					weight = int((percent_low*2+percent_mid)/3)
					if weight < min_weight_stt:
						min_weight_stt = weight
						parsre_dict["customer_isp"] = i["datacenter"]["buckets"][j]["isp_client"]["buckets"][0]["key"]
						parsre_dict['datacenter'] = self.map_datacenter(i["datacenter"]["buckets"][j]["key"])
						parsre_dict["weight"]=weight
						parsre_dict["total"]=i["datacenter"]["buckets"][j]['1']['value']
				parsre_es.append(parsre_dict)
			except :
				print 'EXCEPT ES RESULT'
				return []
		return parsre_es