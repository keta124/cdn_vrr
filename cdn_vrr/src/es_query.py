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
			            "gte": "now-30m",
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
	def parse_network(self):
		query="(percent_low:>40) OR (percent_low:>30 AND percent_mid:>50) OR (percent_low:>20 AND percent_mid:>70) OR (percent_low:>10 AND percent_mid:>90) OR (percent_mid:>99)"
		response = self.query_subnet(query)
		parsre_result =[]
		for i in response:
			parsre_dict ={}
			parsre_dict["total"] = int(i["1"]["value"])
			parsre_dict["network"] = i["key"]
			try:
				parsre_dict["datacenter"] = i["datacenter"]["buckets"][0]["key"]
			except:
				parsre_dict["datacenter"]=""
			try:
				parsre_dict["customer_isp"] = i["datacenter"]["buckets"][0]["isp_client"]["buckets"][0]["key"]
			except:
				parsre_dict["customer_isp"]=""
			try:
				percent_low = i["datacenter"]["buckets"][0]["isp_client"]["buckets"][0]["percent_low"]["value"]
			except:
				percent_low = 0
			try:	
				percent_mid = i["datacenter"]["buckets"][0]["isp_client"]["buckets"][0]["percent_mid"]["value"]
			except:
				percent_mid=0
			weight = int((percent_low*2+percent_mid)/3)
			parsre_dict["weight"]=weight
			parsre_result.append(parsre_dict)
		return parsre_result
