
from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
from redis import Redis
from redisearch.auto_complete import AutoCompleter
import sys,json
import redis_config
import ConfigParser
config_path = "/var/www/cgi-bin/INC_Interface/pysrc_08_07_19/Config.ini"
config = ConfigParser.ConfigParser()
config.read(config_path)
mount_path = config.get('mount_path', 'value')
redis_info = config.get('redis_search', 'user_storage')
ip,port,db = redis_info.split('##')

config_obj = redis_config.TAS_AutoCompleter(ip,port,db,"Default") 
class TAS_Import():
    def __init__(self,index_name,host=ip, port=port, db=db):
        self.client = Client(index_name,host,port)
        self.host = host
        self.port = port
        #self.redis = Redis()

    def add_indexing_schema(self,schema):
        self.client.create_index(schema,False, False, [])
        return ["Done"]

    def add_data(self,rdata,company,doc_id,project):
        for i,rr in enumerate(rdata): 
            index = doc_id+company+"CMDIC"+str(i + 1)+project
	    l1,l2,l3 = rr
	    l1 = config_obj.StringEscape(l1) 
            self.client.add_document(index,DATA=l1,PAGE=l2,BBOX=l3)
        return ["Done"]

    def drop_index(self):
	try:
        	self.client.drop_index()
	except Exception as e:
		#print 'Error',e
		pass

    def start(self,data,doc_id,company,project):	
	status = 1
	index_name = project+"_DOCUMENT_"+str(doc_id)
    	self.drop_index()
        self.client = Client(index_name,self.host,self.port)
	status = 2
        schema = [NumericField('INDEX'),TextField('DATA'),TextField('PAGE'),TextField('BBOX')]	
	status = 3
	self.add_indexing_schema(schema)
	status = 4
	self.add_data(data,company,doc_id,project)
	status = 5
	return [status]
	
        
if __name__ == "__main__":
    redis_info = config.get('redis_search', 'user_storage')
    ip,port,db = redis_info.split('##')
    obj = TAS_Import(ip,port,db)
    obj.start([],'922')
