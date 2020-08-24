from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
from redis import Redis
from redisearch.auto_complete import AutoCompleter
import sys,json
import redis_config
class TAS_Import():
    def __init__(self,index_name,host="172.16.20.7", port=6382, db=0):
        self.client = Client(index_name,host,port)
        self.host = host
        self.port = port
        self.config_obj = redis_config.TAS_AutoCompleter(host,port,db,"Default") 
        #self.redis = Redis()

    def add_indexing_schema(self,schema):
        self.client.create_index(schema,False, False, [])
        return ["Done"]

    def add_data(self,rdata,index_name):
        for i,rr in enumerate(rdata): 
	    #print  rr,type(rr[2])
	    l1,l2,l3,l4,l5,l6,l7, l8, l9 = rr
            index = index_name+str(i+1)+l3+l4+l5+l6
            #print 'index_name', index_name, index, l3, l4, l5, l6 
	    l1 = self.config_obj.StringEscape(l1) 
	    l2 = l2.strip()
            self.client.add_document(index, DATA=l1, SECTION_TYPE=l2, DOCID=l3, PAGE=l4, GRIDID=l5, ROWCOL=l6, BBOX=l7, PAGE_GRID_SE="%s_%s_%s"%(l4,l5,l2), Rowspan=l8, Colspan=l9)
        return ["Done"]

    def drop_index(self):
	try:
        	self.client.drop_index()
	except Exception as e:
		print 'Error',e
		pass

    def start(self,data,index_name):	
	status = 1
    	self.drop_index()
        self.client = Client(index_name,self.host,self.port)
	status = 2
        schema = [NumericField('INDEX'),TextField('DATA'), TextField('SECTION_TYPE'), TextField('DOCID'),TextField('PAGE'), TextField('GRIDID'), TextField("ROWCOL"), TextField('BBOX'), TextField("PAGE_GRID_SE"), TextField('Rowspan'), TextField('Colspan')]	
	#rsObj.set_schema([NumericField('INDEX'), TextField('DOCID'), TextField('CATEGORY'), TextField('TAXONAME'), TextField('VALUE'), TextField('XML_REF'), TextField('REF_KEY')])
	status = 3
	self.add_indexing_schema(schema)
	status = 4
	self.add_data(data, index_name)
	status = 5
	return [status]
	
        
if __name__ == "__main__":
    redis_info = config.get('redis_search', 'user_storage')
    ip,port,db = redis_infos.split('##')
    obj = TAS_Import(ip,port,db)
    obj.start([],'922')
