from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
from redis import Redis
from redisearch.auto_complete import AutoCompleter
import sys,json
class TAS_Import():
    def __init__(self, index_name, host="172.16.20.7", port=6382, db=0):
        self.client = Client(index_name,host,port)
        self.host = host
        self.port = port
        self.index_name = index_name
        self.redis = Redis()
    
    def add_indexing(self,schema):
        self.client.create_index(schema, False, False, [])
        return ["Done"]

    def add_data(self,data):
        for i,rr in enumerate(data): 
            index = i + 1
	    print rr
            name,age,location = rr['name'],rr['age'],rr['location']
            self.client.add_document(index,  NAME= name,  AGE=age,  LOCATION=location)
        return ["Done"]

    def drop_index(self):
	try:
        	self.client.drop_index()
	except:
		pass
        
if __name__ == "__main__":
    obj = TAS_Import("USERS",'172.16.20.7','6382','0')
    obj.drop_index()
    obj = TAS_Import("USERS",'172.16.20.7','6382','0')
    res = obj.add_indexing([TextField('NAME',weight=5.0), TextField('AGE') , TextField('LOCATION')])
    f = open("input.txt","r")
    dd = json.loads(f.read())
    data_red = obj.add_data(dd)
            

    
