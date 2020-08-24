from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
from redis import Redis
from redisearch.auto_complete import AutoCompleter
import sys
import TAS_Redisearch
class TAS_Import():
    def __init__(self, index_name, host="localhost", port=6381, db=0):
	pass
	
    def add_data(self,data,rsObj):
        for i,rr in enumerate(data): 
            index = i + 1
            name,age,location = rr
            rsObj.client.add_document(INDEX=index,  NAME= name,  AGE=age,  LOCATION=location)
	   
if __name__ == "__main__":
    obj = TAS_Redisearch.TAS_Redisearch("USERS",'localhost','6381')
    obj.drop_index()
    res = obj.set_schema([NumericField('INDEX'),TextField('NAME'), TextField('AGE') , TextField('LOCATION')])
    f = open("input.txt","r")
    dd = json.loads(f.read())
    data_red = obj.add_data(dd)
            

    
