from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
from redis import Redis
from redisearch.auto_complete import AutoCompleter
import sys,json
import redis_config
config_obj = redis_config.TAS_AutoCompleter('172.16.20.7','6382','0',"Default") 
class TAS_Autocomplete():
    def __init__(self,autocomplete_name,host="172.16.20.7", port=6382, db=0):
    	self.autocomplete = AutoCompleter(autocomplete_name, host, port)

    def create_auto_complete(self,res):
	for word in res:
        	self.autocomplete.add_suggestions(Suggestion(word, 1.0))
	

    def suggest(self, keyword):
        return self.autocomplete.get_suggestions(keyword)
        #return self.autocomplete.get_suggestions(keyword,fuzzy=True)

    def delete_auto_complete(self,key):
	self.autocomplete.delete(key)

if __name__ == "__main__":
    obj = TAS_Autocomplete('deafult','172.16.20.7','6382','0')
