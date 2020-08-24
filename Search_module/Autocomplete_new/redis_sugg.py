from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
import redis
from redisearch.auto_complete import AutoCompleter
import ConfigParser
config_path = "/var/www/cgi-bin/INC_Interface/pysrc_08_07_19/Config.ini"
config = ConfigParser.ConfigParser()
config.read(config_path)
mount_path = config.get('mount_path', 'value')
redis_info = config.get('redis_search', 'user_storage')
ip,port,db = redis_info.split('##')

class TTAutoCompleter:
    def __init__(self, host=ip, port=port, db=db, autocomplete_name='Default'):
        self.ipAdd = host 
        self.ipPort  = port
        self.db = db
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
    obj = TTAutoCompleter('172.16.20.7','6382','0',"NameSearch")
    obj.create_auto_complete(["Mrer","S"]) #{"name": "Mrer"},{"name":"S"}])
    res = obj.suggest("M")
    print res 
