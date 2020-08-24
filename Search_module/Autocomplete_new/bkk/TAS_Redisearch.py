from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
from redis import Redis
from redisearch.auto_complete import AutoCompleter
import sys
class TAS_Redisearch():
    
    #Constructor 
    def __init__(self, table_name, host="localhost", port=6381):        
        try:
             self.client = Client(table_name, host, port)
             self.host = host
             self.port = port 
             self.table_name = table_name
             self.redis = Redis()
             self.LIMIT = 10
        except Exception as e:
	    print 'yyy'
            print >> sys.stderr, "TAS_Redisearch Error inside Constructor Index:\'", table_name, "\' HOST:\'", host, "\' PORT:\'", port, "\'\n"
            print >> sys.stderr, e 
    
    #Will set the no of results to show
    def set_result_limit(self, num):
        self.LIMIT = num
        return

    #Defines the schema for Redisearch
    def set_schema(self, schema):
        try:                               
            return self.client.create_index(schema, False, False, [])   #last empty list will ensure that default stopwords will not be ignored
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside set_schema Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print >> sys.stderr, e
    
    #Deletes index(table)
    def drop_index(self):
        try:
            return self.client.drop_index()
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside drop_index Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print >> sys.stderr, e
 
    #Deletes a document(row) by document_index
    def delete_document(self, document_index):
        try:
           return self.client.delete_document(document_index)
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside delete_document Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print >> sys.stderr, e
    

    #############################################SEARCHES BELOW####################################### 

    #Uses python libraries
    def py_search(self, query, result_limit=-1):
        if result_limit==-1:
            result_limit = self.LIMIT
        try:
            return self.client.search(Query(query).paging(0, result_limit))
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside py_search Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print sys.stderr, e
 
    #Search with default parameters [will return dictionary]
    def generic_search(self, search_text, result_limit=-1):
        if result_limit==-1:
            result_limit = self.LIMIT                        
        query_string = "FT.SEARCH "+self.table_name+" "+search_text+" LIMIT 0 "+str(result_limit)
        try:
            res = self.redis.execute_command(query_string)       
            return Result(res, True)
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside generic_search Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print >> sys.stderr, e
 
    
    def free_exact_search(self, key, result_limit=-1):        
        org_key = key
        l = []
        try:
            if result_limit==-1:
                result_limit = self.LIMIT
            key = self.clean_string(key)
            returned = self.py_search("*", result_limit)
            for result in returned.docs:
              result_dict = vars(result)
              if org_key in result_dict.values():
                l.append(result_dict)            
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside value_search Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"               
            print >> sys.stderr, e
        return l 
    
    #{fieldname:[value1, value2], fieldname:[value1, value2]}
    def exact_search(self, input_dict, result_limit=-1):
       formed_str = ""
       l = []
       for field, value_list in input_dict.items():
            formed_str += "@"+field+":("
            for key in value_list:
                key = self.clean_string(key)
                formed_str += "(\'"+key+"\') | "
            formed_str = formed_str.rstrip(' |')
            formed_str += ") "
       print "PASSED: ", formed_str
       returned = self.py_search(formed_str, result_limit)
       print "RETURNED:", returned
       for result in returned.docs:
            result_dict = vars(result)
            for itr, ktr in input_dict.items():            
                if result_dict[itr] in ktr:
                    l.append(result_dict)
        
       return l
             
        
    
        
 
    #Search with the passed query
    def custom_search(self, query_string):
        try:
            res = self.redis.execute_command(query_string)
            return Result(res, True)
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside custom_search Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print >> sys.stderr, e
 
    #Search in 'search_in_field' [if any of the element in 'list_to_union' is found then include it in the result
    def union_search(self, list_to_union, search_in_field):
        query_string = "FT.SEARCH "+self.table_name+" "
        union_text = "@"+search_in_field+":("
        for text in list_to_union:
            union_text +=  text + "|"
        
        union_text = union_text.rstrip("|")
        union_text += ")"
        query_string += union_text
        try:
           res = self.redis.execute_command(query_string)
           return Result(res, True)
        except Exception as e:
            print >> sys.stderr, "TAS_Redisearch Error inside union_search Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print >> sys.stderr, e
    
    #will return all the dictionary for all the categories if no arguments are passed
    def category_taxonomy_dict(self, category='*'):
       try:
           cat_taxo_dict = {}
           total_docs =  self.client.info()['num_docs']
           query_string = "";
           if category == '*':
               query_string = category       
           else:
               query_string = "@CATEGORY:"+category
           result = self.py_search(query_string, total_docs)
           for single_result in result.docs:
                try:
                    category = single_result.CATEGORY
                    taxoname = single_result.TAXONAME
                except Exception as ex:
                    pass
                if not category in cat_taxo_dict:
                    cat_taxo_dict[category] = []
                elif taxoname not in cat_taxo_dict[category]:
                    cat_taxo_dict[category].append(taxoname)
       except Exception as e:            
            sys.stderr, "TAS_Redisearch Error inside category_taxonomy_dict Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
            print >> sys.stderr, e
       return cat_taxo_dict
        
    
    def total_record(self):
        try:
            return int(self.client.info()['num_docs'])    
        except Exception as e:
            sys.stderr, "TAS_Redisearch Error inside total_records Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"                
            print >> sys.stderr, e
    

    def get_all_records(self):
        try:
         total = str(self.total_record())
         res = self.redis.execute_command("FT.SEARCH "+self.table_name+" * LIMIT 0 "+total)
         return Result(res, True)   
        except Exception as e:
           sys.stderr, "TAS_Redisearch Error inside total_records Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
           print >> sys.stderr, e

    def clean_string(self, key):
        key = key.replace(',', ' ')
        key = key.replace('.', ' ')
        key = key.replace('<', ' ')
        key = key.replace('>', ' ')
        key = key.replace('{', ' ')
        key = key.replace('}', ' ')
        key = key.replace('[', ' ')
        key = key.replace(']', ' ')
        key = key.replace('"', ' ')
        key = key.replace('\'', ' ')
        key = key.replace(':', ' ')
        key = key.replace(';', ' ')
        key = key.replace('!', ' ')
        key = key.replace('@', ' ')
        key = key.replace('#', ' ')
        key = key.replace('$', ' ')
        key = key.replace('%', ' ')
        key = key.replace('^', ' ')
        key = key.replace('&', ' ')
        key = key.replace('*', ' ')
        key = key.replace('(', ' ')
        key = key.replace(')', ' ')
        key = key.replace('-', ' ')
        key = key.replace('+', ' ')
        key = key.replace('=', ' ')
        key = key.replace('~', ' ')

        return key
        


    #############################AUTO SUGGESTIONS###############################

class TAS_Autocompleter:

        #constructor
        def __init__(self, autocomplete_name, host='localhost', port=6379):
            try:
                self.host = host
                self.port = port
                self.table_name = autocomplete_name
                self.ac = AutoCompleter(autocomplete_name, host, port)
            except Exception as e:
                print >> sys.stderr, "TAS_Autocompleter Error inside constructor Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
                print >> sys.stderr, e
        
        #will add the list to auto-complete entries in the respective name        
        def add_to_autocomplete(self, new_words):
            try:
                for word in new_words:
                    self.ac.add_suggestions(Suggestion(word, 1.0))   
            except Exception as e:
                print >> sys.stderr, "TAS_Autocompleter Error inside add_to_autocomplete Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
                print >> sys.stderr, e
    
        #not working as is intented, researching more...
        #btw, will add all the words in autocomplete dictionary
        def add_to_autocomplete_combination(self, new_words):
            try:
                for word in new_words:
                    splitted = word.split(' ')
                    splittedLength = len(splitted)
                    for index in range(0, splittedLength):
                        toAdd = ' '.join(splitted[index:splittedLength])
                        self.ac.add_suggestions(Suggestion(toAdd, 1.0)) 

            except Exception as e:
                print >> sys.stderr, "TAS_Autocompleter Error inside add_to_autocomplete_combination Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"   
                print >> sys.stderr, e  

         

        #will return auto-suggestions for the give prefix 
        def suggest(self, prefix):
            try:
                return self.ac.get_suggestions(prefix, fuzzy=True)
            except Exception as e:
                print >> sys.stderr, "TAS_Autocompleter Error inside suggest Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
                print >> sys.stderr, e

        def delete(self, prefix):
            try:
                return self.ac.delete(prefix)
            except Exception as e:
                print >> sys.stderr, "TAS_Autocompleter Error inside delete Index:\'", self.table_name, "\' HOST:\'", self.host, "\' PORT:\'", self.port, "\'\n"
                print >> sys.stderr, e            




