from redisearch import Client, TextField, NumericField, Query, Result, AutoCompleter, Suggestion
import redis,json
from redisearch.auto_complete import AutoCompleter
import re
class TAS_AutoCompleter:
    def __init__(self, host='172.16.20.7', port=6382, db=0, autocomplete_name='Default'):
	self.client = Client(autocomplete_name,host,port)
        self.ipAdd = host 
        self.ipPort  = port
        self.db = db
        self.redisConn = redis.StrictRedis(host=self.ipAdd, port=self.ipPort, db=self.db)
        self.autocomplete = AutoCompleter(autocomplete_name, host, port)
        self.escape1 = re.compile(r'&#\d+;')
        self.escape2 = re.compile(r',|\.|<|>|{|}|[|]|"|\'|:|;|!|@|#|\$|%|\^|&|\*|\(|\)|-|\+|=|~')
        self.escape3 = re.compile(r'\s+')
    
    def search_using_FT(self,search_text,index):
	search_text = search_text.replace(' ','*')
        query_string = 'FT.SEARCH '+index+' '+search_text+' LIMIT 0 100'
        res = self.redisConn.execute_command(query_string)       
	fs = []
	for i,rr in enumerate(res):
		if i == 0:continue
		if i % 2 != 0:continue
		fs.append(rr)	
	return fs

    def search_exact_Query_using_ft(self,index,query):
        query_string = 'FT.SEARCH '+index+' '+query+' LIMIT 0 1000'
        res = self.redisConn.execute_command(query_string)       
	fs = []
	for i,rr in enumerate(res):
		if i == 0:continue
		if i % 2 != 0:continue
		fs.append(rr)	
	return fs

    def StringEscape(self, search_str):
        search_str = re.sub(self.escape1, '', search_str)
        search_str = re.sub(self.escape2, '', search_str)
        search_str = re.sub(self.escape3, ' ', search_str)
        return search_str.strip()  

    def simple_search(self,text):
	res = self.client.search(text)
	fs = []
	if res:
    		for i,rr in enumerate(res.docs):
			fs.append([rr.DOCID,rr.SECTION_TYPE,rr.GRIDID,rr.BBOX,rr.ROWCOL,rr.DATA,rr.id,rr.PAGE])
	return fs
	
    def search_exact_Query(self,query):
	return self.client.search(Query(query).paging(0, 1000))

    def search_query_convert_bk(self,query):
	res = self.search_exact_Query(query)
	fs = {}
	if res:
    		for i,rr in enumerate(res.docs):
			vv = rr.DOCID+"_"+rr.PAGE+"_"+rr.GRIDID
			if vv in fs:
				fs[vv]['count'] = fs[vv]['count']+1
				fs[vv]['info'].append([rr.DOCID,rr.SECTION_TYPE,rr.GRIDID,rr.BBOX,rr.ROWCOL,rr.DATA,rr.id,rr.PAGE])
			else:
				fs[vv] = {'count': 1 ,'info': [[rr.DOCID,rr.SECTION_TYPE,rr.GRIDID,rr.BBOX,rr.ROWCOL,rr.DATA,rr.id,rr.PAGE]]}
			#fs.append([rr.DOCID,rr.SECTION_TYPE,rr.GRIDID,rr.BBOX,rr.ROWCOL,rr.DATA,rr.id,rr.PAGE])
	return fs

    def search_query_convert(self,query,fs):
	res = self.search_exact_Query(query)
	if res:
    		for i,rr in enumerate(res.docs):
			vv = rr.PAGE+"_"+rr.GRIDID
			if vv in fs:
				fs[vv] = fs[vv]+1
			else:
				fs[vv] = 1
			#fs.append([rr.DOCID,rr.SECTION_TYPE,rr.GRIDID,rr.BBOX,rr.ROWCOL,rr.DATA,rr.id,rr.PAGE])
	return fs

    def search_query_convert_result(self,query):
	res = self.search_exact_Query(query)
	fs = []
	if res:
    		for i,rr in enumerate(res.docs):
			fs.append([rr.DATA,rr.PAGE,rr.BBOX])
	return fs
		
    def search_query_convert_result_auto(self,query, fs):
	res = self.search_exact_Query(query)
	if res:
    		for i,rr in enumerate(res.docs):
			DATA,DOCID,GRIDID,PAGE,BBOX = rr.DATA,rr.DOCID,rr.GRIDID,rr.PAGE,rr.BBOX
			if not DATA:continue
			grid_id = str(DOCID)+"##"+str(PAGE)+"$"+str(GRIDID)
			if DATA not in fs:
				fs[DATA] = {}
			if grid_id not in fs[DATA]:
				fs[DATA][grid_id] =[]
			fs[DATA][grid_id].append(BBOX)
	return fs

    def search_query_convert_result_page_grouping(self,query):
	res = self.search_exact_Query(query)
	fs = {}
	if res:
    		for i,rr in enumerate(res.docs):
			#print rr
			page = rr.PAGE
			bbox = rr.BBOX
			if page not in fs:
				fs[page] = []
			fs[page].append([rr.DATA,rr.BBOX])
	return fs

    def search_using_Query(self,search_text,index):
	search_text = search_text
	query = '@DATA:"%s"'%search_text
	#,search_text+"*")
	#query = '@BBOX:"%s"'%('109')
	res = self.client.search(Query(query).paging(0, 10000))
	fs = []
	if res:
    		for i,rr in enumerate(res.docs):
			fs.append([rr.DOCID,rr.SECTION_TYPE,rr.GRIDID,rr.BBOX,rr.ROWCOL,rr.DATA,rr.id,rr.PAGE])
	return fs
           
if __name__ == "__main__":
    obj = TAS_AutoCompleter('172.16.20.7','6382','0',"921_grid")
    res = obj.search_using_FT("Net interest revenue","921_grid")
    print res,'\n'
    res = obj.search_using_Query("Net interest revenue","921_grid")
    print json.dumps(res)
    '''res = obj.search_using_FT("information corresponding to interest","921_grid")
    print res
    res = obj.search_using_Query("information corresponding to interest","921_grid")
    print 'CCC', res'''
    #res = obj.search_using_Query("March 31,2019","921_grid")
    #res = obj.generic_search("information corresponding to interest-earning","921_grid")
