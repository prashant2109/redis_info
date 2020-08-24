#!/usr/bin/python
# -*- coding:utf-8 -*-
import json, sys
import ast,os,re
import ConfigParser
import db_layer.get_db_layer_info_new as get_info
import Autocomplete_new.redis_config as redis_search
import db_layer.save_focus_mgmt_new as save_mgmt1
import operator
import shelve
def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class indexing:
	def __init__(self,config_path):
		self.config_path = config_path
        	self.config = ConfigParser.ConfigParser()
        	self.config.read(config_path)
                self.escape1 = re.compile(r'&#\d+;')
                self.escape2 = re.compile(r',|\.|<|>|{|}|[|]|"|\'|:|;|!|@|#|\$|%|\^|&|\*|\(|\)|-|\+|=|~')
                self.escape3 = re.compile(r'\s+')
	
	def get_page_info(self,doc_id,db_string):
        	obj         = get_info.pdf_cloud_db(self.config_path)
        	res         = obj.get_page_info(db_string,doc_id)
		return res 	

	def start_grid_wise(self,doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid,project):
        	obj         = get_info.pdf_cloud_db(self.config_path)
		search_result = {}
                
		ddoc_key     = "GRID_"+ddoc
                ci,cp,cg = self.config(project,'suser_storage').split('##')
		#dredis_obj = redis_search.TAS_AutoCompleter('172.16.20.7','6382','0', ddoc_key)
		dredis_obj = redis_search.TAS_AutoCompleter(ci,cp,cg,ddoc_key)
		data = obj.getTableInfoSql(db_string,doc_id,page,grid)	
		data = json.loads(data)
		if not data:
			return ["Data Not Found"]
		for rkeys,rdic in data['data'].items():
			section_type = rdic.get('ldr','') 
			if section_type and section_type == 'hch':
				values = rdic.get('data','')
				if not values:continue
				clean_values = dredis_obj.StringEscape(values) 
				query = '@DATA:"%s"'%clean_values
				try:
					get_alltext  = dredis_obj.search_query_convert(query,search_result)
				except Exception as e:
					print [e, query]
					pass
		sorted_x = sorted(search_result.items(), key=operator.itemgetter(1),reverse=True)	
		last_three = sorted_x[0:5]
		return last_three

	def start_grid_wise_multiple(self,docs,ddoc,company,table_type,project):#doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
		search_result = {}
		for vjson in docs:
			db_string   = vjson.get("db_string","")
			doc_id	    = vjson['doc_id']
			page 	    = vjson['pageno']
		 	grid	  = vjson['groupid']
			ProjectID = vjson['ProjectID']
			workspace_id = vjson['workspace_id']
			obj         = get_info.pdf_cloud_db(self.config_path)
			ddoc_key     = project+"_GRID_"+ddoc
                        si,sp,sdb = self.config.get(project,'suser_storage').split('##')
			dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
			#print [doc_id,page,grid]
			data = obj.getTableInfoSql(db_string,doc_id,page,grid)	
			if not data:continue
			data = json.loads(data)
			if not data:
				return ["Data Not Found"]
			for rkeys,rdic in data['data'].items():
				section_type = rdic.get('ldr','') 
				if section_type and section_type == 'hch':
					values = rdic.get('data','')
					if not values:continue
					clean_values = dredis_obj.StringEscape(values) 
					query = '@DATA:"%s"'%clean_values
					#print query
					try:
						get_alltext  = dredis_obj.search_query_convert_docs_wise(query,search_result)
					except Exception as e:
						#print [e, query]
						pass
		sorted_x = sorted(search_result.items(), key=operator.itemgetter(1),reverse=True)	
		db_string = self.config.get(project,'pdf_cloud_data_db') #, 'value')
		last_three = sorted_x[0:5]
		grid_avail = map(lambda a: a[0], last_three)
		all_grids = obj.get_page_info_cval(db_string,ddoc)		
		save_obj = save_mgmt1.save_mgmt("/var/www/cgi-bin/INC_Interface/pysrc_08_07_19/Config.ini")
		all_grids = save_obj.get_only_suggestion(ddoc,company,db_string,table_type)
		#all_grids  = {} 
		for gg in all_grids:
			if gg not in grid_avail:
				last_three.append((gg,0))	
		return last_three
        
	def start_grid_wise_multiple_doc_wise_contains(self,docs,ddocs,company,table_type,project):#doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
		search_result = {}
                for ddoc in ddocs:
                    for vjson in docs:
                            db_string   = vjson.get("db_string","")
                            doc_id	    = vjson['doc_id']
                            page 	    = vjson['pageno']
                            grid	  = vjson['groupid']
                            ProjectID = vjson['ProjectID']
                            workspace_id = vjson['workspace_id']
                            obj         = get_info.pdf_cloud_db(self.config_path)
                            ddoc_key     = project+"_GRID_"+ddoc
                            si,sp,sdb = self.config.get(project,'suser_storage').split('##')
                            dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
                            #print [doc_id,page,grid]
                            data = obj.getTableInfoSql(db_string,doc_id,page,grid)	
                            if not data:continue
                            data = json.loads(data)
                            if not data:
                                    return ["Data Not Found"]
                            for rkeys,rdic in data['data'].items():
                                    section_type = rdic.get('ldr','') 
                                    if section_type and section_type == 'hch':
                                            values = rdic.get('data','')
                                            if not values:continue
                                            clean_values = dredis_obj.StringEscape(values) 
                                            query = '@DATA:"%s"'%clean_values
                                            #print query
                                            try:
                                                    get_alltext  = dredis_obj.search_query_convert_docs_wise_v1(query,search_result, ddoc)
                                            except Exception as e:
                                                    #print [e, query]
                                                    pass
                new_results = []
                for doc_id,results in search_result.items():
			sorted_x = sorted(results.items(), key=operator.itemgetter(1),reverse=True)	
			db_string = self.config.get(project,'pdf_cloud_data_db') #, 'value')
			last_three = sorted_x[0:5]
			grid_avail = map(lambda a: a[0], last_three)
			all_grids = obj.get_page_info_cval(db_string,ddoc)		
			save_obj = save_mgmt1.save_mgmt("/var/www/cgi-bin/INC_Interface/pysrc_08_07_19/Config.ini")
			all_grids = save_obj.get_only_suggestion(ddoc,company,db_string,table_type)
			#all_grids  = {} 
			for gg in all_grids:
				if gg not in grid_avail:
					last_three.append((gg,0))	
			new_results.append((int(doc_id),last_three))
                new_results.sort()
                return new_results

        def get_labels(self, docs, obj):
            label_result_dic = {}
            for vjson in docs:
                    db_string   = vjson.get("db_string","")
                    doc_id	    = vjson['doc_id']
                    page 	    = vjson['pageno']
                    grid	  = vjson['groupid']
                    ProjectID = vjson['ProjectID']
                    workspace_id = vjson['workspace_id']
                    data = obj.getTableInfoSql(db_string,doc_id,page,grid)	
                    if not data:continue
                    data = json.loads(data)
                    if not data:
                            return ["Data Not Found"]
                    for rkeys,rdic in data['data'].items():
                            section_type = rdic.get('ldr','') 
                            if section_type and section_type == 'hch':
                                    values = rdic.get('data','')
                                    #print 'label _info ', [doc_id, page, grid, values]
                                    if not values:continue
                                    clean_values = self.escape_special_charatcers(values) 
                                    query = '@DATA:"%s"'%clean_values
                                    label_result_dic[query] = label_result_dic.get(query, 0)+1
            return label_result_dic
                                    
                                     
	def start_grid_wise_multiple_doc_wise_v1test(self,docs,ddocs,company,table_type,project,order_doc):#doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
		search_result = {}
                obj         = get_info.pdf_cloud_db(self.config_path)
                label_result_dic = self.get_labels(docs, obj)
                print label_result_dic
                for ddoc in ddocs:
                    ddoc_key     = project+"_GRID_"+ddoc
                    #print 'tt', ddoc_key
                    si,sp,sdb = self.config.get('redis_search','storage').split('##')
                    dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
                    for query, cnt in label_result_dic.items():
                        #print 'query', query
                        try:
                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v1(query, search_result, ddoc, 1)
                        except Exception as e:
                                #print [e, query]
                                pass
                new_results = []
                for doc_id,results in search_result.items():
                        #print results
			sorted_x = sorted(results.items(), key=operator.itemgetter(1),reverse=True)	
			db_string = self.config.get(project,'pdf_cloud_data_db') #, 'value')
			last_three = sorted_x[0:5]
			grid_avail = map(lambda a: a[0], last_three)
			#all_grids = obj.get_page_info_cval(db_string,ddoc)		
                        if table_type:
                            save_obj = save_mgmt1.save_mgmt("/var/www/cgi-bin/INC_Interface/pysrc_08_07_19/Config.ini")
                            all_grids = save_obj.get_only_suggestion(ddoc,company,db_string,table_type)
                            print ',,,,,,,,', all_grids
                            #all_grids  = {} 
                            for gg in all_grids:
                                    if gg not in grid_avail:
                                            last_three.append((gg,0))	
			new_results.append((int(doc_id),last_three))
                print new_results
                new_results.sort()
                dd = sorted(new_results,key=lambda x: order_doc.get(x[0],999))
                return [] #dd #new_results

	def start_grid_wise_multiple_doc_wise_v1(self,docs,ddocs,company,table_type,project,order_doc):#doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
		search_result = {}
                obj         = get_info.pdf_cloud_db(self.config_path)
                label_result_dic = self.get_labels(docs, obj)
                for ddoc in ddocs:
                    ddoc_key     = project+"_GRID_"+ddoc
                    #print 'tt', ddoc_key
                    si,sp,sdb = self.config.get('redis_search','storage').split('##')
                    dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
                    for query, cnt in label_result_dic.items():
                        #print 'query', query
                        try:
                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v1(query, search_result, ddoc, 1)
                        except Exception as e:
                                #print [e, query]
                                pass
                new_results = []
                for doc_id in ddocs:#search_result.items():
                        results = search_result.get(doc_id, {})
                        #print results
			sorted_x = sorted(results.items(), key=operator.itemgetter(1),reverse=True)	
			db_string = project.split('__')[1] #self.config.get(project,'pdf_cloud_data_db') #, 'value')
			last_three = sorted_x[0:5]
			grid_avail = map(lambda a: a[0], last_three)
			#all_grids = obj.get_page_info_cval(db_string,ddoc)		
                        if table_type:
                            save_obj = save_mgmt1.save_mgmt("/var/www/cgi-bin/INC_Interface/pysrc_20_19_20/Config.ini")
                            all_grids = save_obj.get_only_suggestion(doc_id ,company,db_string,table_type)
                            #all_grids  = {} 
                            for gg in all_grids:
                                    if gg not in grid_avail:
                                            last_three.append((gg,0))	
                        #print [doc_id, last_three, ddoc]
			new_results.append((int(doc_id),last_three))
                new_results.sort()
                dd = sorted(new_results,key=lambda x: order_doc.get(x[0],999))
                return dd #new_results

        def escape_special_charatcers(self, search_str, v = ''):
                search_str = re.sub(self.escape1, '', search_str)
                search_str = re.sub(self.escape2, '', search_str)
                search_str = re.sub(self.escape3, ' ', search_str)
                search_str = search_str.strip()   
                if not v:
                    return search_str #+"|"+search_str+"*"
                else:
                    return search_str #+"|"+search_str+"*"
            

	def start_grid_wise_multiple_doc_wise_v2(self,docs,ddocs,company,table_type,project, texts, m_data, flag_v= 'N'):#doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
		search_result = {}
                querys = []
                mquery = []
                for qk,qv in texts.items(): 
                     vv = '|'.join(map(lambda x: self.escape_special_charatcers(x,1),qv))
                     cque = "@DATA:"+vv+" @SECTION_TYPE:"+qk
                     if qk in m_data:
                         mquery.append(cque)
                         #querys.append(cque)
                     else:
                         querys.append(cque)
                         #querys.append("@DATA:"+vv+" @SECTION_TYPE:"+qk)
                #print querys
                order_result  = {}
                for ddoc in docs:
                            ddoc_key     = project+"_GRID_"+str(ddoc)
                            si,sp,sdb = self.config.get('redis_search','storage').split('##')
                            #print si,sp,sdb,ddoc_key
                            dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
                            if not mquery: 
                                for query in querys:
                                    #print query
                                    try:
                                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v2(query,search_result, ddoc)
                                    except Exception as e:
                                            #print [e, query]
                                            pass
                            else:
                                mt_res_flg = False
                                search_result_m = {}
                                query_wise_res  = {}
                                for query in mquery:
                                    #print 'mmmmm', query
                                    #print query
                                    try:
                                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_mquery(query,search_result_m, ddoc, query_wise_res)
                                    except Exception as e:
                                            #print [e, query]
                                            pass
                                union_m_res = self.get_common_terms(query_wise_res)
                                #print '0000', union_m_res, query_wise_res
                                for doc_info in union_m_res:
                                    mt_res_flg = True
                                    search_result[doc_info] = search_result_m[doc_info]
                                    
                                if mt_res_flg:
                                    for query in querys:
                                        #print query
                                        try:
                                                get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_order(query, order_result, ddoc)
                                        except Exception as e:
                                                #print [e, query]
                                                pass
                                
                                    
                order_keys  = sorted(search_result.keys(), key=lambda x:order_result.get(x, 9999))
                if flag_v == 'Y':
                    fs = []
                    doc_id_index = {}
                    for kk in order_keys:
                         doc_id , page, grid = kk.split('_')
                         hh = "%s_%s"%(page, grid)
                         if doc_id not in doc_id_index:
                             keys_len = len(doc_id_index.keys())
                             doc_id_index[doc_id] = keys_len
                             fs.append([doc_id, [[hh, 1]]]) 
                         else:
                             fs[doc_id_index[doc_id]][1].append([hh, 1])
                    return fs, {}
                return search_result, order_keys

        def get_common_terms(self, res_dic):
                '''all_values = []
                for ff in  res_dic.values():
                    if not all_values:
                        all_values = ff
                    all_values = list(set(all_values) & set(ff))
                return all_values'''
                return reduce(set.intersection, [set(l_) for l_ in res_dic.values()])
	        	
	def start_grid_wise_response(self,doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
        	obj         = get_info.pdf_cloud_db(self.config_path)
		search_result = {}
		ddoc_key     = "GRID_"+ddoc
		dredis_obj = redis_search.TAS_AutoCompleter('172.16.20.7','6382','0', ddoc_key)
		data = obj.getTableInfoSql(db_string,doc_id,page,grid)	
		data = json.loads(data)
		if not data:
			return ["Data Not Found"]
		for rkeys,rdic in data['data'].items():
			section_type = rdic.get('ldr','') 
			if section_type and section_type == 'hch':
				values = rdic.get('data','')
				if not values:continue
				clean_values = dredis_obj.StringEscape(values) 
				query = '@DATA:"%s"'%clean_values
				try:
					get_alltext  = dredis_obj.search_query_convert_result(query)
					search_result[clean_values] = get_alltext	
					
				except Exception as e:
					print [e, query]
					pass
		search_len = {}
		for k,v in search_result.items():
			search_len[k] = len(v)
		return [search_result, search_len]

	def start_descrption_wise(self,data,ddoc):
        	obj         = get_info.pdf_cloud_db(self.config_path)
		search_result = {}
		ddoc_key     = "GRID_"+ddoc
		dredis_obj = redis_search.TAS_AutoCompleter('172.16.20.7','6382','0', ddoc_key)
		for values in data:
			clean_values = dredis_obj.StringEscape(values) 
			query = '@DATA:"%s"'%clean_values
			try:
				get_alltext  = dredis_obj.search_query_convert(query,search_result)
			except Exception as e:pass
		sorted_x = sorted(search_result.items(), key=operator.itemgetter(1),reverse=True)	
		last_three = sorted_x[0:5]
		return last_three

        def start_grid_wise_multiple_doc_wise_v1_grid(self,docs,ddocs,company,table_type,project,order_doc):#doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
            search_result = {}
            obj         = get_info.pdf_cloud_db(self.config_path)
            label_result_dic = self.get_labels(docs, obj)
            for ddoc in ddocs:
                ddoc_key     = project+"_GRID_"+ddoc
                si,sp,sdb = self.config.get('redis_search','storage').split('##')
                dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
                for query, cnt in label_result_dic.items():
                    try:
                        get_alltext  = dredis_obj.search_query_convert_docs_wise_v1(query, search_result, ddoc, 1)
                    except Exception as e:
                            pass
            new_results = []
            for doc_id in ddocs:#search_result.items():
                    results = search_result.get(doc_id, {})
                    sorted_x = sorted(results.items(), key=operator.itemgetter(1),reverse=True)	
                    db_string = project.split('__')[1] #self.config.get(project,'pdf_cloud_data_db') #, 'value')
                    last_three = sorted_x[0:5]
                    grid_avail = map(lambda a: a[0], last_three)
                    if table_type:
                        save_obj = save_mgmt1.save_mgmt("/var/www/cgi-bin/INC_Interface/pysrc_20_19_20/Config.ini")
                        all_grids = save_obj.get_only_suggestion(doc_id ,company,db_string,table_type)
                        for gg in all_grids:
                                if gg not in grid_avail:
                                        last_three.append((gg,0))	
                    new_results.append((int(doc_id),last_three))
            new_results.sort()
            dd = sorted(new_results,key=lambda x: order_doc.get(x[0],999))
            import doc_stats
            doc_stats_obj = doc_stats.stats()
            return doc_stats_obj.convert_scop_grid_format(dd, ddocs, project) #dd #new_results

	def start_grid_wise_multiple_doc_wise_v2_grid_format(self,docs,ddocs,company,table_type,project, texts, m_data, flag_v= 'N'):#doc_id,db_string,ProjectID,workspace_id,ddoc,page,grid):
		search_result = {}
                querys = []
                mquery = []
                for qk,qv in texts.items(): 
                     vv = '|'.join(map(lambda x: self.escape_special_charatcers(x,1),qv))
                     cque = "@DATA:"+vv+" @SECTION_TYPE:"+qk
                     if qk in m_data:
                         mquery.append(cque)
                         #querys.append(cque)
                     else:
                         querys.append(cque)
                         #querys.append("@DATA:"+vv+" @SECTION_TYPE:"+qk)
                #print querys
                order_result  = {}
                for ddoc in docs:
                            ddoc_key     = project+"_GRID_"+str(ddoc)
                            si,sp,sdb = self.config.get('redis_search','storage').split('##')
                            #print si,sp,sdb,ddoc_key
                            dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
                            if not mquery: 
                                for query in querys:
                                    #print query
                                    try:
                                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v2(query,search_result, ddoc)
                                    except Exception as e:
                                            #print [e, query]
                                            pass
                            else:
                                mt_res_flg = False
                                search_result_m = {}
                                query_wise_res  = {}
                                for query in mquery:
                                    #print 'mmmmm', query
                                    #print query
                                    try:
                                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_mquery(query,search_result_m, ddoc, query_wise_res)
                                    except Exception as e:
                                            #print [e, query]
                                            pass
                                union_m_res = self.get_common_terms(query_wise_res)
                                #print '0000', union_m_res, query_wise_res
                                for doc_info in union_m_res:
                                    mt_res_flg = True
                                    search_result[doc_info] = search_result_m[doc_info]
                                    
                                if mt_res_flg:
                                    for query in querys:
                                        #print query
                                        try:
                                                get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_order(query, order_result, ddoc)
                                        except Exception as e:
                                                #print [e, query]
                                                pass
                                
                order_keys  = sorted(search_result.keys(), key=lambda x:order_result.get(x, 9999))
                if flag_v == 'Y':
                    fs = []
                    doc_id_index = {}
                    for kk in order_keys:
                         doc_id , page, grid = kk.split('_')
                         hh = "%s_%s"%(page, grid)
                         if doc_id not in doc_id_index:
                             keys_len = len(doc_id_index.keys())
                             doc_id_index[doc_id] = keys_len
                             fs.append([doc_id, [[hh, 1]]]) 
                         else:
                             fs[doc_id_index[doc_id]][1].append([hh, 1])
                    return fs, {}
                import doc_stats
                doc_stats_obj = doc_stats.stats()
                docs = map(lambda x: str(x), docs)
                return doc_stats_obj.convert_scop_grid_format_search_keywords(search_result, order_keys, docs, project) #dd #new_results
                #return search_result, order_keys

        def get_page_cords(self, ijson):
            pid, db_string = ijson['Project'].split('__')
            docs = ijson['docs']
            page_cords = {}
            for ddoc in docs:
                if ddoc not in page_cords:
                    v_sh_path = "/var/www/cgi-bin/INC_Interface/pysrc_29_01_20/page_cords/"
                    v_sh_path = v_sh_path+"/"+pid+"/"+str(ddoc)+".sh"
                    if not os.path.exists(v_sh_path):
                        import get_txt_info_new
                        vobj = get_txt_info_new.text_layer(self.config_path)
                        mount_path = self.config.get('mount_path', 'value')
                        path = mount_path+pid+"/"+"1"+"/pdata/docs/"
                        try:
                            vobj.page_cords(str(ddoc),path,ijson['Project'],pid)
                        except:pass
                    try:
                        d = shelve.open(v_sh_path)
                        page_cords[ddoc] = d['data']
                        d.close()
                    except:pass
            import db_api 
            doc_str = ','.join(map(lambda x: str(x), docs))
            pdf_cloud_data = self.config.get('pdf_cloud_data', 'value')
            db_str = pdf_cloud_data+"#"+db_string
            db_obj = db_api.db_api(db_str)
            docs_info = db_obj.get_docs_meta_info(doc_str)
            p_type_map = {}
            for doc_info in docs_info:
                doc_id, doc_name, doc_type, meta_data = doc_info
                doc_type = doc_type
                if 'html' in doc_type.lower():
                    p_type_map[doc_id] = 'html'
                else: 
                    p_type_map[doc_id] = doc_type
            return page_cords, p_type_map 

	
if __name__ == '__main__':
    obj = indexing("/var/www/cgi-bin/INC_Interface/pysrc_08_07_19/Config.ini")
    d = [[{"workspace_id": "1", "pageno": "142", "db_string": "AECN_INC", "ProjectID": "34", "company": "BankofAmericaCorp", "doc_id": "2298", "groupid": 1}, {"workspace_id": "1", "pageno": "143", "db_string": "AECN_INC", "ProjectID": "34", "company": "BankofAmericaCorp", "doc_id": "2298", "groupid": 3}, {"workspace_id": "1", "pageno": "146", "db_string": "AECN_INC", "ProjectID": "34", "company": "BankofAmericaCorp", "doc_id": "2298", "groupid": 3}], ["2298"], "BankofAmericaCorp", "Modified Troubled Debt Restructuring", "INC", {"2283": 1}]
    res = obj.start_grid_wise_multiple_doc_wise_v1test(d[0], d[1], d[2], d[3], d[4], d[5])
    print json.dumps(res) 
