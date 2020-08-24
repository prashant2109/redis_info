#!/usr/bin/python
# -*- coding:utf-8 -*-
import json, sys
import ast,os
import ConfigParser
import db_layer.get_db_layer_info_new as get_info
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
	
	def get_page_info(self,doc_id,db_string):
        	obj         = get_info.pdf_cloud_db(self.config_path)
        	res         = obj.get_page_info(db_string,doc_id)
		return res 	

	def start_indexing(self,res,doc_id,company,project):
		import Autocomplete_new.insert_doc_id as insert_doc
                si,sp,sdb = self.config.get('redis_search', 'storage').split('##')
		insert_obj = insert_doc.TAS_Import(project+"_GRID_"+str(doc_id),si,sp,sdb)
		res = insert_obj.start(res,doc_id,company,project)	
		return res
			
		
	def start(self,companies,project):
		obj         = get_info.pdf_cloud_db(self.config_path)
		#doc_idddd = ['1189']
		for companydic in companies:
                        company  = companydic
                        #company = companydic['id']
			#if company not in ['HyundaiEngineeringandConstructionCoLtd']:continue
			#if company not in ['BOCHongKongHoldingsLtd']:continue
			all_docs  =  obj.get_company_info_cmp(company,project)
			obj         = get_info.pdf_cloud_db(self.config_path)
			for docs in all_docs:
                                #print docs
				doc_id = docs['doc_id']
                                #print doc_id
                                #if doc_id != "13953":continue
				#print doc_id
				#if doc_id not in doc_idddd:continue	
				db_string = docs['db_string'] 
				page_info = self.get_page_info(doc_id,db_string)
				res = []
				for page,grids in page_info.items():
					for grid in grids:
                                                try:
						    data = obj.get_row_col_db_info(db_string,doc_id,page,grid,project)
                                                except: 
                                                     print 'grid Error',[doc_id,page,grid,project]
                                                     continue
						if not data:continue
						if 'data' not in data:continue
						for rkeys,rdic in data['data'].items():
							r,c = rkeys.split('_')
							section_type= rdic.get('ldr','')
							if(type(section_type) == type([])):
								section_type = ''.join(section_type)
							res.append([rdic.get('data',''),section_type,str(doc_id),str(page),str(grid),str(rkeys),rdic.get('bbox','')])
				vdata = self.start_indexing(res,doc_id,company,project)
				print [doc_id,page,vdata]
		#return vdata

	def start_doc_id_wise(self,db_string,doc_id,project_id,workspace_id,company):
		obj         = get_info.pdf_cloud_db(self.config_path)
		page_info = self.get_page_info(doc_id,db_string)
		res = []
		for page,grids in page_info.items():
			for grid in grids:
				data = obj.get_row_col_db_info(db_string,doc_id,page,grid)
				for rkeys,rdic in data['data'].items():
					r,c = rkeys.split('_')
					section_type= rdic.get('ldr','')
					if(type(section_type) == type([])):
						section_type = ''.join(section_type)
					res.append([rdic.get('data',''),section_type,str(doc_id),str(page),str(grid),str(rkeys),rdic.get('bbox','')])
		vdata = self.start_indexing(res,doc_id,company)
		return ["Done"]
		
	def get_companies(self,project):
        	dbobj         = get_info.pdf_cloud_db(self.config_path)
		companies = dbobj.get_company_info(project)
		#print companies
		self.start(companies,project)	
		


if __name__ == '__main__':
    obj = indexing("/var/www/cgi-bin/INC_Interface/pysrc_29_01_20/Config.ini")
    #res  =  obj.get_companies(sys.argv[1])
    d = sys.argv[1].split('##') 
    project = sys.argv[2]
    #d = {"c_flag":3,"doc_id":"1189","pageno":"11","groupid":4,"db_string":"AECN_INC","ProjectID":"34","workspace_id":"1"}
    #d = ["BOCHongKongHoldingsLtd"]
    #v = ['BOCHongKongHoldingsLtd','HyundaiEngineeringandConstructionCoLtd','HDFCBankLtd','BOCHongKongHoldingsLtd','ErsteGroupBankAG','TataMotorsLimited']
    res = obj.start(d,project)
    print json.dumps(res) 
