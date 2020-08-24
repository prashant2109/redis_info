import json, sys
import ast,os
class indexing:
    def __init__(self):
        self.redsis_info = "172.16.20.7##6382##0"

    def start_index(self, res, index_name):
        import Autocomplete_new.insert_doc_id as insert_doc
        si,sp,sdb = self.redis_info.split('##')
	insert_obj = insert_doc.TAS_Import(index,si ,sp ,sdb)
	res = insert_obj.start(res,doc_id,company,project)	
	return res
        
    def read_grids(self, cmp_id):
        doc_wise_grids = {}
        import sqlite as sql_api
        db_path = '/mnt/eMB_db/company_management/{0}/{1}.db'.format(cmp_id, 'table_info')
        sql_obj = sql_api.sqlite_api(db_path) 
        dd = sql_obj.get_grids()
        for each in dd:
            doc_wise_grids.setdefault(each[0], {})['%s_%s'%(each[1], each[2])] =  1
        return doc_wise_grids  

    def update_index_info(self, info, ind_info):
        for r, rcinfo in info.items():
            for c, cinfo in rcinfo.items():
                print r,c , cinfo
    def doc_indexing(self, info):
        sys.path.append("/root/tablets/tablets_mapping/pysrc/modules/tablets/")
        import tablets as tbs
        tbs_obj = tbs.Tablets()
        for doc, pageinfo in info.items():
            indexing_info = []
            for pagegrid, gg in page_info.items():
                grid = '%s_%s'%(doc, pagegrid)
                cpath = os.getcwd()
                os.chdir('/root/tablets/tablets_mapping/pysrc/modules/tablets/')
                grid_info = tbs_obj.create_inc_table_data(cmp_id, grid)
                os.chdir(cpath)
                self.update_index_info(grid_info, indexing_info)
                
    
    def indexing(self, ijson):
        docs = self.read_grids(ijson['cmp_id'])
        self.doc_indexing(docs)            

if  __name__ == '__main__':
    obj =  indexing()
    ijson = {"c_flag":323,"Project":"34__AECN_INC","company":"OFGBancorp","ProjectID":34,"workspace_id":"1","db_string":"AECN_INC","data":[5131,4862,4863,4864,4866,4868,4869,4871,4872,4875,4876,4877,4878,4879,4880,4881,4882,4883,4884,4887,4889,4892,4893,4894,4896,4897,4899,4901,4902,4903,4906,4907,4908,4909,4910,4911,4912,4913,4914,4915,4916,5129,5130,5132,5133,5134,5135,5136,5163,5380],"search_dict":{"value":["143"]},"m_dict":{"value":"Y"},"user":"harsha", "cmp_id":"1117"}
    #{"c_flag":323,"Project":"1406__DataBuilder_1406","company":"KeyCorp","ProjectID":1406,"workspace_id":"1","db_string":"DataBuilder_1406","data":[37,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,33,34,35,36,38,39,41,42,43],"search_dict":{"value":["890"]},"m_dict":{"value":"Y"},"user":"aniket", "PRINT":"Y"}
    res = obj.indexing(ijson)
    print json.dumps(res)
