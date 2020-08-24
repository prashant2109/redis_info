import Autocomplete_new.redis_config as redis_search
import re
class exe:
    def __init__(self):
        self.redsis_info = "172.16.20.7##6382##0"
        self.escape1 = re.compile(r'&#\d+;')
        self.escape2 = re.compile(r',|\.|<|>|{|}|[|]|"|\'|:|;|!|@|#|\$|%|\^|&|\*|\(|\)|-|\+|=|~')
        self.escape3 = re.compile(r'\s+')

    def escape_special_charatcers(self, search_str, v = ''):
        search_str = re.sub(self.escape1, '', search_str)
        search_str = re.sub(self.escape2, '', search_str)
        search_str = re.sub(self.escape3, ' ', search_str)
        search_str = search_str.strip()   
        if not v:
            return search_str #+"|"+search_str+"*"
        else:
            return search_str #+"|"+search_str+"*"

    def get_common_terms(self, res_dic):
        if not res_dic:
            return []
        return reduce(set.intersection, [set(l_) for l_ in res_dic.values()])

    def search_elm(self, ijson):
	search_result = {}
        querys = []
        mquery = []
        docs = ijson['data']
        data           = ijson['data']
        project = ijson['Project']
        m_data         = ijson.get('m_dict', {})
        dest            = ''
        company          = ijson.get('company','')
        table_type =  ijson.get('tablename','')
        texts       = ijson.get('search_dict',{})
        flag_v  = ijson.get('all_flg', 'N')
        for qk, qv in texts.items(): 
            vv = '|'.join(map(lambda x: self.escape_special_charatcers(x,1),qv))
            cque = "@DATA:"+vv+" @SECTION_TYPE:"+qk
            if qk in m_data:
                mquery.append(cque)
            else:
                querys.append(cque)
        order_result  = {}
        for ddoc in docs:
            ddoc_key     = project+"_GRID_"+str(ddoc)
            si,sp,sdb = self.redsis_info.split('##')
            dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
            if not mquery: 
                for query in querys:
                    try:
                        get_alltext  = dredis_obj.search_query_convert_docs_wise_v2(query,search_result, ddoc)
                    except Exception as e:
                        pass
            else:
                mt_res_flg = False
                search_result_m = {}
                query_wise_res  = {}
                for query in mquery:
                    try:
                        get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_mquery(query,search_result_m, ddoc, query_wise_res)
                    except Exception as e:
                        pass
                print  'DEEE', query_wise_res
                union_m_res = self.get_common_terms(query_wise_res)
                for doc_info in union_m_res:
                    mt_res_flg = True
                    search_result[doc_info] = search_result_m[doc_info]
                if mt_res_flg:
                    for query in querys:
                        try:
                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_order(query, order_result, ddoc)
                        except Exception as e:
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

if  __name__ == '__main__':
    obj =  exe()
    ijson = {"c_flag":323,"Project":"1406__DataBuilder_1406","company":"KeyCorp","ProjectID":1406,"workspace_id":"1","db_string":"DataBuilder_1406","data":[37,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,33,34,35,36,38,39,41,42,43],"search_dict":{"value":["890"]},"m_dict":{"value":"Y"},"user":"aniket", "PRINT":"Y"}
    res = obj.search_elm(ijson)
    print res

    
