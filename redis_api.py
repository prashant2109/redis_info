import Search_module.Autocomplete_new.redis_config as redis_search
import re
import json
class exe:
    def __init__(self, redsis_info, cmp_id, doc):
        self.redsis_info = redsis_info #"172.16.20.7##6384##0"
        si,sp,sdb = self.redsis_info.split('##')
        ddoc_key = '%s_%s'%(cmp_id, doc)
        self.dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)


    def make_exec(self, page, grid, stype):
        query = "@SECTION_TYPE:%s* @PAGE:%s @GRIDID:%s"%(stype, page, grid)
        gheaders = self.dredis_obj.get_header(query)
        return gheaders

    def read_sction_data(self, page, grid, stype):
        query = "(@SECTION_TYPE:%s) (@PAGE:%s) (@GRIDID:%s)"%(stype, page, grid)
        #query = "@PAGE:%s"%(page)
        #query = "(@PAGE:%s) (@GRIDID:%s)"%(page, grid)
        query = "@PAGE_GRID_SE:'%s'"%("%s_%s_%s"%(page, grid, stype))
        gheaders = self.dredis_obj.get_header_all(query)
        return gheaders
        

if  __name__ == '__main__':
    obj = exe("172.16.20.7##6384##0", 1053729, 3)
    text = obj.read_sction_data(35, 1, 'hch')
    print text
