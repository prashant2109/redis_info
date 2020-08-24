import Search_module.Autocomplete_new.redis_config as redis_search
import re
import json
class exe:
    def __init__(self):
        self.redsis_info = "172.16.20.7##6384##0"
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
 
    def update_bbox(self, hl, ht, hw, hh, bboxs):
        for bbox in bboxs.split('$$'):
            if not bbox:continue
            x0, y0, x1, y1 =  map(lambda x: int(x), bbox.split('_'))
            new_height = (y1 - y0)
            hh = hh+new_height 

        return hh

    def consolidated_bbox(self, bbox):
        bboxs = filter(lambda x:x, bbox.split('$$'))
        bboxs =  map(lambda x: map(lambda y:int(y), x.split('_')), bboxs)
        new_bbox = []
        if len(bboxs) == 1:
            new_bbox = [bboxs[0][0], bboxs[0][1], bboxs[0][2], bboxs[0][3]]
        else:
            nl, nt, nw, nh = 0, 0 , 0, 0
            for bbox in bboxs:
                if nl == 0 or nl > bbox[0]:
                    nl = bbox[0]
                if nt ==0 or nt > bbox[1]:
                    nt = bbox[1]
                if nw == 0 or nw < bbox[2]:
                    nw = bbox[2]
                if nh == 0 or nh < bbox[3]:
                    nh = bbox[3]
            new_bbox = [nl, nt, nw , nh]
            #print 'neww', new_bbox
        return [new_bbox[0], new_bbox[1], (new_bbox[2] - new_bbox[0]), (new_bbox[3] - new_bbox[1])]
            
    def row_wise_top_height(self, bbox, res, row):
        bboxs = filter(lambda x:x, bbox.split('$$'))
        bboxs =  map(lambda x: map(lambda y:int(y), x.split('_')), bboxs)
        for bbox in bboxs:
            if res.get(row):
                break
            res[row] = [bbox[1], (bbox[3]- bbox[1]), bbox]
        
            

    def get_cords_info(self, rc_info, cell_info, gridinfo):
        doc, page, grid = gridinfo
        tb_area = rc_info['bbox'][int(page)]
        cell_bbox = {}
        row_wise_cord = {}
        row_wise_info = {}
        for cell in cell_info:
            r, c = cell[0].split('_')
            row_wise_info.setdefault(r, {})[cell[0]] = cell
            cell_bbox[cell[0]] = cell[1]
        temp  = []
        print 'tb_area', tb_area
        done_rheight = {}
        hl, ht, hw, hh = tb_area[0], tb_area[1], tb_area[2],0 
        for row, rinfo in rc_info['rc_d'].items():
            for cell , cinfo in rinfo.items():
                new_rc = '%s_%s'%(row, cell)
                stype = cinfo.get('ldr')
                bbox = cinfo.get('bbox')    
                if not bbox:continue
                self.row_wise_top_height(bbox, row_wise_cord, row)
                if stype.lower() in ['vch', 'gh']:
                    if not done_rheight.get(row):
                        done_rheight[row] = 1
                        hh = self.update_bbox(hl, ht, hw, hh, bbox)
        print 'row_wise_cord', row_wise_cord
        print 'header_bbox', [hl, ht, hw, hh]
        #print row_wise_cord
        vtemp = {'slices':["S1"], "d": doc, "p": page, "g": grid, "S1": {"bbox":[hl, ht, hw, hh], "cells":[]}}
        for row, cellinfo in row_wise_info.items():
            print 'crow', row
            sl_len = len(vtemp['slices'])+1
            skey = 'S%s'%(sl_len)
            cells = {}
            for rc, rcinfo in cellinfo.items():
                r,c  = rc.split('_')
                cbbox = self.consolidated_bbox(rcinfo[1])
                if not cells.get('bbox'):
                    print 'cell bbox', [cbbox]
                    cells = {'bbox':[tb_area[0], cbbox[1], tb_area[2], cbbox[3]], 'cells':{}}
                
                cells['cells'][rc] = {'ref_k':[doc, page, grid, '', rcinfo[0]], 'bbox': cbbox, 'cls':rcinfo[3], 'cls-d':'hch-d'}
                #cells['cells'].append({'ref_k':[doc, page, grid, '', rcinfo[0]], 'bbox': cbbox, 'cls':rcinfo[3]})
            #print 'cfff', cells, 
            if cells.get('bbox'):
                vtemp['slices'].append(skey)
                vtemp[skey] = cells
        return vtemp

    def get_bounding_box(self, bbox_lst):
        nl, nt, nr, nb = 0, 0, 0, 0
        new_bbox = [nl, nt, nr, nb]
        for cellbbox in bbox_lst:
            bboxs = filter(lambda x:x, cellbbox.split('$$'))
            bboxs =  map(lambda x: map(lambda y:int(y), x.split('_')), bboxs)
            for bbox in bboxs:
                if nl == 0 or nl > bbox[0]:
                    nl = bbox[0]
                if nt ==0 or nt > bbox[1]:
                    nt = bbox[1]
                if nr == 0 or nr < bbox[2]:
                    nr = bbox[2]
                if nb == 0 or nb < bbox[3]:
                    nb = bbox[3]
            new_bbox = [nl, nt, nr, nb]
        return [new_bbox[0], new_bbox[1], (new_bbox[2] - new_bbox[0]), (new_bbox[3] - new_bbox[1])]

    def get_section_type(self, row_info):
        ldr_dic = {}
        for row , ldrs in row_info.items():
            if not ldrs:
                ldr = 'vch'
            else:
                ldr  = max(set(ldrs), key = ldrs.count) 
            if (ldr in ['gh', 'vch']) and (ldr_dic.get('maxr', 0) <= row):
                ldr_dic['maxr']= row
            ldr_dic[row] = ldr
        return ldr_dic
    
    def row_group(self, rows):
        rows = map(lambda x: int(x), rows)
        rows.sort()
        row_group = []
        for dd in rows:
            if len(row_group) == 0:
                row_group.append([dd])
                continue
            previouselm = row_group[-1]
            if previouselm[-1] == (dd - 1):
                row_group[-1].append(dd)
            else:
                row_group.append([dd])      
        return row_group

    def cget_cords_info(self, grid_info,  grid_rows, cellinfo, grids):
        doc, page, grid = grids
        tb_area = grid_info['bbox'][int(page)]
        row_wise_info = {}
        for cell in cellinfo:
            r, c = cell[0].split('_')
            row_wise_info.setdefault(r, {})[cell[0]] = cell
        row_sorted_order = self.row_group(row_wise_info.keys())
        ldr_info = self.get_section_type(grid_rows['ldr'])
        noheader = 'N'
        if ldr_info.get('maxr', -1) ==  -1:
            noheader = 'Y' 
            vtemp = {'slices':[], "d":doc, "p":page, "g": grid}
        else:
            cbboxs= []
            for e_r in range(ldr_info['maxr'], -1, -1):
                cbboxs += grid_rows['bbox'].get(e_r, [])
            bou_box = self.get_bounding_box(cbboxs) 
            print '--------', bou_box
            vtemp = {'slices':["S1"], "d": doc, "p": page, "g": grid, "S1": {"bbox":[tb_area[0], tb_area[1], tb_area[2], bou_box[3]], "cells":[]}}
        for rows in row_sorted_order:
            rgroup_boxs = []
            for row in rows:    
                rgroup_boxs += grid_rows['bbox'][row]
            group_bbox = self.get_bounding_box(rgroup_boxs)
            print 'hhhh', group_bbox
            #continue
            cells = {}
            for row in rows:    
                row  = str(row)
                cellinfo = row_wise_info[row]
                sl_len = len(vtemp['slices'])+1
                skey = 'S%s'%(sl_len)
                for rc, rcinfo in cellinfo.items():
                    r,c  = rc.split('_')
                    #rcord_info = row_wise_cord[int(r)]
                    cbbox = self.consolidated_bbox(rcinfo[1])
                    print 'row bbox', [tb_area[0], cbbox[1], tb_area[2], cbbox[3]]
                    if not cells.get('bbox'):
                        print 'cell bbox', [cbbox]
                        cells = {'bbox':[tb_area[0], cbbox[1], tb_area[2], cbbox[3]], 'cells':{}}
                    
                    cells['cells'][rc] = {'ref_k':[doc, page, grid, '', rcinfo[0]], 'bbox': cbbox, 'cls':rcinfo[3], 'cls-d':'hch-d'}
                    #cells['cells'].append({'ref_k':[doc, page, grid, '', rcinfo[0]], 'bbox': cbbox, 'cls':rcinfo[3]})
                #print 'cfff', cells, 
                if cells.get('bbox'):
                    vtemp['slices'].append(skey)
                    vtemp[skey] = cells
        return vtemp
        
        
        

    def get_page_cords(self, doc_info, company_id, page_dic):
        doc, page, grid = doc_info
        if page_dic.get(doc, ''):
            return
        db_path = '/mnt/eMB_db/company_management/{0}/equality/{1}.db'.format(company_id, doc)
        import sqlite_api as sqlapi
        sqlapi_obj = sqlapi.sqlite_api(db_path)
        page_info  = sqlapi_obj.read_page_cords()
        sqlapi_obj.cur.close()
        for dd in page_info:
            page_dic.setdefault(doc, {})[str(dd[0])] = eval(dd[1])
    
    def get_page_num(self, xml):
        pages = map(lambda y: y.split('_')[1], filter(lambda x: x, xml.split('$$')))
        return pages
    

    def crow_wise_info(self, rc_info, grid):
        row_info = {'ldr':{}, 'bbox':{}}
        for row, rinfo in rc_info['rc_d'].items():
            for cell , cinfo in rinfo.items():
                new_rc = '%s_%s'%(row, cell)
                style = cinfo.get('ldr')
                if grid[1] not in self.get_page_num(cinfo.get('xml_ids', '')):
                    continue
                bbox = cinfo.get('bbox')    
                if bbox:
                    row_info['bbox'].setdefault(row, []).append(bbox)
                if style:
                    row_info['ldr'].setdefault(row, []).append(style)
            
        return row_info

    def get_only_grids(self, sres, ijson):
        si,sp,sdb = self.redsis_info.split('##')
        res = []
        grid_data = {'map':{}, 'data':[], 'col_def':[{"k":"sn","type":"SL","n":"S.No"},{"k":"p_g","n":"Page-Grid"},{"k":"grid_header","n":"Grid header"}], 'message':'done'}
        rid = 1
        for grids, elm in sres.items():
            doc, page , grid = grids.split('_')
            ddoc_key     = "%s_%s"%(ijson['cmp_id'], doc)# project+"_GRID_"+str(ddoc)
            dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
            query = "@SECTION_TYPE:gh* @PAGE:%s @GRIDID:%s"%(page, grid)
            gheaders = dredis_obj.get_header(query)
            temp = {'p_g': {'v': '%s_%s'%(page, grid)}, 'grid_header': {'v': gheaders}, 'sn': {'v': rid}, 'rid':rid, 'cid':rid, 'tid': '%s#%s#%s'%(doc, page,  grid)}
            grid_data['data'].append(temp)
            print gheaders, grids
            rid = rid + 1
        return [grid_data] #[{'message':'done', 'data': res}]

    def start_grid_wise(self, sres, ijson, cmp_n):
        if ijson.get('grid', 'N') == 'Y':
            return self.get_only_grids(sres, ijson)
        cm = ijson['cmp_id']
        import modules.tablets.tablets as tb_api
        tb_obj  = tb_api.Tablets()
        page_cords_dic = {}
        fs_info = []
        for grid, cellinfo in sres.items():
            #if '5131_101_2' not  in grid:continue
            self.get_page_cords(grid.split('_'), cm, page_cords_dic)
            grid_info = tb_obj.create_inc_table_data(cm, grid, '')
            grid_rows = self.crow_wise_info(grid_info, grid.split('_'))
            if not grid_info:continue
            doc, page, bgrid = grid.split('_')
            #cc = self.get_cords_info(grid_info, cellinfo, grid.split('_'))
            cc = self.cget_cords_info(grid_info,  grid_rows, cellinfo, grid.split('_'))  
            if len(cc['slices']):
                #fs_info.append(cc)
                cpage_cord = {}
                cpage_cord.setdefault(doc, {})[page] = page_cords_dic.get(doc, {}).get(page, [0,0,0,0])
                fs_info.append({"ref_path": {"ref_image": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/html_pages/{0}-page-{1}.png","ref_html": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/html_output/{1}.html","ref_pdf": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/pages/{1}.pdf", "ref_svg": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/html_pages/{0}-page-{1}.svg"},"cards": cc, "page_cord": cpage_cord, 'tid':grid})
        return [{'message':'done', 'data': fs_info}]

    def search_elm(self, ijson):
	search_result = {}
        querys = []
        mquery = []
        docs = ijson['data']
        #company = ijson['company']
        data           = ijson['data']
        #project = ijson['Project']
        m_data         = ijson.get('m_dict', {})
        dest            = ''
        company          = ijson.get('company','')
        table_type =  ijson.get('tablename','')
        texts       = ijson.get('search_dict',{})
        flag_v  = ijson.get('all_flg', 'N')
        cmp_id = ijson['cmp_id']
        for qk, qv in texts.items(): 
            vv = '|'.join(map(lambda x: self.escape_special_charatcers(x,1),qv))
            cque = "@DATA:'"+vv+"' @SECTION_TYPE:'"+qk+"'"
            cque = "@DATA:"+vv+" @SECTION_TYPE:"+qk+""
            if qk in m_data:
                mquery.append(cque)
            else:
                querys.append(cque)
        order_result  = {}
        for ddoc in docs:
            ddoc_key     = "%s_%s"%(cmp_id, ddoc)# project+"_GRID_"+str(ddoc)
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
        if ijson.get('ONLY_TABLE') == 'Y':
            tdd  = {}
            for kk in order_keys:
                tdd[kk]  = 1
            return tdd
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
            #return fs, {}
            return self.start_grid_wise(fs, ijson, company) 
        return self.start_grid_wise(search_result, ijson, company) 
        return search_result, order_keys

if  __name__ == '__main__':
    obj =  exe()
    #ijson = {"cmd_id":67,"company":"OFGBancorp","data":[5131],"search_dict":{"hch":["interest income"]},"m_dict":{"hch":"Y"},"user":"demo","cmp_id":1117}
    ijson = {"cmd_id":67,"company":"OFG Bancorp","data":[5131],"search_dict":{"hch":["income"]},"m_dict":{"value":"Y"},"user":"demo","cmp_id":1117} 
    #{"c_flag":323,"company":"OFGBancorp","db_string":"AECN_INC","data":[5131,4862,4863,4864,4866,4868,4869,4871,4872,4875,4876,4877,4878,4879,4880,4881,4882,4883,4884,4887,4889,4892,4893,4894,4896,4897,4899,4901,4902,4903,4906,4907,4908,4909,4910,4911,4912,4913,4914,4915,4916,5129,5130,5132,5133,5134,5135,5136,5163,5380],"search_dict":{"value":["143"]},"m_dict":{"value":"Y"},"user":"harsha", "cmp_id":"1117"}
    res = obj.search_elm(ijson)
    print json.dumps(res)

    
