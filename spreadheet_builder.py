import os, sys, sqlite3, lmdb, ast, json, copy
import report_year_sort as rys
from collections import OrderedDict as OD
import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class SpreadSheet(object):

    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur

    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def get_company_name(self, project_id, deal_id):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn  = sqlite3.connect(db_path)
        cur  =  conn.cursor()
        read_qry = 'select company_name from company_info where project_id="%s" and toc_company_id="%s" ;'%(project_id, deal_id)
        cur.execute(read_qry)
        table_data = cur.fetchone()
        conn.close()
        company_name = table_data[0]
        return company_name

    def read_reporting_type(self, company_name, project_id):
        db_path = '/mnt/eMB_db/user_wise_company_details.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = 'select reporting_type, project_name from user_wise_company_details where project_id="%s"  and company_name="%s" '%(project_id, company_name) 
        cur.execute(read_qry)    
        t_data = cur.fetchall()
        conn.close()
        res_dct = {}
        for row_data in t_data:
            reporting_type, project_name = row_data 
            res_dct[project_name] = reporting_type
        return res_dct
        
    def read_project_details(self, ijson):
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        company_name = self.get_company_name(project_id, deal_id)
        db_path = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, project_id)
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT project_name, disp_name FROM company_config; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        reporting_data_dct = self.read_reporting_type(company_name, project_id)
        res_lst = []
        for row_data in t_data:
            project_name, disp_name = row_data
            t_list = self.all_industry_type_data({'project_id': project_name})
            r_type = reporting_data_dct.get(project_name, 'Raw')
            dt_dct = {'k':project_name, 'n':disp_name if disp_name else project_name, 'r_type':r_type, 't_list': t_list}
            res_lst.append(dt_dct)
        return [{'message':'done', 'data':res_lst}]

    def all_industry_type_data(self, ijson):
        db_path = '/mnt/eMB_db/template_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        project_id = ijson.get('project_id', '')
        if project_id == 'Airlines':
            project_id = 'Airline'
        read_qry = 'SELECT row_id, sheet_name, project_name, display_name, industry, time_series from meta_info where (project_name like "%s" or industry == "All")'%(project_id)
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        res_lst = []
        for row in table_data:
            row_id, sheet_name, project_name, display_name, industry, time_series = row[:]
            project_name = ''.join(project_name.split())
            industry     = ''.join(industry.split())
            tsheet_name  = ''.join(sheet_name.split())
            k = '-'.join([industry, project_name])
            if k in ['FoodProcessing-EquityBuyout', 'PassengerTransportation-Airline'] and sheet_name in ['PassengerTransportation-Airline', 'Equity_Buyout_KraftHeinzCompany']:
                k = '-'.join([industry, project_name])
            else:
                k = '-'.join([industry, project_name, tsheet_name])

            if time_series == 'N':
                k = '-'.join([k, 'without_time_series'])
            data_dct = {'k':k, 'n':display_name, 'template_id':row_id, 'sheet_name':sheet_name, 'pname':project_name, 'time_series':time_series}
            res_lst.append(data_dct)
        return res_lst
        
    def read_taxo_data(self, ijson):
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        company_name = self.get_company_name(project_id, deal_id)
        db_path = '/mnt/eMB_db/%s/%s/taxonomy_formula.db'%(company_name, project_id) 
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT class, index_id, tas_taxonomy, tas_taxonomy_id FROM taxonomy; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()

        id_wise_data_dct = {}
        parent_child_comp = OD() 

        for row_data in t_data:
            class_info, index_id, tas_taxonomy, tas_taxonomy_id = row_data
            index_id_splt = index_id.split('.')
            idx_len = len(index_id_splt)
            if idx_len > 2:continue
            if idx_len > 1:
                p_key = '.'.join(index_id_splt[:-1])
                parent_child_comp.setdefault(p_key, []).append(index_id)
            id_wise_data_dct[index_id] = {'ci':{'v':class_info}, 'tt':{'v':tas_taxonomy}, 'tti':{'v':tas_taxonomy_id}, 'idx':{'v':index_id}} 
        
        def collect_chld_info(cm_lst, ch_cnt):
            cnt = copy.deepcopy(ch_cnt)
            c_lst = []
            for ch_inf in cm_lst:
                c_data = id_wise_data_dct.get(ch_inf, {})
                c_data['rid'] = cnt
                c_data['sn'] = {'v':cnt}
                c_data['cid'] = cnt 
                c_data['$$treeLevel'] = 1
                c_lst.append(c_data)
                cnt += 1
            return c_lst, cnt 
     
        row_lst = []
        row_cnt = 1
        for prnt_ix, chld_lst in parent_child_comp.iteritems():
            prnt_ix_splt_len = len(prnt_ix.split('.'))
            if prnt_ix_splt_len > 1:continue
            p_data = id_wise_data_dct.get(prnt_ix, {})
            p_data['rid'] = row_cnt
            p_data['sn'] = {'v':row_cnt}
            p_data['cid'] = row_cnt
            p_data['$$treeLevel'] = 0
            p_data['cls'] = 'disable_cell'
            row_lst.append(p_data)
            row_cnt += 1
            
            if chld_lst:    
                pc_lst, pc_cnt = collect_chld_info(chld_lst, row_cnt) 
                row_lst.extend(pc_lst)
                pc_cnt -= 2
                row_cnt += pc_cnt
    
        rmt_lst = []
        for ix, k in enumerate(row_lst, 1):
            p_txt = k['tt']['v']
            if p_txt == 'LINEITEM':
                c_idx = k['idx']['v']
                rmt_lst[-1]['idx'] = {'v':c_idx}
                del rmt_lst[-1]['cls']
                continue
            k['rid'] = ix
            k['sn'] = {'v':ix}
            k['cid'] = ix
            rmt_lst.append(k)
        
        col_def_lst = [{'k':'sn', 'n':'S.No', 'type':'SL'}, {'k':'tt', 'n':'Taxonomy', 'v_opt':1}]
        inf_map = {}
        res = [{'message':'done', 'data':rmt_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res
        
    def read_formula_info_flg(self, cur):
        read_qry = """ SELECT distinct(rawdb_row_id) FROM formula_table; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        res_set = {e[0] for e in t_data}
        return res_set        

    def read_child_info(self, company_name, project_id, child_id):
        db_path = '/mnt/eMB_db/%s/%s/taxonomy_formula.db'%(company_name, project_id)
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT row_id, class, index_id, tas_taxonomy, tas_taxonomy_id FROM taxonomy; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        row_hash_dct = {}
        index_row_hash_map = {}
         
        it_dat_dct = {}
        p_dct = {}
        level_info = {}
        for row_data in t_data:
            row_id, class_info, index_id, tas_taxonomy, tas_taxonomy_id = row_data
            chk_lst = index_id.split('.')
            if len(chk_lst) < 2:continue
            check_str = '.'.join(chk_lst[:2])
            if check_str != child_id:continue
            row_hash_dct[tas_taxonomy_id] = index_id 
            index_row_hash_map[index_id] = tas_taxonomy_id
            p_dct[index_id] = row_id
            level_info[index_id] = len(chk_lst) - 2
            if len(chk_lst) > 3:
                p_key = '.'.join(chk_lst[:-1])
                it_dat_dct.setdefault(p_key, []).append(index_id)
                
        def rec_ch_func(ch_lst):
            r_ls = []
            for ks in ch_lst:
                r_ls.append(ks)
                ss_ch = it_dat_dct.get(ks, [])
                if ss_ch:
                    dt_lst = rec_ch_func(ss_ch)
                    r_ls.extend(dt_lst)
            return r_ls
        
        des_dct = {}
        row_lst = []
        for pd in it_dat_dct:
            if len(pd.split('.')) != 3:continue
            row_lst.append(pd)
            g_ch_lst = it_dat_dct.get(pd, [])
            #sys.exit()
            if g_ch_lst:
                all_ch_data = rec_ch_func(g_ch_lst)
                row_lst.extend(all_ch_data)
        return row_hash_dct, index_row_hash_map, row_lst, level_info
        
    def DecimalToAlphaConverter(self, dec_val):
        if(dec_val<26):
           return chr(65+dec_val)
        if(dec_val > 25 and dec_val < 52):
            return chr(65)+chr(65+(dec_val-26))
        if(dec_val > 51 and dec_val < 78):
            return chr(66)+chr(65+(dec_val-52))
        if(dec_val > 77 and dec_val < 104):
            return chr(67)+chr(65+(dec_val-78))
        return "@@"
        
    def all_headers(self):
        r_lst = []
        for i in range(104):
            cy = self.DecimalToAlphaConverter(i)
            dt_dct = {"width":100,"type":"text","title":cy}
            if cy == 'A':
                dt_dct = {"width":200,"type":"text","title":cy}
            r_lst.append(dt_dct)
        return r_lst

    def create_style_data(self, hdr_lst, no_of_rows, restated_formula_info_dct):
        style_dct = {}
        map_dct = {'formula':'font-weight:bold;', 'Res':'color:#ccccff'}
        for i in range(no_of_rows+1):
            for hx, hdr_dct in enumerate(hdr_lst):
                ttl = hdr_dct['title']
                if hx == 0:
                    dta = ''.join(['text-align:left;', 'font-weight: 500;', 'color: #012b3b;'])
                else:   
                    formula_res  = restated_formula_info_dct.get((i, hx))
                    st_lst = ['text-align:right;']
                    for fl, cl_al in map_dct.iteritems():
                        if formula_res:
                            stl = formula_res.get(fl) 
                            if stl:
                                st_lst.append(cl_al)
                    dta = ''.join(st_lst)
                ttl_str = '%s%s'%(ttl, i)
                style_dct[ttl_str] = dta
        return style_dct

    def read_data_builder_data(self, ijson):
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        project_name = ijson['project_id']
        r_type  = ijson['data_id']         
        tas_taxo_id = ijson['taxo_id']
        parent_id, child_id = tas_taxo_id.split('.')[0], tas_taxo_id
        company_name = self.get_company_name(project_id, deal_id)

        row_hash_dct, index_row_hash_map, row_lst, level_info = self.read_child_info(company_name, project_id, child_id)    
        
        all_hashes = ', '.join(['"'+str(e)+'"' for e in row_hash_dct])
        
        #for kmt in row_lst: 
        #    print kmt, '\n' 
        #sys.exit()
        project_name = ''.join(project_name.split())
        map_dct = {
                'Raw':'/mnt/eMB_db/%s/%s/rawdb_info.db'%(company_name, project_id),
                'Restated':'/mnt/eMB_db/%s/%s/restated/%s.db'%(company_name, project_id, project_name),
                'Reported':'/mnt/eMB_db/%s/%s/reported/%s.db'%(company_name, project_id, project_name)
                    }
        db_path = map_dct.get(r_type) 
        #print db_path
        conn, cur = self.connect_to_sqlite(db_path)
        formula_check = self.read_formula_info_flg(cur)
        read_qry = """ SELECT row_id, row, col, value, cell_type, row_hash FROM data_builder WHERE row_hash IN (%s); """%(all_hashes)
        #print read_qry
        try:
            cur.execute(read_qry)
            t_data = cur.fetchall()
        except:
            return [{'message':'No Data'}]
        finally:
            conn.close()
        
        data_info_dct = {}
        for row_data in t_data:
            row_id, row, col, value, cell_type, row_hash = row_data
            formula_flg = 'N'
            if row_id in formula_check:
                formula_flg = 'Y'
            data_info_dct.setdefault(row_hash, {})[col] = (value, row_id, formula_flg)
                
        data_lst = []
        rowGroup = {}
        ref_info = {}
        restated_formula_info_dct = {}
        inf_map = {}
        for ix, k_idx in enumerate(row_lst):
            level_id = level_info[k_idx]
            row_hash_str = index_row_hash_map[k_idx]    
            row_hash_dct = data_info_dct[row_hash_str]
            r_inf_lst = []  
            all_cols = row_hash_dct.keys()
            all_cols.sort(key=lambda x:int(x))    
            for cl_ix, cl in enumerate(all_cols):
                val_tup = row_hash_dct[cl]
                val, rid, f_flg = val_tup 
                r_inf_lst.append(val)
                alpha_val = self.DecimalToAlphaConverter(cl_ix)
                rc_str = '%s%s'%(alpha_val, ix)
                inf_map[rc_str] = {'f_flg':f_flg, 'ref':rid}
            data_lst.append(r_inf_lst)
            rowGroup[ix] = level_id
        
        hdr_lst = self.all_headers()
        style_dct = self.create_style_data(hdr_lst, len(data_lst), {}) 
        inf_map['ref_info'] = ref_info
        inf_map['rowGroup'] = rowGroup
        res = [{'message':'done', 'data':data_lst, 'col_def':hdr_lst, 'map':inf_map, 'style':style_dct}]
        return res 

    def read_reference_info(self, ijson):
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        project_name = ijson['project_id']
        r_type  = ijson['data_id']         
        cell_row_id = ijson['rid']
        company_name = self.get_company_name(project_id, deal_id)
        project_name = ''.join(project_name.split())
        map_dct = {
                'Raw':'/mnt/eMB_db/%s/%s/rawdb_info.db'%(company_name, project_id),
                'Restated':'/mnt/eMB_db/%s/%s/restated/%s.db'%(company_name, project_id, project_name),
                'Reported':'/mnt/eMB_db/%s/%s/reported/%s.db'%(company_name, project_id, project_name)
                    }
        
        db_path = map_dct.get(r_type) 
        print db_path
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT xml_id, bbox, page_coords, doc_id, page_no, table_id FROM reference_table WHERE rawdb_row_id=%s; """%(cell_row_id)
        try:
            cur.execute(read_qry)
        except:
            return [{'message':'No Data'}]
        t_data = cur.fetchone()
        conn.close()
        xml_id, bbox, page_coords, doc_id, page_no, table_id = t_data 
        page_coords = eval(page_coords)
        bbox  = eval(bbox)
        path1   = '/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/'
        html_path = '/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/html/{1}_celldict.html'
        ref_path    = {
                        'ref_pdf':'%s{0}/pages/{1}.pdf'%(path1),
                        'ref_html':html_path
                        } 
        doc_id = str(doc_id)    
        table_id = str(table_id)
        data = {'d':doc_id, 'bbox':bbox, 'pno':page_no, 't':table_id, 'page_cord':{doc_id:{page_no:page_coords}}}  
        res = [{'message':'done', 'data':data, 'ref_path':ref_path}]
        return res

    def split_by_camel(self, txt):
        if isinstance(txt, unicode) :
            txt = txt.encode('utf-8')  
        if ' ' in txt:
            return txt
        txt_ar  = []
        for c in txt:
            if c.upper() == c:
                txt_ar.append(' ')
            txt_ar.append(c)
        txt = ' '.join(''.join(txt_ar).split())
        return txt

    def read_kpi_data(self, ijson, tinfo={}):
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        company_name = self.get_company_name(project_id, deal_id)
        model_number = project_id

        disp_name       = ''
        empty_line_display  = 'Y'
        if not ijson.get("template_id"):
            db_file     = '/mnt/eMB_db/page_extraction_global.db'
            conn, cur   = conn_obj.sqlite_connection(db_file)
            sql = "select industry_type, industry_id from industry_type_storage"
            cur.execute(sql)
            res = cur.fetchall()
            conn.close()
            exist_indus = {}
            for rr in res:
                industry_type, industry_id  = rr
                industry_type   = industry_type.lower()
                exist_indus[industry_type]  = industry_id
            tindustry_type  = ijson['table_type'].lower()
            industry_id = exist_indus[tindustry_type]
            db_file     = "/mnt/eMB_db/industry_kpi_taxonomy.db"
            conn, cur   = conn_obj.sqlite_connection(db_file)
        else:
            industry_id = ijson['template_id']
            db_file     = "/mnt/eMB_db/template_info.db"
            conn, cur   = conn_obj.sqlite_connection(db_file)
            sql = "select display_name, empty_line_display from meta_info where row_id=%s"%(industry_id)
            cur.execute(sql)
            tmpres  = cur.fetchone()
            disp_name   = tmpres[0]
            empty_line_display   = tmpres[1]
        empty_line_display  = 'Y'
        #print 'disp_name ', disp_name
        try:    
            sql = "alter table industry_kpi_template add column yoy TEXT"
            cur.execute(sql)
        except:pass
        try:    
            sql = "alter table industry_kpi_template add column editable TEXT"
            cur.execute(sql)
        except:pass
        try:
                sql = "alter table industry_kpi_template add column formula_str TEXT"
                cur.execute(sql)
        except:pass
        try:
                sql = "alter table industry_kpi_template add column taxo_type TEXT"
                cur.execute(sql)
        except:pass
        sql         = "select taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, target_currency from industry_kpi_template where industry_id=%s and taxo_type !='INTER'"%(industry_id)
        cur.execute(sql)
        res         = cur.fetchall()
        data_d      = {}
        grp_d       = {}
        all_table_types = {}
            
        for rr in res:
            taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo,yoy, editable, target_currency   = rr
            if client_taxo:
                taxo_label  = self.split_by_camel(client_taxo)
            #if scale in ['1.0', '1']:
            #    scale   = ''
            #if str(deal_id) == '51':
            #    scale   = ''
            grp_d.setdefault(parent_id, {})[prev_id]    = taxo_id
            data_d[taxo_id]  = (taxo_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, target_currency)
        final_ar    = []
        taxo_exists = {}
        found_open  = {}
        pc_d        = {}
        get_open_d  = {'done':"N"}
        print grp_d
        sys.exit()
        def form_tree_data(dd, level, pid, p_ar):
            prev_id = -1
            iDs = []
            pid = -1
            done_d  = {}
            if (pid not in dd) and dd:
                ks  = dd.keys()
                ks.sort()
                pid = ks[0]

            while 1:
                if pid not in dd:break
                ID  = dd[pid]
                if (ID, pid) in done_d:break #AVOID LOOPING
                done_d[(ID, pid)]  = 1
                pid = ID
                iDs.append(ID)
            tmp_ar  = []
            prev_id = -1
            for iD in iDs:
                if p_ar:
                    pc_d.setdefault(data_d[p_ar[-1]][1], {})[data_d[iD][1]]        = 1
                final_ar.append(data_d[iD]+(level, pid, prev_id))
                if tinfo and (tinfo.get(data_d[iD][1], {}).get('desc', {}).get('f') == 'Y' or tinfo.get(data_d[iD][1], {}).get('desc', {}).get('fm') == 'Y'):
                    for tmpid in p_ar+[iD]:
                        taxo_exists[data_d[tmpid][1]] = 1
                    if get_open_d['done'] == 'N':
                        for tmpid in p_ar+[iD]:
                            found_open[data_d[tmpid][1]]    = 1
                            
                        
                c_ids   = grp_d.get(iD, {})
                if c_ids:
                    form_tree_data(c_ids, level+1, iD, p_ar+[iD])
                prev_id = iD
                if level == 1 and found_open:
                    get_open_d['done']  = "Y"
        root    = grp_d[-1]
        form_tree_data(root, 1, -1, [])
        #print final_ar
        #sys.exit()
        return final_ar, taxo_exists, found_open, pc_d, disp_name, empty_line_display
        
    def read_phcsv_info(self, cur):
        read_qry = """ SELECT rawdb_row_id, period_type, period, currency, scale, value_type FROM phcsv_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        res_dct = {}
        for row_data in t_data:
            rawdb_row_id, period_type, period, currency, scale, value_type = row_data
            res_dct[rawdb_row_id] = {'pt':period_type, 'p':period, 'c':currency, 's':scale, 'vt':value_type}
        return res_dct

    def read_raw_DB(self, ijson):
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        company_name = self.get_company_name(project_id, deal_id)
        model_number = project_id
        table_type = ijson['table_type']
        group_id   =  ijson['grpid']
        if ijson.get('d_type') == 'Restated':
            project_name    =  ''.join(ijson['project_name'].split())
            db_path = '/mnt/eMB_db/%s/%s/Restated/%s.db'%(company_name, project_id, project_name)
        elif ijson.get('d_type') == 'Reported':
            project_name    =  ''.join(ijson['project_name'].split())
            db_path = '/mnt/eMB_db/%s/%s/Reported/%s.db'%(company_name, project_id, project_name)
        else:
            db_path = '/mnt/eMB_db/%s/%s/raw_DB.db'%(company_name, project_id)
        conn, cur = self.connect_to_sqlite(db_path)
        #phcsv_info_dct = self.read_phcsv_info(cur)
        read_qry = """ SELECT row_id, row, col, value, taxo_id, cell_ph, formula_flag, taxo_label, cell_type FROM data_builder WHERE table_type='%s' AND group_id='%s'; """%(table_type, group_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        data_dct = {}
        col_dct = {}
        for row_data in t_data:
            row_id, row, col, value, taxo_id, cell_ph, formula_flag, taxo_label, cell_type  = row_data
            if taxo_label:
                value = taxo_label
            if cell_type in ['GV'] and not value:continue
            #title_dct = phcsv_info_dct.get(row_id, {})
            #dt = {'v':value, 'title':title_dct, 'ref_k':row_id, 'ctype':cell_type}
            dt = {'v':value, 'ref_k':row_id, 'ck':cell_type}
            if formula_flag == 'Y':
                dt['cls'] = 'res_f'
            data_dct.setdefault(row, {})[col] = dt
            col_dct.setdefault(col, {})[cell_ph]  = 1 
            

        col_def_lst = []
        hgh_cl = sorted(col_dct.keys())
        for h_name, c in enumerate(hgh_cl, 1):
            ph_dct = col_dct[c]
            d_dct = {'k':c, 'n':''.join(ph_dct), 'w':80}
            if c == 0:
                d_dct = {'k':c, 'n':'Description', 'w':250}
            col_def_lst.append(d_dct)
            
        inf_map = {}
        data_lst = []
        row_keys = data_dct.keys()
        row_keys.sort()
        for ix, r_inf in enumerate(row_keys, 1):
            dt_dct = data_dct[r_inf] 
            for cl, cell_dct in dt_dct.items():
                rid = cell_dct['ref_k']
                #ctyp = cell_dct['ctype']
                #del cell_dct['ref_k']
                #del cell_dct['ctype']
                ##del cell_dct['title']
                #inf_map['%s_%s'%(ix, cl)] = {'ref_k':rid, 'ctype':ctyp}
            dt_dct['sn'] = ix 
            dt_dct['cid'] = ix 
            dt_dct['rid'] = ix 
            data_lst.append(dt_dct)
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res
        
    def gv_reference(self, company_name, project_id, rawdb_row_id, ijson):
        if ijson.get('d_type') == 'Restated':
            project_name    =  ''.join(ijson['project_name'].split())
            db_path = '/mnt/eMB_db/%s/%s/Restated/%s.db'%(company_name, project_id, project_name)
        elif ijson.get('d_type') == 'Reported':
            project_name    =  ''.join(ijson['project_name'].split())
            db_path = '/mnt/eMB_db/%s/%s/Reported/%s.db'%(company_name, project_id, project_name)
        else:
            db_path = '/mnt/eMB_db/%s/%s/raw_DB.db'%(company_name, project_id)
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT operand_row_id FROM formula_table WHERE rawdb_row_id='%s'; """%(rawdb_row_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        if t_data:
            t_data = tuple(t_data) + ((rawdb_row_id, ), )
        print t_data
        rid_str = ', '.join({str(es[0]) for es in t_data}) 
        if rid_str:
            r_qry = """ SELECT rawdb_row_id, xml_id, bbox, page_coords, doc_id, page_no, table_id FROM reference_table WHERE rawdb_row_id in (%s); """%(rid_str)
        else:   
            r_qry = """ SELECT rawdb_row_id, xml_id, bbox, page_coords, doc_id, page_no, table_id FROM reference_table WHERE rawdb_row_id='%s';  """%(rawdb_row_id)
        cur.execute(r_qry)
        mt_data = cur.fetchall()
        conn.close()
        data_lst = []
        for row_data in mt_data:
            rrid, xml_id, bbox, page_coords, doc_id, page_no, table_id = row_data
            dt_dct = {'x':xml_id, 'pno':page_no, 'd':doc_id, 'coord':eval(page_coords), 'bbox':eval(bbox), 't':table_id, 'F':'O', 'clr_flg':0}
            if not rid_str:
                del dt_dct['F']
                del dt_dct['clr_flg']
            if str(rawdb_row_id) == str(rrid):
                if rid_str:
                    dt_dct['F'] = 'R'
            data_lst.append(dt_dct)
        return data_lst

    def hgh_reference(self, company_name, project_id, rawdb_row_id):
        if ijson.get('d_type') == 'Restated':
            project_name    =  ''.join(ijson['project_name'].split())
            db_path = '/mnt/eMB_db/%s/%s/Restated/%s.db'%(company_name, project_id, project_name)
        elif ijson.get('d_type') == 'Reported':
            project_name    =  ''.join(ijson['project_name'].split())
            db_path = '/mnt/eMB_db/%s/%s/Reported/%s.db'%(company_name, project_id, project_name)
        else:
            db_path = '/mnt/eMB_db/%s/%s/raw_DB.db'%(company_name, project_id)
        conn, cur = self.connect_to_sqlite(db_path)
        r_qry = """ SELECT xml_id, bbox, page_coords, doc_id, page_no, table_id FROM reference_table WHERE rawdb_row_id='%s'; """%(rawdb_row_id)
        print r_qry
        cur.execute(r_qry)
        mt_data = cur.fetchone()
        print 'SSS', mt_data
        conn.close()
        data_lst = []
        if mt_data:
            xml_id, bbox, page_coords, doc_id, page_no, table_id = mt_data
            data_lst = [{'x':xml_id, 'pno':page_no, 'd':doc_id, 'coord':eval(page_coords), 'bbox':eval(bbox), 't':table_id}]
        return data_lst
        
    def data_builder_reference(self, ijson):
        rawdb_row_id = ijson['ref_k']
        ctype        = ijson['ctype']
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        company_name = self.get_company_name(project_id, deal_id)
        path1   = '/pdf_canvas/viewer.html?file=/var_html_path/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/pdfpagewise/{0}/{1}.pdf'%(project_id, deal_id)
        html_path   =  '/var_html_path/fundamentals_intf/output/%s_%s/Table_Htmls/{0}.html'%(project_id, deal_id)
        ref_path    = {
                        'ref_html':html_path,
                        'ref_pdf':path1,
                        }
        data_lst = []
        if ctype == 'GV':
            data_lst = self.gv_reference(company_name, project_id, rawdb_row_id, ijson)
        elif ctype == 'HGH':
            data_lst = self.hgh_reference(company_name, project_id, rawdb_row_id, ijson)
        return [{'message':'done', 'ref':data_lst, 'ref_path':ref_path}] 


        
if __name__ == '__main__':
    obj = SpreadSheet() 
    ijson =  {"cmd_id":29,"company_id":"20_149","project_id":"Key Banking Capital and Profitability Figures","template_id":10,"user":"TAS - UI","industry_type":"Financial%20Services", "table_type":"IS", "grpid":""}
    print obj.read_raw_DB(ijson)
    #print obj.read_kpi_data(ijson)
    #ijson = {'company_id':'20_16', 'project_id':'Key Banking Capital and Profitability Figures', 'data_id':'Raw', 'taxo_id':'1.1', 'rid':66985}  
    #print obj.read_reference_info(ijson)
    #print obj.DecimalToAlphaConverter(102)
    #print obj.read_data_builder_data(ijson)
    #print obj.read_taxo_data(ijson)
    #print obj.read_project_details(ijson)
