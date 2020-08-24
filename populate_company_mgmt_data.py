import os, sys, json, shelve
import config

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class INC_Company_Mgmt(object):
    
    def read_country(staelf, m_cur, row_id):
        read_qry = """ SELECT country FROM country WHERE id=%s; """%(row_id)
        m_cur.execute(read_qry)
        mt_data = m_cur.fetchone()
        country = ''
        if mt_data:
            country = mt_data[0]
        return country

    def read_company_list(self):
        m_cur, m_conn = conn_obj.MySQLdb_conn(config.Config.company_info_db) 
        read_qry = """ SELECT cm.row_id, cm.company_display_name, tk.ticker FROM company_mgmt AS cm INNER JOIN Ticker AS tk ON cm.row_id=tk.company_id; """
        m_cur.execute(read_qry)
        mt_data = m_cur.fetchall()
        data = [{'k':es[0], 'n':es[1], 't':es[2], 'c':self.read_country(m_cur, es[0])} for es in mt_data]   
        m_conn.close()
        res = [{'message':'done', 'data':data}]
        return res

    def get_inc_project_id(self, db_name):
        m_cur, m_conn = conn_obj.MySQLdb_conn(config.Config.inc_project_info) 
        read_qry = """ SELECT ProjectID FROM ProjectMaster WHERE ProjectCode='%s'; """%(db_name)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        m_conn.close()
        inc_project_id = ''
        if t_data:
            inc_project_id = t_data[0]
        return inc_project_id

    def get_level_info(self, inc_project_id, doc_id_lst):
        doc_wise_level_info_dct = {} 
        for doc_id in doc_id_lst:
            slv_path = config.Config.doc_slv_path.format(inc_project_id, doc_id)
            #print slv_path
            sh = shelve.open(slv_path)
            sh_data = sh['data']
            sh.close()
            if sh_data:
                for table_id, rc_dct_tup in sh_data.iteritems():
                    rc_dct = rc_dct_tup[0]
                    did, page, grid, dmy = table_id.split('#')
                    t_tup = tuple(map(int, [did, page, grid]))
                    for rc, dt_tup in rc_dct.iteritems():
                        level_id = dt_tup[0]
                        doc_wise_level_info_dct.setdefault(doc_id, {}).setdefault(t_tup, {})[rc] = level_id
        return doc_wise_level_info_dct

        
    def insert_table_type_info(self, table_type_set):
        db_path = '/mnt/eMB_db/company_management/global_info.db'        
        conn, cur   = conn_obj.sqlite_connection(db_path)

        crt_qry = """ CREATE TABLE IF NOT EXISTS all_table_types(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type TEXT, short_form VARCHAR(256)); """ 
        cur.execute(crt_qry)
    
        r_qry = """ SELECT table_type, short_form FROM all_table_types; """  
        cur.execute(r_qry)
        mt_data = cur.fetchall()      
        check_set = {(es[0], es[1]) for es in mt_data}
        diff_set = table_type_set - check_set 

        if diff_set:
            insert_stmt = """ INSERT INTO all_table_types(table_type, short_form) VALUES(?, ?); """
            cur.executemany(insert_stmt, diff_set)
            conn.commit()
        read_qry = """ SELECT row_id, table_type, short_form FROM all_table_types; """
        cur.execute(read_qry)
        mst_data = cur.fetchall()
        conn.close()
        row_id_map_tt = {(est[1], est[2]):est[0] for est in mst_data} 
        return row_id_map_tt

    def read_rowid_sf(self):
        db_path = '/mnt/eMB_db/company_management/global_info.db'        
        conn, cur   = conn_obj.sqlite_connection(db_path)

        read_qry = """ SELECT row_id, short_form FROM all_table_types; """
        cur.execute(read_qry)
        mst_data = cur.fetchall()
        conn.close()
        row_id_map_tt = {est[1]: est[0] for est in mst_data} 
        return row_id_map_tt
        
    def read_focus_data_mgmt(self, m_cur, m_conn, doc_str):
        read_qry = """ SELECT doc_id, table_name, page_no, grid_id FROM Focus_Data_mgmt WHERE doc_id in (%s) """%(doc_str)
        m_cur.execute(read_qry)
        mt_data = m_cur.fetchall()

        all_tabs_new_inc = {}
        table_type_set = set()
        for row in mt_data:
            doc_id, table_name, page_no, grid_id = row
            #= ''.join([es[0].upper() for es in table_name.split()]) 
            short_tt = '' 
            tt_str_lst = table_name.split()
            if len(tt_str_lst) == 1:
                short_tt = ''.join(tt_str_lst)
                short_tt = short_tt.upper()
            elif len(tt_str_lst) > 1:
                short_tt = ''.join([es[0].upper() for es in tt_str_lst])
            tt_tup = (table_name, short_tt)
            all_tabs_new_inc[(doc_id, page_no, grid_id)] = short_tt
            table_type_set.add(short_tt) 
            
        row_id_map_tt = self.read_rowid_sf() 
        #row_id_map_tt = self.insert_table_type_info(table_type_set)
        return all_tabs_new_inc, row_id_map_tt 
        
    def inject_rc_infomation(self, dpg_tup, gdata_dct, hgh_level_dct):
        did, page, grid = dpg_tup   
        data_dct = hgh_level_dct.get(did, {}).get(dpg_tup, {})
        if data_dct:
            for rc_tup, level_id in data_dct.iteritems():
                rc_str = '_'.join(map(str, rc_tup))
                gdata_dct['data'][rc_str]['level_id'] = level_id 
        return gdata_dct 
        
    def read_batch_mgmt_upload(self, m_cur, doc_str):
        read_qry = """ SELECT doc_id, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s); """%(doc_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        meta_data_lst = []
        for row_data in t_data:
            doc_id, doc_name, doc_type, meta_data = row_data
            if meta_data:
                meta_data = eval(meta_data)
            pt = meta_data.get('periodtype', '')
            year = meta_data.get('Year', '')
            filling_type = meta_data.get('FilingType', '')
            fye = meta_data.get('FY', '')
            meta_data = json.dumps(meta_data)
            dt_tup = (doc_id, doc_name, doc_type, pt, year, filling_type, fye, meta_data)
            meta_data_lst.append(dt_tup)
        return meta_data_lst
 
    def insert_document_meta_data(self, m_cur, cid_path, doc_str):
        ## READ batch_mgmt_upload
        meta_data_lst = self.read_batch_mgmt_upload(m_cur, doc_str)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        crt_stmt = """ CREATE TABLE IF NOT EXISTS document_meta_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, doc_name TEXT, doc_type TEXT, period_type VARCHAR(32), period VARCHAR(32), filing_type VARCHAR(32), fye VARCHAR(256), meta_data TEXT); """
        cur.execute(crt_stmt)
        try:
            del_stmt = """ DELETE FROM document_meta_info WHERE doc_id in (%s); """%(doc_str)
            cur.execute(del_stmt)
        except:pass 
        
        if meta_data_lst:
            insert_stmt = """ INSERT INTO document_meta_info(doc_id, doc_name, doc_type, period_type, period, filing_type, fye, meta_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?); """
            cur.executemany(insert_stmt, meta_data_lst)
            conn.commit()
        conn.close()
        return 

    def gen_page_coords(self, i_path, doc_id, db_path):
        doc_id  = str(doc_id)
        import get_all_bbox_coordinate_multiprocess
        page_dict   = get_all_bbox_coordinate_multiprocess.get_page_bbox(doc_id, i_path)
        i_ar    = []
        for k, v in page_dict.items():
            i_ar.append((k, str(v)))
        conn, cur   = conn_obj.sqlite_connection(db_path)
        try:
            del_stmt = """ DELETE FROM pagedet; """
            cur.execute(del_stmt)
        except:pass
        cur.executemany("insert into pagedet(pageno, pagesize)values(?, ?)", i_ar)
        conn.commit()
        conn.close()
        return 

    def poplate_equality(self, db_path):
        conn, cur   = conn_obj.sqlite_connection(db_path)
        
        sql  = 'CREATE TABLE IF NOT EXISTS equalityInfo (eid integer NOT NULL PRIMARY KEY AUTOINCREMENT, table_id1 text(50) NOT NULL, xml_ref1 text(100) NOT NULL,  r_c1 text(25) NOT NULL,  table_id2 text(50) NOT NULL, xml_ref2 text(100) NOT NULL,  r_c2 text(25) NOT NULL,  scale integer NOT NULL,  fType1 text(10) NOT NULL,  fType2 text(10) NOT NULL,  vType1 text(10) NOT NULL,  vType2 text(10) NOT NULL)'
        cur.execute(sql)
        sql = "select table_id1, r_c1, fType1, vType1, table_id2, r_c2, fType2, vType2 from equalityInfo"
        cur.execute(sql)
        res = cur.fetchall()
        if res:
            print 'equalityInfo Exists'
            conn.close()
            return
        print 'Populate equalityInfo'
        sql = "delete from equalityInfo"
        cur.execute(sql)
        sql = "SELECT groupid, row, col, celltype, gridids, res_opr, comp_type  FROM rawdb WHERE datatype in ('Table2')"
        cur.execute(sql)
        res = cur.fetchall()
        dd  = {}
        for r in res:
            groupid, row, col, celltype, gridids, res_opr, comp_type = r
            dd.setdefault(groupid, {})[(row, col)]  = (celltype, gridids, res_opr, comp_type)
        i_ar    = []
        for groupid, rc_info in dd.items():
            t1_d    = rc_info[(1, 1)]
            t2_d    = rc_info[(2, 2)]
            comp_type   = t1_d[3]
            table_id1   = '#'.join(t1_d[1].split('#')[:3])
            r_c1        = t1_d[0]
            fType1      = 'VR' if comp_type == 'G:C' else 'HR'
            vType1      =  t1_d[2]
            table_id2   = '#'.join(t2_d[1].split('#')[:3])
            r_c2        = t2_d[0]
            fType2      = 'VR' if comp_type == 'G:C' else 'HR'
            vType2      =  t2_d[2]
            i_ar.append((table_id1, r_c1, fType1, vType1, '', table_id2, r_c2, fType2,vType2, '', '1'))
        for irs in i_ar:
            #print 'IIII', irs, '\n'
            cur.executemany("insert into equalityInfo(table_id1, r_c1, fType1, vType1, xml_ref1, table_id2, r_c2, fType2, vType2, xml_ref2, scale)values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [irs])
        conn.commit()
        conn.close()
        
    def update_table_boundry(self, grid_dct, pno):
        ddict = grid_dct.get('data', {})
        r_cs = ddict.keys()
        r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
        rc_d    = {}
        x1      = {}
        y1      = {}
        w1      = {}
        h1      = {}
        bbox_d  = {}
        for r_c in r_cs:
            row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
            rc_d.setdefault(row, {})[col]   =  ddict[r_c]
            if ddict[r_c]['ldr'] in ['value', 'hch', 'gh', 'vch']:
                        #print (table_id, ddict[r_c]['xml_ids'])
                        pno = int(ddict[r_c]['xml_ids'].split('$$')[0].split('_')[1])
                        bl = []
                        bbox_lst = ddict[r_c]['bbox'].split('$$')
                        for bx in bbox_lst:#xmin_ymin_xmax_ymax
                            if bx:
                                b = map(lambda x:int(x), bx.split('_'))
                                x   = b[0]
                                y   = b[1]
                                w   = b[2]
                                h   = b[3] 
                                bbox_d.setdefault(int(pno), {'x':{}, 'y':{}, 'w':{}, 'h':{}})
                                bbox_d[pno]['x'][x]   = 1
                                bbox_d[pno]['y'][y]   = 1
                                bbox_d[pno]['w'][w]   = 1
                                bbox_d[pno]['h'][h]   = 1
        tmpbbox_d   = {}
        for k, v in bbox_d.items():
            x1  = v['x'].keys()
            x1.sort()
            x   = x1[0]
            y1  = v['y'].keys()
            y1.sort()
            y   = y1[0]
            w1  = v['w'].keys()
            w1.sort()
            w   = w1[-1] - x
            h1  = v['h'].keys()
            h1.sort()
            h   = h1[-1] - y
            tmpbbox_d[k]    = [x, y, w,h]
        #print tmpbbox_d
        tmpbbox_d = tmpbbox_d.get(pno, [])
        grid_dct['table_boundry'] = tmpbbox_d
        return grid_dct 

    def read_inc_grids_doc_wise(self, cur, conn, db_name, doc_id_lst, company_id, cid_path, inc_project_id):
        cmd = 'mkdir -p /mnt/eMB_db/company_management/%s/equality/'%(company_id)
        os.system(cmd)
        i_path  = config.Config.inc_path.format(inc_project_id, 1)
        for doc_id in doc_id_lst:
            db_path = '/mnt/eMB_db/company_management/%s/equality/%s.db'%(company_id, doc_id)
            if not os.path.exists(db_path):
                cmd = 'cp /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/newvalidation_test_db/TestDB/%s.db  /mnt/eMB_db/company_management/%s/equality/'%(doc_id, company_id)
                print cmd
                ##os.system(cmd)
            self.gen_page_coords(i_path, doc_id, db_path)
            #self.poplate_equality(db_path)
        sys.exit()
        hgh_level_dct =  self.get_level_info(inc_project_id, doc_id_lst)
        doc_str = ', '.join(map(str, doc_id_lst))
        inc_host_dct = config.Config.inc_host_info
        inc_host_dct['db'] = db_name
        m_cur, m_conn = conn_obj.MySQLdb_conn(inc_host_dct) 
        self.insert_document_meta_data(m_cur, cid_path, doc_str)
        # INSERT DOCUMENT META INFO
        read_qry = """ SELECT docid, pageno, groupid, sdata, udata FROM db_data_mgmt_grid_slt WHERE docid in (%s) AND  active_status = 'Y' AND groupid < 1000 ORDER BY docid ; """%(doc_str)
        m_cur.execute(read_qry)
        res = m_cur.fetchall()
        all_tabs_new_inc, row_id_map_tt = self.read_focus_data_mgmt(m_cur, m_conn, doc_str)
        m_conn.close()
        #try:
        #    drop_stmt = """ DROP TABLE table_mgmt; """
        #    cur.execute(drop_stmt)
        #except:pass
        crt_qry = """ CREATE TABLE IF NOT EXISTS table_mgmt(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, page_no INTEGER NOT NULL, grid_id INTEGER NOT NULL, classified_id INTEGER NOT NULL); """
        cur.execute(crt_qry)
        try:
            alter_stmt = """ ALTER TABLE table_mgmt ADD COLUMN highly_connected VARCHAR(10); """
            cur.execute(alter_stmt)
        except:pass

        sql = "delete from table_mgmt where doc_id in (%s) AND grid_id <1000"%(', '.join(map(lambda x:str(x), doc_id_lst)))
        cur.execute(sql)
        
        rms_qry = """ SELECT row_id, doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id in (%s); """%(doc_str)
        cur.execute(rms_qry)
        c_data = cur.fetchall()
        check_data = {tuple(map(int, r_d[1:])):r_d[0] for r_d in c_data}
        
        insert_rows = []
        update_rows = []

        sdata_udata_dct = {}
        for row_data in res:
            (docid, pageno, grid_id), (sdata, udata) = map(int, row_data[:3]), row_data[3:]
            tt_tup = all_tabs_new_inc.get((docid, pageno, grid_id), '')
            #if tt_tup not in ('IS', 'BS', 'CF'):continue
            t_rowid = row_id_map_tt.get(tt_tup, 0)
            if tt_tup not in ('IS', 'CF', 'BS'):
                t_rowid = 0
            hc_flg = 'Y'    
            if not t_rowid:
                hc_flg = 'N'
            dt_tup = (docid, pageno, grid_id, t_rowid, hc_flg)
            if (docid, pageno, grid_id) not in check_data:
                insert_rows.append(dt_tup)
            elif (docid, pageno, grid_id) in check_data:
                u_rid = check_data[(docid, pageno, grid_id)]
                update_rows.append((t_rowid, hc_flg, u_rid))
            dpg_tup = (docid, pageno, grid_id)
            gdata_dct = {}
            if udata:
                gdata_dct = json.loads(udata)
            else:
                gdata_dct = json.loads(sdata)
                ### INJECT
            gdata_dct = self.update_table_boundry(gdata_dct, pageno)
            gdata_dct = self.inject_rc_infomation(dpg_tup, gdata_dct, hgh_level_dct)
            sdata_udata_dct[(docid, pageno, grid_id)] = gdata_dct
            

        if insert_rows:
            insert_stmt = """ INSERT INTO table_mgmt(doc_id, page_no, grid_id, classified_id, highly_connected) VALUES(?, ?, ?, ?, ?); """ 
            cur.executemany(insert_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE table_mgmt SET classified_id=?, highly_connected=? WHERE row_id=?; """
            cur.executemany(update_stmt, update_rows)
        conn.commit()
        self.create_table_wise_json_file(company_id, cur, conn, sdata_udata_dct, doc_str)
        return  

    def create_table_wise_json_file(self, company_id, cur, conn, sdata_udata_dct, doc_str):
        read_qry = """  SELECT row_id, doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id in (%s); """%(doc_str)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        
        json_dir = config.Config.table_json_path.format(company_id)
        if not os.path.exists(json_dir):
            j_cmd = 'mkdir -p %s'%(json_dir)
            os.system(j_cmd) 

        for row_data in t_data:
            row_id, doc_id, page_no, grid_id = row_data
            dt_tup = (doc_id, page_no, grid_id)     
            gdata = sdata_udata_dct.get(dt_tup, '')
            dpg_info = '_'.join(map(str, [doc_id, page_no, grid_id]))
            if gdata:
                json_file_path = os.path.join(json_dir, '{0}.json'.format(dpg_info))        
                with open(json_file_path, 'w') as j:
                    json.dump(gdata, j)  
        return 
        
    def populate_table_information(self, ijson):
        company_id   = ijson['company_id']
        db_name  = ijson['db_name']
        doc_ids      = ijson['doc_ids']
        inc_project_id = self.get_inc_project_id(db_name)
        cid_path = config.Config.company_id_path.format(company_id)
        if not os.path.exists(cid_path):
            mk_cmd = 'mkdir -p %s'%(cid_path) 
            os.system(mk_cmd) 

        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        self.read_inc_grids_doc_wise(cur, conn, db_name, doc_ids, company_id, cid_path, inc_project_id)
        conn.close()
        return 'done' 
        
    def collect_all_highly_connected_tables(self, company_id, doc_ids):
        cid_dir = config.Config.equality_path.format(company_id)
        all_equa_tabs = {}
        for doc_id in doc_ids:
            cid_path = os.path.join(cid_dir, '{0}.db'.format(doc_id))
            conn, cur   = conn_obj.sqlite_connection(cid_path)
            read_qry = """ SELECT distinct(table_id1) FROM equalityInfo; """
            cur.execute(read_qry)
            t_data = cur.fetchall()
            conn.close()
            for rd in t_data:
                all_equa_tabs[rd[0]] = 1
        return all_equa_tabs
        
    def update_using_equality_info(self, ijson):
        company_id   = ijson['company_id']
        db_name  = ijson['db_name']
        doc_ids      = ijson['doc_ids']
        all_equa_tabs = self.collect_all_highly_connected_tables(company_id, doc_ids)
        dir_path = config.Config.company_id_path.format(company_id)
        cid_path = os.path.join(dir_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(cid_path)
        alter_table = """ ALTER TABLE table_mgmt ADD COLUMN highly_connected VARCHAR(10) DEFAULT 'N'; """
        try:
            cur.execute(alter_table)
        except:pass
        read_qry = """ SELECT row_id, doc_id, page_no, grid_id FROM table_mgmt; """        
        cur.execute(read_qry)
        t_data = cur.fetchall()
        update_rows = []
        for row_data in t_data: 
            row_id, doc_id, page_no, grid_id = row_data
            dpg_str = '{0}#{1}#{2}'.format(doc_id, page_no, grid_id)
            if dpg_str in all_equa_tabs:
                #print ('Y', row_id)
                update_rows.append(('Y', row_id)) 
        if update_rows:
            update_stmt = """ UPDATE table_mgmt SET highly_connected=? WHERE row_id=?; """
            cur.executemany(update_stmt, update_rows)
            conn.commit()
        conn.close()
        return [{'message':'done'}]

 
if __name__ == '__main__':
    ic_Obj = INC_Company_Mgmt()
    ijson = {"company_id":"1117", "db_name":"AECN_INC", "doc_ids":[5131]}     #OFG
    ##print ic_Obj.populate_table_information(ijson) ## D FUNC
    #print ic_Obj.update_using_equality_info(ijson) ## update highly connected column
