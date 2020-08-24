import os, sys, json, copy, sqlite3, lmdb, ast
import datetime
#import db_config as db_c
import config

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class Raw_Preview_DB(object):
    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur

    def cal_bobox_data_new(self, company_id):
        lmdb_folder      = os.path.join('/var/www/html/fundamentals_intf/output/', company_id, 'doc_page_adj_cords')
        doc_page_dict    = {}
        if os.path.exists(lmdb_folder):
            env = lmdb.open(lmdb_folder, readonly=True)
            txn = env.begin()
            if 1:
                cursor = txn.cursor()
                for doc_id, res_str in cursor:
                    if res_str:
                        page_dict = ast.literal_eval(res_str)
                        p_dct = {}
                        for pg, coord in page_dict.iteritems(): 
                            p_dct[str(pg)] = coord
                        doc_page_dict[doc_id] = p_dct 
        return doc_page_dict 
        
    def read_norm_data_mgmt(self, company_id):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT norm_resid, docid, pageno FROM norm_data_mgmt; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        
        res_dct = {}
        for row_data in t_data:
            norm_resid, docid, pageno = map(str, row_data)
            res_dct[norm_resid] = pageno
        return res_dct

    def read_hgh_data(self, row_data, table_type, row_id, page_map, page_coord, grp_id=''):
        col_id = 0
        ref_table_dct = {}
        taxo_name = row_data['taxo']
        t_l       = row_data['t_l']
        hgh_xml   = row_data['x']
        table_id  = row_data['t'] 
        doc_id    = row_data['d'] 
        l         = row_data['l']
        taxo_id   = row_data['t_id']
        bbox      = row_data['bbox']
        date_time = str(datetime.datetime.now())
        lchange   = row_data.get('lchange', 'N')
        page_no   = page_map[table_id]
        coord = page_coord.get(doc_id, {}).get(page_no, [])
        hgh_tup   = (table_type, grp_id, row_id, col_id, taxo_name, 'HGH', 'N', 'N', lchange, taxo_id, '', 'TAS-System', date_time) 
        ref_table_dct[(table_type, grp_id, row_id, col_id)] = (hgh_xml, str(bbox), str(coord), doc_id, page_no, table_id)
        return hgh_tup, ref_table_dct
        
    def read_gv_data(self, row_data, phs_lst, table_type, row_id, page_map, page_coord, grp_id=''):
        taxo_id   = row_data['t_id']
        gv_data_lst = []
        ref_table_dct = {} 
        phcsv_data_dct     = {} 
        for col_id, col_inf in enumerate(phs_lst, 1):
            col_pk = col_inf['k']
            if col_pk not in row_data:continue
            cell_info_dct = row_data[col_pk]
            r = cell_info_dct['r'] 
            table, col, dmy = col_pk.split('-')
            bbox       = cell_info_dct['bbox'] 
            value      = cell_info_dct['v'] 
            xml_id     = cell_info_dct['x'] 
            table_id   = cell_info_dct['t']        
            doc_id     = cell_info_dct['d']
            #page_no    = cell_info_dct['p']
            table_id   = cell_info_dct['t']
            phcsv_dct  = cell_info_dct['phcsv']
            period, period_type = phcsv_dct['p'], phcsv_dct['pt']
            currency, scale, value_type = phcsv_dct['c'], phcsv_dct['s'], phcsv_dct['vt']
            fcol_data   = cell_info_dct.get('f_col', '')
            f_col_flg = 'N'
            if fcol_data:
                f_col_flg = 'Y'    
            cell_ph = ''.join((period_type, period))
            date_time = str(datetime.datetime.now())
            page_no   = page_map[table_id]
            coord = page_coord.get(doc_id, {}).get(page_no, [])
            dt_tup = (table_type, grp_id, row_id, col_id, value, 'GV', 'N', f_col_flg, 'N', taxo_id, cell_ph, 'TAS-System', date_time)
            gv_data_lst.append(dt_tup)
            ref_table_dct[(table_type, grp_id, row_id, col_id)] = (xml_id, str(bbox), str(coord), doc_id, page_no, table_id)
            phcsv_data_dct[(table_type, grp_id, row_id, col_id)] = (period_type, period, currency, scale, value_type) 
        return gv_data_lst, ref_table_dct, phcsv_data_dct

    def insert_raw_db_info(self, conn, cur, insert_rows, company_name, model_number):    
        try:
            drop_stmt = """ DROP TABLE reference_table;  """    
            cur.execute(drop_stmt)
        except:
            pass
        crt_stmt = """ CREATE TABLE IF NOT EXISTS rawdb(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type VARCHAR(100), group_id VARCHAR(100), row INTEGER, col INTEGER, value TEXT,  cell_type VARCHAR(100), restated_flag VARCHAR(2) DEFAULT 'N', formula_flag VARCHAR(2) DEFAULT 'N', label_change_flag VARCHAR(2), taxo_id INTEGER, cell_ph VARCHAR(10), user_name VARCHAR(50), datetime TEXT); """
        cur.execute(crt_stmt)
        insert_stmt = """ INSERT INTO rawdb(table_type, group_id, row, col, value, cell_type, restated_flag, formula_flag, label_change_flag, taxo_id, cell_ph, user_name, datetime) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """
        cur.executemany(insert_stmt, insert_rows)
        conn.commit()
        reaq_qry = """ SELECT table_type, group_id, row, col, row_id FROM rawdb; """    
        cur.execute(reaq_qry)
        t_data = cur.fetchall()
        res_dct = {(e[0], e[1], e[2], e[3]):e[4]  for e in t_data}
        return res_dct

    def alter_table_coldef(self, conn, cur, table_name, coldef):
        col_info_stmt   = 'pragma table_info(%s);'%table_name
        cur.execute(col_info_stmt)
        all_cols        = cur.fetchall()
        cur_coldef      = set(map(lambda x:str(x[1]), all_cols))
        exists_coldef   = set([es[0] for es in coldef])
        new_cols        = list(exists_coldef.difference(cur_coldef))
        col_list = []
        for new_col in coldef:
            if new_col[0] not in new_cols:continue
            col_list.append(' '.join(new_col))
        for col in col_list:
            alter_stmt = 'alter table %s add column %s;'%(table_name, col)
            #print alter_stmt
            try:
                cur.execute(alter_stmt)
            except:pass
        conn.commit()
        return 'done'
        
    def insert_raw_db_info_rw(self, conn, cur, insert_rows, restated_flg, delete_tt):    
        delete_tt_str = ', '.join(['"'+str(e)+'"' for e in delete_tt]) 

        reaq_qry = """ SELECT row_id FROM data_builder WHERE table_type in (%s); """%(delete_tt_str)   
        try:
            cur.execute(reaq_qry)
            t_data = cur.fetchall()
        except:
            t_data  = []
        
        delete_rids = ', '.join([str(e[0]) for e in t_data])

        try:
            #drop_stmt = """ DROP TABLE data_builder;  """    
            drop_stmt = """ DELETE FROM data_builder WHERE row_id in (%s);  """%(delete_rids)
            cur.execute(drop_stmt)
        except:
            pass

        crt_stmt = """ CREATE TABLE IF NOT EXISTS data_builder(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type VARCHAR(100), taxo_group_id VARCHAR(100), row INTEGER, col INTEGER, value TEXT,  cell_type VARCHAR(100), restated_flag VARCHAR(2) DEFAULT 'N', formula_flag VARCHAR(2) DEFAULT 'N', label_change_flag VARCHAR(2), taxo_id TEXT, cell_ph VARCHAR(10), user_name VARCHAR(50), datetime TEXT, row_hash VARCHAR(100), src_row VARCHAR(16), src_col VARCHAR(16), check_sum TEXT); """
        cur.execute(crt_stmt)
        self.alter_table_coldef(conn, cur, 'data_builder', [('col_groupid', 'INTEGER'), ('ref_groupid', 'INTEGER'), ('super_key', 'TEXT'), ('super_key_poss', 'TEXT')])
        #insert_stmt = """ INSERT INTO rawdb(table_type, taxo_group_id, row, col, value, cell_type, restated_flag, formula_flag, label_change_flag, taxo_id, cell_ph, user_name, datetime, row_hash, src_row, src_col) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """
        for r in insert_rows:
            #print r, '\n'
            insert_stmt = """ INSERT INTO data_builder(table_type, taxo_group_id, row, col, value, cell_type, restated_flag, formula_flag, label_change_flag, taxo_id, cell_ph, user_name, datetime, row_hash, src_row, src_col, check_sum, col_groupid, super_key, super_key_poss) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """
            #print 'SSSSSSSSSSSSS', [r]
            try:
                cur.executemany(insert_stmt, [r])
            except:
                insert_stmt = """ INSERT INTO data_builder(table_type, taxo_group_id, row, col, value, cell_type, restated_flag, formula_flag, label_change_flag, taxo_id, cell_ph, user_name, datetime, row_hash, src_row, src_col, check_sum, col_groupid, super_key, super_key_poss) VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s") """%(r)
                cur.execute(insert_stmt)
        conn.commit()

        reaq_qry = """ SELECT table_type, taxo_group_id, row, col, row_id, src_row, src_col FROM data_builder; """    
        cur.execute(reaq_qry)
        t_data = cur.fetchall()
        #res_dct = {(e[0], e[1], e[2], e[3]):e[4]  for e in t_data}
        res_dct = {}
        res_dct_src = {}
        for e in t_data:
            t_t, tx_group_id, rw, cl, rw_id, src_r, src_c = e
            res_dct[(str(t_t), str(tx_group_id), str(src_r), str(src_c))]  = rw_id
            res_dct_src[(str(t_t), str(tx_group_id), str(src_r), str(src_c))] = rw_id
            res_dct_src[(str(t_t), str(src_r), str(src_c))] = rw_id

        return res_dct, delete_rids, res_dct_src

    def insert_ref_data(self, conn, cur, ref_table_info_dct, tt_rc_map_dct, delete_rids):
        #print tt_rc_map_dct
        insert_rows = []
        for tt_info, dt_tup in ref_table_info_dct.iteritems():
            auto_id = tt_rc_map_dct[tt_info]
            dt_tup = (auto_id, ) + dt_tup   
            insert_rows.append(dt_tup)
        if insert_rows:
            try:
                #drop_stmt = """ DROP TABLE reference_table;  """    
                drop_stmt = """ DELETE FROM reference_table WHERE rawdb_row_id in (%s); """%(delete_rids)
                cur.execute(drop_stmt)
            except:
                pass
            crt_qry = """ CREATE TABLE IF NOT EXISTS reference_table(row_id INTEGER PRIMARY KEY AUTOINCREMENT, rawdb_row_id INTEGER, xml_id TEXT, bbox TEXT, page_coords TEXT, doc_id INTEGER, page_no INTEGER, table_id INTEGER); """  
            cur.execute(crt_qry)
            insert_stmt = """  INSERT INTO reference_table(rawdb_row_id, xml_id, bbox, page_coords, doc_id, page_no, table_id) VALUES(?, ?, ?, ?, ?, ?, ?); """ 
            cur.executemany(insert_stmt, insert_rows)
            conn.commit()
        return 
    
    def insert_phcsv_data(self, conn, cur, phcsv_info_dct, tt_rc_map_dct, delete_rids):
        insert_rows = []
        #print 'RRRRRRRRRRRRRRRRR'
        for tt_info, dt_tup in phcsv_info_dct.iteritems():
            #print [tt_info, dt_tup]
            auto_id = tt_rc_map_dct[tt_info]
            dt_tup = (auto_id, ) + dt_tup   
            insert_rows.append(dt_tup)
        if insert_rows:
            try:
                #drop_stmt = """ DROP TABLE phcsv_info;  """    
                drop_stmt = """ DELETE FROM phcsv_info WHERE rawdb_row_id in (%s);  """%(delete_rids)
                cur.execute(drop_stmt)
            except:
                pass
            crt_qry = """ CREATE TABLE IF NOT EXISTS phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, rawdb_row_id INTEGER, period_type VARCHAR(10), period VARCHAR(10), currency VARCHAR(100), scale VARCHAR(100), value_type VARCHAR(100)); """
            cur.execute(crt_qry)
            
            #insert_stmt = """  INSERT INTO phcsv_info(rawdb_row_id, period_type, period, currency, scale, value_type) VALUES(?, ?, ?, ?, ?, ?); """ 
            
            #cur.executemany(insert_stmt, insert_rows)
            for ms in insert_rows:  
                insert_stmt = """  INSERT INTO phcsv_info(rawdb_row_id, period_type, period, currency, scale, value_type) VALUES('%s', '%s', '%s', '%s', '%s', '%s'); """%ms
                print ms
                cur.execute(insert_stmt) 
            conn.commit()
        return 

    def read_for_raw_db(self, ijson):
        company_name    = ijson['company_name']
        model_number    = ijson['model_number']
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        page_map        = self.read_norm_data_mgmt(company_id) 
        page_coord      = self.cal_bobox_data_new(company_id)
        from get_db_data import Validate
        v_Obj = Validate()
        tt_data_dct = v_Obj.raw_builder(ijson)  
        row_id = 0
        ref_table_info_dct = {}
        phcsv_info_dct     = {}
        insert_m_rows = []
        #print '#' * 70
        for (table_type, grp_id), data_info in tt_data_dct.iteritems():
            data_info_dct = data_info[0] 
            r_data = data_info_dct['data']
            r_phs  = data_info_dct['phs'] 
            for row_id, row_data in enumerate(r_data):
                hgh_tup, ref_table_dct_hgh              = self.read_hgh_data(row_data, table_type, row_id, page_map, page_coord, grp_id='')
                insert_m_rows.append(hgh_tup)
                gv_data_lst, ref_table_dct, phcsv_dct   = self.read_gv_data(row_data, r_phs, table_type, row_id, page_map, page_coord, grp_id='')
                insert_m_rows.extend(gv_data_lst) 
                ref_table_info_dct.update(ref_table_dct)
                #print ['MMMMMMMMMMMMMMMMMM', (table_type, grp_id), row_id, row_data], phcsv_dct
                phcsv_info_dct.update(phcsv_dct)
        db_path = '/mnt/eMB_db/%s/%s/raw_preview.db'%(company_name, model_number)
        conn, cur  = self.connect_to_sqlite(db_path)            
        tt_rc_map_dct = {}
        if insert_m_rows:
            tt_rc_map_dct = self.insert_raw_db_info(conn, cur, insert_m_rows, company_name, model_number)
        if ref_table_info_dct:
            self.insert_ref_data(conn, cur, ref_table_info_dct, tt_rc_map_dct)
        if phcsv_info_dct:  
            #print phcsv_info_dct
            self.insert_phcsv_data(conn, cur, phcsv_info_dct, tt_rc_map_dct)
        conn.close()
        
    def read_doc_page_coords(self, company_id, doc_id):
        dir_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(dir_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT pageno, pagesize FROM pagedet; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()        

        page_dct = {}
        for row_data in t_data:
            pageno, pagesize = row_data
            page_dct[pageno]  = pagesize
        return page_dct 
    
    def insert_col_grp_info(self, conn, cur,  col_i_dct):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS column_group(row_id INTEGER PRIMARY KEY AUTOINCREMENT, group_name TEXT); """
        cur.execute(crt_stmt)
        read_qry = """ SELECT group_name, row_id FROM column_group; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        column_grp_dct = {es[0]:es[1] for es in t_data}
        insert_rows = []
        for uid, grp_dct in col_i_dct.iteritems():
            grp_name = grp_dct['group_name']
            if grp_name in grp_name:continue
            insert_rows.append((grp_name, )) 
        if insert_rows:
            insert_stmt = """ INSERT INTO column_group(group_name) VALUES(?); """
            cur.executemany(insert_stmt, insert_rows)
        conn.commit()

        read_qry = """ SELECT group_name, row_id FROM column_group; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        column_grp_dct = {es[0]:es[1] for es in t_data}
        return column_grp_dct
        
    def insert_rawdb_info(self, db_path, data_dct, patt, col_group_info, formula_lst, row_lc_d):
        #print 'FFFFFFFFFFFFFFFFFFFFFFF', formula_lst
        #return
        #sys.exit()
        print 'DDDDDDDDDDDDDDD', db_path
        company_id = db_path.split('/')[4]
        #sys.exit()
        page_coord = {}
        #dir_path = db_c.Config.doc_builder_path.format(company_id)
        #if not os.path.exists(dir_path):
        #    os.makedirs('{0}'.format(dir_path))
        #db_path = os.path.join(dir_path, '{0}.db'.format(db_doc))
        hgh_info_dct        = {}
        reference_table_dct = {}
        phcsv_table_dct     = {}
        row_lst             = []
        row_id = 0
        delete_tt = {}
        doc_id_page_cords = {}
        col_class_info = {}
        target_col_cls_info_dct = {}
        for row_hash, col_dct in data_dct.iteritems():
            cols = col_dct.keys()
            cols.sort()
            gv_col = 1
            for cl in cols:
                gv_hgh_info_lst = col_dct[cl]
                #print 'GGGGGGGGGGGGG', gv_hgh_info_lst, '\n'
                #continue
                row, col, row_taxo, taxo_level,  row_taxo_id, row_order, rowd_txt, grp_text, grp_id, cellph, cellcsvc, cellcsvs, cellcsvv, gvbbox, pgh_text, pgh_xml_ids, gh_text, gh_xml_ids, pvgh_text, pvgh_xml_ids, vgh_text, vgh_xml_ids, hgh_text, hgh_xml_ids, pgh_bbox, gh_bbox, pvgh_bbox, vgh_bbox, hgh_bbox, clean_value, doc_id, table_id, gv_text, gv_xml_ids, hkey, rep_flg, res_flg , line_hashkey, table_type, col_d_key, st_i, column_class_lst, target_col_cls_dct, col_g_id, super_key, super_key_poss = gv_hgh_info_lst
                if target_col_cls_dct:
                    #print 'TTTTTTTTTTTTTTTTT', gv_hgh_info_lst, 
                    #print (str(table_type), str(line_hashkey), str(row), str(col)), '\n'
                    pass
                
                #print 'MMMMMMMMMMMMMMMMMMMMMMMM', target_col_cls_dct
                #hgh_data = hgh_info_dct.get((row_hash, row), ()) 
                date_time = str(datetime.datetime.now())
                #page_no = gv_xml_ids[0].split('_')[-1] #page_map[str(table_id)]
                page_no = int(table_id.split('_')[1])
                #coord   = page_coord.get(str(doc_id), {}).get(page_no, '[]') 

                if int(doc_id) not in page_coord:
                    page_c_dct = self.read_doc_page_coords(company_id, doc_id)
                    page_coord[int(doc_id)] = page_c_dct
                coord   = page_coord.get(int(doc_id), {}).get(int(page_no), '[]') 
                    
                gv_tup = (str(table_type), str(line_hashkey), row_id, gv_col, ''.join(gv_text), st_i, 'N', 'N', 'N', str(row_taxo_id), cellph, 'TAS-System', date_time, str(row_hash), str(row), str(col), 0, col_g_id, super_key, super_key_poss) 
                row_lst.append(gv_tup)
                period_type, period = cellph[-4:], cellph[:-4]
                reference_table_dct[(str(table_type), str(line_hashkey), str(row), str(col))] = (''.join(gv_xml_ids), str(gvbbox), str(coord), doc_id, page_no, table_id) 
                phcsv_table_dct[(str(table_type), str(line_hashkey), str(row), str(col))] = (period_type, period, cellcsvc, cellcsvs, cellcsvv) 
                delete_tt[str(table_type)] = 1
                col_class_info[(str(table_type), str(line_hashkey), str(row), str(col))] = column_class_lst 
                target_col_cls_info_dct[(str(table_type), str(line_hashkey), str(row), str(col))] = target_col_cls_dct
                gv_col += 1
            row_id += 1 
    
    
    
        #print     
        #for rid, r_tup in hgh_info_dct.iteritems():
        #    row_lst.append(r_tup)          

        conn, cur  = self.connect_to_sqlite(db_path)            
        column_grp_dct = self.insert_col_grp_info(conn, cur,  col_group_info)
        conn.close()

        tt_rc_map_dct = {}
        if row_lst:
            conn, cur  = self.connect_to_sqlite(db_path)            
            tt_rc_map_dct, delete_rids, tt_rc_map_dct_src = self.insert_raw_db_info_rw(conn, cur, row_lst, 'N', delete_tt)
            conn.close()
            
        if reference_table_dct:
            conn, cur  = self.connect_to_sqlite(db_path)            
            self.insert_ref_data(conn, cur, reference_table_dct, tt_rc_map_dct, delete_rids)
            conn.close()

        if phcsv_table_dct:  
            conn, cur  = self.connect_to_sqlite(db_path)            
            self.insert_phcsv_data(conn, cur, phcsv_table_dct, tt_rc_map_dct, delete_rids)
            conn.close()

        if col_class_info:
            self.insert_column_class_info(db_path, col_class_info, tt_rc_map_dct, delete_rids)

        if target_col_cls_info_dct:
            #conn, cur  = self.connect_to_sqlite(db_path)            
            self.insert_column_class_info_target(db_path, target_col_cls_info_dct, tt_rc_map_dct, delete_rids, tt_rc_map_dct_src)
        if formula_lst:
            #conn, cur  = self.connect_to_sqlite(db_path)            
            self.insert_formula_info(db_path, formula_lst, delete_rids)
            #conn.close()
        if row_lc_d:
            self.insert_label_changes_info(db_path, row_lc_d, delete_rids, tt_rc_map_dct_src)
            
        #if col_group_info:
        #    self.insert_column_group_info(conn, cur, col_group_info, delete_rids, tt_rc_map_dct_src, column_grp_dct)
        conn.close()
        return 
        
    def insert_column_group_info(self, conn, cur, col_group_info, delete_rids, tt_rc_map_dct_src, group_id_dct):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS column_group_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, group_name TEXT, rawdb_row_id INTEGER); """
        cur.execute(crt_stmt)
        del_stmt = """ DELETE FROM column_group_info WHERE rawdb_row_id in (%s); """%(delete_rids)
        cur.execute(del_stmt)
        update_rows = []
        insert_rows = []#insert_col_grp_info(self, conn, cur,  col_i_dct)
        # table_type, taxo_group_id, row, col
        for uid, gp_dct in col_group_info.iteritems():
            grp_name, info_lst = gp_dct['group_name'], gp_dct['info']
            act_gp_id = group_id_dct[grp_name]
            for i_tup in info_lst:
                tt, grp_id, row, col, i_text = i_tup
                rawdb_rid = tt_rc_map_dct_src[(tt, grp_id, row, col)]
                insert_rows.append((act_gp_id, i_text, rawdb_rid))
                update_rows.append((act_gp_id, tt, grp_id, row, col, uid))
        if insert_rows:
            insert_stmt = """ INSERT INTO column_group_info(group_id, group_name, rawdb_row_id) VALUES(?, ?, ?); """
            cur.executemany(insert_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE data_builder SET ref_groupid=? WHERE table_type=? AND taxo_group_id=? AND src_row=? AND src_col=? AND col_groupid=?; """
            cur.executemany(update_stmt, update_rows)
        conn.commit()
        return
        
    def insert_column_class_info(self, db_path, col_class_info, tt_rc_map_dct, delete_rids):
        #crt_qry = """ CREATE TABLE IF NOT EXISTS phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, rawdb_row_id INTEGER, period_type VARCHAR(10), period VARCHAR(10), currency VARCHAR(100), scale VARCHAR(100), value_type VARCHAR(100)); """
        insert_rows = [] 
        for tt_tup, cls_lst in col_class_info.iteritems(): 
            rid = tt_rc_map_dct[tt_tup]
            for cls_i in cls_lst:
                d_tup = (rid, cls_i) 
                insert_rows.append(d_tup)
    
        conn, cur  = self.connect_to_sqlite(db_path)            
        try:
            del_stmt = """ DELETE FROM column_classification WHERE rawdb_row_id in (%s); """%(delete_rids)
            cur.execute(del_stmt)
        except:pass
        crt_qry = """ CREATE TABLE IF NOT EXISTS column_classification(row_id INTEGER PRIMARY KEY AUTOINCREMENT, rawdb_row_id INTEGER, column_class TEXT); """
        cur.execute(crt_qry)
        insert_stmt = """ INSERT INTO column_classification(rawdb_row_id, column_class) VALUES(?, ?); """
        cur.executemany(insert_stmt, insert_rows)
        conn.commit()
        conn.close()
        return

    def insert_label_changes_info(self, db_path, row_lc_d, delete_rids, tt_rc_map_dct_src):
        insert_rows = [] 
        for ttype, rowd in row_lc_d['ROW'].items():
            for row, cold in rowd.items():
                for  label, cols in cold.items():
                    for col in cols.keys():
                        rel_tup = (ttype, str(row), str(col))
                        rel_id = tt_rc_map_dct_src[rel_tup]
                        insert_rows.append((ttype, 'ROW', rel_id, label))

        for ttype, rowd in row_lc_d['COL'].items():
            for col, rowd in rowd.items():
                for label, rows in rowd.items():
                    for row in rows.keys():
                        rel_tup = (ttype, str(row), str(col))
                        rel_id = tt_rc_map_dct_src[rel_tup]
                        insert_rows.append((ttype, 'COL', rel_id, label))
        
        #print 'IIIIIIIIIIIIIIIIIIII', insert_rows
        conn, cur  = self.connect_to_sqlite(db_path)            
        try:
            del_stmt = """ DELETE FROM label_changes WHERE rawdb_row_id in (%s); """%(delete_rids)
            cur.execute(del_stmt)
        except:pass
        try:
            cur.execute('alter table label_changes add column table_type INTEGER')
        except:pass
        crt_qry = """ CREATE TABLE IF NOT EXISTS label_changes(row_id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, rawdb_row_id INTEGER, label TEXT, table_type INTEGER); """
        cur.execute(crt_qry)
        #self.alter_table_coldef(conn, cur, 'target_column_classification', ['relation_primary_key'])
        insert_stmt = """ INSERT INTO label_changes(table_type, type, rawdb_row_id,  label) VALUES(?, ?, ?, ?); """
        cur.executemany(insert_stmt, insert_rows)
        conn.commit()
        conn.close()
        return

    def insert_column_class_info_target(self, db_path, col_class_info, tt_rc_map_dct, delete_rids, tt_rc_map_dct_src):
        #crt_qry = """ CREATE TABLE IF NOT EXISTS phcsv_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, rawdb_row_id INTEGER, period_type VARCHAR(10), period VARCHAR(10), currency VARCHAR(100), scale VARCHAR(100), value_type VARCHAR(100)); """
        insert_rows = [] 
        for tt_tup, cls_dct in col_class_info.iteritems(): 
            rid = tt_rc_map_dct[tt_tup]
            for cls_i, relation_lst in  cls_dct.iteritems():
                if not relation_lst:
                    d_tup = (rid, cls_i, 0)
                    insert_rows.append(d_tup)
                for rel_tup in relation_lst:
                    rel_tup = tuple(map(str, rel_tup))
                    rel_id = tt_rc_map_dct_src[rel_tup]
                    d_tup = (rid, cls_i, rel_id)
                    insert_rows.append(d_tup)        
        
        #print 'IIIIIIIIIIIIIIIIIIII', insert_rows
        conn, cur  = self.connect_to_sqlite(db_path)            
        try:
            del_stmt = """ DELETE FROM target_column_classification WHERE rawdb_row_id in (%s); """%(delete_rids)
            cur.execute(del_stmt)
        except:pass
        crt_qry = """ CREATE TABLE IF NOT EXISTS target_column_classification(row_id INTEGER PRIMARY KEY AUTOINCREMENT, rawdb_row_id INTEGER, column_class TEXT, relation_primary_key INTEGER); """
        cur.execute(crt_qry)
        #self.alter_table_coldef(conn, cur, 'target_column_classification', ['relation_primary_key'])
        insert_stmt = """ INSERT INTO target_column_classification(rawdb_row_id, column_class, relation_primary_key) VALUES(?, ?, ?); """
        cur.executemany(insert_stmt, insert_rows)
        conn.commit()
        conn.close()
        return
    
    def get_rawdb_row_id(self, conn, cur):
        read_qry = """ SELECT row_hash, src_col, table_type, taxo_group_id, row_id, value FROM data_builder;  """
        cur.execute(read_qry)
        t_data = cur.fetchall()   
        
        taxo_tt_dct = {}
        for row_data in t_data:
            row_hash, src_col, table_type, taxo_group_id, row_id, value = map(str, row_data)
            taxo_tt_dct[(table_type, taxo_group_id, row_hash, src_col)] = (row_id, value)
        return taxo_tt_dct
    
    def insert_formula_info(self, db_path, formula_array, del_ids):
    
        conn, cur  = self.connect_to_sqlite(db_path)            
        taxo_map_comp = self.get_rawdb_row_id(conn, cur) 
        conn.close()

        insert_rows = []
        chk_sum_update = {}
        chk_sum_rows_update = []
        data_lst = formula_array[:]
        update_rawdb_formula_flg = []

        '''
        [['', formula_id, (resultant_sign, res_row, res_col, res_table_type, res_group_id, res_taxo_level), [(operand_sign, operand_row, operand_col, operand_table_type, operand_group_id, opr_taxo_level), (operand_sign_2, operand_row_2, operand_col_2, operand_table_type_2, operand_group_id_2, opr_taxo_level_2)], check_sum], []...]
        '''
        res_tx_lvl = op_tx_lvl = ''  
    
        for f_id, formula_lst in enumerate(data_lst, 1):
            d_key, src_form_id, res_gv, opr_lst, chk_sm = formula_lst[0], formula_lst[1], formula_lst[2], formula_lst[3:-1], formula_lst[-1]
            #res_sign, rw, cl, tt, tx_grp_id, res_tx_lvl = map(str, res_gv[:])
            res_sign, f_cel_type, const_value, tt, tx_grp_id, rw, cl = map(str, res_gv)
            
            gv_rid, res_txt = taxo_map_comp[(tt, tx_grp_id, rw, cl)]
            chk_info  = chk_sum_update.get(gv_rid, ())
            if not chk_info:
                chk_sum_update[gv_rid] = 1
                chk_sum_rows_update.append(('0', gv_rid))
            for opr_tup in opr_lst[0]:
                #op_sign, op_rw, op_cl, op_tt, op_tx_grp_id, op_tx_lvl = map(str, opr_tup[:])
                #res_sign, f_cel_type, const_value, tt, tx_grp_id, rw, cl = map(str, res_gv)
                #print 'OOOOOOOOOOOOOOOOO', opr_tup
                op_sign, op_f_cel_type, const_value, op_tt, op_tx_grp_id, op_rw, op_cl = map(str, opr_tup)
                if op_f_cel_type == 'v':
                    op_const_tup = (gv_rid, f_id, op_sign, 0, const_value, '0', src_form_id, op_f_cel_type, d_key)
                    insert_rows.append(op_const_tup)
                    continue
                op_gv_rid, op_txt = taxo_map_comp[(op_tt, op_tx_grp_id, op_rw, op_cl)]
                try:
                    op_txt = op_txt.decode('utf-8')
                except:pass
                insert_rows.append((gv_rid, f_id, op_sign, op_gv_rid, op_txt, '0', src_form_id,  op_f_cel_type, d_key)) 
            update_rawdb_formula_flg.append(('Y', gv_rid))

        conn, cur  = self.connect_to_sqlite(db_path)            
            
        ''' 
        for ins in insert_rows:
            print 'IIIIIIIIIIIIIIIIIIII', ins, '\n'        
        return
        '''
        if insert_rows:
            try:
                del_stmt = """ DELETE FROM formula_table WHERE rawdb_row_id in (%s); """%(del_ids)
                cur.execute(del_stmt)
            except:pass
            crt_stmt = """ CREATE TABLE IF NOT EXISTS formula_table(row_id INTEGER PRIMARY KEY AUTOINCREMENT, rawdb_row_id INTEGER, formula_id INTEGER, formula_type VARCHAR(10), operator VARCHAR(64), operand_row_id INTEGER, operand_type VARCHAR(100), value TEXT, check_sum TEXT, source_formula_id TEXT, res_taxo_level TEXT, opr_taxo_level TEXT); """
            cur.execute(crt_stmt)
            for ks in insert_rows:
                insert_stmt = """ INSERT INTO formula_table(rawdb_row_id, formula_id, operator, operand_row_id, value, check_sum, source_formula_id, operand_type, formula_type) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?); """
                cur.executemany(insert_stmt, [ks])
        if chk_sum_rows_update:
            for kts in chk_sum_rows_update:
                update_stmt = """ UPDATE data_builder SET check_sum='%s' WHERE row_id='%s'; """%(kts)
                cur.execute(update_stmt)    
        if update_rawdb_formula_flg:
            update_stmt = """ UPDATE data_builder SET formula_flag=? WHERE row_id=?; """
            cur.executemany(update_stmt, update_rawdb_formula_flg)
        conn.commit()
        conn.close()
        return  
        
    def insert_taxo_and_formula(self, company_name, model_number, company_id, taxo_data, taxo_header, formula_data, formula_header):
        company_id, db_doc =  company_id.split('_')
        ##db_path = '/mnt/eMB_db/%s/%s/taxonomy_formula.db'%(company_name, model_number) 

        dir_path = db_c.Config.doc_builder_path.format(company_id)
        db_path = os.path.join(dir_path, '{0}.db'.format(db_doc))    

        conn, cur  = self.connect_to_sqlite(db_path)
        taxo_header_str = ', '.join([' '.join(('_'.join(es.split()), 'TEXT')) for es in taxo_header])
        taxo_header_lst = []
        for es in taxo_header:
            es = es.lower()
            if es == 'index':
                es = 'index_id'
            if '-' in es:
                es = es.replace('-', '_')
            es = '_'.join(es.split())
            taxo_header_lst.append(es)
        e_hdr = taxo_header_lst[:4]
        insert_taxo_col = ', '.join(e_hdr)
        taxo_header_str = ', '.join(taxo_header_lst)
        crt_stmt = """ CREATE TABLE IF NOT EXISTS taxonomy(row_id INTEGER PRIMARY KEY AUTOINCREMENT, %s); """%(taxo_header_str)
        #print crt_stmt
        try:    
            cur.execute('DROP TABLE taxonomy;')
        except:s = ''
        cur.execute(crt_stmt)
        formula_header_str = ', '.join([' '.join(('_'.join(es.lower().split()), 'TEXT')) for es in formula_header])
        insert_taxo_fm_col = ', '.join(('_'.join(es.lower().split()) for es in formula_header))
        crt_stmt = """ CREATE TABLE IF NOT EXISTS taxonomy_calc_lbase(row_id INTEGER PRIMARY KEY AUTOINCREMENT, %s); """%(formula_header_str)
        #print crt_stmt
        try:    
            cur.execute('DROP TABLE taxonomy_calc_lbase;')
        except:s = ''
        cur.execute(crt_stmt)
        ln_e_hdr = len(e_hdr)
        tx_vls = ', '.join(['?']*ln_e_hdr)
        tx_fm_vls = ', '.join(['?']*len(formula_header)) 
        #print taxo_data
        if taxo_data:
            for rks in taxo_data:
                insert_stmt = """ INSERT INTO taxonomy(%s) VALUES(%s); """%(insert_taxo_col, tx_vls)
                #print insert_stmt
                #print rks
                ins = insert_stmt[:]
                try:
                    cur.executemany(insert_stmt, [rks])
                except:pass
                    #tx_vls = ', '.join(['"'+'%s'+'"']*ln_e_hdr)
                    #insert_stmt = """ INSERT INTO taxonomy(%s) VALUES(%s); """%(insert_taxo_col, tx_vls)
                    #insert_stmt = insert_stmt%rks
                    #cur.execute(insert_stmt)
        if formula_data:
            for frs in formula_data:
                insert_stmt = """ INSERT INTO taxonomy_calc_lbase(%s) VALUES(%s); """%(insert_taxo_fm_col, tx_fm_vls)
                try:
                    cur.executemany(insert_stmt, [frs])
                except:pass
                    #tx_fm_vls = ', '.join(['"'+'%s'+'"']*len(formula_header))
                    #insert_stmt = """ INSERT INTO taxonomy_calc_lbase(%s) VALUES(%s); """%(insert_taxo_fm_col, tx_fm_vls)
                    #insert_stmt = insert_stmt%frscur.execute(insert_stmt)
        conn.commit()
        conn.close()
        return

    def get_error_tables(self, company_id, project_id):
        db_path = '/mnt/eMB_db/company_management/{0}/data_builder/{1}/data_builder.db'.format(company_id, project_id)
        conn, cur  = self.connect_to_sqlite(db_path)            

        read_qry = """ SELECT rawdb_row_id, column_class FROM column_classification;  """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        err_ids = {}
        for rc_id  in t_data:
            rawdb_row_id, column_class = rc_id
            if column_class in ('range', 'group'):
                err_ids.setdefault(rawdb_row_id, {})[column_class] = 1

        read_qry = """ SELECT table_type, row_id FROM data_builder;  """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        ttr_dct = {}
        for ttr in t_data:
            table_type, row_id = ttr
            ttr_dct[row_id] = table_type
        
        r_qry = """ SELECT table_id, rawdb_row_id FROM reference_table WHERE doc_id=4;  """    
        cur.execute(r_qry)
        t_data = cur.fetchall()
        t_id_dct = {}
        for r in t_data:
            table_id, rawdbrowid = r
            t_id_dct[rawdbrowid] = table_id
            
        res_dct = {} 
        for rid, rc_i_dct in err_ids.iteritems():
            if len(rc_i_dct) != 2:continue
            tab_id = t_id_dct.get(rid, '')
            if not tab_id:continue
            tt = ttr_dct[rid]
            res_dct.setdefault(tab_id, {})[tt] = 1
        #print res_dct 

if __name__ == '__main__':
    r_Obj = Raw_Preview_DB()
    r_Obj.get_error_tables(1604, 5)



