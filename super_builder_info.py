import os, sys, sets
import config
import report_year_sort

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class SuperBuilder:
        
    def read_distinct_taxo_group_id(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT DISTINCT taxo_group_id FROM data_builder; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        res_lst = []
        for row_data in t_data:
            taxo_group_id = row_data[0]
            row_dct = {'k':taxo_group_id, 'n':taxo_group_id}
            res_lst.append(row_dct)
        res = [{'message':'done', 'data':res_lst}]
        return res
        
        
    def read_sb_super_key_info(self, conn, cur, table_type):
        read_qry = """ SELECT db_row_id, super_key FROM super_key_info WHERE table_type='{0}'; """.format(table_type)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        
        sk_dct = {}
        for row_data in t_data:
            db_row_id, super_key = row_data
            sk_dct[str(db_row_id)] = super_key
        return sk_dct     
        
    def regular_DB_super_poss(self, company_id, project_id, filename):
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        
        read_qry = """ SELECT table_type, db_row_id, super_key, signature FROM super_key_poss_info WHERE status='Y'; """
        if filename:
            read_qry = """ SELECT table_type, db_row_id, super_key, signature FROM super_key_poss_info WHERE filename='{0}' AND status='Y'; """.format(filename)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        sk_poss_dct = {}
        for row_data in  t_data:
            table_type, db_row_id, super_key, signature = row_data
            tt_db_rid = '{0}-{1}'.format(table_type, db_row_id)
            sk_poss_dct.setdefault((super_key, signature), {})[tt_db_rid] = 1
            sk_poss_dct.setdefault(super_key, {})[tt_db_rid] = 1
        return sk_poss_dct
        
    def read_sk_group_column_id(self, conn, cur):
        read_qry = """ SELECT column_id, group_text FROM column_texts; """
        cur.execute(read_qry)   
        t_data = cur.fetchall() 
        sk_group_col = {}
        for row_dat in t_data:
            column_id, group_text =  row_dat
            sk_group_col[group_text] = column_id
        return sk_group_col

    def superkey_data_builder_info(self, ijson):
        company_id    = ijson['company_id']
        project_id    = ijson['project_id']
        table_type    = ijson['table_type']
        taxo_grp_ids  = ijson['taxo_group_ids']           
        filename      = ijson.get('file_name')
        sig_flg       = ijson.get('sgn', 'N')
        taxo_str      = ', '.join(['"'+e+'"'for e  in taxo_grp_ids]) 
        sk_poss_dct  = self.regular_DB_super_poss(company_id, project_id, filename)
        db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        tt_sk_dct = self.read_sb_super_key_info(conn, cur, table_type)
        sk_group_col = self.read_sk_group_column_id(conn, cur)

        lc_d    = {} 
        super_key_flg   = {}
        
        read_qry = """ SELECT row_id, taxo_group_id, src_row, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id FROM data_builder WHERE table_type='%s' AND taxo_group_id in (%s); """%(table_type, taxo_str)
        #read_qry = """ SELECT row_id, taxo_group_id, src_row, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id FROM data_builder WHERE table_type='%s'; """%(table_type)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        
        col_key_lst = []
        for rd in taxo_grp_ids:
            if not rd:continue
            column_id = sk_group_col[rd]
            col_key_lst.append(int(column_id))
        
        col_key_str = '#'.join([str(e) for e in sorted(col_key_lst)])
        sig_key = 'PEQ:~EQ:{0}'.format(col_key_str) 
        print sig_key
        
        sn = 1
        res_dct = {}
        collect_col_wise_grp = {}
        group_wise_rids_dct = {}
        sk_hashed_dct = {}
        sk_row_dct = {}
        for row_data in t_data: 
            row_id, taxo_group_id, row, col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id = row_data
            res_dct.setdefault(int(row), {})[int(col)] = {'v':value, 'row_id':row_id, 'tx_id':taxo_group_id, 'ct':cell_type, 'cell_ph':cell_ph, 'ff':formula_flag, 't':table_id}
            if 1:#cell_type != 'HGH':
                collect_col_wise_grp.setdefault(int(col), {})[taxo_group_id] = 1
                group_wise_rids_dct.setdefault(taxo_group_id, {})[row_id] = 1

        
        sn = 1
        data_lst = [] 
        rows = sorted(res_dct.keys()) 
        collect_ph = {}
        inf_map = {}
        hgh_col = {}
        for row in rows: 
            act_sk = tt_sk_dct.get(str(row), '')
            if sig_flg == 'Y':
                if (act_sk, sig_key) not in sk_poss_dct:continue
                #sk_rids = sk_poss_dct[(act_sk, sig_key)]
                
            else:
                if act_sk not in sk_poss_dct:continue
                #sk_rids = sk_poss_dct[act_sk]
            col_dct = res_dct[row]
            inf_map['{0}_skey_f'.format(sn)] = {'sk':act_sk, 'ref_k': ''}
            row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn,'db_rid':row, 'skey_f':{'v':'O'}}
            # {'v':value, 'row_id':row_id, 'tx_id':taxo_group_id}
            cols = sorted(col_dct.keys())
            rlc_d    = {}
            row_d   = lc_d.get('ROW', {})
            col_d   = {}
            for col in cols:
                vl_dct = col_dct[col]
                val = vl_dct['v'] 
                tab_row_id = vl_dct['row_id']
                tx_id   = vl_dct['tx_id']
                c_type  = vl_dct['ct']
                c_ph    = vl_dct['cell_ph']
                #act_sk  = vl_dct['sk']

                t_b_id  = vl_dct['t']
                col_desc = col
                collect_ph.setdefault(col, set()).add(c_ph)
                if c_type == 'HGH': 
                    hgh_col[col] = 1
                val_dct_i = {'v':val}
    
                row_dct[col_desc] = val_dct_i
                map_rc = '%s_%s'%(sn, col_desc)
                inf_map[map_rc] = {'ref_k':tab_row_id}
                #if act_sk:
                #    inf_map[map_rc]['sk'] = act_sk
                
            data_lst.append(row_dct)
            sn += 1
         
        #col_def_lst = [{'k':'desc', 'n':'Description', 'g':''}]
        col_def_lst = [{'v_opt': 3, 'k': "checkbox", 'n': "",'pin':'pinnedLeft'}, {'k':'sn', 'n':'S NO','type':"SL", 'pin':'pinnedLeft'}, {'k':'skey_f', 'n':'S.Key Flg', 'v_opt':2,'pin':'pinnedLeft'}]
        all_cols = sorted(collect_col_wise_grp.keys())
        for c_l in all_cols:
            g_i_str = ' '.join(collect_col_wise_grp[c_l])
            p_h_lst = collect_ph[c_l] 
            p_h = ''
            if col not in hgh_col:
                p_h = '~'.join(p_h_lst)
            k_dct = {'k':c_l, 'n':p_h, 'g':g_i_str}
            col_def_lst.append(k_dct)
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res 
        
    def give_sk_poss_builder_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        super_key  = ijson['sk']
        table_type    = ijson['table_type']
        taxo_grp_ids  = ijson['taxo_group_ids']           
        filename      = ijson.get('file_name')
        taxo_str      = ', '.join(['"'+e+'"'for e  in taxo_grp_ids]) 

        lc_d    = {} 
        super_key_flg   = {}
        
        db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        sk_group_col = self.read_sk_group_column_id(conn, cur)
        conn.close()

        
        col_key_lst = []
        for rd in taxo_grp_ids:
            if not rd:continue
            column_id = sk_group_col[rd]
            col_key_lst.append(int(column_id))
        
        col_key_str = '#'.join([str(e) for e in sorted(col_key_lst)])
        sig_key = 'PEQ:~EQ:{0}'.format(col_key_str) 
        print sig_key
         

        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        if not taxo_grp_ids:
            read_qry = """ SELECT DISTINCT table_type, db_row_id FROM super_key_poss_info WHERE super_key='{0}' AND status='Y'; """.format(super_key)
        else:
            read_qry = """ SELECT DISTINCT table_type, db_row_id FROM super_key_poss_info WHERE super_key='{0}' AND signature='{1}' AND file_name='{2}' AND status='Y'; """.format(super_key, sig_key, filename)
        print read_qry
            
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        t_data.sort()
        print t_data
       
        sn_row = 1   
        output_lst = []
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        rowId   = 0
        for row_data in t_data:
            table_type, db_row_id = row_data
            r_qry = """ SELECT row_id, table_type, taxo_group_id, src_row, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id,src_row FROM data_builder WHERE table_type='%s' and src_row='%s';  """%(table_type, db_row_id)
            cur.execute(r_qry)
            ct_data = cur.fetchall()
                
            row_wise_cell_dct = {} 
            for rd in ct_data:
                row_id, table_type, taxo_group_id, src_row, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id,src_row = rd    
                output_lst.append((row_id, table_type, taxo_group_id, rowId, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id,src_row))
                #row_wise_cell_dct.setdefault(src_row, {})[src_col] = rd
            rowId   += 1
        conn.close()
            
        from modules.databuilder import form_builder_from_template as fbft
        tb_Obj = fbft.TaxoBuilder() 
        doc_mdata   = tb_Obj.read_document_meta_data(ijson)
        ph_order    = list(sets.Set(map(lambda x:doc_mdata[x]['ph'], doc_mdata.keys())))
        ph_order    = report_year_sort.year_sort(ph_order)
        ph_order.reverse()
        tscope_d = tb_Obj.read_all_table_types(company_id)
        res = tb_Obj.form_builder_data_sky(ph_order, output_lst, ijson, tscope_d)
        return res 
        
    def read_col_text_data(self, company_id, project_id):
        db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)

        read_qry = """ SELECT column_id, group_text FROM column_texts; """
        cur.execute(read_qry)   
        t_data = cur.fetchall() 
        sk_group_col = {}
        for row_dat in t_data:
            column_id, group_text =  row_dat
            sk_group_col[str(column_id)] = group_text
        return sk_group_col
    
    def read_sign_col_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT signature, table_type, db_row_id FROM super_key_poss_info WHERE status='Y'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        sign_dct = {}
        for row_data in t_data:
            signature, table_type, db_row_id = row_data
            sign_dct.setdefault(signature, {}).setdefault(table_type, {})[db_row_id]  =1 
        
        tt_sk_dct = self.read_col_text_data(company_id, project_id)        
        
        res_lst = []
        for sign, tt_dct in sign_dct.iteritems():
            row_dct = {'k':sign}
            col_str = sign.split(':')[-1]
            col_lst = col_str.split('#')
            col_head_lst = []
            for col in col_lst:
                c_h = tt_sk_dct[col]
                col_head_lst.append(c_h)
            row_dct['n'] = col_head_lst
            tt_info_dct = {}
            for tt, db_rid_dct in tt_dct.iteritems():
                tt_info_dct[tt] = len(db_rid_dct)
            row_dct['g'] = tt_info_dct
            res_lst.append(row_dct)
        res = [{'message':'done', 'data':res_lst}]
        return res
        
    def create_col_txt_with_peq_flg(self, signature, tt_sk_dct):
        res_sig_cols = []
        for sig in signature.split('~'):
            sig_type, sig_cols = sig.split(':')
            sig_cols = sig_cols.split('#')
            pk = False
            if sig_type == 'PEQ':
                pk = True
            for cl in sig_cols:
                if cl:
                    c_h = tt_sk_dct[cl]
                    row_dct = {'ch':c_h, 'pk':pk}
                    res_sig_cols.append(row_dct)
        return res_sig_cols

    def read_fe_signature(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        filename = ijson['file_name']
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT DISTINCT signature, table_type FROM super_key_poss_info WHERE status='Y' AND filename='{0}'; """.format(filename)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        sign_tt_dct = {}
        for sig, tt in t_data:
            sign_tt_dct.setdefault(sig, {})[tt] = 1 
        
        
        data_lst = []
        inf_map = {}
        tt_sk_dct = self.read_col_text_data(company_id, project_id)        
        for sign, tt_dct in sign_tt_dct.iteritems():
            col_str = sign.split(':')[-1]
            col_lst = col_str.split('#')
            row_dct = {'sign':sign, 'cnt':{'v':len(tt_dct)}}
            col_head_lst = []
            for col in col_lst:
                c_h = tt_sk_dct[col]
                if c_h not in col_head_lst:
                    col_head_lst.append(c_h)
            #col_head_lst = self.create_col_txt_with_peq_flg(signature, tt_sk_dct)
            row_dct['gh'] = {'v':col_head_lst}
            data_lst.append(row_dct)
        data_lst.sort(key=lambda x:len(x['gh']['v']), reverse=True) 
            
        sn = 1
        data = []
        for rw in data_lst:
            rw['sn'] = {'v':sn}
            rw['cid'] = sn
            rw['rid'] = sn
            sgn = rw['sign']
            del rw['sign']
            inf_map['{0}_gh'.format(sn)] = {'ref_k':sgn}
            data.append(rw)
            sn += 1

        col_def_lst = [{'k':'sn', 'n':'S NO','type':"SL", 'pin':'pinnedLeft'}, {'k':'cnt', 'n':'Table Type Count'}, {'k':'gh', 'n':'Group Header'}]
        res = [{'message':'done', 'data':data, 'col_def':col_def_lst, 'map':inf_map}] 
        return res

    def fe_column_text(self, conn, cur):
        read_qry = """ SELECT table_type, column_id, group_text FROM column_texts; """
        cur.execute(read_qry)   
        t_data = cur.fetchall() 
        sk_group_col = {}
        for row_dat in t_data:
            table_type, column_id, group_text =  row_dat
            sk_group_col[(str(table_type), str(column_id))] = group_text
        return sk_group_col

    def sk_column_text(self, company_id, project_id):
        db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT table_type, column_id, group_text FROM column_texts; """
        cur.execute(read_qry)   
        t_data = cur.fetchall() 
        conn.close()
        sk_group_col = {}
        for row_dat in t_data:
            table_type, column_id, group_text =  row_dat
            sk_group_col[(str(table_type), str(column_id))] = group_text
        return sk_group_col

    def read_table_type_name(self):
        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT row_id, table_type  FROM all_table_types; """
        cur.execute(r_qry)
        mst_data = cur.fetchall()
        conn.close()
        
        tt_name_dct = {}
        for rw_dt in mst_data:
            row_id, table_type = rw_dt
            tt_name_dct[str(row_id)] = table_type
        return tt_name_dct

    def sk_read_tt_wise_DB_row_id(self, company_id, project_id, sk_sig_txt_dct, sk_table_type):
        db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        for idx, data_dct in sk_sig_txt_dct.iteritems():
            txt = data_dct['txt']
            read_qry = """ SELECT row_id FROM data_builder WHERE taxo_group_id="{0}" AND value != '' AND table_type='{1}' LIMIT 1; """.format(txt, sk_table_type)        
            print read_qry
            cur.execute(read_qry)
            rid = cur.fetchone()[0]
            sk_sig_txt_dct[idx]['rid'] = rid  

    def create_signature_col_lst(self, signature):
        res_sig_cols = []
        for sig in signature.split('~'):
            sig_type, sig_cols = sig.split(':')
            sig_cols = sig_cols.split('#')
            for cl in sig_cols:
                if cl:
                    res_sig_cols.append(cl)
        return res_sig_cols
        
    def create_sk_sig_txt(self, company_id, project_id, sk_signature, sk_table_type):
        sk_sig_lst = self.create_signature_col_lst(sk_signature)
        txt_info = self.sk_column_text(company_id, project_id)
        
        sk_sig_txt_dct = {}
        for idx, col in enumerate(sk_sig_lst):
            txt = txt_info[(str(sk_table_type), col)]      
            sk_sig_txt_dct[idx] = {'col':col, 'txt':txt}
        return sk_sig_txt_dct
    
    def read_fe_DB_row_id(self, conn, cur, tt, taxo_group_id):
        read_qry = """ SELECT row_id FROM data_builder WHERE taxo_group_id="{0}" AND table_type='{1}' AND value != ''  LIMIT 1; """.format(taxo_group_id, tt)        
        cur.execute(read_qry)
        rid = cur.fetchone()[0]
        return rid

    def read_sk_fe_DB_row_id(self, conn, cur, tt, src_row):
        read_qry = """ SELECT row_id FROM data_builder WHERE src_row='{0}' AND table_type='{1}' AND value != ''  LIMIT 1; """.format(src_row, tt)        
        cur.execute(read_qry)
        rid = cur.fetchone()[0]
        return rid
        
    def read_sk_fe_row_id(self, sk_conn, sk_cur, fe_conn, fe_cur, super_key):
        read_qry = """ SELECT table_type, db_row_id FROM super_key_poss_info WHERE super_key='{0}'; """.format(super_key)
        fe_cur.execute(read_qry)
        t_data = fe_cur.fetchone()
        fe_tt, fe_dbrid = t_data
        fe_rid = self.read_sk_fe_DB_row_id(fe_conn, fe_cur, fe_tt, fe_dbrid)

        read_qry = """ SELECT table_type, db_row_id FROM super_key_info WHERE super_key='{0}'; """.format(super_key)
        sk_cur.execute(read_qry)
        t_data = sk_cur.fetchone()
        sk_tt, sk_dbrid = t_data
        sk_rid =  self.read_sk_fe_DB_row_id(sk_conn, sk_cur, sk_tt, sk_dbrid)
        return fe_rid, sk_rid 
        
    def sk_fe_sign_map(self, company_id, project_id, sk_signature, fe_col_lst, tt_ch_dct, sk_table_type):
        sk_sig_txt_dct = self.create_sk_sig_txt(company_id, project_id, sk_signature, sk_table_type)
        self.sk_read_tt_wise_DB_row_id(company_id, project_id, sk_sig_txt_dct, sk_table_type)
        
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)

        #db_path = config.Config.super_key_db_path.format(company_id, project_id)
        #sk_conn, sk_cur   = conn_obj.sqlite_connection(db_path)
    
        tt_sk_dct = {}
        for fe_tup in fe_col_lst:
            tt, fe_lst, sk, db_row_id = fe_tup
            tt_sk_dct.setdefault(tt, {'fe':[], 'sk':''})
            if fe_lst not in tt_sk_dct[tt]['fe']:
                tt_sk_dct[tt]['fe'].append(fe_lst)
            tt_sk_dct[tt]['sk'] = sk 
        res_lst = []
        for t_type, i_dct in tt_sk_dct.iteritems():
            fe_ls = i_dct['fe']
            sk_i  = i_dct['sk']
            for fe_l in fe_ls:
                fe_l = list(set(fe_l))
                fe_l.sort(key=lambda x:int(x))
                for idx, fe_col in enumerate(fe_l):
                    fe_txt = tt_ch_dct[(tt, fe_col)]
                    fe_rid = self.read_fe_DB_row_id(conn, cur, tt, fe_txt)
                    #try:
                    #    fe_rd, sk_rd = self.read_sk_fe_row_id(sk_conn, sk_cur, conn, cur, sk_i)
                    #    print ['fs', fe_rd, sk_rd] 
                    #except:pass
                    sk_info = sk_sig_txt_dct[idx]
                    sk_info['fe_txt'] = fe_txt
                    sk_info['fe_rid'] = fe_rid
                    sk_info['fe_col'] = fe_col
                    res_lst.append(sk_info)
        conn.close()
        #sk_conn.close()
        return res_lst 
        
    def read_column_signature_stats(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        signature  = ijson['signature']
        sk_table_type = ijson['table_type']
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT row_id, table_type, db_row_id, db_signature, super_key FROM super_key_poss_info WHERE signature='{0}' AND status='Y'; """.format(signature)
        print read_qry
        cur.execute(read_qry)
        t_data = cur.fetchall()
        tt_ch_dct = self.fe_column_text(conn, cur)
        conn.close()
        
        tt_name_dct = self.read_table_type_name()
    
        tt_info_dct = {}
        fe_col_lst = []
        for row_data in t_data:
            row_id, table_type, db_row_id, db_signature, super_ky = map(str, row_data)
            if not db_signature:continue
            if db_signature == 'None':continue
            col_str = db_signature.split(':')[-1]
            sig_col_lst = self.create_signature_col_lst(db_signature)
            if (table_type, sig_col_lst, super_ky, db_row_id) not in fe_col_lst:
                fe_col_lst.append((table_type, sig_col_lst, super_ky, db_row_id))
            col_lst = map(int, sig_col_lst) #map(int, col_str.split('#'))
            col_lst.sort() 
            ch_tup = []
            for ch_col in col_lst:
                ch_col = str(ch_col)
                col_head = tt_ch_dct[(table_type, ch_col)] 
                ch_tup.append(col_head)
            ch_tup = tuple(ch_tup)
            tt_info_dct.setdefault(ch_tup, {'sig_ids':{}, 'class_ids':{}})
            tt_info_dct[ch_tup]['sig_ids'][db_signature] = 1
            tt_info_dct[ch_tup]['class_ids'].setdefault(table_type, {})[int(row_id)] = 1
        
        def return_stxt_string(inp_tup):
            inp_tup = list(inp_tup)
            inp_lst = list(set(inp_tup))
            inp_lst.sort(key=lambda x:inp_tup.index(x)) 
            inp_str = ','.join(inp_lst)
            op_str = '(' +inp_str+ ')'
            return op_str
            
        data_lst = []
        for ch_txt_tup, hc_info_dct in tt_info_dct.iteritems():
            sig_ids               = hc_info_dct['sig_ids']
            class_ids_info_dct    = hc_info_dct['class_ids']
            ch_txt = return_stxt_string(ch_txt_tup)
            row_dct = {'s_txt':ch_txt, 'sig_ids':sig_ids.keys(), 'headers':[], 'values':{}}
            for tt, row_id_dct in class_ids_info_dct.iteritems():
                class_id_name = tt_name_dct[tt]
                head_row_dct = {'k':tt, 'n':class_id_name}
                row_dct['headers'].append(head_row_dct)
                row_dct['values'][tt] = {'cnt':len(row_id_dct), 'row_ids':row_id_dct.keys()}
            data_lst.append(row_dct)
           
 
        #print signature
        #print 'FFFFFFFFFFFFFF', fe_col_lst
        #print 'TTTTTTTTTTTT', tt_ch_dct
        sk_map_fe = self.sk_fe_sign_map(company_id, project_id, signature, fe_col_lst, tt_ch_dct, sk_table_type)
        res = [{'message':'done', 'data':data_lst, 'sk_fe_map': sk_map_fe}]
        return res
        
    def return_row_tt_db_row_ids(self, conn, cur, rids):
        tt_ch_dct = self.fe_column_text(conn, cur)
        tt_dbrid_sk_dct = {}
        tt_dbrid_dct = {}
        tt_dbrid_ch_dct = {}
        for row_id in rids:
            read_qry = """ SELECT table_type, db_row_id, super_key, db_signature FROM super_key_poss_info WHERE row_id={0} AND status='Y'; """.format(row_id)
            cur.execute(read_qry)
            t_data = cur.fetchone()
            if not t_data:continue
            table_type, db_row_id, super_key, db_signature = t_data
            tt_dbrid = '{0}-{1}'.format(table_type, db_row_id)
            tt_dbrid_sk_dct.setdefault(tt_dbrid, {})[super_key] = 1
            tt_dbrid_dct[(int(table_type), int(db_row_id))] = 1
            db_sign_cols = self.create_signature_col_lst(db_signature)
            for col in db_sign_cols:
                col_head = tt_ch_dct[(str(table_type), col)]
                tt_dbrid_ch_dct[(str(table_type), str(db_row_id), col_head)] = 1
        return tt_dbrid_sk_dct, tt_dbrid_dct, tt_dbrid_ch_dct

    def create_save_db_row_ids_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        row_ids    = ijson['row_ids']
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        tt_dbrid_sk_dct, tt_dbrid_dct, tt_dbrid_ch_dct = self.return_row_tt_db_row_ids(conn, cur, row_ids)
        conn.close()
        del ijson['row_ids']
        ijson['db_row_ids'] = tt_dbrid_sk_dct
        import pyapi as pyf
        p_Obj = pyf.PYAPI()
        res = p_Obj.update_super_key_to_db_row(ijson) 
        return res
        
    def read_multi_DB_data_builder(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        row_ids    = ijson['row_ids']
        #print len(row_ids)
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        tt_dbrid_sk_dct, tt_dbrid_dct, tt_dbrid_ch_dct = self.return_row_tt_db_row_ids(conn, cur, row_ids)
        conn.close()
            
        tt_dbrid_lst = tt_dbrid_dct.keys()
        tt_dbrid_lst.sort()    
        
        sn_row = 1   
        output_lst = []
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        rowId   = 0
        for row_data in tt_dbrid_lst:
            table_type, db_row_id = row_data
            r_qry = """ SELECT row_id, table_type, taxo_group_id, src_row, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id,src_row FROM data_builder WHERE table_type='%s' and src_row='%s';  """%(table_type, db_row_id)
            cur.execute(r_qry)
            ct_data = cur.fetchall()
                
            row_wise_cell_dct = {} 
            for rd in ct_data:
                row_id, table_type, taxo_group_id, src_row, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id,src_row = rd    
                if (str(table_type), str(src_row), taxo_group_id) in tt_dbrid_ch_dct:
                    formula_flag = 'Y'
                    #print 'HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH'
                output_lst.append((row_id, table_type, taxo_group_id, rowId, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id,src_row))
                #row_wise_cell_dct.setdefault(src_row, {})[src_col] = rd
            rowId   += 1
        conn.close()
            
        from modules.databuilder import form_builder_from_template as fbft
        tb_Obj = fbft.TaxoBuilder() 
        doc_mdata   = tb_Obj.read_document_meta_data(ijson)
        ph_order    = list(sets.Set(map(lambda x:doc_mdata[x]['ph'], doc_mdata.keys())))
        ph_order    = report_year_sort.year_sort(ph_order)
        ph_order.reverse()
        tscope_d = tb_Obj.read_all_table_types(company_id)
        res = tb_Obj.form_builder_data_sky(ph_order, output_lst, ijson, tscope_d)
        return res 
    
            
        
if __name__ == '__main__':
    sb_Obj = SuperBuilder()
    ijson = {"company_id":"1053730", "project_id":5, "table_type":2, 'signature':'PEQ:~EQ:20'} 
    #print sb_Obj.superkey_data_builder_info(ijson)
    #print sb_Obj.read_fe_signature(ijson)
    #print sb_Obj.read_column_signature_stats(ijson)



