import os, sys, sqlite3, json
from itertools import combinations

import redis_api as pre

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class SuperKeyInfo:
        
    def read_taxo_tagged_info(self, conn, cur, tt_str):
        read_qry = """ SELECT table_type, column_header, tagged_key, key_type FROM taxo_tagged WHERE table_type IN (%s); """%(tt_str)
        #print read_qry
        cur.execute(read_qry)
        t_data = cur.fetchall()
        tt_taxo_tagged_dct = {}
        for row_data in t_data:
            table_type, column_header, tagged_key, key_type = row_data
            tt_taxo_tagged_dct.setdefault(str(table_type), {}).setdefault(column_header, {})[tagged_key] = key_type 
        return tt_taxo_tagged_dct

    def read_doc_type_info(self, company_id):
        db_path = '/mnt/eMB_db/company_management/{0}/document_info.db'.format(company_id)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """ SELECT doc_id, doc_type, period_type, period FROM document_meta_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_type_dct = {}
        for rd in t_data:
            doc_id, doc_type, period_type, period = rd
            ph = '{0}{1}'.format(period_type, period)
            doc_type_dct[str(doc_id)] = {'dt':doc_type, 'ph':ph}
        return doc_type_dct

    def read_json_file(self, table_id, company_id):
        json_dir = '/mnt/eMB_db/company_management/{0}/json_files'.format(company_id)
        json_file_path = os.path.join(json_dir, '{0}.json'.format(table_id))        
        json_dct = {}
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        return json_dct
        
    def prep_rule_info(self, company_id, project_id, table_id):
        json_dct = self.read_json_file(table_id, company_id) 
        grid_dct = json_dct.get('data', {})
        doc, page, grid = table_id.split('_') 
        col_header_dct = {}
        rObj = pre.exe("172.16.20.7##6384##0", int(company_id), int(doc)) 
        for stype in ['vch', 'hch']:
            disableprint()
            info_lst = rObj.read_sction_data(page, grid, stype)
            enableprint()
            for cell_dict in info_lst:
                val_txt = cell_dict['txt']
                rc  = cell_dict['rc']
                row, col = rc.split('_') 
                col_header_dct[val_txt] = int(col)
        #for rc, cell_dct in grid_dct.iteritems():
        #    section_type = cell_dct.get('ldr')
        #    if section_type not in ('hch', 'vch'):continue
        #    row, col = rc.split('_')
        #    val_txt = cell_dct['data'].strip() 
        #    col_header_dct[val_txt] = int(col) 
        return col_header_dct
        
    def read_tables(self, company_id, doc_id, tableid_list=[]):
        db_path = '/mnt/eMB_db/company_management/{0}/table_info.db'.format(company_id)
        con         = sqlite3.connect(db_path, timeout=30000)
        cur         = con.cursor()
        sql         = "select doc_id, page_no, grid_id from table_mgmt where doc_id=%s"%(doc_id)
        cur.execute(sql)
        res         = cur.fetchall()
        done_d  = {}
        for r in res:
            doc_id, page_no, grid_id    = r
            table_id    = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            if tableid_list:
                if table_id not in tableid_list:continue
            if table_id in done_d:continue
            json_file   = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
            grid_data   = json.loads(open(json_file, 'r').read())
            done_d[table_id]    = (int(page_no), (int(grid_data['table_boundry'][1]), int(grid_data['table_boundry'][1])+int(grid_data['table_boundry'][3])))
        table_ids   = done_d.keys()
        table_ids.sort(key=lambda x:done_d[x])
        #for r in table_ids:
        #    print r, done_d[r]
        return table_ids 
        
    def sorted_ph_doc_info(self, company_id, doc_type_dct, des_docs):
        csv_flg = 0
        dist_ph = []
        ph_doc = {}
        for doc_id in des_docs:
            dt_dct = doc_type_dct[doc_id]
            doc_type, ph = dt_dct['dt'], dt_dct['ph']
            if doc_type == 'CSV':
                csv_flg = 1
        return csv_flg 
        
    def prep_not_existing_taxo_info(self, company_id, classified_id, table_id, taxo_info, tt_taxo_tagged_dct):
        doc, page, grid = table_id.split('_')
        rObj = pre.exe("172.16.20.7##6384##0", int(company_id), int(doc)) 
        col_taxo = {}
        for stype in ['vch', 'hch']:
            disableprint()
            info_lst = rObj.read_sction_data(page, grid, stype)
            enableprint()
            for cell_dict in info_lst:
                val_txt = cell_dict['txt']
                rc  = cell_dict['rc']
                row, col = rc.split('_') 
                col_taxo[col] = val_txt.strip()
        taxo_info_lst = json.loads(taxo_info) 
        tagged_col_header_dct = {}
        for row_dct in taxo_info_lst:
            tagged_key = row_dct['k']
            rc_lst     = row_dct['v']
            for r_c in rc_lst:
                rw, cl = r_c.split('_')
                col_head = col_taxo[cl]
                tt_taxo_tagged_dct.setdefault(classified_id, {}).setdefault(col_head, {})[tagged_key] = '1'
        
    def tt_not_exists_taxo_tagged(self, conn, cur, company_id, tt_taxo_tagged_dct, table_types):
        des_tt = set(table_types) - set(tt_taxo_tagged_dct) 
        tt_str = ', '.join(des_tt)
        read_qry = """ SELECT table_id, classified_id, taxo_info FROM scoped_gv WHERE classified_id IN (%s); """%(tt_str)
        cur.execute(read_qry) 
        t_data = cur.fetchall()
        if t_data:
            for row_data in t_data:
                table_id, classified_id, taxo_info = row_data
                if not taxo_info:continue
                self.prep_not_existing_taxo_info(company_id, classified_id, table_id, taxo_info, tt_taxo_tagged_dct)
        
    def collect_table_wise_super_key_cols(self, company_id, table_id, col_head_dct):
        json_dct = self.read_json_file(table_id, company_id) 
        grid_dct = json_dct.get('data', {})
        doc, page, grid = table_id.split('_') 
        col_header_set = set()
        rObj = pre.exe("172.16.20.7##6384##0", int(company_id), int(doc)) 
        super_col_set = set()
        for stype in ['vch', 'hch']:
            disableprint()
            info_lst = rObj.read_sction_data(page, grid, stype)
            enableprint()
            for cell_dict in info_lst:
                val_txt = cell_dict['txt'].strip()
                rc  = cell_dict['rc']
                row, col = rc.split('_') 
                if val_txt in col_head_dct:
                    super_col_set.add(int(col))
        super_cols = list(super_col_set)
        super_cols.sort()
        return super_cols
    
    def create_super_key(self, company_id, project_id, ne_flg=0):
        db_path = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)  
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """ SELECT table_type, column_header, tagged_key FROM taxo_tagged WHERE tagged_key in ('super_primary_key', 'range') AND table_type != '-1'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        tt_col_head_dct = {}
        dist_col_heads = {}        
        range_dist_col_heads = {}        
        for tt, col_head, tagged_key in t_data:
            col_head = col_head.strip()
            tt_col_head_dct[str(tt)] = col_head
            if tagged_key == 'super_primary_key':
                dist_col_heads[col_head] = 1
            elif tagged_key == 'range':
                range_dist_col_heads[col_head] = 1
        tt_str = ', '.join(tt_col_head_dct)
        
        r_qry = """ SELECT table_id FROM scoped_gv WHERE classified_id IN (%s); """%(tt_str)
        cur.execute(r_qry)
        t_data = cur.fetchall()
        
        tab_super_cols = {}
        for table_id in t_data:
            table_id = table_id[0]
            super_cols = self.collect_table_wise_super_key_cols(company_id, table_id, dist_col_heads) 
            range_super_cols = self.collect_table_wise_super_key_cols(company_id, table_id, range_dist_col_heads) 
            range_super_cols     = list(set(range_super_cols) - set(super_cols))
            range_super_cols.sort()
            tab_super_cols[table_id] = (super_cols, range_super_cols)
        #print tab_super_cols
        
        super_key_tup = ('', '') 
        des_table = max(tab_super_cols, key=lambda x:len(tab_super_cols[x][0]))
        des_cols, range_super_cols = tab_super_cols[des_table]
        return (des_table, des_cols, range_super_cols) 
                  
    def prep_super_key_csv_info(self, ijson):
        # {'company_id':company_id, 'project_id':project_id, 'table_types':[str(rule_id)], 'table_ids':[], 'row_ids':[]}
        company_id   = ijson['company_id']
        project_id   = ijson['project_id']
        table_types  = ijson['table_types']
        tt_str  = ', '.join(map(str, table_types))
        db_path = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)  
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = """ SELECT table_id  FROM group_tables"""
        cur.execute(read_qry) 
        t_data = cur.fetchall()
        t_exists_d =  {}
        for r in t_data:
            t_exists_d[r[0]]    = 1
        conn.close()
        db_path = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)  
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        tt_taxo_tagged_dct = self.read_taxo_tagged_info(conn, cur, tt_str)
        #self.tt_not_exists_taxo_tagged(conn, cur, company_id, tt_taxo_tagged_dct, table_types)
        read_qry = """ SELECT doc_id, table_id, classified_id, table_type FROM scoped_gv WHERE classified_id IN (%s); """%(tt_str)
        cur.execute(read_qry) 
        t_data = cur.fetchall()
        read_qry = """ SELECT doc_id, table_id, classified_id FROM group_tables WHERE classified_id IN (%s); """%(tt_str)
        cur.execute(read_qry) 
        tr_data = cur.fetchall()
        conn.close()
        
        self.super_primary = {'super_primary_key', 'primary_key'}
        self.group_range   = {'range',}

        doc_table_col_head_dct  = {}
        super_key_doc           = {} 
        res_rule_dct            = {}
        done_tables             = {}
        f_type_II  = ''
        for row_tup in t_data:
            doc_id, table_id, classified_id, t_type_info = map(str, row_tup)
            #print row_tup
            if t_type_info != 'Type II':
                t_type_info = ''
            else:
                f_type_II   = 'Type II'
        done_idx    = {}
        for row_tup in t_data+map(lambda x:x+(f_type_II, ), tr_data):
            doc_id, table_id, classified_id, t_type_info = map(str, row_tup)
            if table_id not in t_exists_d:
                print table_id
                continue
            if table_id in done_idx:continue
            done_idx[table_id]    = 1
            #print row_tup
            if f_type_II != 'Type II':
                t_type_info = 'Type I'
            else:
                t_type_info = 'Type II'
                f_type_II   = 'Type II'
            taxo_tagged = tt_taxo_tagged_dct.get(str(classified_id))
            #print taxo_tagged
            #if not taxo_tagged:continue # Commented By Muthu
            if table_id not in doc_table_col_head_dct:
                doc_table_col_head_dct[table_id] = self.prep_rule_info(company_id, project_id, table_id)
            col_header_dct = doc_table_col_head_dct.get(table_id)
            #print col_header_dct
            rule_col_set = set()
            range_prime = set()
            if t_type_info == 'Type II':
                for col_head, col in col_header_dct.iteritems():
                    sk_data = taxo_tagged.get(col_head, {})
                    if not sk_data:continue
                    range_flg = 0
                    sk_p_flg  = 0
                    for tagged_key, key_type in sk_data.iteritems():
                        if tagged_key in self.super_primary:
                            rule_col_set.add(col)
                            sk_p_flg = 1
                        if tagged_key in self.group_range:
                            range_flg = 1
                    if range_flg and sk_p_flg:
                        range_prime.add(col)
            
            if rule_col_set or range_prime:
                rule_col_lst = list(rule_col_set)
                rule_col_lst.sort()
                range_prime_lst = list(range_prime)
                range_prime_lst.sort()
                res_rule_dct.setdefault(doc_id, []).append((table_id, t_type_info, rule_col_lst, range_prime_lst))
            else: # Added By Muthu
                res_rule_dct.setdefault(doc_id, []).append((table_id, t_type_info, [], []))
        
    
        rule_lst = []
        for doc, doc_row in res_rule_dct.iteritems():
            rule_lst.append(doc_row) 

        doc_type_dct   = self.read_doc_type_info(company_id)
        csv_flg  = self.sorted_ph_doc_info(company_id, doc_type_dct, res_rule_dct)
        super_key_tup = self.create_super_key(company_id, project_id)
        return super_key_tup, rule_lst,  csv_flg
        
if __name__ == '__main__':
    sk_Obj = SuperKeyInfo()
    ijson = {'company_id':sys.argv[1], 'project_id':5, 'table_types':sys.argv[2].split('#'), 'table_ids':[], 'row_ids':[]}
    print sk_Obj.prep_super_key_csv_info(ijson)
    #print sk_Obj.create_super_key(1053730, 5)



