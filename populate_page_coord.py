import os, sys, lmdb, json, ast

class Page_Coord(object):
    def __init__(self, company_id):
        self.company_id                       = company_id
        self.project_id, self.deal_id          = company_id.split('_')
        self.lmdb_folder        = os.path.join('/var/www/html/fundamentals_intf/output/', self.company_id, 'doc_page_adj_cords')
        self.company_list_path  = '/mnt/eMB_db/company_info/compnay_info.db'
        self.company_name       = self.get_company_name(self.project_id, self.deal_id)
        self.doc_meta_path      = '/mnt/eMB_db/%s/%s/tas_company.db'%(self.company_name, self.project_id)
        
    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur
        
    def get_company_name(self, project_id, deal_id):
        conn, cur = self.connect_to_sqlite(self.company_list_path)
        read_qry = """ SELECT company_name FROM company_info; """
        cur.execute(read_qry)
        company_name = cur.fetchone()[0]
        return company_name
        
    def cal_bobox_data_new(self):
        doc_wise_page_coords = {}
        if os.path.exists(self.lmdb_folder):
            env = lmdb.open(self.lmdb_folder, readonly=True)
            txn = env.begin()
            if 1:
                cursor = txn.cursor()
                page_dict = {}
                for doc_id, res_str in cursor:
                    if res_str:
                        page_dict = ast.literal_eval(res_str)
                        insert_rows = []
                        sorted_pages = sorted(page_dict.keys(), key=lambda x:int(x))
                        for pg in sorted_pages:
                            c_lst = page_dict[pg]
                            c_str = '_'.join(map(lambda x:str(float(x)), c_lst))
                            insert_rows.append((int(pg), c_str))
                        doc_wise_page_coords[doc_id] = insert_rows
        return doc_wise_page_coords
        
    def insert_page_coord(self, doc_id, insert_rows):
        db_path = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/newvalidation_test_db/{0}/{1}.db'.format(self.company_id, doc_id)
        print db_path, '\n'
        conn, cur = self.connect_to_sqlite(db_path)
        try:
            del_stmt = """ DROP TABLE pagedet; """
            cur.execute(del_stmt)
        except:pass
        crt_qry = """ CREATE TABLE IF NOT EXISTS pagedet(pageno INTEGER PRIMARY KEY, pagesize TEXT) """ 
        cur.execute(crt_qry)
        insert_stmt = """ INSERT INTO pagedet(pageno, pagesize) VALUES(?, ?)  """
        cur.executemany(insert_stmt, insert_rows)
        conn.commit()
        conn.close()
        return 
        
    def populate_page_coords_fn(self):
        conn, cur = self.connect_to_sqlite(self.doc_meta_path)
        read_qry = """ SELECT doc_id FROM company_meta_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_wise_page_coords = self.cal_bobox_data_new() 
        for row in t_data[:]:
            doc_id = str(row[0])
            ins_rows = doc_wise_page_coords.get(doc_id)
            if not ins_rows:continue
            print doc_id, ins_rows
            self.insert_page_coord(doc_id, ins_rows)
        return 
        
if __name__ == '__main__':
    company_id = sys.argv[1]
    pc_Obj  = Page_Coord(company_id)
    pc_Obj.populate_page_coords_fn()
        
