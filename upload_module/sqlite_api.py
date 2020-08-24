import sqlite3
import config_1
config_obj = config_1.Config()
class sqlite_api:

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()
    
    def create_table(self, sql):
        sql = sql
        self.cur.execute(sql)
        self.conn.commit()
        return '1'

    def max_template_id(self):
        sql = "select max(template_id) from Template_mgmt"
        self.cur.execute(sql)
        res = self.cur.fetchone()
        if res and res[0]:
            return res[0]+1
        return 1
  
    def insert_template(self, tid, name, industry, project, user):
        sql = "insert into Template_mgmt (template_id, template_name, industry, project, user) values(%s, '%s', '%s', '%s', '%s')"%(tid, name, industry, project, user)
        self.cur.execute(sql)
        self.conn.commit()
        return '1'

    def insert_sheet_single(self, lst):
        sql = "insert into model_info (template_id, sheet_id, row, col, value, taxonomy) values(%s, %s, %s, %s, '%s', '%s')"%lst
        self.cur.execute(sql)
        self.conn.commit()
        return '1'

    def insert_sheet_data(self, lst):
        sql = "insert into model_info (template_id, sheet_id, row, col, value, taxonomy, formular_str, cell_alph) values(?, ?, ?, ?, ?, ?, ?, ?)"
        self.cur.executemany(sql, lst)
        self.conn.commit()
        return '1'

    def insert_sheets(self, lst):
        sql = "insert into sheet_mgmt (template_id, template_name, sheet_id, sheet_name, user, extra_info) values(?, ?, ?, ?, ?, ?)"
        self.cur.executemany(sql, lst)
        self.conn.commit()
        return '1'
 
    def read_templates(self, industry):
        sql = "select template_id, template_name, industry, project, user from Template_mgmt where industry='%s'"%(industry)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []
       
    def read_sheets(self, template_id, industry):
        sql = "select sheet_id, sheet_name from sheet_mgmt where template_id=%s ORDER BY sheet_id"%(template_id)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_sheet_extra_info(self, template_id, sheet_id):
        sql = "select extra_info from sheet_mgmt  where template_id=%s and sheet_id=%s"%(template_id, sheet_id)
        self.cur.execute(sql)
        res = self.cur.fetchone()
        if res and res[0]:
            return res
        return ["{}"]
         
    def read_all_sheet_data(self, template_id, sheets):
        sql = "select template_id, sheet_id, row, col, value, taxonomy, formular_str from model_info where template_name=%s and sheet_id in ('%s')"%(template_id, sheets)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_sheet_data(self, template_id, sheet_id):
        sql = "select row, col, value, taxonomy,formular_str,cell_alph from model_info where template_id=%s and sheet_id=%s"%(template_id, sheet_id)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []
        
    def create_default_table(self):
        template_schema = config_obj.Template_schema
        self.create_table(template_schema)
        sheet_schema = config_obj.sheet_schema
        self.create_table(sheet_schema)
        model_schema = config_obj.model_schema
        self.create_table(model_schema)
        return ['done']
          

if __name__ == '__main__':
    tdb = config_obj.Template_storage
    obj = sqlite_api(tdb)
    print obj.create_default_table()

