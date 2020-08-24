import sqlite3
import config
config_obj = config.Config()
class sqlite_api:

    def __init__(self, db_path):
        self.db = db_path
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
  
    def insert_template(self,  name, industry, project, user):
        sql = "insert into Template_mgmt (template_name, industry, project, user) values('%s', '%s', '%s', '%s')"%( name, industry, project, user)
        self.cur.execute(sql)
        self.conn.commit()
        self.cur.execute('SELECT last_insert_rowid()')
        res = self.cur.fetchone()
        return int(res[0])

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
        sql = "insert into sheet_mgmt (template_id, sheet_id, sheet_name, user, extra_info) values(?, ?, ?, ?, ?)"
        self.cur.executemany(sql, lst)
        self.conn.commit()
        return '1'
 
    def read_templates(self, project_id):
        sql = "select template_id, template_name, industry, project, user from Template_mgmt where project='%s'"%(project_id)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []
       
    def read_sheets(self, template_id):
        sql = "select sheet_id, sheet_name from sheet_mgmt where template_id=%s ORDER BY sheet_id"%(template_id)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []
         
    def read_sheet_extra_info(self, template_id, sheet_id):
        #sql = "select sheet_name, extra_info from sheet_mgmt  where template_id=%s and sheet_id=%s"%(template_id, sheet_id)
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

    def read_all_sheet_data_v1(self, template_id, sheets):
        sql = "select template_id, sheet_id, row, col, value, taxonomy, formular_str from model_info where template_id=%s and sheet_id=%s and row < 5"%(template_id, sheets)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_sheet_data_v1(self, template_id, sheet_id):
        sql = "select row_id, row, col, value, taxonomy,formular_str,cell_alph from model_info where template_id=%s and sheet_id=%s"%(template_id, sheet_id)
        #print sql
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_sheet_data(self, template_id, sheet_id):
        sql = "select row, col, value, taxonomy,formular_str,cell_alph from model_info where template_id=%s and sheet_id=%s"%(template_id, sheet_id)
        #print sql
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

    def read_page_cords(self):
        sql = "select pageno, pagesize from pagedet"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []
          
    def get_grids(self):
        sql = "select doc_id, page_no , grid_id from table_mgmt"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_taxo_default_data(self, table_name):
        sql = "select id, value from %s"%(table_name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        return res

    def get_grids_tb(self, doc_id):
        sql = "select doc_id, page_no, grid_id from table_mgmt where doc_id='%s'"%(doc_id)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_eq_info(self, table_id):
        sql = "select distinct(table_id1) from equalityInfo"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_eq_num_info(self, table_id):
        sql = "select distinct(gp1) from equalitydb"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_raw_info(self):
        sql = "select distinct(gridids) from rawdb"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_eq_num_info_full(self):
        sql = "select gp1,row1, col1, gp2, row2, col2 from equalitydb"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_eq_info_full(self):
        #return []
        sql = "select table_id1, table_id2, r_c1, r_c2,relationship_Type from equalityInfo"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_eq_info_rc(self, table_id, rc):
        #return []
        sql = "select table_id2, r_c2 from equalityInfo where table_id1= '%s' and r_c1='%s'"%(table_id, rc)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def cell_wise_eq(self, table_id, r, c):
        sql = "select gp2, row2, col2 from equalitydb where row1='%s' and col1='%s' and gp1 like '%s'"%(r,c,'%'+table_id+'%')
        print sql
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_db_builder(self):
        sql = "select table_type, taxo_group_id, restated_flag, formula_flag, label_change_flag from data_builder"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_db_builder1(self):
        sql = "select row_id, table_type, taxo_group_id, restated_flag, formula_flag, label_change_flag from data_builder"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_rc_info(self, table_type, taxo_group_id):
        sql = "select row, col, value, cell_type, row_id from data_builder where table_type='%s' and taxo_group_id='%s'"%(table_type, taxo_group_id)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_reference_table(self):
        sql = "select row_id, rawdb_row_id, xml_id, bbox, page_no from reference_table" 
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_formula_info(self, row_id):
        sql = "select formula_id, operator, operand_row_id, value, rawdb_row_id  from formula_table where rawdb_row_id='%s'"%(row_id)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_only_rows(self, row_ids):
        sql = "select row, row_id from data_builder where row_id in (%s)"%(row_ids)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_only_des(self, rows):
        sql = "select row, value from data_builder where row in (%s) and col=0"%(rows)
        print sql
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print res
        if res:
            return res
        return []

    def read_fids(self):
        sql = "select distinct(rawdb_row_id) from formula_table"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_reference_table_rid(self, rids):
        sql = "select row_id, rawdb_row_id, xml_id, bbox, page_no from reference_table where rawdb_row_id in (%s)"%(rids)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []
        
    def read_db_builder_v1(self):
        sql = "select row_id, table_type, taxo_group_id from data_builder"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_reference_table_v1(self):
        sql = "select row_id, rawdb_row_id, xml_id, table_id, bbox, page_no from reference_table" 
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def update_map_info(self, info):
        #sql = "update mapping_info set table_type=?, group_id = ?, db_row_id=? where xml_id = ? and table_id=?"
        sql = "update mapping_info set db_row_id=? where id=?"
        self.cur.executemany(sql, info)
        self.conn.commit()
        return '1'

    def read_grid_info(self):
        sql = "select table_type, taxo_group_id from data_builder" 
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_all_tb(self):
        sql = "select row_id, table_type,short_form from all_table_types" 
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_map_info(self):        
        sql = "select xml_id, table_id, table_type, id from mapping_info" 
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def read_db_builder_v2(self, rid):
        sql = "select  value, cell_type from data_builder where row_id='%s'"%(rid)
        self.cur.execute(sql)
        res = self.cur.fetchone()
        if res:
            return res
        return []

    def read_reference_table_rid_v1(self, rid):
        sql = "select row_id, rawdb_row_id, xml_id, bbox, page_no from reference_table where rawdb_row_id=%s"%(rid)
        self.cur.execute(sql)
        res = self.cur.fetchone()
        if res:
            return res
        return []

    def read_output_config(self, template_id):
        sql = "select export_config, file_name, zip_file_name  from output_configuration where template_id=%s"%(template_id)
        self.cur.execute(sql)
        res = self.cur.fetchone()
        if res:
            return res
        return []

    def read_grouped_primary_keys(self):
        sql         = "CREATE TABLE IF NOT EXISTS primary_key_group(row_id  INTEGER PRIMARY KEY AUTOINCREMENT, primary_key text, other_primary_key text, user_name text, datetime text)"
        self.cur.execute(sql)
        sql = "select primary_key, other_primary_key from primary_key_group"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        return res
        

    def read_all_primary_keys(self):
        sql = "select table_type, taxo_group_id from data_builder where row_id in (select rawdb_row_id from column_classification where column_class='primary_key')"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def execute_sql(self, sql):
        self.cur.execute(sql)
        self.conn.commit()
        return '1'
        
    def insert_lookup_mgmt(self, lst):
        sql = "insert into lookup_mgmt (lookup_key) values(?)"
        self.cur.executemany(sql, lst)
        self.conn.commit()
        return '1'
    
    def read_lookup_mgmt(self):
        sql  = "select row_id, lookup_key from lookup_mgmt"     
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []

    def insert_lookup_sheet_column_mgmt(self, lst):
        sql = "insert into lookup_sheet_column_mgmt (sheet_name, column_name, lookup_key) values(?, ?, ?)"
        self.cur.executemany(sql, lst)
        self.conn.commit()
        return '1'

    def insert_lookup_value_mgmt(self, lst):
        sql = "insert into lookup_value_mgmt (value, type, extra_info, lookup_key) values(?, ?, ?, ?)"
        self.cur.executemany(sql, lst)
        self.conn.commit()
        return '1'

    def read_lookup_value_mgmt(self):
        return [] 
        pass
        sql = "select row_id , value, type, extra_info, lookup_key from lookup_value_mgmt "
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if not res:
            return []   
        return res
   
    def delete_all_info(self):
        sql = "delete from lookup_sheet_column_mgmt"
        self.cur.execute(sql)
        sql = "delete from lookup_value_mgmt"
        self.cur.execute(sql)
        sql = "delete from lookup_mgmt"
        self.cur.execute(sql)
        self.conn.commit()

    def read_meta_info(self):
        sql = "select period_type, period , meta_data from  document_meta_info"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if not res:
            return []   
        return res
        
    def read_tb_ids_info(self):
        sql = "select row_id, table_type from all_table_types"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if not res:
            return []   
        return res
               
 
if __name__ == '__main__':
    tdb = config_obj.Template_storage
    obj = sqlite_api(tdb)
    print obj.create_default_table()

