import sqlite3
class sqlite_api:

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()
    
    def get_grids(self):
        sql = "select doc_id, page_no , grid_id from table_mgmt"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            return res
        return []
