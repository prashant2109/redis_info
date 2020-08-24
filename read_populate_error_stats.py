import MySQLdb,sqlite3
import sys,json
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config

class READSTATS():
    def __init__(self):
        pass

    def read_data(self, ijson):
        company_id = ijson['company_id']
        db_path = config.Config.company_table_path.format(company_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        data = []
        sql = "select table_id,error_info from error_tables"
        exequry = cur.execute(sql)
        err = exequry.fetchall()
        dum = {}
        t_g = []
        for ed in err:
            d = eval(ed[1])
            g = ed[0];
            t_g.append(g)
            for x in d.keys():
                if x in dum:
                    cnt = dum[x]["c"] + 1
                    dum[x]["c"] = cnt
                    dum[x]["grids"].append(g)
                else:
                    dum[x] = {"c": 1, "grids":[]}
                    dum[x]["grids"].append(g)
        
        for k in dum:
            data.append({"k": k , "n": k, "c": dum[k]["c"], "grids": dum[k]["grids"]})
        data.insert(0,{"k": "Total no of Tables", "n": "Total no of Tables", "c": len(err), "grids": t_g})
        res = [{"message":"done", "data":data}]
        return res

    def err_highlight(self, ijson):
        company_id = ijson['company_id']
        table_id   = ijson['table_id']
        error_txt  = ijson['error_txt']
        db_path = config.Config.company_table_path.format(company_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        data = []
        sql = "select table_id,error_info from error_tables where table_id = '%s'" %(table_id)
        exequry = cur.execute(sql)
        err = exequry.fetchall()
        for ed in err:
            d = eval(ed[1])
            for k in d.keys():
                if error_txt == "MULTIPLE SPAN -- col_span":
                    for x in d[k]:
                        data.append(x)
        res = [{"message":"done", "data":data}] 
        return res

    def grouping(self, ijson):
        company_id = ijson['company_id']
        db_path = config.Config.project_table_group.format(company_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        data = ijon["data"]
        for d in data:
            sql = "insert into (group_id, table_type, group_ph) values (%s, %s,%s)"%(gid, tt, gp)
            print(sql) 
    
    def create_table(self, ijson):
        company_id = ijson['company_id']
        db_path = config.Config.project_table_group.format(company_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        sql = "create table if not exists grouping_info (row_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL,table_type varchar(255) NOT NULL, group_ph varchar(255) NOT NULL )"
        exequry = cur.execute(sql) 
         
if __name__ == "__main__":
    obj = READSTATS()
    print obj.grouping({"company_id":1053729, "data": [{"RADIO":[85, 98, 199]}]})
