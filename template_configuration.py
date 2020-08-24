import os, sys, json
########### In Built  ##############

import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
############  custom imports   #############

class TemplateConfig:
        
    def insert_output_config(self, ijson):
        company_id = ijson['company_id']
        project_id  = ijson['project_id']
        template_id = ijson['template_id']
        parsed_data = ijson['data']
        tabs        = parsed_data.get('tabs', {})
        map_d   = {
                    '1' : 'PH',
                    '2' : 'G',
                    }
        conf_d  = {}
        for c in tabs.get('Col Conf', []):
            conf_d[c]   = 1

        config_path = config.Config.project_config.format(project_id) 
        if not os.path.exists(config_path):
            os.system('mkdir -p {0}'.format(config_path))
        config_db = os.path.join(config_path, 'config.db')       
        conn, cur   = conn_obj.sqlite_connection(config_db)
        
        crt_stmt = """ CREATE TABLE IF NOT EXISTS output_configuration(row_id INTEGER PRIMARY KEY AUTOINCREMENT, template_id TEXT, export_config TEXT, file_name TEXT, zip_file_name TEXT); """
        cur.execute(crt_stmt)
            
        read_qry = """ SELECT template_id FROM output_configuration; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        check_data = {str(row_data[0]) for row_data in t_data}
        parsed_data['e_c']['col_config']    = conf_d
        export_config, file_name, zip_file_name = map(json.dumps, [parsed_data['e_c'], parsed_data['f_c'], parsed_data['zf_c']])
        
            
        if str(template_id) in check_data:
            update_tup = [(export_config, file_name, zip_file_name, template_id)]
            update_stmt = """ UPDATE output_configuration SET export_config=?, file_name=?, zip_file_name=? WHERE template_id=?; """ 
            cur.executemany(update_stmt, update_tup)
            conn.commit()

        elif template_id not in check_data:
            insert_rows = []
            insert_tup = (template_id, export_config, file_name, zip_file_name)
            insert_rows.append(insert_tup)
            if insert_rows:
                insert_stmt = """ INSERT INTO output_configuration(template_id, export_config, file_name, zip_file_name) VALUES(?, ?, ?, ?); """
                cur.executemany(insert_stmt, insert_rows)
                conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res
    
    def read_template_info(self, ijson):
        company_id = ijson['company_id']
        project_id  = ijson['project_id']
        template_id = ijson['template_id']

        config_path = config.Config.project_config.format(project_id) 
        if not os.path.exists(config_path):
            os.system('mkdir -p {0}'.format(config_path))
        config_db = os.path.join(config_path, 'config.db')       
        conn, cur   = conn_obj.sqlite_connection(config_db)
        read_qry = """ SELECT export_config, file_name, zip_file_name FROM output_configuration WHERE template_id='{0}'; """.format(template_id)
        cur.execute(read_qry)
        t_data  = cur.fetchone()
        conn.close()
        
        data = {}
        if t_data:
            export_config, file_name, zip_file_name = map(json.loads, t_data)
            data = {'e_c':export_config, 'f_c':file_name, 'zf_c':zip_file_name}
        res = [{'message':'done', 'data':data}]
        return res        

if __name__ == '__main__':
    tc_Obj = TemplateConfig()
