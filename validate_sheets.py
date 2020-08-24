import os, sys, json
import config
class Import():
    def col_wise_config_info(self, rows):
        colconfig_data = {}
        for cell_v in rows:
            template_id, sheet_id, row, col, value, taxonomy, formular_str = cell_v 
            cell_d = eval(value)
            if cell_d.get('c_type', '') == 'H' and cell_d.get('c_c', ''):
                value = cell_d['value'] 
                colconfig_data[value] = [col, cell_d['c_c']]
        return colconfig_data
                
            
    def check_validation(self, category, cl_value, sys_value):
        if category == 'DataType':
            print '\n Date time', cl_value, sys_value 
            c, s = cl_value.get('DataType', ''), cl_value.get('DataType', '')
            if c and (c != s):
                return '%s not matched'%(category)
        elif category == 'IsMetadata':
            c, s = cl_value.get('IsMetadata', ''), cl_value.get('IsMetadata', '')
            if c and (c != s):
                return '%s not matched'%(category)

        elif category == 'PossibleValue' and cl_value.get('type', '') == 'LV':
            c, s = cl_value.get('LV', ''), cl_value.get('LV', '')
            if c and (c != s):
                return '%s not matched'%(LV)

        elif category == 'Nullable':
            c, s = cl_value.get('Nullable', ''), cl_value.get('Nullable', '')
            if c and (c != s):
                return '%s not matched'%(category)

        elif category == 'LinkTo':
            c, s = cl_value.get('LinkTo', ''), cl_value.get('LinkTo', '')
            if not c:
                return False
            if not isinstance(c, dict):
                csheet, ccol = c.split('.')
            else:
                csheet, ccol = c.get('s', '') , c.get('c', '')
            
            if not s:
                return '%s not matched'%(category)
            if not isinstance(s, dict):
                ssheet, scol = s.split('.')
            else:
                ssheet, scol = s.get('s', '') , s.get('c', '')
            if (csheet and ccol) and ((csheet != ssheet) or (ccol != scol )):
                return '%s not matched'%(category)
        return False
            
                      

    def import_sheets(self, ijson):
        fname   = '/var/www/html/muthu/clo_shee_info.txt'
        data    = json.loads(open(fname, 'r').read())
        import modules.template_mgmt.model_api as ma 
        ma_obj = ma.model_api()
        template_id = ijson['template_id']
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        import sqlite_api
        db_path = config.Config.mapping_path.format(company_id, project_id, template_id) #config_obj.Template_storage
        txt_info = "/var/www/html/sudhakar/%s_%s_%s.txt"%(company_id, project_id, template_id)
        sql_obj = sqlite_api.sqlite_api(db_path)
        sheets = sql_obj.read_sheets(template_id)


        sh_config_info = {}
        if os.path.isfile(txt_info) and ijson.get('must', '') != 'Y':
            f = open(txt_info, "r")
            sh_config_info = json.loads(f.read())
        else:
            for sheet in sheets:
                sheet_id, name = sheet
                sheet_info = sql_obj.read_all_sheet_data_v1(template_id, sheet_id)            
                config_data = self.col_wise_config_info(sheet_info)
                sh_config_info[name] = config_data
            f = open(txt_info, "w")
            f.write(json.dumps(sh_config_info))

        for sheet in data['sheets']:
            sname   = sheet['name']
            #if sname != "deal":continue
            print 'name ', sname
            header  = []
            r       = 0
            col     = 0
            #if sname not in ['assetContracts']:continue
            rows    = sheet['rows']
            rows.sort(key=lambda x:x['index'])
            for row in rows:
                if not header:
                    header  = map(lambda x:x['value'], filter(lambda x1:x1.get('value'), row['cells']))
                    continue
                rdd  = {}
                for cin in row['cells']:
                    if cin.get('value'):
                        rdd[header[cin['index']]]    = cin
                Field       = rdd.get('Field', {}).get('value', '')
                if not Field:continue
                Type        = rdd.get('Type', {}).get('value', '').lower()
                if 'varchar' in Type or ('char' in Type):
                    Type    = 'String'
                elif 'date' in Type:
                    Type    = 'Date'
                elif 'float' in Type:
                    Type    = 'Float'
                
                Nullable    = rdd.get('Nullable', {}).get('value', '')
                PossibleValue    = rdd.get('Possible Values', {})
                IsPrimaryKey    = rdd.get('Unique', {}).get('value', '')
                LinkTo    = rdd.get('Link To', {}).get('value', '')
                Condition    = rdd.get('Condition', {})
                if 'value' not in PossibleValue:
                    PossibleValue['value']  = ''
                dd          = {'LinkTo':LinkTo, 'Condition':Condition}
                dd['c_c']   = {'DataType':Type, 'Nullable':Nullable, 'IsPrimaryKey':IsPrimaryKey, 'LinkTo':LinkTo, 'Condition':Condition}
                if PossibleValue['value'].strip():
                    if PossibleValue.get('underline') == True and (PossibleValue['value'][0] == '[' or PossibleValue['value'][:2] == '?[')  and  PossibleValue['value'][-1] == ']':
                        dd['c_c']['PossibleValue']  = {'type':'LV', 'LK':PossibleValue['value'].strip('?').strip('[').strip(']')}
                    else:
                        dd['c_c']['PossibleValue']  = {'type':'DV', 'dp':PossibleValue['value']}
                    
                dd['v']     = Field
                dd['c_type']    = 'H'
                #print sname, dd['v'], dd['c_c'],' --- ' ,sh_config_info.get(sname, {}).get(dd['v'], {})
                sys_config = sh_config_info.get(sname, {}).get(dd['v'], [])[1]
                category = {'DataType': 'DataType', 'IsMetadata': 'IsMetadata', 'PossibleValue':'PossibleValue', 'Nullable':'Nullable', 'LinkTo': 'LinkTo'}
                for catgory ,lk in category.items():
                    result = self.check_validation(lk, dd['c_c'], sys_config)         
                    if result:
                        print '   Error ',catgory , result, dd['c_c'].get(catgory, ''), sys_config.get(catgory)
                col += 1
                
if __name__ == '__main__':
    obj = Import()
    ijson = {"template_id":4,"company_id":1053729,"doc_id":61,"project_id":5,"sheet_id":1,"cmd_id":207,"user":"demo"}
    obj.import_sheets(ijson)
            
