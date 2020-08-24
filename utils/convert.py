import sqlite3
class Convert():
    def __init__(self):
        db_file = '/mnt/eMB_db/unit_conversion/conversion_factor.db'
        conn = sqlite3.connect(db_file)
        cur  = conn.cursor()
        self.scale_map_d    = {}
        self.num_obj        = {}
        sql = "select scale, disp_name, value from scale_info"
        cur.execute(sql)
        res = cur.fetchall()
        for rr in res:
            scale, disp_name, value = rr
            if not value:continue
            self.scale_map_d[scale]    = scale
            self.num_obj[scale]      = float(value)
            self.scale_map_d[scale.lower()]    = scale
            self.num_obj[scale.lower()]      = float(value)
        self.con_factor     = {}
        db_file = '/mnt/eMB_db/unit_conversion/conversion_factor.db'
        conn = sqlite3.connect(db_file)
        cur  = conn.cursor()
        sql = "select from_scale, to_scale, value from conversion_factor"
        cur.execute(sql)
        res = cur.fetchall()
        for rr in res:
            from_scale, to_scale, value = rr
            if not value:continue
            self.con_factor[(from_scale, to_scale)] = float(value)
            self.con_factor[(from_scale.lower(), to_scale.lower())] = float(value)
        self.no_scale_factor_map    = {}
        self.scale_factor_map    = {'Available':{}, 'Not-Available':{}, 'Not-Available-Scale':{}}

    def convert_frm_to_1(self,frm, to, value, infotup=()):
        if not value or value in [0, 0.00, '0', '0.00']:
            return value, ''
        #print (frm, to, (frm, to) in self.con_factor)
        if frm == to:
            final_val   = self.convert_floating_point(value)
            return final_val, '' #.replace(",", "")
        if (frm, to) in self.con_factor:
            self.scale_factor_map['Available'][(frm, to)]   = self.con_factor[(frm, to)]
            self.no_scale_factor_map.setdefault((frm, to), {})[infotup]   = 1
            return self.convert_frm_to_with_factor(frm, to, value, self.con_factor[(frm, to)]), self.con_factor[(frm, to)]
        if 1:#(frm in self.scale_map_d and to in self.scale_map_d):
            self.scale_factor_map['Not-Available'][(frm, to)]   = ''
            self.no_scale_factor_map.setdefault((frm, to), {})[infotup]   = 1
        if frm not in self.scale_map_d:
            self.scale_factor_map['Not-Available-Scale'][frm]   = ''
            self.no_scale_factor_map.setdefault(frm, {})[infotup]   = 1
        if to not in self.scale_map_d:
            self.no_scale_factor_map.setdefault(to, {})[infotup]   = 1
            self.scale_factor_map['Not-Available-Scale'][to]   = ''
        
        #NO MAP FOUND
        final_val   = self.convert_floating_point(value)
        return final_val, '' #.replace(",", "")


    def convert_frm_to(self,frm, to, value, infotup=()):
        if not value or value in [0, 0.00, '0', '0.00']:
            return value
        #print (frm, to, (frm, to) in self.con_factor)
        if frm == to:
            final_val   = self.convert_floating_point(value)
            return final_val #.replace(",", "")
        if (frm in self.scale_map_d and to in self.scale_map_d):
            if (frm, to) in self.con_factor:
                self.scale_factor_map['Available'][(frm, to)]   = self.con_factor[(frm, to)]
                return self.convert_frm_to_with_factor(frm, to, value, self.con_factor[(frm, to)])
            self.scale_factor_map['Not-Available'][(frm, to)]   = ''
            self.no_scale_factor_map.setdefault((frm, to), {})[infotup]   = 1
        if frm not in self.scale_map_d:
            self.scale_factor_map['Not-Available-Scale'][frm]   = ''
            self.no_scale_factor_map.setdefault(frm, {})[infotup]   = 1
        if to not in self.scale_map_d:
            self.no_scale_factor_map.setdefault(to, {})[infotup]   = 1
            self.scale_factor_map['Not-Available-Scale'][to]   = ''
        
        #NO MAP FOUND
        final_val   = self.convert_floating_point(value)
        return final_val #.replace(",", "")

    def convert_frm_to_with_factor(self,frm, to, value, div_val):
        num         = float(value)
        final_val   = float(num * div_val)
        final_val   = self.convert_floating_point(final_val)
        return final_val 

    def convert_floating_point(self, string, r_num=2):
         if string == '':
             return ''
         try:
             string = float(string)
         except:
             return string
         f_num  = '{0:.10f}'.format(string).split('.')
         #print [string, f_num]
         r_num  = 0
         if f_num[0] in ['0', '-0']:
             if len(f_num) ==2:
                  r_num     = 0 #len(f_num[1])
                  f_value   = 0
                  #print '\t','f_num ', f_num[1]  
                  for num in f_num[1]:
                      #print '\t\t', [num, r_num]
                      r_num += 1
                      if f_value and r_num > 2:break
                      if num != '0':
                        f_value = 1
                  if f_value == 0:
                        r_num = 0
         else:
             if len(f_num) ==2:
                  r_num     = 0 #len(f_num[1])
                  #print '\t',[r_num]
                  f_val     = []
                  for i, num in enumerate(f_num[1]):
                      #print '\t\tNUM',[num, r_num]
                      if num != '0':
                        f_val.append(i)
                  f_val.sort()
                  if f_val:
                    if f_val[0] == 0:
                        r_num   = 2
                    else:
                        r_num   = f_val[0]+1

             else:
                r_num   = 2
             if r_num < 0:
                 r_num = 0
             if r_num > 2:
                r_num = 2
         tr_num = 2
         form_str   = "{0:,."+str(tr_num)+"f}"
         #print [string, form_str, r_num, f_num]
         if (form_str.format(string) in ['0.00', '-0.00'] and abs(string) > 0.00) or f_num[0] == '0':
            form_str   = "{0:,."+str(r_num)+"f}"
         #print form_str
         return form_str.format(string)
