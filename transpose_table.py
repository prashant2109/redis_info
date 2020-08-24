import os, sys, copy, json
import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
class Tables():
    def flip_table(self, table_id, data):
        cell_d  = {}
        map_d   = {'hch':'VGH', 'vch':"HGH", 'value':"GV", 'gh':"GH"}
        for rc in data.keys():
            tkey    = str(table_id)+'_'+rc
            row, col    = map(lambda x:int(x), rc.split('_'))
            cell_d[tkey]    = (row, col, 'DD', map_d[data[rc]['ldr']], int(data[rc]['colspan']), int(data[rc]['rowspan']), map_d[data[rc]['ldr']], 'INC_'+rc)
        theder  = {}
        rc_d    = {}
        hrc_d   = {}
        gv_d    = {}
        cols_d  = {}
        for tkey, valtup in cell_d.items():
            row, col, txt, section_type, colspan, rowspan, sh_type, xml_id  = valtup
            if sh_type in ['GH', 'PGH']:
                theder.setdefault(sh_type, []).append((row, col, txt, xml_id))
            elif sh_type == 'GV':
                r   = int(row)
                c   = int(col)
                gv_d.setdefault(c, {})[r] = (txt, xml_id)
            elif sh_type    == 'HGH':
                r   = int(row)
                c   = int(col)
                #hrc_d.setdefault(r, {})[c]   = (txt, xml_id)
                for ri in range(int(rowspan)):
                    hrc_d.setdefault(r+ri, []).append((c, txt, xml_id))
            elif sh_type == 'VGH':
                r   = int(row)
                c   = int(col)
                rc_d.setdefault(r, {})[c]   = (txt, xml_id)
                for ri in range(int(rowspan)):
                    rc_d.setdefault(r+ri, {})[c]   = (txt, xml_id)
                for ci in range(int(colspan)):
                    cols_d[c+ci]    = 1
                    rc_d[r][c+ci]   = (txt, xml_id)

        col_txt = {}
        cols    = cols_d.keys()
        cols.sort()
        rows    = rc_d.keys()
        rows.sort()
        for c in cols:
            txt_ar  = []
            xml_d  = []
            for r in rows:
                txt = rc_d[r].get(c, '')
                if txt:
                    xml_d.append(txt[1])
                    txt             = txt[0]
                    if isinstance(txt, unicode):
                        txt = txt.encode('utf-8')
                    txt_ar.append(txt)
            txt  = ' '.join(txt_ar)
            #txt  =  self.convert_html_entity(txt)
            txt  = ' '.join(txt.split())
            xml  = ':@:'.join(xml_d)
            col_txt[c]  = (txt, xml)
        r_ind   = 0
        n_rc_d  = {}
        gh_d    = {}
        for sh_type in ['PGH', 'GH']:
            if sh_type not in theder:continue
            t_ar    = theder[sh_type]
            t_ar.sort()
            txt_ar  = []
            xml_d  = []
            for txt in t_ar:
                if txt:
                    xml_d.append(txt[3])
                    txt             = txt[2]
                    if isinstance(txt, unicode):
                        txt = txt.encode('utf-8')
                    txt_ar.append(txt)
            txt  = ' '.join(txt_ar)
            #txt  =  self.convert_html_entity(txt)
            txt  = ' '.join(txt.split())
            xml  = ':@:'.join(xml_d)
            n_rc_d[(r_ind, 0)]  = (r_ind, 0, txt, sh_type, '1', '1', sh_type,xml)
            gh_d[(r_ind, 0)]    = 1
            r_ind   += 1
            
        ph_split    = False
        #ph_split    = False
        cols  = gv_d.keys()
        cols.sort()
        vgh_ph_ind  = r_ind
        if ph_split == True:
            r_ind   += 1
        if hrc_d:
            vgh_ind = r_ind
            r_ind   += 1
        vgh_cols    = {}
        ph_cols     = {} 
        for c in cols:
            rows    = gv_d[c].keys()
            rows.sort()
            c_index   = 0
            for r in rows:
                c_index += 1
                t, x    = gv_d[c][r]
                key = table_id+'_'+self.get_quid(x)
                ph_map  = ''
                if not ph_map:
                    ph_map  = '^'.join(['', '', '', '', ''])
                if ph_map:
                    period_type, period, currency, scale, value_type   = ph_map.split('^')
                else:
                    period_type, period, currency, scale, value_type   = '', '', '', '', ''
                ph  = period_type+period
                ph_cols.setdefault(c_index, {}).setdefault(ph, {})[(c, r)]  = 1
                
        xml_cnt = 1
        f_vgh   = ''
        for c in cols:
            rows    = gv_d[c].keys()
            rows.sort()
            c_ind   = 0
            n_rc_d[(r_ind, c_ind)]  = (r_ind, c_ind, col_txt.get(c, ('', ''))[0], 'HGH', '1', '1', 'HGH', col_txt.get(c, ('', ''))[1])
            c_ind   += 1
            oc_ind  = 0
            #print c, rows
            for r in rows:
                oc_ind  += 1
                rtxts   = hrc_d.get(r, [])
                rtxts.sort(key=lambda x:x[0])
                t_ar    = []
                x_ar    = []
                for rtup in rtxts:
                    ci, t, x =rtup
                    if t:
                        t_ar.append(t)
                    if x:
                        x_ar.append(x)
                t1   = ' '.join(t_ar)
                x1   = ':@:'.join(x_ar)
                if ph_split == True:
                    n_rc_d[(vgh_ph_ind, oc_ind)]  = (vgh_ph_ind, oc_ind, t1, 'VGH', len(ph_cols[oc_ind].keys()), '1', 'VGH', x1)
                    for ph, v_d in ph_cols[oc_ind].items():
                        #n_rc_d[(vgh_ind, c_ind)]  = (vgh_ind, c_ind, t1, 'VGH', '1', '1', 'VGH',x1)
                        tmpx    = 'x-'+str(xml_cnt)+'_'+str(p_no)
                        xml_cnt += 1
                        n_rc_d[(vgh_ind, c_ind)]  = (vgh_ph_ind, oc_ind, ph, 'VGH', '1', '1', 'VGH', tmpx)
                        vgh_cols[c_ind]    = 1
                        if (c, r) in v_d:
                            t, x    = gv_d[c][r]
                        else:
                            t, x    = '', 'x-'+str(xml_cnt)+'_'+str(p_no)
                            xml_cnt += 1
                        n_rc_d[(r_ind, c_ind)]  = (r_ind, c_ind, t, 'GV', '1', '1', 'GV',x)
                        c_ind   += 1
                else:
                    if rtxts:
                        f_vgh   = 'Y'
                        n_rc_d[(vgh_ind, c_ind)]  = (vgh_ind, c_ind, t1, 'VGH', '1', '1', 'VGH',x)
                        vgh_cols[c_ind]    = 1
                    t, x    = gv_d[c][r]
                    n_rc_d[(r_ind, c_ind)]  = (r_ind, c_ind, t, 'GV', '1', '1', 'GV',x)
                    c_ind   += 1
            r_ind   += 1
        pno = 1
        p_no = 1
        if hrc_d:
            n_rc_d[(vgh_ind, 0)]  = (vgh_ind, 0, '', 'VGH', '1', '1', 'VGH','x-'+str(xml_cnt)+'_'+str(p_no))
        xml_cnt += 1
        if ph_split == True:
            n_rc_d[(vgh_ph_ind, 0)]  = (vgh_ph_ind, 0, '', 'VGH', '1', '1', 'VGH', 'x-'+str(xml_cnt)+'_'+str(p_no))
            xml_cnt += 1
        cols    = len(vgh_cols.keys())+1
        if ph_split == True:
            for i in range(vgh_ph_ind):
                tup = n_rc_d[(i, 0)]
                tup = tup[:4]+(str(cols), )+tup[5:]
                n_rc_d[(i, 0)]  = tup
        elif hrc_d:
            for i in range(vgh_ind):
                tup = n_rc_d[(i, 0)]
                tup = tup[:4]+(str(cols), )+tup[5:]
                n_rc_d[(i, 0)]  = tup
        rc_d    = {}
        for k in n_rc_d.keys():
            rc_d.setdefault(k[0], {})[k[1]] = 1
        rs  = rc_d.keys()
        rs.sort()
        if gh_d:
            max_cols  = 0 
            for r in rs:
                cols    = rc_d[r].keys()
                cols.sort()
                col_cnt = 0
                for c in cols:
                    col_cnt += int(n_rc_d[(r, c)][4])
                max_cols = max(col_cnt, max_cols)
            for k in gh_d.keys():
                tup = n_rc_d[k]
                tup = tup[:4]+(str(max_cols), )+tup[5:]
                n_rc_d[k]  = tup
                    
        n_ar    = []
        table_str   = '<table>'
        for r in rs:
            cols    = rc_d[r].keys()
            cols.sort()
            tmp_ar  = map(lambda x:n_rc_d[(r, x)], cols)
            n_ar.append(tmp_ar)
        return n_ar

    def transpose(self, project_id):
        cur, conn = conn_obj.MySQLdb_conn(config.Config.pinfo_db) 
        sql = "select ProjectCode from ProjectMaster where ProjectID=%s"%(project_id)
        cur.execute(sql)
        res     = cur.fetchone()
        conn.close()
        dbname  = res[0]
        dbinfo  = copy.deepcopy(config.Config.pinfo_db)
        dbinfo['db']    = dbname
        cur, conn       = conn_obj.MySQLdb_conn(dbinfo) 
        sql             = "select docid, pageno, groupid, sdata, udata from db_data_mgmt_grid_slt where groupid <=1000 and active_status='Y'"
        cur.execute(sql)
        res             = cur.fetchall()
        rev_map_d       = {
                            'GV'    : 'value',
                            'HGH'   : 'hch',
                            'VGH'   : 'vch',
                            'GH'    : 'gh',
                            }
        for r in res:
            docid, pageno, groupid, sdata, udata    = r
            gdata    = {}
            if udata:
                gdata    = json.loads(udata)
            elif sdata:
                gdata    = json.loads(sdata)
            data    = gdata.get('data', {})
            ks  = data.keys()
            gv_cols = {}
            gv_rows = {}
            hch_rows = {}
            hch_cols = {}
            vch_rows = {}
            vch_cols = {}
            gh_cells    = {}
            hch_cells   = {}
            for k in ks:
                row, col    = map(lambda x:int(x), k.split('_'))
                dd  = data[k]
                if dd['ldr']    == 'gh':
                    gh_cells[(row, col)]    = 1
                elif dd['ldr']    == 'value':
                    gv_rows.setdefault(row, {})[k]    = 1
                    gv_cols.setdefault(col, {})[k]    = 1
                elif dd['ldr']    == 'hch':
                    hch_rows.setdefault(row, {})[k]    = 1
                    hch_cols.setdefault(col, {})[k]    = 1
                    for r in range(int(dd['rowspan'])):
                        hch_cells.setdefault(row+r, {})[(row, col)] = 1
                elif dd['ldr']    == 'vch':
                    vch_rows.setdefault(row, {})[k]    = 1
                    vch_cols.setdefault(col, {})[k]    = 1
            trans       = 0
            vch_rows    = {}
            for k, v in gv_cols.items():
                if k in hch_cols:
                    trans   = 1
                    break
            for k, v in gv_rows.items():
                if k in hch_rows:
                    trans   = 0
                    break
            if trans    == 1:
                empty_cell    = {u'value_taxo_str': '', u'md_key': '', u'colspan': '', u'topic_name': '', u'ldr': '', u'md_taxo': '', u'dparent_ids': '', u'rowspan': '', u'slevel': '', u'parent_id': '', u'value_filter_str': '', u'md_txph': '', u'txph': '', u'xml_ids': '', u'pdf_xmlids': '', u'chref': '', u'cell_id': '', u'value_txph': '', u'bbox': '', u'md_s_range': '', u'data': '', u'rects': '', u'custids': '', u'sph_index': '', u'md_val': '', u'md_pos': '', u'sn_cn_t': ''}
                ndata   = self.flip_table('1', data)
                print (docid, pageno, groupid)
                #row, col, txt, section_type, colspan, rowspan, sh_type, xml_id  = valtup
                row     = 0
                done_d  = {}
                tmpd    = {}
                for r in ndata:
                    col = -1 
                    #print
                    for c in r:
                        col += 1
                        tmpxs       = []
                        otherxs     = []
                        #print '\t', row, c
                        for x in c[7].split(':@:'):
                            if 'INC' in x:
                                tmpxs.append(x)
                            else:
                                otherxs.append(x)
                        if tmpxs:
                            otherxs = []
                        if otherxs:
                            otherxs = [':@:'.join(otherxs)]
                        for xi, x in enumerate(tmpxs):
                            if 'INC' in x:
                                rc  = '_'.join(x.split('_')[1:])
                                if rc in done_d:continue
                                done_d[rc]  = 1
                                key = '%s_%s'%(row, col)
                                tmpd[key]    = copy.deepcopy(data[rc])
                                tmpd[key]['colspan'] = c[4]
                                tmpd[key]['rowspan'] = c[5]
                                tmpd[key]['ldr']     = rev_map_d[c[3]]
                                tmpd[key]['cell_id'] = key
                                if c[3] not in  ['GV', 'VGH'] and xi < (len(tmpxs)- 1):
                                    row += 1
                            else:
                                key = '%s_%s'%(row, col)
                                tmpd[key]    = copy.deepcopy(empty_cell)
                                tmpd[key]['colspan'] = c[4]
                                tmpd[key]['rowspan'] = c[5]
                                tmpd[key]['ldr']     = rev_map_d.get(c[3], '')
                                tmpd[key]['cell_id'] = key
                                if c[3] not in  ['GV', 'VGH']:
                                    row += 1
                    row += 1
                gdata['data']   = tmpd
                if tmpd:
                    path    = config.Config.doc_path.format(project_id, docid)
                    path    = '%sgrid_data/'%(path)
                    os.system("mkdir -p "+path)
                    path    = '%s%s_%s.json'%(path, pageno, groupid)
                    print path
                    fout    = open(path, 'w')
                    fout.write(json.dumps(gdata))
                    fout.close()
                if 0:
                    ks  = tmpd.keys()
                    ks.sort(key=lambda x:tuple(map(lambda x1:int(x1), x.split('_'))))
                    prev_r  = ''
                    for k in ks:
                        if k.split('_')[0] != prev_r:
                            print '\n==============================='
                        prev_r  = k.split('_')[0]
                        print '\t', k, tmpd[k]['ldr'], [tmpd[k]['data']]
                    sys.exit()
    def get_quid(self, x):
        return x
if __name__ == '__main__':
    obj = Tables()
    obj.transpose(sys.argv[1])
                    
                
                
                
                    
        

        
        
        
