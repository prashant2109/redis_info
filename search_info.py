import os, sys, json, copy, sets, lmdb, hashlib
from collections import OrderedDict as OD
from igraph  import Graph
    
import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
    
class Search_Information(object):

    def find_alp(self, dec_val):
        if(dec_val<26):
            return chr(65+dec_val)
        if(dec_val > 25 and dec_val < 52):
            return chr(65)+chr(65+(dec_val-26))
        if(dec_val > 51 and dec_val < 78):
            return chr(66)+chr(65+(dec_val-52))
        if(dec_val > 77 and dec_val < 104):
            return chr(67)+chr(65+(dec_val-78))
        return "@@"

    def search_final_results_between(self, search_res1, search_res2, company_id, doc_id):
        graph_path = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/tablets_"+str(company_id)+"/"+str(doc_id)+"/" 
        fname_dict = {}
        for search_elm in search_res1:
            search_elm_sp = search_elm.split('#')
            hkeycol = search_elm_sp[0]+'#'+search_elm_sp[1]
            fname = search_elm_sp[2]
            if fname not in fname_dict:
               fname_dict[fname] = []   
            fname_dict[fname].append(hkeycol)

        #print fname_dict   

        fname_dict2 = {}
        for search_elm in search_res2:
            search_elm_sp = search_elm.split('#')
            hkeycol = search_elm_sp[0]+'#'+search_elm_sp[1]
            fname = search_elm_sp[2]
            if fname not in fname_dict2:
               fname_dict2[fname] = []   
            fname_dict2[fname].append(hkeycol)

        #print fname_dict2
 
        results_f = {}
        for fname, vs in fname_dict.items():
            ar = fname.split('_') 
            new_ar = ar[:1] + [ 'FORMULA' ] + ar[1:] 
            gpath = graph_path + '_'.join(new_ar)
            G = Graph.Read_GraphML(gpath)
            all_formula_vs = G.vs.select(lambda v:v["hashkeycol"] in vs) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            if 'RAW' in '_'.join(ar[1:]):
               sfname = 'RAW' 
            elif 'REP' in '_'.join(ar[1:]):
               sfname = 'REP' 
            elif 'RES' in '_'.join(ar[1:]):
               sfname = 'RES' 
            if sfname not in results_f:
               results_f[sfname] = {}
            for sh_v in all_formula_vs:
                #print ' -- ', sh_v  
                fid = sh_v['fid']
                rid = sh_v['rid']
                fname = sh_v['filenames']
                k = fname.split('_')[0] 
                if k+'_'+fid+'_'+rid not in results_f[sfname]:
                   results_f[sfname][k+'_'+fid+'_'+rid] = []
                results_f[sfname][k+'_'+fid+'_'+rid].append(sh_v['ID']+'#'+ sh_v['colid']+'@'+fname)
            del G



        results2_f = {}
        for fname, vs in fname_dict2.items():
            ar = fname.split('_') 
            new_ar = ar[:1] + [ 'FORMULA' ] + ar[1:]
            gpath = graph_path + '_'.join(new_ar)
            G = Graph.Read_GraphML(gpath)
            all_formula_vs = G.vs.select(lambda v:v["hashkeycol"] in vs) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            if 'RAW' in '_'.join(ar[1:]):
               sfname = 'RAW' 
            elif 'REP' in '_'.join(ar[1:]):
               sfname = 'REP' 
            elif 'RES' in '_'.join(ar[1:]):
               sfname = 'RES'
            if sfname not in results2_f:
               results2_f[sfname] = {}
            for sh_v in all_formula_vs:
                fid = sh_v['fid']
                rid = sh_v['rid']
                fname = sh_v['filenames']
                k = fname.split('_')[0] 
                if k+'_'+fid+'_'+rid not in results2_f[sfname]:
                   results2_f[sfname][k+'_'+fid+'_'+rid] = []
                results2_f[sfname][k+'_'+fid+'_'+rid].append(sh_v['ID']+'#'+ sh_v['colid']+'@'+fname)
            del G


        #print ' results_f: ', results_f
        #print ' results2_f: ', results2_f 

        all_results = []
        for sfname , fids1_dict in results_f.items():
            fids2_dict = results2_f[sfname]
            fname = graph_path + sfname+'ADJF.graphml' 

            shortest_paths = {}
            G = Graph.Read_GraphML(fname)

            #print fname
            fids1 = fids1_dict.keys()
            fids2 = fids2_dict.keys()
            filename_map_ddict = {}            

            all_temp_short = []
            all_temp_short_n = []
            for s in fids1:
                for d in fids2:
                    res = G.get_shortest_paths(s, to=d, output='vpath')
                    #print s, d, res 
                    for n in res: 
                           if not n: continue
                           temp_short = eval("{}".format(G.vs[n]['name']))
                           all_temp_short.append(temp_short[:])
                           all_temp_short_n.append(n)

            if all_temp_short:
                       del_inds = []
                       for i1, elm1 in enumerate(all_temp_short):
                           s1 = set(elm1)
                           for i2, elm2 in enumerate(all_temp_short):
                               s2 = set(elm2) 
                               if (i1 == i2): continue
                               if (s1 != s2) and (s1.issubset(s2)):
                                  del_inds.append(i2)  

                       #print del_inds
                       #sys.exit()  

                       for temp_ind, temp_short in enumerate(all_temp_short):

                           #print 'temp_short: ', temp_short  
                           if temp_ind in del_inds: continue
                           n = all_temp_short_n[temp_ind]
                           
                           s = temp_short[0]
                           d = temp_short[-1]    

                           fname_dict1 = {}
                           for elm in fids1_dict[s]:
                               if elm.split('@')[1] not in fname_dict1:
                                  fname_dict1[elm.split('@')[1]] = []   
                               fname_dict1[elm.split('@')[1]].append(elm.split('@')[0])
                            

                           fname_dict2 = {}
                           for elm in fids2_dict[d]:
                               if elm.split('@')[1] not in fname_dict2:
                                  fname_dict2[elm.split('@')[1]] = []   
                               fname_dict2[elm.split('@')[1]].append(elm.split('@')[0])
                            

                           highlight1 = []
                           for fname, hcols in fname_dict1.items():
                               Gdata = Graph.Read_GraphML(graph_path + fname)
                               ms = Gdata.vs.select(lambda v:v["hashkeycol"] in hcols)
                               for m in ms:
                                   highlight1.append((m["table_types"], m["grp_id"], m["row_taxo_id"], m["col"]))
                               del Gdata   

                           highlight2 = []
                           for fname, hcols in fname_dict2.items():
                               Gdata = Graph.Read_GraphML(graph_path + fname)
                               ms = Gdata.vs.select(lambda v:v["hashkeycol"] in hcols)
                               for m in ms:
                                   highlight2.append((m["table_types"], m["grp_id"], m["row_taxo_id"], m["col"]))
                               del Gdata   
                           
                           fname_dict = {} 
                           for i in range(1, len(n)):
                               int_str =  G.es[G.get_eid(n[i-1], n[i])]['intersection']
                               for elm in int_str.split(':@:'):
                                   filename, abs_key, line_key, colid, hashkey = elm.split('#')
                                   if filename not in filename_map_ddict:
                                      filename_map_ddict[filename] = {}

                                   if (s, d) not in filename_map_ddict[filename]:
                                      filename_map_ddict[filename][(s, d)] = {}
                                   if (temp_short[i-1], temp_short[i]) not in filename_map_ddict[filename][(s, d)]: 
                                      filename_map_ddict[filename][(s, d)][(temp_short[i-1], temp_short[i])] = []
                                   filename_map_ddict[filename][(s, d)][(temp_short[i-1], temp_short[i])].append(hashkey+'#'+colid)

                                   if filename not in fname_dict:
                                      fname_dict[filename] = {} 
                                   fname_dict[filename][(temp_short[i-1], temp_short[i])] =  hashkey+'#'+colid 
                            
                           shortest_paths[(s, d)] =  [ temp_short, highlight1, highlight2, copy.deepcopy(fname_dict) ]
            del G                  

            for (s, d), mresults in shortest_paths.items():
                results = [ mresults[0], mresults[1], mresults[2] ]  
                ref_dict = {}
                for fname, vdict in mresults[3].items():
                    Gdata = Graph.Read_GraphML(graph_path + fname)
                    for (n1, n2), hashkeycols in vdict.items():
                        ref_dict[(n1, n2)] = []
                        ms = Gdata.vs.select(lambda v:v["hashkeycol"] in hashkeycols)
                        for m in ms:
                            ref_dict[(n1, n2)].append((m["table_types"], m["grp_id"], m["row_taxo_id"], m["col"]))
                    del Gdata  
                results.append(copy.deepcopy(ref_dict))
                all_results.append(results[:])         
        return all_results 
        
    def create_search_final_results_between(self, ijson):
        company_id = ijson['company_id']        
        doc_id     = ijson['doc_id']
        search_data = ijson['data']
        search_res1, search_res2 = search_data
        
        tab_lets_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(tab_lets_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
         
        grd_cell_grp_map, groupid_wise_data = self.read_rawdb_info(conn, cur)

        res_data = self.search_final_results_between(search_res1, search_res2, company_id, doc_id)
        import modules.tablets.tablets as mtt
        m_Obj  = mtt.Tablets()
        data = m_Obj.get_search_traverse_path_result(company_id, doc_id, res_data) 
        return data 
        
    def search_final_results(self, search_res, company_id, doc_id):
        #graph_path = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/tablets_"+str(company_id)+"/"+str(doc_id)+"/" 
        graph_path = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/"+str(company_id)+"/"+str(doc_id)+"/" 
        fname_dict = {}
        for search_elm in search_res:
            search_elm_sp = search_elm.split('#')
            hkeycol = search_elm_sp[0]+'#'+search_elm_sp[1]
            fname = search_elm_sp[2]
            if fname not in fname_dict:
               fname_dict[fname] = []   
            fname_dict[fname].append(hkeycol)

        results = []
        for fname, vs in fname_dict.items():
            gpath = graph_path + fname
            print 'gpath', gpath
            G = Graph.Read_GraphML(gpath)
            all_formula_vs = G.vs.select(lambda v:v["hashkeycol"] in vs) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            for v in all_formula_vs:
                results.append((v['table_types'], v['grp_id'], v['row_taxo_id'], v['col'], v['hashkey'], fname))
            del G
        return results      
        
    def get_cell_info(self, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv):
        gvtext   = ' '.join(eval(gvtext))
        hghtext  = ' '.join(eval(hghtext)) 
        cellph   = ' '.join(eval(cellph))
        cellcsvc = ' '.join(eval(cellcsvc))
        cellcsvs = ' '.join(eval(cellcsvs))
        cellcsvv = ' '.join(eval(cellcsvv))
        return gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv
        
    def read_rawdb_info(self, conn, cur):
        read_qry = """ SELECT gridids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv FROM rawdb; """
        cur.execute(read_qry)   
        t_data = cur.fetchall()
            
        grd_cell_grp_map = {} 
        groupid_wise_data = {}
        for row_data in t_data:
            grid_ids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv = row_data 
            gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv = self.get_cell_info(gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv)
            grid_ids = '_'.join(grid_ids.split('#')[:-1])
            grd_cell_grp_map[(grid_ids, celltype)] = groupid            
            groupid_wise_data.setdefault(groupid, {})[row_col_groupid] = (grid_ids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv)
        return grd_cell_grp_map, groupid_wise_data
    
    def create_search_infos_table_lets(self, ijson):
        company_id = ijson['company_id']        
        doc_id     = ijson['doc_id']
        search_data = ijson['data']
        
        tab_lets_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(tab_lets_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
         
        grd_cell_grp_map, groupid_wise_data = self.read_rawdb_info(conn, cur)

        res_data = self.search_final_results(search_data, company_id, doc_id)
        
        group_inf = {}
        for row_tup in res_data:
            grid_id, grp_id, row, col, hashkey, fname = row_tup
            cell_str = '{0}_{1}'.format(row, col)
            grd_cell_tup = (grid_id, cell_str)
            #print grd_cell_tup
            grp_id = grd_cell_grp_map.get(grd_cell_tup, '')
            if not grp_id:continue
            group_inf.setdefault(grp_id, {}).setdefault(grid_id, set()).add(cell_str)
        
        group_rc_dct = OD() 
        group_r_hgh_dct = {}
        group_col_dct = OD()
        highlight_col = OD()
        for grp, gr_dct in group_inf.iteritems():
            rcg_dct = groupid_wise_data[grp]
            for rcg, gv_data in rcg_dct.iteritems():
                grid_ids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv = gv_data
                period_type, period = cellph[:-4], cellph[-4:]
                gcr_dct = {'v':gvtext, 'title':{'pt':period_type, 'p':period, 'c':cellcsvc, 's':cellcsvs, 'vt':cellcsvv}, 'ref_k':row_col_groupid}
                cm_set = gr_dct.get(grid_ids, set())
                if celltype in cm_set:
                    gcr_dct['cls'] = 'Matched' 
                group_rc_dct.setdefault(groupid, OD()).setdefault(row, OD())[col] = gcr_dct 
                group_r_hgh_dct.setdefault(groupid, {}).setdefault(row, {}).setdefault('txt_lst', []).append((hghtext, row_col_groupid))
                group_col_dct.setdefault(groupid, OD()).setdefault(col, []).append(cellph)
                highlight_col.setdefault(groupid, OD()).setdefault(col, []).append(row_col_groupid)

        data_lst = []
        inf_map  = {}
        sn = 1
        cl_nw_key_dt =  {}
        for grp_sn, (grp_id, rc_dct) in enumerate(group_rc_dct.iteritems(), 1):
            ck_dct = group_col_dct[grp_id]
            rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':'Group-%s (%s)'%(grp_sn, grp_id), 'cls':'grid-header'}, '$$treeLevel':0}
            group_sn = copy.deepcopy(sn) 
            group_gv_hlgt_dct = {'GV':{}, 'HGH':{}}
            col_map = {}
            col_key_idx = 1
            for clo, ph_lst in ck_dct.iteritems():
                c_key = 'COL-%s'%(col_key_idx)
                col_map[clo] = c_key
                cl_nw_key_dt[c_key] = 0
                col_key_idx += 1
                sorted_ph = []
                try:
                    sorted_ph = rys.year_sort(set(ph_lst)) 
                    sorted_ph.reverse()
                except:pass
                cph_str = '~'.join(sorted_ph)
                rw_dct[c_key] = {'v':cph_str, 'cls':'grid-header'}
                gv_rf_ky_lst = highlight_col[grp_id][clo]
                gv_ref_str = '~~'.join(gv_rf_ky_lst)
                gv_ref_str_ctype = '-'.join((gv_ref_str, 'GV'))
                inf_map['%s_%s'%(sn, c_key)]  = {'ref_k':gv_ref_str_ctype}
                group_gv_hlgt_dct['GV'][gv_ref_str] = 1

            data_lst.append(rw_dct)    
            sn += 1
            for rw, clmn_dct in rc_dct.iteritems(): 
                rcw_dct = {'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':1}
                for cl, dt_inf in clmn_dct.iteritems():  
                    cl_ky = col_map[cl]
                    ref_k_info = dt_inf['ref_k']
                    del dt_inf['ref_k']
                    rcw_dct[cl_ky] = dt_inf
                    inf_map['%s_%s'%(sn, cl_ky)] = {'ref_k':'%s-%s'%(ref_k_info, 'GV')}
                    cl_nw_key_dt[cl_ky] = 1 

                hgh_t_lst = group_r_hgh_dct[grp_id][rw]['txt_lst']
                desc_tup    = hgh_t_lst[0]
                desc, rcg = desc_tup
                group_gv_hlgt_dct['HGH'][rcg] = 1
                rcw_dct['desc'] = {'v':desc}
                inf_map['%s_%s'%(sn, 'desc')] = {'ref_k':'%s-%s'%(rcg, 'HGH')}
                data_lst.append(rcw_dct)
                sn += 1

            res_group_hghlt_dct = {}
            for gh_key, vl_dct in group_gv_hlgt_dct.iteritems():
                mrg = '-'.join(('~~'.join(vl_dct), gh_key))
                res_group_hghlt_dct[mrg] = 1
            res_hglt_mrg = '##'.join(res_group_hghlt_dct)
            inf_map['%s_%s'%(group_sn, 'desc')] = {'ref_k':res_hglt_mrg}
            empty_rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':0}
            data_lst.append(empty_rw_dct)
            sn += 1
        col_def = [{'k':'checkbox', 'n':'', 'v_opt':3}, {'k':'desc', 'n':'Description', 'w':265, 'v_opt':1, 'col_type':'HGH'}]
        c_def = []
        for cs, nm in cl_nw_key_dt.iteritems():
            c_alpha_int = int(cs.split('-')[1]) - 1
            csa = self.find_alp(c_alpha_int) 
            dt_dct = {'k':cs, 'n':csa, 'col_type':'GV', 'w':80}
            c_def.append(dt_dct)
        c_def.sort(key=lambda x:int(x['k'].split('-')[1]))
        col_def.extend(c_def)
        #ref_path_data = self.ref_path_info_workspace('34', deal_id)
        inf_map['ref_path'] = {}
        return [{'message':'done', 'data':data_lst, 'col_def':col_def, 'map':inf_map}]

    def test_create_search_infos_table_lets(self, ijson):
        company_id = ijson['company_id']        
        doc_id     = ijson['doc_id']
        search_data = ijson['data']
        
        tab_lets_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(tab_lets_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
         
        grd_cell_grp_map, groupid_wise_data = self.read_rawdb_info(conn, cur)

        res_data = self.search_final_results(search_data, company_id, doc_id)
        
        group_inf = {}
        for row_tup in res_data:
            grid_id, grp_id, row, col, hashkey, fname = row_tup
            cell_str = '{0}_{1}'.format(row, col)
            grd_cell_tup = (grid_id, cell_str)
            #print grd_cell_tup
            grp_id = grd_cell_grp_map.get(grd_cell_tup, '')
            if not grp_id:continue
            group_inf.setdefault(grp_id, {}).setdefault(grid_id, set()).add(cell_str)
        
        group_rc_dct = OD() 
        for grp, gr_dct in group_inf.iteritems():
            rcg_dct = groupid_wise_data[grp]
            for rcg, gv_data in rcg_dct.iteritems():
                grid_ids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv = gv_data
                period_type, period = cellph[:-4], cellph[-4:]
                cm_set = gr_dct.get(grid_ids, set())
                matched_flg = 0
                if celltype in cm_set:
                    matched_flg = 1
                group_rc_dct.setdefault(groupid, OD()).setdefault(grid_ids, {})[celltype] = matched_flg 
        return group_rc_dct 

    def read_basic_search_info(self, ijson):
        company_id = ijson['company_id']        
        doc_id     = ijson['doc_id']
        search_data = ijson['data']
        
        res_data = self.search_final_results(search_data, company_id, doc_id)
        
        group_inf = {}
        for row_tup in res_data:
            grid_id, grp_id, row, col, hashkey, fname = row_tup
            cell_str = '{0}_{1}'.format(row, col)
            group_inf.setdefault(grid_id, set()).add(cell_str)
        return group_inf 

    def read_rawdb_info_cell_types(self, conn, cur):
        read_qry = """ SELECT gridids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv FROM rawdb; """
        cur.execute(read_qry)   
        t_data = cur.fetchall()
            
        grd_cell_grp_map = {} 
        groupid_wise_data = {}
        for row_data in t_data:
            grid_ids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv = row_data 
            gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv = self.get_cell_info(gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv)
            grid_ids = '_'.join(grid_ids.split('#')[:-1])
            grd_cell_grp_map[(grid_ids, celltype)] = groupid            
            groupid_wise_data.setdefault(groupid, {})[row_col_groupid] = (grid_ids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv)
        return grd_cell_grp_map, groupid_wise_data

    def read_basic_search_info_with_all_celltypes(self, ijson):
        company_id = ijson['company_id']        
        doc_id     = ijson['doc_id']
        search_data = ijson['data']
        
        tab_lets_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(tab_lets_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
        grd_cell_grp_map, groupid_wise_data = self.read_rawdb_info_cell_types(conn, cur)

        res_data = self.search_final_results(search_data, company_id, doc_id)
        
        group_inf = {}
        for row_tup in res_data:
            grid_id, gp_id, row, col, hashkey, fname = row_tup
            cell_str = '{0}_{1}'.format(row, col)
            grd_cell_tup = (grid_id, cell_str)
            grp_id = grd_cell_grp_map.get(grd_cell_tup, '')
            if not grp_id:continue
            group_inf.setdefault(grp_id, {}).setdefault(grid_id, set()).add(cell_str)
        
        group_rc_dct = OD() 
        for grp, gr_dct in group_inf.iteritems():
            rcg_dct = groupid_wise_data[grp]
            for rcg, gv_data in rcg_dct.iteritems():
                grid_ids, celltype, groupid, row_col_groupid, row, col, gvtext, hghtext, cellph, cellcsvc, cellcsvs, cellcsvv = gv_data
                period_type, period = cellph[:-4], cellph[-4:]
                cm_set = gr_dct.get(grid_ids, set())
                matched_flg = 0
                if celltype in cm_set:
                    matched_flg = 1
                group_rc_dct.setdefault(grid_ids, {})[celltype] = matched_flg 
        return group_rc_dct 



if __name__=="__main__":
 #{"cmd_id":54,"company_id":1117,"doc_id":5131,"data":[["6401648d79511309293eb493b8c12eea#1#1_RAW.graphml#HGH","6401648d79511309293eb493b8c12eea#2#1_RAW.graphml#HGH"],["c8b2c602d0b758beddedabb2e0c5194f#1#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#2#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#3#2_RAW.graphml#HGH"]],"user":"demo"}
  obj = Search_Information() 
  ar1 = ["6401648d79511309293eb493b8c12eea#1#1_RAW.graphml#HGH","6401648d79511309293eb493b8c12eea#2#1_RAW.graphml#HGH"]
  ar2 = ["c8b2c602d0b758beddedabb2e0c5194f#1#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#2#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#3#2_RAW.graphml#HGH"]
  # {"cmd_id":54,"company_id":1117,"doc_id":5131,"data":[["2019aa28d41ba9e931634f7b4e494e29#1#2_RAW.graphml#HGH","2019aa28d41ba9e931634f7b4e494e29#2#2_RAW.graphml#HGH","5c7f00459a7f24433e0fda8aafcf669d#1#2_RAW.graphml#HGH","5c7f00459a7f24433e0fda8aafcf669d#2#2_RAW.graphml#HGH"],["4774039f300bffa37b4c5b05758b9227#1#3_RAW.graphml#HGH","4774039f300bffa37b4c5b05758b9227#2#3_RAW.graphml#HGH","4774039f300bffa37b4c5b05758b9227#3#3_RAW.graphml#HGH","cb5a0096e744a017d0f59ac975f22ba7#1#3_RAW.graphml#HGH","cb5a0096e744a017d0f59ac975f22ba7#2#3_RAW.graphml#HGH"]],"user":"demo"}
  ar1 = ["2019aa28d41ba9e931634f7b4e494e29#1#2_RAW.graphml#HGH","2019aa28d41ba9e931634f7b4e494e29#2#2_RAW.graphml#HGH","5c7f00459a7f24433e0fda8aafcf669d#1#2_RAW.graphml#HGH","5c7f00459a7f24433e0fda8aafcf669d#2#2_RAW.graphml#HGH"]
  ar2 = ["4774039f300bffa37b4c5b05758b9227#1#3_RAW.graphml#HGH","4774039f300bffa37b4c5b05758b9227#2#3_RAW.graphml#HGH","4774039f300bffa37b4c5b05758b9227#3#3_RAW.graphml#HGH","cb5a0096e744a017d0f59ac975f22ba7#1#3_RAW.graphml#HGH","cb5a0096e744a017d0f59ac975f22ba7#2#3_RAW.graphml#HGH"]

  ar1 = ["66e67b0e0b464b2fb7145b36701e5111#1#95_RAW.graphml#HGH","66e67b0e0b464b2fb7145b36701e5111#2#95_RAW.graphml#HGH","4e555f325d7a15d195c050b66fbc6cdf#1#95_RAW.graphml#HGH","4e555f325d7a15d195c050b66fbc6cdf#2#95_RAW.graphml#HGH"]
  ar2 = ["f7df1ef079d72a8e4abac6a7020135e9#1#136_RAW.graphml#HGH","f7df1ef079d72a8e4abac6a7020135e9#2#136_RAW.graphml#HGH","f7df1ef079d72a8e4abac6a7020135e9#3#136_RAW.graphml#HGH","f7df1ef079d72a8e4abac6a7020135e9#4#136_RAW.graphml#HGH","f7df1ef079d72a8e4abac6a7020135e9#5#136_RAW.graphml#HGH","f7df1ef079d72a8e4abac6a7020135e9#6#136_RAW.graphml#HGH","6dd2d3de82004569a9cffa884381bd3b#1#136_RAW.graphml#HGH","6dd2d3de82004569a9cffa884381bd3b#2#136_RAW.graphml#HGH","6dd2d3de82004569a9cffa884381bd3b#5#136_RAW.graphml#HGH","6dd2d3de82004569a9cffa884381bd3b#6#136_RAW.graphml#HGH","0ccdd9c11b475a1c0e30527c01679674#3#136_RAW.graphml#HGH","0ccdd9c11b475a1c0e30527c01679674#4#136_RAW.graphml#HGH","81eaa5c161653f9f933fccc2f4e3b1fd#1#104_RAW.graphml#HGH","81eaa5c161653f9f933fccc2f4e3b1fd#2#104_RAW.graphml#HGH","81eaa5c161653f9f933fccc2f4e3b1fd#3#104_RAW.graphml#HGH","81eaa5c161653f9f933fccc2f4e3b1fd#4#104_RAW.graphml#HGH","81eaa5c161653f9f933fccc2f4e3b1fd#5#104_RAW.graphml#HGH","81eaa5c161653f9f933fccc2f4e3b1fd#6#104_RAW.graphml#HGH","603e27903b3e2874811b3cd0673760df#1#104_RAW.graphml#HGH","603e27903b3e2874811b3cd0673760df#2#104_RAW.graphml#HGH","603e27903b3e2874811b3cd0673760df#5#104_RAW.graphml#HGH","603e27903b3e2874811b3cd0673760df#6#104_RAW.graphml#HGH","59e64c3e24306ca51de1600d8c38ab96#1#95_RAW.graphml#HGH","59e64c3e24306ca51de1600d8c38ab96#2#95_RAW.graphml#HGH","ea55c3e1ac72c59d5037a8bf253eafad#1#95_RAW.graphml#HGH","ea55c3e1ac72c59d5037a8bf253eafad#2#95_RAW.graphml#HGH"] 
 
  res = obj.search_final_results_between(ar1, ar2, '1117', '5131')
  for i, r in enumerate(res):
      print i, ' == ', r
      sys.exit()

   
