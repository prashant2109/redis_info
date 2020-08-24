import sets
import os, re
import getPercText as getPercText
import copy
import shelve
#import read_shelve_match
#import configuration_setting
import date_pat_match as date_pat_match
import generate_semantic_pattern as generate_semantic_pattern
#import RateNp
#import ServiceMatchingNew_SP
#import PropMatchRules

#obj_ratenp = RateNp.RateNp()
obj_ext=getPercText.getPercText()
#conf_obj = configuration_setting.configuration_setting()

#obj_prop_rules = PropMatchRules.PropMatchRules()

obj_gen_struct = generate_semantic_pattern.generate_semantic_pattern()

#import find_Equity_group_structure
#obj_utilities = find_Equity_group_structure.find_equity_group()

norm_date = date_pat_match.date_pattern_match()

#isin_obj = ServiceMatchingNew_SP.UploadFile(1)

class UploadFile:
   def __init__(self):
       self.select_entity_on_priority = {}
       self.mega_match_dict = {}
       self.match_dict_code = {} 
       self.entity_dict = {}
       self.file_row_mapper = {}
       self.filter_dict = {}
       self.asset_class_rule_dict_cell_wise_add = {}
       self.asset_class_rule_dict_cell_wise = {} 

       self.freezed_entities = []
       self.dis_amb_rule_list = {}
       self.dis_key_dict = {}
       self.create_new_entity = {}
       self.positive_rules = {}


        

       
   def nouse1(self,load_flg):
       #self.sedol_isin_dict = {}
       #f1 = open('Sedol_ISINs.txt')
       #lines = f1.readlines()
       #f1.close()
       #for lin in lines[1:]:
       #    lin_sp = lin.strip('\r').strip('\n').split('\t')
       #    self.sedol_isin_dict[lin_sp[2].strip()] = lin_sp[3].strip()   
           #self.sedol_isin_dict[(lin_sp[1].strip(), lin_sp[2].strip(), lin_sp[3].strip(),)] = 1

       #for k,vs in self.sedol_isin_dict.items():
       #    print 'Sedol_ISINs\t'+'\t'.join(list(k))
       #sys.exit() 

       f1 = open('select_entity_on_priority.txt')
       lines = f1.readlines()
       self.select_entity_on_priority = {}
       for line in lines:
           line = line.strip('\n')
           line_sp = line.split('\t')
           self.select_entity_on_priority[line_sp[0].strip()] = {} 
           self.select_entity_on_priority[line_sp[0].strip()][line_sp[1].strip()] = line_sp[2].split('#')


       r_obj = read_shelve_match.UploadFile(0)

       self.mega_match_dict = r_obj.mega_match_dict
       self.match_dict_code = r_obj.match_dict_code  

       #print self.match_dict_code[1].keys() 
       #sys.exit()  

       #print self.mega_match_dict['1_1']['1']
       #sys.exit()


       self.entity_dict = {}
       self.file_row_mapper = {} 
       self.filter_dict = {}
       f1 = open('config_for_input.txt')
       lines = f1.readlines()
       f1.close()

       for line in lines:
          line_sp = line.strip('\n').split('\t')
          self.filter_dict[line_sp[0].strip()] = {}
          self.filter_dict[line_sp[0].strip()]['ToMatch'] = line_sp[1].split('#')[:]
          self.filter_dict[line_sp[0].strip()]['ToShow'] = line_sp[2].split('#')[:]
          self.filter_dict[line_sp[0].strip()]['MatchCond'] = line_sp[3].split('#')[:]

       if load_flg == 1:  
         #self.LoadInput('Asset_Type_Synonym/', self.match_dict, self.file_row_mapper, self.filter_dict)
         #self.LoadInput('APT_SUPP_DATA/', self.match_dict,  self.file_row_mapper, self.filter_dict)
         #self.LoadInput('prop_txt_files/', self.match_dict, self.file_row_mapper, self.filter_dict)
         #fr=shelve.open('file_row_mapper.sh','n')    
         #fr['file_row_mapper']=self.file_row_mapper
         #fr.close()
         #fm=shelve.open('match_dict.sh','n')    
         #fm['match_dict'] = self.match_dict
         #fm.close()        
         pass 
       else:
         ## Load From Shelve 
         #fr=shelve.open('file_row_mapper.sh','r')    
         #self.file_row_mapper=copy.deepcopy(fr['file_row_mapper'])
         #fr.close()
         #fm=shelve.open('match_dict','r')    
         #self.match_dict=copy.deepcopy(fm['match_dict'])
         #fm.close()        
         pass

       #print self.file_row_mapper['bloombergtickerinfo']
       #self.LoadLORules("EntityIdentify.txt", self.entity_dict)
       self.LoadLORules("EntityIdentify1.txt", self.entity_dict)
       #self.asset_class_rule_dict = self.ReadAssetClassRules('assetclass_rules.txt')

       self.asset_class_rule_dict_cell_wise_add = self.ReadAssetClassRules('assets_rules2.txt')

       #self.asset_class_rule_dict['bond'] = ['Bloomberg_Corp_Bond:BloombergTicker|Bloomberg_Equity:Ticker:*:Prop:HoldingsEqu:*:Rate:nP:*:Tasdate7:Tasdate7']  

       #d = shelve.open('rules_asset/d1.sh')
       #m_dict = d['rule']
       #d.close()
       self.asset_class_rule_dict_cell_wise = {}
       #d = shelve.open('rules_asset/d1.sh')
       #self.asset_class_rule_dict_cell_wise = d['rule']
       #d.close()


       #for k,vs in self.asset_class_rule_dict_cell_wise.items():
       #    print k,vs 
       #sys.exit()  



       #self.asset_class_rule_dict_cell_wise = {}
       #self.asset_class_rule_dict_cell_wise[('Cc:Structure','bloombergtickerinfo:BloombergSymbol','Prop:EmgEqu''Prop:MktEqu','Prop:RatePropFxEqu|Synonym:FundEqu', 'rating:APTCode|Bloomberg_Equity:Ticker|EmergingMarketCompanies:BloombergTicker|TRStockExchange:TR Symbol|bloombergtickerinfo:BloombergSymbol', 'Org_end_markers1:Org_end_markers1','g:Structure','Bloomberg_Equity:Ticker')] = ['bond_funds'] 
       #my_tup = ['Cc:Structure','bloombergtickerinfo:BloombergSymbol','Prop:EmgEqu''Prop:MktEqu','Prop:RatePropFxEqu|Synonym:FundEqu', 'rating:APTCode|Bloomberg_Equity:Ticker|EmergingMarketCompanies:BloombergTicker|TRStockExchange:TR Symbol|bloombergtickerinfo:BloombergSymbol', 'Org_end_markers1:Org_end_markers1','g:Structure','Bloomberg_Equity:Ticker']
       #my_tup.sort()
       #self.asset_class_rule_dict_cell_wise[tuple(my_tup)] = ['bond_funds']  
       #print self.asset_class_rule_dict_cell_wise

       #for k,vs in self.asset_class_rule_dict_cell_wise.items():
       #    print '\t'.join(list(k))+'\t'+str(vs[:])
       #sys.exit()
       #self.asset_class_rule_dict = {}
       #for k,vs in m_dict.items():
       #    r  = list(k)
       #    px = self.asset_class_rule_dict.get(vs[0], [])
       #    if not px:
       #       self.asset_class_rule_dict[vs[0]] = []
       #    self.asset_class_rule_dict[vs[0]].append(r[:])
       # 
       #self.asset_class_rule_dict = {}
       #self.asset_class_rule_dict['bond'] = [['Prop:HoldingsEqu', 'Rate:nP','Tasdate7:Tasdate7']]  
       #self.asset_class_rule_dict['bond'] = [['Prop:HoldingsEqu|Bloomberg_Equity:Ticker', 'Rate:nP','Tasdate7:Tasdate7']]  
       #print self.asset_class_rule_dict.keys()
       #print self.asset_class_rule_dict['equity']
       #sys.exit()   

       self.freezed_entities = []
       f = open('freezed_entities.txt')
       lines = f.readlines()
       f.close()
       for line in lines:
          self.freezed_entities.append(line.strip())

       self.dis_amb_rule_list = {}
       self.dis_key_dict = self.ReadDisambiguate_Rules("Disambiguate_Rules.txt", self.dis_amb_rule_list)

       #print self.dis_amb_rule_list
       #sys.exit()

       self.create_new_entity = {}
       #self.ReadDisambiguate_Rules("Create_New_Entity_On.txt", self.create_new_entity)

       self.positive_rules = {}
       #f1 = open('PositiveRules.txt')
       #lines = f1.readlines()
 
       #for line in lines:
       #    line_sp = line.split('\t')
       #    m = map(lambda x:x.strip(), line_sp[1:])
       #    self.positive_rules[line_sp[0].strip()] = m[:]


   def ReadDisambiguate_Rules(self, file_name, data_dict):
       f = open(file_name)
       lines = f.readlines()
       f.close()
       ind = 0
       my_key_dict = {}
       for line in lines:
          m = line.strip('\n').strip().split('\t')
          from_l = []
          to_l = []
          flg = 0
          for m1 in m:
              if m1.strip()==":^:":
                 flg = 1
                 continue
              else:
                 if flg == 0:
                    from_l.append(m1.strip())
                    if not my_key_dict.get(m1.strip(), {}):
                       my_key_dict[m1.strip()] = {}
                    my_key_dict[m1.strip()][ind] = 1
                 else: 
                    to_l.append(m1.strip())
                   
          data_dict[ind] = {}
          data_dict[ind]["FROM"] = from_l[:]
          data_dict[ind]["TO"] = to_l[:]
          ind += 1
       return my_key_dict

   def CheckForOR(self, all_pos, or_list):
       for o in or_list:
         for pos_str in all_pos.split(':*:'):
            for pos_elm in pos_str.split('|'):
               if o == pos_elm:
                  return 1
       return 0

   def CheckForAND(self, all_pos, and_list):
       my_ind = 0 
       for o_r in and_list:
         true_flg = 0 
         my_all_pos_sp = all_pos.split(':*:')
           
         for i in range(my_ind, len(my_all_pos_sp)):
            pos_str = my_all_pos_sp[i]
            o_sp = sorted(o_r.split('|'))
            a_sp = sorted(pos_str.split('|'))
            #print o_sp, a_sp, my_ind, i
            if o_sp[:] == a_sp[:]:
               if my_ind != 0:
                  if my_ind  != i:
                     true_flg = 0
                     break 
               my_ind = i + 1 
               true_flg = 1
               break
         if true_flg == 0:
            return 0
       return 1


   def CheckForAND_OLD(self, all_pos, and_list):
       my_ind = 0 
       for o_r in and_list:
         true_flg = 0 
         my_all_pos_sp = all_pos.split(":*:")
         prev_ind = -1
         for i in range(my_ind, len(my_all_pos_sp)):
            pos_str = my_all_pos_sp[i]
            o_sp = o_r.split('|')
            m_flg = 0
            for o in o_sp:   
              if o in pos_str.split('|'):
                 if my_ind == 0:
                    my_ind = i + 1
                    prev_ind = i + 1
                        
                 elif prev_ind + 1 == i:
                   prev_ind = my_ind
                   true_flg = 1
                   break

            if true_flg == 1:
               break

         if true_flg == 0:
            return 0
       return 1

   def CheckForRule(self, rule_list, cell_all_pos_str_list): 
            #print cell_all_pos_str_list
            #print rule_list 
            #sys.exit() 
            for cell_all_pos_str in cell_all_pos_str_list: 
              ind = -1
              for all_pos in cell_all_pos_str.split(':^:'): 
                ind += 1
                pass_flg = 0
                if 1:#for rule_elm in rule_list:
                  allow_flg = 0
                  and_list = []
                  or_list = []

                  #if '|' in rule_elm:
                  #   or_list = rule_elm.split('|')
                  #else:
                  #   and_list = rule_elm.split('#')
              
                  #if or_list:
                  #   allow_flg = self.CheckForOR(all_pos, or_list)
                      
                  if rule_list:
                     #allow_flg = self.CheckForAND_OLD(all_pos, rule_list)
                     allow_flg = self.CheckForAND(all_pos, rule_list)
                   
                  if allow_flg==1:
                     pass_flg += 1

                  if allow_flg:#pass_flg == len(rule_list):
                     return 1, ind
            return 0, 0

   def MatchAssetsClassAdd(self, cell_pos_dict):
      assets_class_d = {}
      for rule_tup, ind in cell_pos_dict.items():
          mx = self.asset_class_rule_dict_cell_wise_add.get(rule_tup, [])
          if mx:
             #print 'NN: ', rule_tup
             assets_class_d[mx[0]] = 1
      return assets_class_d.keys()


   def MatchAssetsClass(self, cell_pos_dict):
      assets_class_d = {}
      for rule_tup, ind in cell_pos_dict.items():
          if rule_tup in [('S:Structure', 'n:Structure')]: continue 
          if rule_tup in [('S:Structure', 'n:Structure'), ('C:Structure', 'day_num:day_num|month_num:month_num', 'EmergingMarketCompanies:BloombergTicker|bloombergtickerinfo:BloombergSymbol|Bloomberg_Corp_Bond:BloombergTicker|rating:APTCode|Bloomberg_Equity:Ticker|TRStockExchange:TR Symbol|Bloomberg_Govt_Bond:GTicker'), ('day_num:day_num|month_num:month_num', 'EmergingMarketCompanies:BloombergTicker|bloombergtickerinfo:BloombergSymbol|Bloomberg_Corp_Bond:BloombergTicker|rating:APTCode|Bloomberg_Equity:Ticker|TRStockExchange:TR Symbol|Bloomberg_Govt_Bond:GTicker')]: continue 
          mx = self.asset_class_rule_dict_cell_wise.get(rule_tup, [])
          if mx:
             #print '>>', rule_tup
             assets_class_d[mx[0]] = 1
      return assets_class_d.keys()

   def MatchAssetsClass_OLD(self, cell_all_pos_str_list):
      assets_class_d = {}
      #print self.asset_class_rule_dict
      #sys.exit()
      for en, all_rule_list in self.asset_class_rule_dict.items():
          if not en.strip(): continue
          my_flg = 0
          for rule_list in all_rule_list:
            flg, pos_i = self.CheckForRule(rule_list, cell_all_pos_str_list)
            #print "rule_list: ", rule_list, en
            if flg:
             if not assets_class_d.get(pos_i, []):
                assets_class_d[pos_i] = []
             assets_class_d[pos_i].append(en)
             break
      #sys.exit()  
      return assets_class_d

 
   def MatchAssetsClass_OLD(self, cell_all_pos_str):
       assets_class_d = {}
       ind = -1
       for all_pos in cell_all_pos_str.split(':^:'): 
         ind += 1
         for en, rule_list in self.asset_class_rule_dict.items()[:]:
           #print en
           #sys.exit() 
           pass_flg = 1 
           for rule_elm in rule_list:
              and_list = []
              or_list = []
              if '|' in rule_elm:
                 or_list = rule_elm.split('|')
              else:
                 and_list = rule_elm.split('#')
              
              if or_list:
                 allow_flg = self.CheckForOR(all_pos, or_list)
                      
              elif and_list:
                 allow_flg = self.CheckForAND(all_pos, and_list)
            
              #print 'en: ', en 
              #print 'all_pos: ', all_pos 
              #print 'or', or_list  
              #print 'add', and_list
              #print allow_flg
              if allow_flg==0:
                 pass_flg = 0  
                 break 
           if pass_flg==1:
              if not assets_class_d.get(ind, []):
                 assets_class_d[ind] = []

              assets_class_d[ind].append(en)
     
       return assets_class_d


   def ReadAssetClassRules(self, file_name):
       f1 = open(file_name)
       lines = f1.readlines()
       f1.close()
 
       d =  {}
       for line in lines:
           l_sp = line.strip('\n').strip().split('\t')
           if not d.get(l_sp[0].strip(), []):
              d[l_sp[0].strip()] = []
           tup = tuple(l_sp[1:])
           d[tup] = [l_sp[0].strip()]
       return d  


   def ReadAssetClassRules_OLD(self, file_name):
       f1 = open(file_name)
       lines = f1.readlines()
       f1.close()
 
       d =  {}
       for line in lines:
           l_sp = line.strip('\n').split('\t')
           d[l_sp[0].strip()] = l_sp[1].split(':#:')
       return d  
         


   def LoadLORules(self, file_name, data_dict):
       f1 = open(file_name)
       lines = f1.readlines()
       f1.close()
      
       for i in range(len(lines)):
           line_sp = lines[i].strip('\n').split('\t')
           e_list = []
           f_e_list = []
           flg = 0
           for e in line_sp[1:]:
               if e.strip() and flg==0:
                  e_list.append(e.strip())
               else:
                  flg = 1
                  if e.strip():
                     f_e_list.append(e.strip())
           data_dict[line_sp[0]] = {}
           data_dict[line_sp[0]]['Rule'] = e_list[:]
           data_dict[line_sp[0]]['DIS']  = f_e_list[:]



   def LoadInput(self, file_name, data_dict, row_mapper, filter_dict):

     file_list = os.listdir(file_name)  
     for f_name in file_list:
       f = open(file_name+'/'+f_name)
       lines = f.readlines()
       f.close()

       ent_name = f_name.split('.')[0]
       row_no = 0
      
       file_head_head = {}
       h = lines[0].strip('\n').split('\t')
       for i in range(len(h)):
           file_head_head[i] = h[i].strip() 
    
       for line in lines[1:]:
           ind = 0 
           if not row_mapper.get(ent_name, {}):
              row_mapper[ent_name] = {}

           for line_sp_elm in line.split('\t'):  
              syn1 = line_sp_elm
              if not syn1.strip():
                 ind += 1
                 continue 

              syn1 = syn1.strip().lower()
              len_syn1 = len(syn1.split())

              if not data_dict.get(len_syn1, {}): 
                 data_dict[len_syn1] = {}

              if not data_dict[len_syn1].get(syn1, []): 
                 data_dict[len_syn1][syn1] = []

              if not row_mapper[ent_name].get(row_no, []):
                 row_mapper[ent_name][row_no] = []

              #nmx = filter_dict.get(ent_name, {})
              #if nmx:
              #   if ( file_head_head[ind] not in nmx['ToShow'] ):continue
              row_mapper[ent_name][row_no].append((ent_name, file_head_head[ind], row_no, syn1))

              nmx = filter_dict.get(ent_name, {})
              if nmx:
                 if ( file_head_head[ind] not in nmx['ToMatch'] ):
                    ind += 1 
                    continue

              #print row_mapper[ent_name][row_no]
              #print file_head_head, ind

              data_dict[len_syn1][syn1].append((ent_name, file_head_head[ind], row_no, syn1))

              ind += 1

           row_no += 1

     return

   def __del__(self): 
       pass

   def find_OPT_PATT(self, qry_str):
       #obj_ext=getPercText.getPercText()
       ar = []
       for q in qry_str.split(): 
          type_char, words, index_ar = obj_ext.gettype(q)
          ar.append((type_char, words, index_ar))


       flg = 0
       n_struct = -1
       my_glb_ind = 0
       for a in ar:
           if a[0] == 'g:Structure' :
              my_glb_ind += a[2][-1][1]
              continue
           else:
              if flg ==0  and ''.join(a[1]) == 'otc':
                 flg += 1 
                 my_glb_ind += a[2][-1][1]
                  
              if flg == 2 and a[0] in ['n', 'n.n']:
                 n_struct =  a[2]
                 break     
 
              if flg ==1 and ''.join(a[1]) in ['put', 'call']:
                 flg += 1 

               
       return n_struct, flg
 



   def find_n_n_slash_n(self, qry_str):
       #obj_ext=getPercText.getPercText()
       ar = []
       for q in qry_str.split(): 
          type_char, words, index_ar = obj_ext.gettype(q)
          ar.append((type_char, words, index_ar))
       
       #print ar
       #sys.exit()
       if len(ar)==4:
          if (ar[0][0] == 'n') and (1<len(ar) and ar[1][0] =='n') and (2<len(ar) and ar[2][0]=='S' and ar[2][1][0] == '/') and (3<len(ar) and ar[3][0] =='n'):

              tmp = []
              tmp.append(ar[0][1][0])   
              tmp.append(ar[1][1][0])   
              tmp.append(ar[2][1][0])   
              tmp.append(ar[3][1][0])   
              
              if int(tmp[1])<int(tmp[3]):
                 return 1
       return 0

   def MatchForMoreThen2(self, words):
           len_words = len(words)
           n = len_words
           match_results_dict = {}
           return match_results_dict 
           while n>0:
                 for i in range(0, len_words):
                     if (i+n<=len_words):
                        org_qry_str = (' '.join(words[i:i+n]))
                        qry_str = (' '.join(words[i:i+n])).lower()

                        #print 'qry_str: ', qry_str
                        len_comp = len(qry_str.split())
                        len_comp_d = len(qry_str)
                         
                        if i>0:
                           d = len(' '.join(words[:i])) + 1 
                        else:
                           d = 0

                        #if len(words[i:i+n]) in [5, 3]:   # Check next char Slash is not allowed
                        #   #print qry_str
                        #   n_struct, flg = self.find_OPT_PATT(qry_str)
                        #   #print n_struct, flg
                        #   #sys.exit()
                        #   if n_struct and flg == 2:
                        #      st = n_struct[0][0] 
                        #      en = n_struct[-1][1] 
                        #      match_results_dict[(d+st, d+en)] = [("n", "Structure", -1, qry_str)]
                        #      continue

                        if len(words[i:i+n]) == 4:   # Check next char Slash is not allowed
                           if self.find_n_n_slash_n(qry_str):
                              match_results_dict[(d, d+len_comp_d)] = [("RateStructureMixed", "Structure", -1, qry_str)]
                              continue

                        if len(words[i:i+n]) == 2:   #For Class A4
                           #if obj_utilities.find_equity_group(qry_str): 
                           #   match_results_dict[(d, d+len_comp_d)] = [("EquityGroup", "Structure", -1, qry_str)]
                           if obj_utilities.find_equity_group3(org_qry_str):  
                              match_results_dict[(d, d+len_comp_d)] = [("EquityGroup", "Structure", -1, qry_str)]
                           if obj_utilities.find_series_group(org_qry_str):  
                              match_results_dict[(d, d+len_comp_d)] = [("Series2", "Structure", -1, qry_str)]


                           mx = self.CheckForPattern1(words[i:i+n])
                           if mx: # Pending Indexing for BO Z1
                              match_results = self.match_dict_code.get(len(mx[0].split()), {}).get(mx[0].lower(), [])
                              if match_results:
                                 match_results = self.RemoveDuplicateRows(match_results[:], mx[0]) 
                                 w_len = len_comp_d-len(mx[1])-1
                                 match_results_dict[(d, d+w_len)] = match_results[:]
                                 match_results_dict[(1+d+w_len, d+len_comp_d)] = [("InstrumentTime", "Structure", -1, mx[1])]
                           #continue

                        qry_str = re.sub('"', '""', qry_str)
                        if len(words[i:i+n])>1 and (len(words[i:i+n])!=len(words)):
                           #match_results = self.match_dict.get(len(words[i:i+n]), {}).get(qry_str.lower(), [])

                           c = self.CleanSpecialSymbols(qry_str.lower()) 
                           key_str = str(len(c.split()))+'_'+str(len(c))
                           match_results = self.mega_match_dict.get(key_str, {}).get(c, [])

                           #key_str = str(len(words[i:i+n]))+'_'+str(len(qry_str))
                           #match_results = self.mega_match_dict.get(key_str, {}).get(self.CleanSpecialSymbols(qry_str.lower()), [])

                           
                           match_results = self.RemoveDuplicateRows(match_results[:], qry_str) 
                           if not match_results_dict.get((d, d+len_comp_d), []):
                              match_results_dict[(d, d+len_comp_d)] = []  

                           match_results_dict[(d, d+len_comp_d)] += match_results[:]
                 n = n - 1
           #print match_results_dict
           #sys.exit()
           return match_results_dict



   def MatchContextRules(self, index_list, f_tup_dict1, org_str, norm_date_dict):
       found_entity_dict = {}  
       dis_dict = {}

       #print f_tup_dict1
       for entity, rule_dict in self.entity_dict.items():
           #if entity != 'date19': continue
           seq_list = rule_dict['Rule']
           dis_list = rule_dict['DIS']
          
           #print entity, index_list
           #print seq_list
           #sys.exit() 
           if len(index_list)>=len(seq_list):
              word_keys = index_list[:]
              len_words = len(index_list[:])
              n = len_words
              while n>0:
                 for i in range(0, len_words):
                     if (i+n<=len_words):
                        #print "PPPPPPPPPPPPPPPPPPPPPP: ", word_keys[i:i+n]
                        if (len(word_keys[i:i+n])==len(seq_list)):
                           # Check If the seq list is entities and words are same
                           if self.match_case(f_tup_dict1, word_keys[i:i+n], seq_list, entity, org_str):
                              #print "found", entity 
                              #print word_keys[i:i+n], seq_list
                              #sys.exit() 
                              my_list = word_keys[i:i+n]
                              #print org_str[my_list[0][0]:my_list[-1][1]]
                              if not found_entity_dict.get((my_list[0][0], my_list[-1][1]), {}):
                                 found_entity_dict[(my_list[0][0], my_list[-1][1])] = {}

                              if not found_entity_dict[(my_list[0][0], my_list[-1][1])].get(entity, {}):
                                 found_entity_dict[(my_list[0][0], my_list[-1][1])][entity] = {}
                              
                              #print org_str   
                              normalized_date = norm_date.date_pattern_match(entity, org_str, my_list[:])
                              #print normalized_date
                              #sys.exit()
                              found_entity_dict[(my_list[0][0], my_list[-1][1])][entity] = my_list[:]
                              norm_date_dict[(my_list[0][0], my_list[-1][1])] = normalized_date
                 n = n - 1

       #sys.exit()
       #print f_tup_dict1 
       #print "FOUND:;;;;;;;;;;;;;;;;;;"
       #for k,vs in found_entity_dict.items():
       #   print org_str[k[0]:k[1]], vs
       #sys.exit()

       del_ar = []
       sp_del_ar = []
       flg = 0
       addition_dict = {} 
       myKeys = found_entity_dict.keys()
       myKeys.sort()  
       del_sub_sets = self.RemoveSubSets(myKeys)
       #print myKeys
       #print del_sub_sets
       for e in del_sub_sets:
           try:
             del found_entity_dict[e]
             del norm_date_dict[e]
           except: 
             continue
       #print '>>>>>>>>>', found_entity_dict
       #sys.exit() 
       for n_tup, en_dict in found_entity_dict.items():
           #f_tup_dict1[n_tup] = [("TasDate", "TasDate", -1, org_str[n_tup[0]:n_tup[1]])]
           for en, my_list in en_dict.items(): 
               if not f_tup_dict1.get(n_tup, []):
                  f_tup_dict1[n_tup] = []
               
               nmx = f_tup_dict1[n_tup] + [("Tas"+en, "Tas"+en, -1, org_str[n_tup[0]:n_tup[1]])]

               n_d = {}
               for n in nmx:
                  n_d[n] = 1

               nmx = n_d.keys()[:]

               f_tup_dict1[n_tup] = nmx

               flg = 1
               del_ar += my_list[:]
               if 'date' in en: 
                  sp_del_ar += my_list[:]
                  addition_dict[n_tup] = nmx
                 
       #print addition_dict
       #sys.exit() 
        
       for e in del_ar:
         try: 
           del f_tup_dict1[e] 
         except:
           pass
 
       for k,vs in f_tup_dict1.items():
           flg = 0
           for v in vs:
              if v[0]+':'+v[1] in ['day_num:day_num', 'year_num2:year_num2', 'month_num:month_num']:
                 flg = 1 
                 break
        
           if flg==1:  
              addition_dict[k] = [('n', 'Structure')] 

       return f_tup_dict1, flg, sp_del_ar, addition_dict

   def match_case(self, word_dict, word_keys, seq_list, entity, org_str):
       ind = 0 
       #print "***********************************************************************************"
       #print word_dict 
       #for e in word_keys:
       #    print 'ORG::::::',  org_str[e[0]:e[1]]
       #print seq_list
       delim_prev = ""
       prev_i = ()
       for word_key in word_keys:
           m_results = word_dict[word_key]         
           flg = 0
           #print ':::', m_results, word_key
           for m_e in m_results:
               seq_elm_list = seq_list[ind].split('|')
               #print ">>>>", seq_elm_list
               if (m_e[0]+':'+m_e[1] in seq_elm_list):
                  if prev_i:
                     if (word_key[0]-prev_i[1] == 0) or (word_key[0]-prev_i[1] == 1 and org_str[prev_i[1]:word_key[0]] == " "):
                        pass 
                     else: 
                        flg = 0
                        break

                  if delim_prev:
                     if "Structure" in seq_list[ind]:
                        if org_str[word_key[0]:word_key[1]] != delim_prev:
                          flg = 0
                          break
                  
                  if "Structure" in seq_list[ind]:
                     delim_prev = org_str[word_key[0]:word_key[1]]
                  prev_i = word_key
                  flg = 1
                  break
           if flg == 0:
              return 0  
           ind += 1
       #print "***********************************************************************************"
       #sys.exit() 
       return 1
                     
   def RejectNumbers(self, chrs, match_results, wds):
       if ('n.n' == chrs):
          if (len(wds[2]) > 3):
             return 0
          if (len(wds[2]) < 3) and (len(wds[0]) >= 3) :
             return 0
       if ('mn.n' == chrs):
          if (len(wds[3]) > 3):
             return 0
          if (len(wds[3]) < 3) and (len(wds[1]) >= 3) :
             return 0
       return 1

   def CheckForLocationCurrency(self, chrs, match_results, wds):
       n_e = [] 
       if (len(wds) > 1):
             wd1 = ''.join(wds[0])
             key_str = str(len(wd1.split()))+'_'+str(len(wd1))
             my_results = self.mega_match_dict.get(key_str, {}).get(wd1.lower(), [])
             
             for my_r in my_results:
                 if my_r[0]+":"+my_r[1] in ['location_currency1:std_location' 'location_currency1:nonstd_location']:
                    n_e.append(my_r)
                    break
            
             wd2 = ''.join(wds[1])
             key_str = str(len(wd2.split()))+'_'+str(len(wd2))
             my_results = self.mega_match_dict.get(key_str, {}).get(wd2.lower(), [])
             for my_r in my_results:
                 if my_r[0]+":"+my_r[1] == 'currency_marker1:currency_marker1':
                    n_e.append(my_r)
                    break
       return n_e

   def CheckForOptions(self, chrs, match_results, wds):
       n_e = [] 
       if (len(''.join(wds)) > 3):
             wd1 = ''.join(wds)[0:2]
             key_str = str(len(wd1.split()))+'_'+str(len(wd1))
             my_results = self.mega_match_dict.get(key_str, {}).get(wd1.lower(), [])
             for my_r in my_results:
                 if my_r[0]+":"+my_r[1] in ['Prop:CallEqu', 'Prop:PutEqu']:
                    n_e.append(my_r)
                    break
             if n_e:
                flg = 1
                for w in ''.join(wds)[2:]:
                  if (w in '1234567890' or w == '.'):
                     continue
                  else:
                     flg = 0
                     break
                if flg==1:
                   n_e.append(''.join(wds)[2:])  
                   return n_e
       return []



   def CheckForClubCurrencyAndValue(self, chrs, match_results, wds):
       n_e = [] 
       n_e2 = []
       if (len(''.join(wds)) > 3):
             wd1 = ''.join(wds)[0:3]
             key_str = str(len(wd1.split()))+'_'+str(len(wd1))
             my_results = self.mega_match_dict.get(key_str, {}).get(wd1.lower(), [])
             for my_r in my_results:
                 #if my_r[0]+":"+my_r[1] in ['currency:ISOCurrency']:
                 if my_r[0] in ['Currency_Equ']:
                    n_e.append(wd1)
                    n_e2.append(my_r[1])
                    break

             #print n_e
             #sys.exit()  
            
             if n_e:
                flg = 1
                for w in ''.join(wds)[3:]:
                  if (w in '1234567890' or w == '.'):
                     continue
                  else:
                     flg = 0
                     break
                if flg==1:
                   n_e.append(''.join(wds)[3:])  
                   return n_e, n_e2
       return [], []


   def CheckForDemilInWords(self, chrs, match_results, wds, delim, global_count, my_indexes, cell_value):
       
       #print "__________________________________________________________________________________________" , cell_value
       ind = 0
       all_temp_ar = []
       tmp_ar = []
       for w in wds:
          if w in ["-", "_", "+"]:
             if tmp_ar:  
                all_temp_ar.append(tmp_ar[:])
             tmp_ar = []
             ind += 1
             continue
          else: 
             tmp_ar.append((w, chrs[ind], my_indexes[ind]))
          ind += 1

       if tmp_ar:
          all_temp_ar.append(tmp_ar[:])

       #print wds  
       #print all_temp_ar
       #sys.exit() 

       match_dict = self.MatchAPW(all_temp_ar, cell_value, global_count)
       return match_dict  
             

   def MatchAPW(self, all_temp_ar, cell_value, global_count):
       myKeys = all_temp_ar[:]
       len_words = len(myKeys)
       n = len_words
       match_results_dict = {}
       #print "Mywords: ", Mywords  
       while n>0:
           for i in range(0, len(myKeys)):
               if (i+n<=len_words):

                  qry_str = ""
                  my_tuple_list = myKeys[i:i+n]
                  delim_cnt = -1
                  for my_tuple_list_elm in my_tuple_list:
                    if delim_cnt==-1:
                       delim_cnt = my_tuple_list_elm[0][2][0] 
                    my_str = ""
                    for my_tuple_list_elm_elm in my_tuple_list_elm: 
                      my_str += my_tuple_list_elm_elm[0]
                    qry_str += my_str + " "

                  qry_str = qry_str.strip()
                  if i>0:
                     d = delim_cnt
                  else:
                     d = 0 

                  len_comp_d = len(qry_str)
                  #print (d, d+len_comp_d)
                  #qry_str = ' '.join(Mywords[i:i+n]).lower()
                  len_comp_d = len(qry_str)
                  #print "AA: ", qry_str
                  #print "org str:"+cell_value[global_count+d:global_count+d+len_comp_d]+"=="
                  c = self.CleanSpecialSymbols(qry_str.lower()) 
                  key_str = str(len(c.split()))+'_'+str(len(c))
                  match_results = self.mega_match_dict.get(key_str, {}).get(c, [])
                  match_results = self.RemoveDuplicateRows(match_results[:], qry_str) 
                  #print "match_results: ", match_results                   
                  #match_results_dict[(st, en)] = match_results[:]
                  match_results_dict[(global_count+d, global_count+d+len_comp_d)] = match_results[:]
           n = n - 1
       #sys.exit()  
       return match_results_dict 


   def CheckForMultipleCurr(self, chrs, match_results, wds):
       n_e = [] 
       n_e2 = [] 
       if ('C' == chrs):
          if (len(wds[0]) == 6):
             wd1 = ''.join(wds[0][0:3])
             key_str = str(len(wd1.split()))+'_'+str(len(wd1))
             my_results = self.mega_match_dict.get(key_str, {}).get(wd1.lower(), [])
             
             for my_r in my_results:
                 #if my_r[0]+":"+my_r[1] == 'currency:ISOCurrency':
                 if my_r[0] == 'Currency_Equ':
                    n_e.append(my_r)
                    n_e2.append(my_r[1])
                    break
            
             wd2 = ''.join(wds[0][3:6])
             key_str = str(len(wd2.split()))+'_'+str(len(wd2))
             my_results = self.mega_match_dict.get(key_str, {}).get(wd2.lower(), [])
             for my_r in my_results:
                 #if my_r[0]+":"+my_r[1] == 'currency:ISOCurrency':
                 if my_r[0] == 'Currency_Equ':
                    n_e.append(my_r)
                    n_e2.append(my_r[1])
                    break
       return n_e, n_e2

   def CleanSpecialSymbols(self, sstr):
       sstr = sstr.replace("-", " ")
       sstr = sstr.replace(",", "")
       sstr = sstr.replace(".", "")
       #sstr = sstr.replace("_", " ")
 
       sstr = sstr.strip()

       sstr = " ".join(sstr.split())
       #print ">>>>>>>>>>>>>>>>>"+sstr+"=="
       return sstr  

   def CheckForCurrAndCurrEndMarker(self, chrs, match_results, wds):
       n_e = []
       n_e2 = [] 
       if (len(wds[0]) > 3):
             wd1 = ''.join(wds[0][0:3])
             key_str = str(len(wd1.split()))+'_'+str(len(wd1))
             my_results = self.mega_match_dict.get(key_str, {}).get(wd1.lower(), [])
             
             for my_r in my_results:
                 #if my_r[0]+":"+my_r[1] == 'currency:ISOCurrency':
                 if my_r[0] == 'Currency_Equ':
                    n_e.append(my_r)
                    n_e2.append(wd1)
                    break
           
             if n_e: 
                wd2 = ''.join(wds[0][3:])
                key_str = str(len(wd2.split()))+'_'+str(len(wd2))
                my_results = self.mega_match_dict.get(key_str, {}).get(wd2.lower(), [])
                flg = 0  
                for my_r in my_results:
                   #if my_r[0]+":"+my_r[1] == 'currency_marker1:currency_marker1':
                   if my_r[0] == 'Currency_Minor' or (my_r[0]+":"+my_r[1] == 'Synonym:CurrencyEqu'):
                      n_e.append(my_r)
                      n_e2.append(wd2)
                      break
       return n_e, n_e2


   def CheckForRate_Series(self, chrs, match_results, wds, norm_date_dict, index_tup):


       if ( ('nP' == chrs) or ('n.nP' == chrs) or ('.nP' == chrs) or ('mn.nP' == chrs) or  ('sn.nP' == chrs) ):# and (int(wds[0])<100):
          match_results.append(('Rate', 'nP', 0, ''.join(wds)))  

       elif ('nmn' == chrs): ## Need to change
          if (len(wds[0]) == 4) and (wds[1]=='-'):
             match_results.append(('Series', 'Structure', 0, ''.join(wds)))  

       elif ('nmCn' == chrs):
          if (len(wds[0]) == 4) and (wds[1]=='-'):
             match_results.append(('Series', 'Structure', 0, ''.join(wds)))  
       
       elif ('Cnmn' == chrs):
          if (len(wds[3]) == 4) and (wds[1]=='-'):
             match_results.append(('Series', 'Structure', 0, ''.join(wds)))  

       elif ('nmC' == chrs):
          if (len(wds[0]) == 4) and (wds[1]=='-'):
             match_results.append(('Series', 'Structure', 0, ''.join(wds)))  

       elif ('nmCnC' == chrs):
          if (len(wds[0]) == 4) and (wds[1]=='-'):
             match_results.append(('Series', 'Structure', 0, ''.join(wds)))  


       elif ('n' == chrs):
          if len(wds[0])==6:
            if ( (2052>int(wds[0][0:4])>1990) and (12>=int(wds[0][4:6])>=1)):
               match_results.append(('Tasdate1', 'Tasdate1', 0, ''.join(wds)))  
               norm_date_dict[index_tup] = wds[0][0:4]+"-"+wds[0][4:6]+"-01"
          if len(wds[0])==8:
            if ( (31>=int(wds[0][0:2])>=1) and (12>=int(wds[0][2:4])>=1) and (2052>int(wds[0][4:8])>1990) ):
               match_results.append(('Tasdate2', 'Tasdate2', 0, ''.join(wds)))  
               norm_date_dict[index_tup] = wds[0][4:8]+"-"+wds[0][2:4]+"-"+wds[0][0:2]
          if len(wds[0])==8:
            if ( (12>=int(wds[0][2:4])>=1) and (31>=int(wds[0][0:2])>=1) and  (2052>int(wds[0][4:8])>1990) ):
               match_results.append(('Tasdate3', 'Tasdate3', 0, ''.join(wds)))  
               norm_date_dict[index_tup] = wds[0][4:8]+"-"+wds[0][2:4]+"-"+wds[0][0:2]
          if len(wds[0])==8:
            if ( (2052>int(wds[0][0:4])>1990) and (12>=int(wds[0][4:6])>=1) and (31>=int(wds[0][6:8])>=1)):
               match_results.append(('Tasdate4', 'Tasdate4', 0, ''.join(wds)))  
               norm_date_dict[index_tup] = wds[0][0:4]+"-"+wds[0][4:6]+"-"+wds[0][6:8]

       #print '>>', match_results
       return match_results 

   def RemoveDuplicateRows(self, match_results, in_str): 
       tickers = ['NYBCodes:NYBCode','CBTCodes:CBTCode','CMECodes:CMECode','Bloomberg_Corp_Bond:BloombergTicker','Bloomberg_Equity:Ticker','Bloomberg_Equity_Num:Ticker','Bloomberg_EquityIndx:IndexTicker','Bloomberg_FinIndxFut:Ticker','Bloomberg_Govt_Bond:Ticker','Bloomberg_Index:Ticker','Bloomberg_PhysIndxFut:PhyIndTicker','Bloomberg_SpotIndx:Ticker','BloombergFEMATicker:BloombergFEMATicker','bloombergtickerinfo:ConstituentTicker','EmergingMarketCompanies:BloombergTicker', 'bloombergtickerinfo:BloombergSymbol']

       new_match_results = []
       dup_group_avd = {}
       for e in match_results:
          if dup_group_avd.get((e[0], e[1]), 0):
             #print "CONT3"
             continue
          dup_group_avd[(e[0], e[1])] = 1
          mx = self.filter_dict.get(e[0], {})
          if mx:
             m_cond = mx['MatchCond']
             if m_cond[0] == 'AC':
                if not in_str.isupper():
                   #print "CONT1"
                   continue
          if ((e[0]+":"+e[1]) in tickers) or ("aptcode" in (e[0]+":"+e[1]).lower()):
             if not in_str.isupper():
                #print "CONT2"
                continue  

          new_match_results.append(e[:])

       new_match_results = self.CheckForCurrency(new_match_results[:], in_str)
       return new_match_results

   def CheckForCurrency(self, match_results, in_str):
          if in_str[:3].isupper():
             if len(match_results)>1:
                new_e = []
                for e1 in match_results:
                   if "currency:ISOCurrency" in e1[0]+":"+e1[1]:
                      new_e.append(e1)
                      break
                if new_e:
                   return new_e 
          return match_results

   def CheckAndCarryOtherInformation(self, match_results):
       ind = 0  
       new_match_results = []
       dup_group_avd = {}
       for e in match_results:
          if dup_group_avd.get(e[0], 0): continue
          dup_group_avd[e[0]] = 1
          mx = self.filter_dict.get(e[0], {})
          if mx:
             show_cols = mx['ToShow']
             row_list = self.file_row_mapper[e[0]][e[2]]
             new_row_list = []
             for e1 in row_list:
                if (e1[1] in show_cols):
                   new_row_list.append(e1)
             #print 'e: ', e, 'row_list: ', new_row_list
             new_row_list.append(e[:])
             new_match_results += new_row_list[:]
          else:
             new_match_results.append(e[:])
          ind += 1

       return new_match_results


   def checkIRSstructure(self,chr_l_elm):
       wd=''.join(chr_l_elm[1])
       if wd[-1] in "0123456789":           
           if wd[:3] == 'IRS':
              if wd[3:4] in ['P','R']:
                if wd[4:-1] in ['CPI','FR','CP']:
                    return 1
       return 0             
          
   def CheckForMBS(self, match_results):
       ind = 0
       row_numbers = []       
       for mat_elm in match_results:
           if mat_elm[0] == 'MBS':
              row_numbers.append(mat_elm[2])
              del match_results[ind]
           ind += 1
              
       new_mbs_m_r = [] 
       for row in row_numbers:
           mx = self.file_row_mapper['MBS'][row]
           for m in mx[:-1]:
               new_mbs_m_r.append(m)

       match_results += new_mbs_m_r[:]

       return match_results

   def CheckForPattern(self, wds):
           #print match_results
           wd= ''.join(wds)
           #if len(wds)<3: return [] 
           wd1 = wd[:]
           last_chr=wd[-1]
           num_range= ['0','1','2','3','4','5','6','7','8','9']          
           flg=0
           num_list=[]
           first_alpha=''
           if last_chr in num_range:
            wd=wd[::-1] 
            for ch in wd[1:]:
              if ch in num_range:
                  num_list.append(ch)
              else:
                 first_alpha=ch
                 break

            #print "num_list",num_list, 'first_alpha: ', first_alpha
            if len(num_list)<=1 and (first_alpha.upper() in ['F','G','H','J','K','M','N','Q','U','V','Z','X','Z']) : 
                #print 'len: ', len(first_alpha+''.join(num_list)+last_chr)
                #print 'wd1: ', wd1 
                wd1 = wd1[:len(wd1)-len(first_alpha+''.join(num_list)+last_chr)]
                return [wd1, first_alpha+''.join(num_list)+last_chr]
                #return first_alpha+''.join(num_list)+last_chr
           return []


   def CheckForPattern1(self,pstring_ls):
         if len(pstring_ls)>2:
                return []
         if len(pstring_ls) == 2:
            
           wd= pstring_ls[-1]
           #print "-->",wd
           last_chr=wd[-1]
           num_range= ['0','1','2','3','4','5','6','7','8','9']          
           flg=0
           num_list=[]
           first_alpha=''
           if last_chr in num_range:
            wd=wd[::-1] 
            for ch in wd[1:]:
              if ch in num_range:
                  num_list.append(ch)
              else:
                 first_alpha=ch
                 break
            #print "num_list",num_list
            if len(num_list)<=1 and (first_alpha.upper() in ['F','G','H','J','K','M','N','Q','U','V','Z','X','Z']): 
                return [pstring_ls[0],first_alpha+''.join(num_list)+last_chr]
           return []

   def checkSEDOL(self, value, len_sed):
       #print value, len_sed
       #sys.exit() 
       int_str = "0123456789"
       alpha_cap = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" 
       if len(value)==len_sed:
          if value[-1] in int_str:
             flg = 1
             for i in range(0, len_sed):
                if (value[i] in int_str) or (value[i] in alpha_cap):
                   continue
                else:
                   flg = 0
                   break
             if flg == 1:
                return 1
       return 0


   def checkCUSIP(self, value):
       int_str = "0123456789"
       alpha_cap = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" 
       if len(value)==9:
          if value[-1] in int_str:
             flg = 1
             for i in range(0, 9):
                if (value[i] in int_str) or (value[i] in alpha_cap):
                   continue
                else:
                   flg = 0
                   break
             if flg == 1:
                return 1
       return 0

   def checkISIN(self, value):
       int_str = "0123456789"
       alpha_cap = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" 
       if len(value)==12:
         if value[-1] in int_str:
           if (value[0] in alpha_cap) and (value[1] in alpha_cap):
             flg = 1
             for i in range(2, 12):
                if (value[i] in int_str) or (value[i] in alpha_cap):
                   continue
                else:
                   flg = 0
                   break
             if flg == 1:
                return 1
       return 0

   def checkISIN_STD(self, value):
	    value = value.strip().upper()
	    m = re.match('^([A-Z][A-Z])([A-Z0-9]{9}\d)$', value)
	    if not m:
		return False
	    sum_digits_str = ''.join(str(int(c, 36)) for c in value[:11])
	    total_sum = 0
	    parity = len(sum_digits_str) % 2
	    for n, c in enumerate(sum_digits_str):
		a = int(c)
		if n % 2 != parity:
		    a = a * 2
		total_sum += a / 10
		total_sum += a % 10
	    check_digit = (10 - (total_sum % 10)) % 10
	    return value[11] == unicode(check_digit)

   def NewC2C(self, chr_str, chr_list):
       myIndexAr = []
       ind = 0 
       temp_ar = []
       all_temp_ar = []
       #print chr_str
       for c in chr_str:
         #if c=="_": 
         #   ind += 1
         #   continue
         if c == "C":
            if not temp_ar:
               temp_ar.append(ind)
            else:
               all_temp_ar.append(temp_ar)
               temp_ar = []
               temp_ar.append(ind)
         else:
            if temp_ar:
               temp_ar.append(ind)

         ind += 1

       if temp_ar:
          all_temp_ar.append(temp_ar)
        
       each_char_map_list = []
       ind3 = 0 
       #print chr_list
       global_cnt = 0 
       for ch in chr_list:
          ind2 = 0  
          for c in ch[0]:
             my_index = []
             new = ch[2][ind2]
             my_tup = (global_cnt+new[0], global_cnt+new[1])
             each_char_map_list.append((c, ch[1][ind2], my_tup))  
             global_cnt = global_cnt + (new[1]-new[0]) + 1
             ind2 += 1
          ind3 += 1
          global_cnt += 1

       #print each_char_map_list     
       sys.exit()
       #checkC2C_dict={}
       #print chr_list 
       for temp_ar in all_temp_ar:
           #my_str =  
           for t in temp_ar:
              #print t
              my_tup = chr_list[t]  

       #print all_temp_ar
       #sys.exit()

   
   def ISIN_Check(self, match_results):
       my_new_dict = {} 
       for e in match_results:
           if not my_new_dict.get(e[1], []):
              my_new_dict[e[1]] = []
           my_new_dict[e[1]].append(e)
       if len(my_new_dict.keys())==1:
          return [my_new_dict[my_new_dict.keys()[0]][0]]
       else:
          new_ar = [] 
          for k,vs in my_new_dict.items():
              new_ar.append(vs[0]) 
          return new_ar 

   def MatchEntity(self, chr_list, cell_value, chr_str, g_char_dict, norm_date_dict):
           #self.NewC2C(chr_str, chr_list)

           if (chr_str == 'n.n') or (chr_str == 'mn.n'): 
              return {'string': { (0, len(cell_value)) : [('n', "Structure", -1, cell_value)] } }

           #d_dict = data_dict[k_tup]           
           #chr_list = d_dict['chr_list']
           #cell_value = d_dict['COL_VAL'].strip().lower()

           chr_match_dict = {} 
           #print 'cell_value: ', cell_value 

           m_d_dict =  self.Check_Cn(chr_list, cell_value, chr_str, g_char_dict)
           chr_match_dict['nC'] = copy.deepcopy(m_d_dict)

           m_d_dict =  self.Check_Ccn(chr_list, cell_value, chr_str)
           chr_match_dict['Ccn'] = copy.deepcopy(m_d_dict)

           m_d_dict =  self.Check_Cc(chr_list, cell_value, chr_str, g_char_dict, norm_date_dict)
           chr_match_dict['Cc'] = copy.deepcopy(m_d_dict)

           #m_d_dict =  self.Check_CcP(chr_list, cell_value, chr_str, g_char_dict)
           #chr_match_dict['CcP'] = copy.deepcopy(m_d_dict)

           m_d_dict = self.Check_C2C(chr_list, cell_value, chr_str, g_char_dict, norm_date_dict)
           chr_match_dict['C2C'] = copy.deepcopy(m_d_dict)

           #self.Check_n_nSlashn(chr_list, cell_value, chr_str)
           
           par_match_results_dict = self.MatchForMoreThen2(cell_value.split())


           if par_match_results_dict:
             chr_match_dict['p_string'] = copy.deepcopy(par_match_results_dict)

           ind = 0 
           global_count = 0  
           for chr_l_elm in chr_list:
               if not chr_match_dict.get('word', {}):
                  chr_match_dict['word'] = {}

               #print 'wd: 1' 
               ## Match Word here  
               wd = ''.join(chr_l_elm[1][:])
               c = self.CleanSpecialSymbols(wd.lower()) 
               key_str = str(len(c.split()))+'_'+str(len(c))
               match_results = self.mega_match_dict.get(key_str, {}).get(c, [])
               match_results = self.RemoveDuplicateRows(match_results[:], wd) 

               match_results_s = []
               if (str(wd)+"A").isupper():
                  #print 'wd: 2' 
                  m_res = self.checkISIN(wd.strip())
                  if m_res:
                    wd = wd.strip()
                    c = self.CleanSpecialSymbols(wd.lower()) 
                    key_str = str(len(c.split()))+'_'+str(len(c))
                    m_results = isin_obj.mega_match_dict.get(key_str, {}).get(c, [])
                    m_results = isin_obj.RemoveDuplicateRows(m_results[:], wd) 
                    m_results = isin_obj.CheckAndCarryOtherInformation(m_results[:])
                    #print m_results
                    #sys.exit()
                    #match_results_s += [('ISIN', "Structure", -1, wd)]
                    m_results = self.ISIN_Check(m_results)
                    match_results_s += m_results

                  m_res1 = self.checkCUSIP(wd.strip())
                  if m_res1:
                     match_results_s += [('CUSIP', "Structure", -1, wd)]

                  if len(wd.split())>1:
                    m_res2 = self.checkSEDOL(wd.strip(), 7)
                    if m_res2:
                      #ac = self.sedol_isin_dict.get(wd.strip(), "")
                      if 0:#ac:
                         match_results_s += [('SEDOL', ac, -1, wd)]
                      else: 
                        match_results_s += [('SEDOL', "Structure", -1, wd)]

                    m_res3 = self.checkSEDOL(wd.strip(), 8)
                    if m_res3:
                       match_results_s += [('SEDOL', "Structure", -1, wd)]

                  m_res4 =self.checkIRSstructure(chr_l_elm)
                  if m_res4:
                     match_results_s += [('IRSstructure', "Structure", -1, wd)]

                  
               if not self.RejectNumbers(''.join(chr_l_elm[0]), match_results[:], chr_l_elm[1]):
                  global_count = global_count + len(wd) + 1
                  ind += 1
                  continue

               w_ind_tup = chr_l_elm[2]

                  
               if ("_" in chr_l_elm[1] or "-" in chr_l_elm[1] or "+" in chr_l_elm[1]): 
                  match_dict = self.CheckForDemilInWords(''.join(chr_l_elm[0]), match_results[:], chr_l_elm[1], "_", global_count, chr_l_elm[2], cell_value)
                  for k,vs in match_dict.items():
                      chr_match_dict['word'][k] = vs[:]

               m_res, m_res2 = self.CheckForMultipleCurr(''.join(chr_l_elm[0]), match_results[:], chr_l_elm[1])

               if len(m_res)==2:
                  #print '1: ', match_results
                  w_len = global_count+w_ind_tup[-1][1]-3
                  chr_match_dict['word'][(global_count, w_len)] = [m_res[0]]
                  chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [m_res[1]]
               else: 
                  m_res, m_res2 = self.CheckForCurrAndCurrEndMarker(''.join(chr_l_elm[0]), match_results[:], chr_l_elm[1])
                  if len(m_res)==2:
                     w_len = global_count+w_ind_tup[-1][1]-len(m_res2[1])
                     chr_match_dict['word'][(global_count, w_len)] = [m_res[0]]
                     chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [m_res[1]]
                  else:
                     m_res, m_res2 = self.CheckForClubCurrencyAndValue(''.join(chr_l_elm[0]), match_results[:], chr_l_elm[1])
                     if m_res:
                       w_len = global_count+w_ind_tup[-1][1]-len(m_res[1])
                       chr_match_dict['word'][(global_count, w_len)] = [('Currency_Equ', m_res2[0], -1, m_res[0])]
                       chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [('ClubCurrencyNum', "Structure", -1, m_res[1])]
                     else:
                      match_results = self.CheckForRate_Series(''.join(chr_l_elm[0]), match_results[:], chr_l_elm[1], norm_date_dict, (global_count, global_count+w_ind_tup[-1][1]))
                      #for elm in match_results:
                      #    if (elm[0] == 'Tasdate1') or (elm[0] == 'Tasdate1') or (elm[0] == 'Tasdate1') or (elm[0] == 'Tasdate1'):
                      #       norm_date_dict[(global_count, global_count+w_ind_tup[-1][1])] = rate_np_elm 
                      #print match_results
                      #sys.exit()   
                      mx = self.CheckForPattern(''.join(chr_l_elm[1]))
                      if mx:
                         m_results = self.match_dict_code.get(len(mx[0].split()), {}).get(mx[0].lower(), [])
                         if m_results: 
                            #print m_results
                            match_results = self.RemoveDuplicateRows(m_results[:], wd) 
                            #print match_results
                            #sys.exit()
                            w_len = global_count+w_ind_tup[-1][1]-len(mx[1])
                            chr_match_dict['word'][(global_count, w_len)] = match_results[:]
                            chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [("InstrumentTime", "Structure", -1, mx[1])]
                         else:
                            if mx[0].isupper():
                               if mx[0][0] == 'X' and mx[0][-1] == 'Q':
                                  w_len = global_count+w_ind_tup[-1][1]-len(mx[1])
                                  chr_match_dict['word'][(global_count, w_len)] = [("CanDo", "Structure", -1, mx[0])]
                                  chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [("InstrumentTime", "Structure", -1, mx[1])]
                                
                               elif mx[0][0] in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
                                  w_len = global_count+w_ind_tup[-1][1]-len(mx[1])
                                  #chr_match_dict['word'][(global_count, w_len)] = [("CanDo", "Structure", -1, mx[0])]
                                  chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [("InstrumentTime", "Structure", -1, mx[1])]
                               else:        
                                  #w_len = global_count+w_ind_tup[-1][1]-len(mx[1])
                                  #chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [("InstrumentTime", "Structure", -1, mx[1])]
                                  match_results = self.RemoveDuplicateRows(match_results[:], wd) 
                                  chr_match_dict['word'][(global_count, global_count+w_ind_tup[-1][1])] = match_results[:]
                                  for elm in match_results:
                                     if (elm[0] == 'Rate') and (elm[1] == 'nP'):
                                        rate_np_elm = obj_ratenp.num_round(chr_l_elm)
                                        norm_date_dict[(global_count, global_count+w_ind_tup[-1][1])] = rate_np_elm 
                            else:                                         
                               #print '5: ', match_results
                               match_results = self.RemoveDuplicateRows(match_results[:], wd) 
                               chr_match_dict['word'][(global_count, global_count+w_ind_tup[-1][1])] = match_results[:]
                               for elm in match_results:
                                 if (elm[0] == 'Rate') and (elm[1] == 'nP'):
                                   rate_np_elm = obj_ratenp.num_round(chr_l_elm)
                                   norm_date_dict[(global_count, global_count+w_ind_tup[-1][1])] = rate_np_elm 
                      else:
                         mx_res = self.CheckForOptions(''.join(chr_l_elm[0]), match_results[:], chr_l_elm[1])
                         if len(mx_res)==2:
                            w_len = global_count+w_ind_tup[-1][1]-len(mx_res[1])
                            chr_match_dict['word'][(global_count, w_len)] = [mx_res[0]]
                            chr_match_dict['word'][(w_len, global_count+w_ind_tup[-1][1])] = [('n', "Structure", -1, "")]
                         else:
                           match_results = self.RemoveDuplicateRows(match_results[:], wd) 
                           chr_match_dict['word'][(global_count, global_count+w_ind_tup[-1][1])] = match_results[:]
                           for elm in match_results:
                             if (elm[0] == 'Rate') and (elm[1] == 'nP'):
                                 rate_np_elm = obj_ratenp.num_round(chr_l_elm)
                                 norm_date_dict[(global_count, global_count+w_ind_tup[-1][1])] = rate_np_elm 
 
               if match_results_s:
                  match_results = self.RemoveDuplicateRows(match_results_s[:], wd) 
                  if not chr_match_dict['word'].get((global_count, global_count+w_ind_tup[-1][1]), []):
                     chr_match_dict['word'][(global_count, global_count+w_ind_tup[-1][1])] = [] 
                  chr_match_dict['word'][(global_count, global_count+w_ind_tup[-1][1])] += match_results_s[:]

                  for elm in match_results_s:
                    if (elm[0] == 'Rate') and (elm[1] == 'nP'):
                         rate_np_elm = obj_ratenp.num_round(chr_l_elm)
                         norm_date_dict[(global_count, global_count+w_ind_tup[-1][1])] = rate_np_elm 
               #print wd
               #print match_results
               #sys.exit()

               tiker_flg = 1
               if len(chr_l_elm[0])>1:
                  if chr_l_elm[0][0] == 'C' and 'c' in chr_l_elm[0][1:]:
                     tiker_flg = 0

               #'''  
               for i in range(len(chr_l_elm[1])):
                   ## Match char here
                   #print chr_l_elm
                   char = chr_l_elm[1][i]
                   #print char 
                   #sys.exit() 
                   if not chr_match_dict.get('char', {}):
                      chr_match_dict['char'] = {}
                   
                   if len(char)>1 or chr_l_elm[0][i] == 'n':
                      pass 
                   else:
                       continue

                   key_str = str(len(char.split()))+'_'+str(len(char))
                   match_results = self.mega_match_dict.get(key_str, {}).get(char.lower(), [])
                   #print char
                   #print match_results
                   #sys.exit()

                   #match_results = self.match_dict.get(len(char.split()), {}).get(char.lower(), [])
                   d = []
                   for m_elm in match_results:
                       if m_elm[0] in ['Bloomberg_Corp_Bond', 'Bloomberg_EquityIndx', 'Bloomberg_Equity', 'Bloomberg_FinIndxFut', 'Bloomberg_Govt_Bond', 'Bloomberg_Index', 'Bloomberg_PhysIndxFut', 'Bloomberg_SpotIndx', 'bloombergtickerinfo']: continue
                       if m_elm[0]+":"+m_elm[1] in ['bloombergtickerinfo:BloombergSymbol','rating:APTCode','TRStockExchange:TR Symbol','BlmbrgStkExchange:Bloomberg Codes','markets:MarketABBREV','rating:APTCode','markets:MarketABBREV','ImpCodes:CODE']: continue 
                       if tiker_flg==0:
                          if "ticker" in (m_elm[0]+":"+m_elm[1]).lower(): 
                             continue
                       #print ':::::::', m_elm[0]+":"+m_elm[1], tiker_flg, char
                       d.append(m_elm[:])

                   match_results = d[:]
                   #match_results = self.CheckForMBS(match_results[:])
                   #match_results = self.CheckAndCarryOtherInformation(d[:])
                   #chr_match_dict['char'][(ind, i)] = match_results[:]
                   match_results = self.RemoveDuplicateRows(match_results[:], char) 
                   #print char 
                   #print match_results   
                   #print w_ind_tup[i] 
                   #sys.exit()
                   chr_match_dict['char'][(w_ind_tup[i][0]+global_count, global_count+w_ind_tup[i][1])] = match_results[:]
               #'''  

               #print wd
               #sys.exit()  
               global_count = global_count + len(wd) + 1
               ind += 1

           if not chr_match_dict.get('string', {}):
              chr_match_dict['string'] = {}

           c = self.CleanSpecialSymbols(cell_value.lower()) 
           key_str = str(len(c.split()))+'_'+str(len(c))
           match_results = self.mega_match_dict.get(key_str, {}).get(c, [])
           #print self.mega_match_dict[key_str]
           ##print "=="+c+"=="
           #print key_str
           #print "MMMMMMMMMMMMMMM", match_results
           #match_results = self.match_dict.get(len(cell_value.split()), {}).get(cell_value, [])
           #match_results = self.CheckAndCarryOtherInformation(match_results[:])
           match_results = self.RemoveDuplicateRows(match_results[:], cell_value) 
           #print match_results 
           #sys.exit()   
           chr_match_dict['string'] = {}
           chr_match_dict['string'][(0, len(cell_value))] = match_results[:]

           #print chr_match_dict
           #sys.exit()
           return chr_match_dict

   def Check_n_nSlashn(self,chr_list, cell_value, chr_str):
         checkC2C_dict={}
         check_list =  chr_str.split('_')
         #print check_list
         #sys.exit()
         count=0
         all_ar = []
         global_count = count  
         for elm in check_list:             
             #print '>>>>>>>>>>>>>>:::::::::::', elm, chr_list[count]
             #if len(elm) :
             #   p_c_ar_ind = chr_list[count][2]
             #   global_count = global_count + p_c_ar_ind[-1][1]
             #   global_count = global_count + 1 
             #   count = count + 1
             #   continue
               
             p_c_ar = chr_list[count][1]
             p_c_ar_ind = chr_list[count][2]
             
             temp_ar = [] 
             temp_ind_ar = [] 
             prev_global_count = global_count  
             ele_not_in_pic = 0
             #print "elm", elm
             #sys.exit() 
             for i in  range(0,len(elm)):
                 if elm[i] == 'n' and ( i+1<len(elm) and elm[i+1] == 'n' ) and ( i+2<len(elm) and elm[i+2] == 'S' ) and ( i+3<len(elm) and elm[i+3] == 'n' ):
                    #print "IM HERE"
                    #sys.exit() 
                    ele_not_in_pic = 1
                    temp_ar.append(p_c_ar[i]) 
                    temp_ar.append(p_c_ar[i+1])
                    temp_ar.append(p_c_ar[i+2])
                    temp_ar.append(p_c_ar[i+3])
                    if (temp_ar[2]=="/") and ( int(temp_ar[1]) < int(temp_ar[3]) ): 
                       p_str = ''.join(temp_ar[:]) 
                       #print 'p_str:::', p_str, global_count
                       #print '>>', cell_value[global_count:global_count+4]
                       checkC2C_dict[(global_count, global_count+4)] = [("RateMixedStruct", "Structure", -1, )] 
                    temp_ar = [] 
                 else:
                    temp_ar = [] 

                 if count>0:
                    global_count = prev_global_count + p_c_ar_ind[i][1]
                 else: 
                    global_count = p_c_ar_ind[i][1]
                        
             if ele_not_in_pic == 0:
                p_c_ar_ind = chr_list[count][2]
                global_count = global_count + p_c_ar_ind[-1][1] 
             global_count = global_count + 1 
             count=count+1
         s = checkC2C_dict.keys()
         #print checkC2C_dict
         #s.sort()
         #sys.exit()
         return checkC2C_dict      

   def Check_CcP(self,chr_list, cell_value, chr_str, g_char_dict):
         checkC2C_dict={}
         check_list =  chr_str.split('_')
         count=0
         all_ar = []
         global_count = count  
         for elm in check_list:             
             #print '>>>>>>>>>>>>>>:::::::::::', elm, chr_list[count]
             if len(elm) == 1:
                p_c_ar_ind = chr_list[count][2]
                global_count = global_count + p_c_ar_ind[-1][1]
                global_count = global_count + 1 
                count = count + 1
                continue
               
             p_c_ar = chr_list[count][1]
             p_c_ar_ind = chr_list[count][2]
             
             temp_ar = [] 
             temp_ind_ar = [] 
             prev_global_count = global_count  
             ele_not_in_pic = 0
             for i in  range(0,len(elm)):
                 #print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', global_count, p_c_ar[i], p_c_ar_ind[i]
                 if elm[i] == 'C' and ( i+1<len(elm) and elm[i+1] == 'c' ) and i+2<len(elm) and elm[i+2] == 'P':
                    ele_not_in_pic = 1
                    temp_ar.append(p_c_ar[i]) 
                    temp_ar.append(p_c_ar[i+1]) 
                    temp_ar.append(p_c_ar[i+2]) 

                    p_str = ''.join(temp_ar[:]) 
                    #print 'p_str:::', p_str, global_count
                    #print '>>', cell_value[global_count:global_count+3]
                    key_str = str(len(p_str.split()))+'_'+str(len(p_str))
                    
                    m = global_count + len(temp_ar[-3]) + len(temp_ar[-2]) + len(temp_ar[-1]) 
                   
                    match_results = self.mega_match_dict.get(key_str, {}).get(p_str.lower(), [])
                    match_results = self.RemoveDuplicateRows(match_results[:], p_str) 
                    checkC2C_dict[(global_count, m)] = match_results 
                    temp_ar = [] 
                 else:
                    temp_ar = [] 

                 if count>0:
                    global_count = prev_global_count + p_c_ar_ind[i][1]
                 else: 
                    global_count = p_c_ar_ind[i][1]
                        
             #if ele_not_in_pic == 0:
             #   p_c_ar_ind = chr_list[count][2]
             #   global_count = global_count + p_c_ar_ind[-1][1] 
             global_count = global_count + 1 
             count=count+1


         p_s = checkC2C_dict.keys()
         p_s.sort()
         for s in p_s:
            if not cell_value[s[0]:s[1]]:
               del checkC2C_dict[s]

         if checkC2C_dict:
           my_new_match_dict = self.MatchAllPosible(copy.deepcopy(checkC2C_dict), g_char_dict, cell_value, norm_date_dict)
           for k,vs in my_new_match_dict.items():
             if not checkC2C_dict.get(k, []):
                checkC2C_dict[k] = vs[:]
         return checkC2C_dict      



   def Check_Cc(self,chr_list, cell_value, chr_str, g_char_dict, norm_date_dict):
         checkC2C_dict={}
         check_list =  chr_str.split('_')
         count=0
         all_ar = []
         global_count = count  
         for elm in check_list:             
             #print '>>>>>>>>>>>>>>:::::::::::', elm, chr_list[count]
             if len(elm) == 1:
                p_c_ar_ind = chr_list[count][2]
                global_count = global_count + p_c_ar_ind[-1][1]
                global_count = global_count + 1 
                count = count + 1
                continue
               
             p_c_ar = chr_list[count][1]
             p_c_ar_ind = chr_list[count][2]
             
             temp_ar = [] 
             temp_ind_ar = [] 
             prev_global_count = global_count  
             ele_not_in_pic = 0
             for i in  range(0,len(elm)):
                 #print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', global_count, p_c_ar[i], p_c_ar_ind[i]
                 if elm[i] == 'C' and ( i+1<len(elm) and elm[i+1] == 'c' ):
                    ele_not_in_pic = 1
                    temp_ar.append(p_c_ar[i]) 
                    temp_ar.append(p_c_ar[i+1]) 

                    p_str = ''.join(temp_ar[:]) 
                    #print 'p_str:::', p_str, global_count
                    #print '>>', cell_value[global_count:global_count+3]
                    c = self.CleanSpecialSymbols(p_str.lower()) 
                    key_str = str(len(c.split()))+'_'+str(len(c))
                    match_results = self.mega_match_dict.get(key_str, {}).get(c, [])
                    
                    m = global_count + len(temp_ar[-2]) + len(temp_ar[-1])
                   
                    match_results = self.RemoveDuplicateRows(match_results[:], p_str) 
                    checkC2C_dict[(global_count, m)] = match_results 
                    temp_ar = [] 
                 else:
                    temp_ar = [] 

                 if count>0:
                    global_count = prev_global_count + p_c_ar_ind[i][1]
                 else: 
                    global_count = p_c_ar_ind[i][1]
                        
             #if ele_not_in_pic == 0:
             #   p_c_ar_ind = chr_list[count][2]
             #   global_count = global_count + p_c_ar_ind[-1][1] 
             global_count = global_count + 1 
             count=count+1


         p_s = checkC2C_dict.keys()
         p_s.sort()
         for s in p_s:
            if not cell_value[s[0]:s[1]]:
               del checkC2C_dict[s]

         if checkC2C_dict:
           my_new_match_dict = self.MatchAllPosible(copy.deepcopy(checkC2C_dict), g_char_dict, cell_value, norm_date_dict)
           for k,vs in my_new_match_dict.items():
             if not checkC2C_dict.get(k, []):
                checkC2C_dict[k] = vs[:]
         #print checkC2C_dict
         #sys.exit() 
         return checkC2C_dict      


   def Check_Ccn(self,chr_list, cell_value, chr_str):
         checkC2C_dict={}
         check_list =  chr_str.split('_')
         count=0
         all_ar = []
         global_count = count  
         for elm in check_list:             
             #print '>>>>>>>>>>>>>>:::::::::::', elm, chr_list[count]
             if len(elm) == 1:
                p_c_ar_ind = chr_list[count][2]
                global_count = global_count + p_c_ar_ind[-1][1]
                global_count = global_count + 1 
                count = count + 1
                continue
               
             p_c_ar = chr_list[count][1]
             p_c_ar_ind = chr_list[count][2]
             
             temp_ar = [] 
             temp_ind_ar = [] 
             prev_global_count = global_count  
             ele_not_in_pic = 0
             for i in  range(0,len(elm)):
                 #print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', global_count, p_c_ar[i], p_c_ar_ind[i]
                 if elm[i] == 'C' and ( i+1<len(elm) and elm[i+1] == 'c' ) and ( i+2<len(elm) and elm[i+2] == 'n' ):
                    ele_not_in_pic = 1
                    temp_ar.append(p_c_ar[i]) 
                    temp_ar.append(p_c_ar[i+1]) 
                    temp_ar.append(p_c_ar[i+2]) 

                    p_str = ''.join(temp_ar[:]) 
                    #print 'p_str:::', p_str, global_count
                    #print '>>', cell_value[global_count:global_count+3]
                    c = self.CleanSpecialSymbols(p_str.lower()) 
                    key_str = str(len(c.split()))+'_'+str(len(c))
                    match_results = self.mega_match_dict.get(key_str, {}).get(c, [])
                    
                    m = global_count + len(temp_ar[-3]) + len(temp_ar[-2]) + len(temp_ar[-1])
                   
                    match_results = self.RemoveDuplicateRows(match_results[:], p_str) 
                    checkC2C_dict[(global_count, m)] = match_results 
                    temp_ar = [] 
                 else:
                    temp_ar = [] 

                 if count>0:
                    global_count = prev_global_count + p_c_ar_ind[i][1]
                 else: 
                    global_count = p_c_ar_ind[i][1]
                        
             #if ele_not_in_pic == 0:
             #   p_c_ar_ind = chr_list[count][2]
             #   global_count = global_count + p_c_ar_ind[-1][1] 
             global_count = global_count + 1 
             count=count+1
         s = checkC2C_dict.keys()
         s.sort()
         #print checkC2C_dict
         #sys.exit() 
         return checkC2C_dict      


   def Check_Cn(self,chr_list, cell_value, chr_str, g_char_dict):
         #print cell_value 
         checkC2C_dict={}
         check_list =  chr_str.split('_')
         count=0
         all_ar = []
         global_count = count  
         for elm in check_list:             
             #print '>>>>>>>>>>>>>>:::::::::::', elm, chr_list[count][2]
             #print "global_count: ", global_count 
             if len(elm) == 1:
                p_c_ar_ind = chr_list[count][2]
                global_count = global_count + p_c_ar_ind[-1][1]
                global_count = global_count + 1 
                count = count + 1
                continue
               
             p_c_ar = chr_list[count][1]
             p_c_ar_ind = chr_list[count][2]
             
             temp_ar = [] 
             temp_ind_ar = [] 
             prev_global_count = global_count  
             ele_not_in_pic = 0
             for i in  range(0,len(elm)):
                 #print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', global_count, p_c_ar[i], p_c_ar_ind[i]
                 #if elm[i] == 'n' and ( i+1<len(elm) and elm[i+1] in ['C', 'c'] ):
                 if elm[i] == 'n' and ( i+1<len(elm) and elm[i+1] in ['C'] ):
                    ele_not_in_pic = 1
                    temp_ar.append(p_c_ar[i]) 
                    temp_ar.append(p_c_ar[i+1]) 
                    p_str = ''.join(temp_ar[:]) 

                    c = self.CleanSpecialSymbols(p_str.lower()) 
                    key_str = str(len(c.split()))+'_'+str(len(c))
                    match_results = self.mega_match_dict.get(key_str, {}).get(c, [])

                    match_results = self.RemoveDuplicateRows(match_results[:], p_str)

                    m = global_count + len(temp_ar[-2]) + len(temp_ar[-1])
                    #print 'p_str:::', p_str, 'global_count: ', global_count
                    #print '>>', cell_value[global_count:m]

                    #checkC2C_dict[(global_count, global_count+3)] = match_results 
                    checkC2C_dict[(global_count, m)] = match_results 
                    temp_ar = [] 
                 else:
                    temp_ar = [] 

                 if count>0:
                    global_count = prev_global_count + p_c_ar_ind[i][1]
                 else: 
                    global_count = p_c_ar_ind[i][1]
                        
             #if ele_not_in_pic == 0:
             #   p_c_ar_ind = chr_list[count][2]
             #   global_count = global_count + p_c_ar_ind[-1][1] 

             global_count = global_count + 1 
             count=count+1
         s = checkC2C_dict.keys()
         s.sort()
         #if checkC2C_dict:
         #  my_new_match_dict = self.MatchAllPosible(copy.deepcopy(checkC2C_dict), g_char_dict, cell_value)
         #  for k,vs in my_new_match_dict.items():
         #    if not checkC2C_dict.get(k, []):
         #       checkC2C_dict[k] = vs[:]
         #print checkC2C_dict
         #sys.exit()
         return checkC2C_dict      



   def Check_C2C(self,chr_list, cell_value, chr_str, g_char_dict, norm_date_dict):
         checkC2C_dict={}
         #print "::::",chr_list, cell_value, chr_str 
         check_list =  chr_str.split('_')
         count=0
         #print "check_list",check_list 
         #sys.exit() 
         all_ar = []
         global_count = count  
         #print "************************************************************************************************"
         for elm in check_list:             
             #print '>>>>>>>>>>>>>>:::::::::::', elm, chr_list[count]
             if len(elm) == 1:
                p_c_ar_ind = chr_list[count][2]
                global_count = global_count + p_c_ar_ind[-1][1]
                global_count = global_count + 1 
                count = count + 1
                continue
               
             #print chr_list, count
             p_c_ar = chr_list[count][1]
             p_c_ar_ind = chr_list[count][2]
             #print p_c_ar
             #print p_c_ar_ind
             
             temp_ar = [] 
             temp_ind_ar = [] 
             prev_global_count = global_count  
             ele_not_in_pic = 0
             for i in range(0,len(elm)):
                 #print "elm[i]: ", elm[i], global_count,  p_c_ar_ind[i], p_c_ar[i]
                 if elm[i] == 'C':
                    ele_not_in_pic = 1
                    if not temp_ar:
                       temp_ar.append(p_c_ar[i]) 
                       temp_ind_ar.append(p_c_ar_ind[i])
                    else:
                       p_str = ''.join(temp_ar[:]) 
                       #print 'p_str1: ', p_str

                       c = self.CleanSpecialSymbols(p_str.lower()) 
                       key_str = str(len(c.split()))+'_'+str(len(c))
                       match_results = self.mega_match_dict.get(key_str, {}).get(c, [])
                       match_results = self.RemoveDuplicateRows(match_results[:], p_str) 

                       #print ">>"+cell_value[global_count:global_count+ (temp_ind_ar[-1][1]-temp_ind_ar[0][0]) ]+"="

                       checkC2C_dict[(global_count, global_count+(temp_ind_ar[-1][1]-temp_ind_ar[0][0]))] = match_results 
                       global_count = global_count + (temp_ind_ar[-1][1]-temp_ind_ar[0][0])

                       temp_ar = [ p_c_ar[i] ]
                       temp_ind_ar = [ p_c_ar_ind[i] ]
                 else:
                    if temp_ar:
                       temp_ar.append(p_c_ar[i]) 
                       temp_ind_ar.append(p_c_ar_ind[i])
                    else: 
                       global_count = global_count + (p_c_ar_ind[i][1]-p_c_ar_ind[i][0])

             if temp_ar:
                p_str = ''.join(temp_ar[:]) 
                #print 'p_str: ', p_str
                c = self.CleanSpecialSymbols(p_str.lower()) 
                key_str = str(len(c.split()))+'_'+str(len(c))
                match_results = self.mega_match_dict.get(key_str, {}).get(c, [])
                match_results = self.RemoveDuplicateRows(match_results[:], p_str) 
                if 1:# count > 0: 
                   #print "org_str2:"+cell_value[global_count:prev_global_count + temp_ind_ar[-1][1]]+"="
                   checkC2C_dict[(global_count, prev_global_count + temp_ind_ar[-1][1])] = match_results
                   global_count = prev_global_count + temp_ind_ar[-1][1]
                #else:
                #   print "org_str1: ", cell_value[global_count:temp_ind_ar[-1][1]]
                #   checkC2C_dict[(global_count, temp_ind_ar[-1][1])] = match_results
                #   global_count = temp_ind_ar[-1][1]
             
             #if ele_not_in_pic == 0:
             #   p_c_ar_ind = chr_list[count][2]
             #   global_count = global_count + (p_c_ar_ind[i][1]-p_c_ar_ind[i][0])

             global_count = global_count + 1 
             count=count+1
        
         #sys.exit() 
         p_s = checkC2C_dict.keys()
         p_s.sort()
         for s in p_s:
            if not cell_value[s[0]:s[1]]:
               del checkC2C_dict[s]

         if checkC2C_dict: 
           my_new_match_dict = self.MatchAllPosible(copy.deepcopy(checkC2C_dict), g_char_dict, cell_value, norm_date_dict)
           for k,vs in my_new_match_dict.items():
             if not checkC2C_dict.get(k, []):
                checkC2C_dict[k] = vs[:]
         return checkC2C_dict      

   def MatchAllPosible(self, checkC2C_dict, g_char_dict, cell_value, norm_date_dict):

       myKeys = checkC2C_dict.keys()
       myKeys.sort()
       d = max(g_char_dict.keys())  
       if 1:
           missed_out = []
           prev_i = 0
           i = 0
           for all_pos_elm_elm in myKeys:
               if all_pos_elm_elm[0] == 0:
                  prev_i = all_pos_elm_elm[1]
               else:   
                  j = all_pos_elm_elm[0]
                  if (prev_i == j):
                     prev_i = all_pos_elm_elm[1]
                  else:
                     missed_out.append((prev_i, all_pos_elm_elm[0]))
                     prev_i = all_pos_elm_elm[1]

           #sys.exit()         
           if prev_i == d+1:
              pass
           else:
              missed_out.append((prev_i, d+1))

           #print missed_out
           #sys.exit() 
           for missed_out_elm in missed_out:
               new_missed_out_elm = []
               tmp_ar = []  
               for k in range(missed_out_elm[0], missed_out_elm[1]):
                   gx = g_char_dict.get(k, ())
                   if gx:
                      tmp_ar.append(k) 
                   else:
                      new_missed_out_elm.append(tmp_ar[:])
                      tmp_ar = []

               if tmp_ar: 
                  new_missed_out_elm.append(tmp_ar[:])
                  tmp_ar = []  

               final_missed_out = []
               for new_elm in new_missed_out_elm:
                   if new_elm:
                      final_missed_out.append((new_elm[0], new_elm[-1]+1))   
       
               for f in final_missed_out:
                   c_i_ar = []
                   c_e_ar = []
                      
                   i_ind = 0
                   for p in range(f[0], f[1]):
                      (ch, c_e) = g_char_dict[p]  
                      if i_ind == 0:
                         c_i_ar.append(ch)
                         c_e_ar.append(c_e)
                      else:
                         if ch == c_i_ar[-1]:
                            c_e_ar.append(c_e)
                         else:  
                            c_i_ar.append(ch)
                            c_e_ar.append(c_e)
                      i_ind += 1

                   #print ch 
                   c_i_ar_str = ''.join(c_i_ar[:])
                   c_e_ar_str = ''.join(c_e_ar[:])

                   checkC2C_dict[f] = [(c_i_ar_str, "Structure", -1, c_e_ar_str)]

           #print checkC2C_dict
           #sys.exit()

           #for tup, my_ar in checkC2C_dict.items():
           #    if len(my_ar) == 1:
           #       if my_ar[0][0] == 'Cc':
           #          print my_ar
           #          sys.exit() 

           myKeys = checkC2C_dict.keys()
           myKeys.sort()
           Mywords = []
           for k in myKeys:
               Mywords.append(cell_value[k[0]:k[1]])

           my_match_dict = self.MatchAllPosibleNew(checkC2C_dict, cell_value, Mywords, norm_date_dict)
           return my_match_dict

   def MatchAllPosibleNew(self, checkC2C_dict, cell_value, Mywords, norm_date_dict):
       myKeys = checkC2C_dict.keys()
       myKeys.sort()

       len_words = len(myKeys)
       n = len_words
       match_results_dict = {}
       #print "Mywords: ", Mywords  
       while n>0:
           for i in range(0, len(myKeys)):
               if (i+n<=len_words):
                  if i>0:
                     d = len(' '.join(Mywords[:i])) + 1 
                  else:
                     d = 0 
                  my_tuple = myKeys[i:i+n]
                  qry_str = ' '.join(Mywords[i:i+n]).lower()

                  type_char, words, index_ar = obj_ext.gettype(' '.join(Mywords[i:i+n]))

                  if type_char == 'Cc':
                     if len(words[0])>2:
                        o = words[0][:-1]
                        t = words[0][1:]+words[1]

                        o_ind = (my_tuple[0][0], my_tuple[0][0]+len(o))
                        t_ind = (my_tuple[0][0]+len(o), my_tuple[-1][1])

                        c = self.CleanSpecialSymbols(o.lower()) 
                        key_str = str(len(c.split()))+'_'+str(len(c))
                        a_results = self.mega_match_dict.get(key_str, {}).get(c, [])

                        c = self.CleanSpecialSymbols(t.lower()) 
                        key_str = str(len(c.split()))+'_'+str(len(c))
                        b_results = self.mega_match_dict.get(key_str, {}).get(c, [])

                        match_results_dict[o_ind] = a_results[:]
                        match_results_dict[t_ind] = b_results[:]

                     
                  len_comp_d = len(qry_str)
                  #print "AA: ", qry_str

                  c = self.CleanSpecialSymbols(qry_str.lower()) 
                  key_str = str(len(c.split()))+'_'+str(len(c))
                  match_results = self.mega_match_dict.get(key_str, {}).get(c, [])

                  #print "match_results: ", match_results

                  if len(qry_str.split())==1: 
                     type_char, words, index_ar = obj_ext.gettype(qry_str)
                     match_results = self.CheckForRate_Series(type_char, match_results[:], words, norm_date_dict, (my_tuple[0][0], my_tuple[-1][1] ))

                     m_res, m_res2 = self.CheckForClubCurrencyAndValue(type_char, match_results[:], words)
                     if m_res:
                       w_len = my_tuple[-1][1]-len(m_res[1])
                       match_results_dict[(my_tuple[0][0], w_len)] = [('Currency_Equ', m_res2[0], -1, m_res[0])]
                       match_results_dict[(w_len, my_tuple[-1][1] )] = [('ClubCurrencyNum', "Structure", -1, m_res[1])]
                       continue

                     m_res, m_res2 = self.CheckForCurrAndCurrEndMarker(type_char, match_results[:], words)
                     if len(m_res)==2:
                        w_len = my_tuple[-1][1]-len(m_res2[1])
                        match_results_dict[(my_tuple[0][0], w_len)] = [m_res[0]]
                        match_results_dict[(w_len,  my_tuple[-1][1])] = [m_res[1]]
                        continue

                  match_results = self.RemoveDuplicateRows(match_results[:], ' '.join(Mywords[i:i+n])) 
                  match_results_dict[(my_tuple[0][0], my_tuple[-1][1])] = match_results[:]
                  
           n = n - 1
       return match_results_dict 

   def DisambiguateDict(self, f_tup_dict):
       myKeys = f_tup_dict.keys()
       myKeys.sort()

       del_keys = []

       #print "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
       for myKey in myKeys:
           my_list = f_tup_dict[myKey]
           my_tup = 0
           for e in my_list:
               #if e[0]+":"+e[1] in ['ISIN:Structure', 'CUSIP:Structure', 'SEDOL:Structure', 'Rate:nP', 'Series:Structure', 'Prop:Rating Prop', "derivatives_swaps_credit_default_swaps:synonym", "country:country"]:
               #print  e[0]+":"+e[1]
               if e[0]+":"+e[1] in self.freezed_entities:
                  my_tup = e
                  break
           if my_tup: 
              for myKey2 in myKeys:
                 if myKey != myKey2:
                    if (myKey2[0] >= myKey[0]) and (myKey2[1] <= myKey[1]):
                       #f_tup_dict[myKey] = [my_tup]
                       if myKey2 not in del_keys:
                          del_keys.append(myKey2)

       #print del_keys
       #sys.exit()
       for e in del_keys:
           del f_tup_dict[e] 
       return f_tup_dict

  
   def RemoveSubSets(self, myKeys):
       del_keys = []
       for myKey in myKeys:
         for myKey2 in myKeys:
            if myKey != myKey2:
              if myKey2[0] >= myKey[0] and (myKey2[1] <= myKey[1]):
                 if myKey2 not in del_keys:
                    del_keys.append(myKey2)

       return del_keys


   def ParseMyDict(self, myDict, chr_str, g_char_dict, org_str, norm_date_dict, structure_str):
       #import sets 
       f_tup_dict = {}
       for en, d_dict in myDict.items():
          for ind_tup, match_res in d_dict.items():
              if match_res:
                 #if ind_tup[1]+1 < len(org_str) and ind_tup[1]-2 < len(org_str):
                 #  if org_str[ind_tup[0]-2] == "(" and  org_str[ind_tup[1]+1] == ")":
                 #     ind_tup = (ind_tup[0]-2, ind_tup[1]+2) 
                 
                 if not f_tup_dict.get(ind_tup, []):
                    f_tup_dict[ind_tup] = []

                 #print ">>>>>>>>>>>>>>>>>>>>", en, ind_tup, match_res   
                 #print ">>", org_str[ind_tup[0]:ind_tup[1]]
                 mx = f_tup_dict[ind_tup][:] + match_res[:]
                 my_new = {} 
                 for e in mx:
                    my_new[(e[0], e[1])] = e

                 my_list = []
                 for k,vs in my_new.items():
                     my_list.append(vs)   

                 #print '2: ', mx    
                 #n_mx = list(sets.Set(mx[:]))
                 #print "my_new: ", my_new.keys()
                 f_tup_dict[ind_tup] = my_new.keys()[:]

       if not f_tup_dict:
        for en, d_dict in myDict.items():
          if en!='word': continue 
          for ind_tup, match_res in d_dict.items():
              if not f_tup_dict.get(ind_tup, []):
                 f_tup_dict[ind_tup] = []

              c_i_ar = []
              c_e_ar = []
                      
              i_ind = 0
              #print en, ind_tup
              for p in range(ind_tup[0], ind_tup[1]):
                  #print p
                  #print g_char_dict[p]   
                  (ch, c_e) = g_char_dict[p]  
                  if i_ind == 0:
                     c_i_ar.append(ch)
                     c_e_ar.append(c_e)
                  else:
                     if ch == c_i_ar[-1]:
                        c_e_ar.append(c_e)
                     else:  
                        c_i_ar.append(ch)
                        c_e_ar.append(c_e)
                  i_ind += 1


              c_i_ar_str = ''.join(c_i_ar[:])
              c_e_ar_str = ''.join(c_e_ar[:])
              f_tup_dict[ind_tup] = [(c_i_ar_str, "Structure", -1, c_e_ar_str)]

       #print 'f_tup_dict: ', f_tup_dict 
       #sys.exit()      
       mega_asset_dict = {}
       #mega_asset_dict = obj_prop_rules._ProcessMain(f_tup_dict)
 
       f_tup_dict = self.DisambiguateDict(copy.deepcopy(f_tup_dict))

       f_tup_dict = self.GetMissingElms(copy.deepcopy(f_tup_dict), g_char_dict)

       #print f_tup_dict
       #sys.exit()
       #print norm_date_dict
       #sys.exit()
 
       myKeys = f_tup_dict.keys()
       myKeys.sort()

       #tup_dict2, flg, del_ar, addition_dict = self.MatchContextRules(myKeys[:], copy.deepcopy(f_tup_dict), org_str, norm_date_dict)
       addition_dict = {}
       del_ar = []
       for k,vs in addition_dict.items():
           f_tup_dict[k] = vs

       for e in del_ar:
         try:
           del f_tup_dict[e] 
           del norm_date_dict[e]  
         except: 
           pass 

   
       #f_tup_dict = self.CreateNewEntity(org_str, copy.deepcopy(f_tup_dict))

       f_tup_dict = self.DisambiguateDict(copy.deepcopy(f_tup_dict))

       my_f_f_tup_dict = copy.deepcopy(f_tup_dict)

       #print f_tup_dict
       #sys.exit()

       #print f_tup_dict.keys()
       #ar_possibility = self.get_possibilities_patterns(f_tup_dict, g_char_dict)
       ar_possibility = []
       #print ar_possibility
       #print len(ar_possibility) 
       #sys.exit()

       new_ar_possibility = []
       all_f_tup_dict_ar = []
       del_keys = []  
       for ar_possibility_elm in ar_possibility:
           tup_dict = ar_possibility_elm[1]
           s = tup_dict.keys()
           s.sort() 
           tup_dict2, flg, del_sub_ar, a_d = self.MatchContextRules(s[:], copy.deepcopy(tup_dict), org_str, norm_date_dict) 
           if flg:
              for k,vs in a_d.items():
                  tup_dict2[k] = vs
              ar_possibility2 = self.get_possibilities_patterns(tup_dict2, g_char_dict)
              for e in ar_possibility2: 
                  new_ar_possibility.append(e[:][0])
                  all_f_tup_dict_ar.append(e[:][1])
           else:
              new_ar_possibility.append(ar_possibility_elm[0])
              all_f_tup_dict_ar.append(ar_possibility_elm[1])

       s_str = ":^:".join(new_ar_possibility)
       #print s_str
       #print s_str, all_f_tup_dict_ar
       #sys.exit()  
       return s_str, all_f_tup_dict_ar, mega_asset_dict, my_f_f_tup_dict


   def GetMissingElms(self, f_tup_dict, g_char_dict):
       s_indexs = f_tup_dict.keys()
       s_indexs.sort()

       d = max(g_char_dict.keys())  
       if 1:
           missed_out = []
           prev_i = -1
           i = 0
           for all_pos_elm_elm in s_indexs:
               if all_pos_elm_elm[0] == 0:
                  prev_i = all_pos_elm_elm[1]
               else:   
                  j = all_pos_elm_elm[0]
                  if (prev_i == j):
                     prev_i = all_pos_elm_elm[1]
                  else:
                     missed_out.append((prev_i, all_pos_elm_elm[0]))
                     prev_i = all_pos_elm_elm[1]

           if prev_i == d+1:
              pass
           else:
              missed_out.append((prev_i, d+1))

           keys = f_tup_dict.keys()
           for e in keys:
               if (e not in s_indexs):
                  del f_tup_dict[e]    

           for missed_out_elm in missed_out:
               new_missed_out_elm = []
               tmp_ar = []  
               for k in range(missed_out_elm[0], missed_out_elm[1]):
                   gx = g_char_dict.get(k, ())
                   if gx:
                      tmp_ar.append(k) 
                   else:
                      new_missed_out_elm.append(tmp_ar[:])
                      tmp_ar = []
                      #tmp_ar.append(k) 

               if tmp_ar: 
                  new_missed_out_elm.append(tmp_ar[:])
                  tmp_ar = []  

               #print new_missed_out_elm
               final_missed_out = []
               for new_elm in new_missed_out_elm:
                   if new_elm:
                      final_missed_out.append((new_elm[0], new_elm[-1]+1))   
       
               #print 'missed_out_elm: ', missed_out_elm        
               #print 'final_missed_out: ', final_missed_out
               for f in final_missed_out:
                   c_i_ar = []
                   c_e_ar = []
                      
                   i_ind = 0
                   for p in range(f[0], f[1]):
                      (ch, c_e) = g_char_dict[p]  
                      if i_ind == 0:
                         c_i_ar.append(ch)
                         c_e_ar.append(c_e)
                      else:
                         if ch == c_i_ar[-1]:
                            c_e_ar.append(c_e)
                         else:  
                            c_i_ar.append(ch)
                            c_e_ar.append(c_e)
                      i_ind += 1


                   c_i_ar_str = ''.join(c_i_ar[:])
                   c_e_ar_str = ''.join(c_e_ar[:])

                   f_tup_dict[f] = [(c_i_ar_str, "Structure", -1, c_e_ar_str)]

       return f_tup_dict
 
   
   def get_possibilities_patterns(self, f_tup_dict, g_char_dict):
      
       s_indexs = f_tup_dict.keys()
       s_indexs.sort()
    
       #print 's_indexs: ', f_tup_dict
       import index_possibility
       all_pos_ar = index_possibility.index_possibility().find_pos_tup_ar(s_indexs[:])
 
       #print '::::', all_pos_ar
       #sys.exit()   
       d = max(g_char_dict.keys())  

       cell_all_pos_map = ""   
       ar_possibility = []
       for all_pos_elm in all_pos_ar:
           missed_out = []
           prev_i = -1
           i = 0
           for all_pos_elm_elm in all_pos_elm:
               if all_pos_elm_elm[0] == 0:
                  prev_i = all_pos_elm_elm[1]
               else:   
                  j = all_pos_elm_elm[0]
                  if (prev_i == j):
                     prev_i = all_pos_elm_elm[1]
                  else:
                     missed_out.append((prev_i, all_pos_elm_elm[0]))
                     prev_i = all_pos_elm_elm[1]
           #sys.exit()         
           if prev_i == d+1:
              pass
           else:
              missed_out.append((prev_i, d+1))
           #print g_char_dict 
           #print 'Pos: ', all_pos_elm  
           #print 'Missedout: ', missed_out
           f_tup_dict1 = copy.deepcopy(f_tup_dict)
           keys = f_tup_dict1.keys()
           #print 'f_tup_dict1: ', f_tup_dict1.keys()
           for e in keys:
               if (e not in all_pos_elm):
                  del f_tup_dict1[e]    

           #print 'f_tup_dict1: ', f_tup_dict1.keys()
           #print "****************************************************************************"
           for missed_out_elm in missed_out:
               #missed_out_elm = (0, 15)
               new_missed_out_elm = []
               tmp_ar = []  
               for k in range(missed_out_elm[0], missed_out_elm[1]):
                   gx = g_char_dict.get(k, ())
                   if gx:
                      tmp_ar.append(k) 
                   else:
                      new_missed_out_elm.append(tmp_ar[:])
                      tmp_ar = []
                      #tmp_ar.append(k) 

               if tmp_ar: 
                  new_missed_out_elm.append(tmp_ar[:])
                  tmp_ar = []  

               #print new_missed_out_elm
               final_missed_out = []
               for new_elm in new_missed_out_elm:
                   if new_elm:
                      final_missed_out.append((new_elm[0], new_elm[-1]+1))   
       
               #print 'missed_out_elm: ', missed_out_elm        
               #print 'final_missed_out: ', final_missed_out
               for f in final_missed_out:
                   c_i_ar = []
                   c_e_ar = []
                      
                   i_ind = 0
                   for p in range(f[0], f[1]):
                      (ch, c_e) = g_char_dict[p]  
                      if i_ind == 0:
                         c_i_ar.append(ch)
                         c_e_ar.append(c_e)
                      else:
                         if ch == c_i_ar[-1]:
                            c_e_ar.append(c_e)
                         else:  
                            c_i_ar.append(ch)
                            c_e_ar.append(c_e)
                      i_ind += 1


                   c_i_ar_str = ''.join(c_i_ar[:])
                   c_e_ar_str = ''.join(c_e_ar[:])
                   #print 'c_i_ar_str: ', c_i_ar_str
                   #print 'c_e_ar_str: ', c_e_ar_str
                   #print 'f: ', f 

                   f_tup_dict1[f] = [(c_i_ar_str, "Structure", -1, c_e_ar_str)]
     
             
           s = f_tup_dict1.keys()
           s.sort()
           #print s
                 
           all_pos_str = ""
           for pos_tup in s:
                p_str = "" 
                m_res = f_tup_dict1[pos_tup]  
                #print "pos_tup: ", pos_tup, " == ", "m_res: ", m_res
                if m_res:
                  for e in m_res:
                     p_str += e[0]+":"+e[1] + "|" 
                else:
                  #print 'EEEEEEEEEEEE'
                  sys.exit() 
                p_str = p_str.strip("|")
                all_pos_str += p_str + ":*:"  

           #sys.exit() 
           #print 'Before: ', s
           all_pos_str = all_pos_str.strip(":*:")
           #ar_possibility.append((all_pos_str, copy.deepcopy(f_tup_dict1))) 
           ar_possibility.append((all_pos_str, copy.deepcopy(f_tup_dict1))) 
       return ar_possibility 

   def GetTermsAndCondtions(self, tax_dict, all_str, all_f_tup_dict_ar):
       asset_class_dict = {}
       for pos_i, asset_class_list in tax_dict.items():
         for c in asset_class_list:
           asset_class_dict[c] = pos_i

       asset_class_about_cols = {}
       for asset_class, pos_i in asset_class_dict.items():
          f_tup_dict = all_f_tup_dict_ar[pos_i]
          pos_id = 0
          for pos_elm in all_str.split(':^:')[pos_i].split(':*:'):    
                 s = f_tup_dict.keys()
                 s.sort()
                 index_tup = s[pos_id]
                 text_s = f_tup_dict[index_tup][0][3]
                 tas_TC = conf_obj.map_config_dict.get(pos_elm, [])
                 about_list = []
                 for tc in tas_TC:
                    if (asset_class == tc.split(":")[0]):
                       about_list.append(tc)
                 if not about_list:
                    pos_id += 1
                    continue  
                 if not asset_class_about_cols.get(asset_class, []):
                    asset_class_about_cols[asset_class] = [] 

                 asset_class_about_cols[asset_class].append((about_list[:], pos_i, pos_id))
                 pos_id += 1

       return asset_class_about_cols

   def ApplyRulePositive(self, all_pos, my_input_str, f_tup_dict, structure_str):
       s_s = f_tup_dict.keys()
       s_s.sort()
       for strut_str, rule_elm in self.positive_rules.items():
         if strut_str!=structure_str: continue
         my_list = []
         n_ind = 0
         my_ind = 0
         if "|" not in all_pos: return all_pos
         all_pos_sp = all_pos.split(':*:')
         for rule in rule_elm:
           if rule in ['TAS_SPACE', 'TAS_NOSPACE']:
              my_list.append(((), "TAS_SPACE", -1))

           for i in range(my_ind, len(all_pos_sp)):
               index_tup = s_s[i]
               org_str = my_input_str[index_tup[0]:index_tup[1]]
               rule_list_or = rule.split("|")
               found_e_list = []
               for rule_list_or_elm in rule_list_or:   
                  if rule_list_or_elm in all_pos_sp[i].split("|"):
                     found_e_list.append(rule_list_or_elm)

               if found_e_list:
                  #if my_ind != 0:
                  #   if my_ind != i:
                  #      break  
                  #print rule, ">>>>>>>>",  all_pos_sp[i]
                  my_ind = i + 1
                  my_list.append((index_tup, "|".join(found_e_list), i))
                  break
               
           n_ind += 1

         #print my_list
         #sys.exit()   
         if len(my_list)==len(rule_elm):
          allow_flg = 1
          for i in range(len(my_list)):
             if i != 0:
                 if my_list[i][1] == "TAS_SPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    if my_input_str[s[0]:s[1]] != " ":
                       allow_flg = 0
                       break 
                 if my_list[i][1] == "TAS_NOSPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    if my_input_str[s[0]:s[1]] != "":
                       allow_flg = 0
                       break

          if allow_flg==1:
             rev_my_list = {}
             for m in my_list:
                 rev_my_list[m[2]] = m[1]

             new_all_pos_sp = []

             all_pos_sp = all_pos.split(':*:')
             for i in range(0, len(all_pos_sp)):  
                 mx = rev_my_list.get(i, "")
                 if mx:
                    new_all_pos_sp.append(mx)
                 else:
                    new_all_pos_sp.append(all_pos_sp[i])
           
             all_pos = ":*:".join(new_all_pos_sp)
       return all_pos

   def GetRules(self, all_pos):
       rule_ids = []  
       all_pos_sp = all_pos.split(':*:')
       for pos in all_pos_sp:
           mx = self.dis_key_dict.get(pos, {})
           if mx:
              #print "pos: ", pos   
              rule_ids += mx.keys()
       return rule_ids       

 
   def ApplyRule(self, all_pos, my_input_str, f_tup_dict):

       rule_ids = self.GetRules(all_pos)

       #print rule_ids 
       #print "ccomp: ", self.dis_amb_rule_list.keys() 

       s_s = f_tup_dict.keys()
       s_s.sort()
       #print self.dis_amb_rule_list 
       for ind, rule_dict in self.dis_amb_rule_list.items():
         if ind not in rule_ids: continue
         #print ind, rule_dict
         #if ind!=56: continue
         rule_elm = rule_dict["FROM"] 
         my_list = []
         n_ind = 0
         my_ind = 0
         #if "|" not in all_pos: return all_pos
         all_pos_sp = all_pos.split(':*:')
         for rule in rule_elm:
           if rule in ['TAS_SPACE', 'TAS_NOSPACE']:
              my_list.append(((), rule, -1))
           for i in range(my_ind, len(all_pos_sp)):
               index_tup = s_s[i]
               org_str = my_input_str[index_tup[0]:index_tup[1]]
               if rule in all_pos_sp[i]:
                  my_ind = i + 1
                  #print "ind: ", ind
                  my_list.append((index_tup, rule_dict["TO"][n_ind] , i))
                  break
           n_ind += 1

         if len(my_list)==len(rule_elm):
          allow_flg = 1
          for i in range(len(my_list)):
             if i != 0:
                 if my_list[i][1] == "TAS_SPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    #print ">>"+my_input_str[s[0]:s[1]]+">>"
                    if my_input_str[s[0]:s[1]] != " ":
                       allow_flg = 0
                       break 
                 if my_list[i][1] == "TAS_NOSPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    #print ">>"+my_input_str[s[0]:s[1]]+">>"
                    if my_input_str[s[0]:s[1]] != "":
                       allow_flg = 0
                       break

          #print my_list, allow_flg
          #sys.exit() 
          if allow_flg==1:
             rev_my_list = {}
             for m in my_list:
                 rev_my_list[m[2]] = m[1]

             new_all_pos_sp = []

             all_pos_sp = all_pos.split(':*:')
             for i in range(0, len(all_pos_sp)):  
                 mx = rev_my_list.get(i, "")
                 if mx:
                    new_all_pos_sp.append(mx)
                 else:
                    new_all_pos_sp.append(all_pos_sp[i])
           
             all_pos = ":*:".join(new_all_pos_sp)
             rule_ids = self.GetRules(all_pos)
       return all_pos
                      

   def EntityDisAmbiguate(self, all_pos_str, my_input_str, all_f_tup_dict_ar, structure_str):
       if all_pos_str:
          new_all_pos_str_sp = []
          ind = 0
          for all_pos in all_pos_str.split(":^:"):
              if 1:#"|" in all_pos:
                 all_pos = self.ApplyRuleCreateNewEntity(all_pos, my_input_str, all_f_tup_dict_ar[ind], structure_str)
                 all_pos = self.ApplyRule(all_pos, my_input_str, all_f_tup_dict_ar[ind])
                 all_pos = self.ApplyRulePositive(all_pos, my_input_str, all_f_tup_dict_ar[ind], structure_str)
              new_all_pos_str_sp.append(all_pos)
              ind += 1
          return ":^:".join(new_all_pos_str_sp) 
       return all_pos_str

   def ApplyRuleCreateNewEntity(self, all_pos, my_input_str, f_tup_dict, structure_str):
       del_ar = []
       for ind, rule_dict in self.create_new_entity.items():
         rule_elm = rule_dict["FROM"] 
         my_list = []
         n_ind = 0
         my_ind = 0
         #if "|" not in all_pos: return all_pos
         all_pos_sp = all_pos.split(':*:')
         #print all_pos_sp  
         s_s = f_tup_dict.keys()
         s_s.sort()
         for rule in rule_elm:
           if rule in ['TAS_SPACE']:
              my_list.append(((), "TAS_SPACE", -1))
           if rule in ['TAS_NOSPACE']:
              my_list.append(((), "TAS_NOSPACE", -1))

           for i in range(my_ind, len(all_pos_sp)):
               index_tup = s_s[i]
               org_str = my_input_str[index_tup[0]:index_tup[1]]
               rule_list_or = rule.split("|")
               found_e_list = []
               for rule_list_or_elm in rule_list_or:   
                  if rule_list_or_elm in all_pos_sp[i].split("|"):
                     found_e_list.append(rule_list_or_elm)

               if found_e_list:
                  #print rule, ">>>>>>>>",  all_pos_sp[i], found_e_list
                  my_ind = i + 1
                  my_list.append((index_tup, rule_dict["TO"][0], i))
                  break
               
           n_ind += 1

         #print my_list
         #print rule_elm 
         #sys.exit()   
         if len(my_list)==len(rule_elm):
          allow_flg = 1
          for i in range(len(my_list)):
             if i != 0:
                 if my_list[i][1] == "TAS_SPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    if my_input_str[s[0]:s[1]] != " ":
                       allow_flg = 0
                       break 
                 if my_list[i][1] == "TAS_NOSPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    if my_input_str[s[0]:s[1]] != "":
                       allow_flg = 0
                       break

          if allow_flg==1:
             rev_my_list = {}
             for m in my_list:
                 if m[0]:  
                    rev_my_list[m[2]] = 1

             all_key = rev_my_list.keys()
                          
             st = all_key[0]

             s_index = s_s[all_key[0]]
             e_index = s_s[all_key[-1]]
 
             m_res = rule_dict["TO"][0]

             new_all_pos_sp = []

             all_pos_sp = all_pos.split(':*:')
             
             for i in range(0, len(all_pos_sp)):  
                 mx = rev_my_list.get(i, "")
                 if mx:
                    if i == st:
                       new_all_pos_sp.append(m_res)
                       f_tup_dict[(s_index[0], e_index[-1])] = m_res                       
                    del_ar.append(s_s[i])
                    continue
                 else:
                    new_all_pos_sp.append(all_pos_sp[i])
             all_pos = ":*:".join(new_all_pos_sp)
        
       for e in del_ar:
           del f_tup_dict[e]       
       return all_pos


   def CreateNewEntity(self, my_input_str, f_tup_dict):
       for ind, rule_dict in self.create_new_entity.items():
         rule_elm = rule_dict["FROM"] 
         my_list = []
         n_ind = 0
         my_ind = 0
         #if "|" not in all_pos: return all_pos
         s_s = f_tup_dict.keys()
         s_s.sort()
         for rule in rule_elm:
           if rule in ['TAS_SPACE', 'TAS_NOSPACE']:
              my_list.append(((), "TAS_PACE"))
           for i in range(my_ind, len(s_s)):
               index_tup = s_s[i]
               my_match_list = f_tup_dict[index_tup]  
               if (my_match_list[0][0]+":"+my_match_list[0][1] in rule.split("|")):
                  my_ind = i
                  my_list.append((index_tup, rule_dict["TO"][0]))
                  break
           n_ind += 1

         if len(my_list)==len(rule_elm):
          allow_flg = 1
          for i in range(len(my_list)):
             if i != 0:
                 if my_list[i][1] == "TAS_SPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    if my_input_str[s[0]:s[1]] != " ":
                       allow_flg = 0
                       break 
                 if my_list[i][1] == "TAS_NOSPACE":
                    s = (my_list[i-1][0][1], my_list[i+1][0][0])
                    if my_input_str[s[0]:s[1]] != "":
                       allow_flg = 0
                       break
        
          if allow_flg==1:
             rev_my_list = {}
             for m in my_list:
                 if m[0]:  
                    rev_my_list[m[0]] = m[1]
 
             m_s = rev_my_list.keys()
             m_s.sort()
             #print m_s
             n_index_tup = (m_s[0][0], m_s[-1][1])   


             m_res = rule_dict["TO"][0].split(":")
             f_tup_dict[n_index_tup] = [(m_res[0], m_res[1], -1, my_input_str[n_index_tup[0]:n_index_tup[1]])]

             for m in m_s:
                del f_tup_dict[m]

       return f_tup_dict

   def create_all_poss(self, posibilities):
       #print '---> ', posibilities 
       #sys.exit()
       # for each cell
       mydict = {}  
       for posibility in posibilities:       
           #print posibility
           #sys.exit()   
           for d in range(1, len(posibility)+1):  
               for i in range(0, len(posibility)): 
                   if (i+d <= len(posibility)):
                       elm = posibility[i:i+d]
                       allow_flg = 1 
                       #if elm[0] in ['TAS_SPACE', 'TAS_NO_SPACE']:
                       #   allow_flg = 0  
                       #if elm[-1] in ['TAS_SPACE', 'TAS_NO_SPACE']: 
                       #   allow_flg = 0 
                       if allow_flg:  
                          mydict[tuple(elm)] = 1
       #print "mydict: ", mydict  
       return mydict   



   def getSheetData(self, line_sp_elm):
               #import sys
               #print "line_sp_elm: ", line_sp_elm 
               #reload(sys)
               #sys.setdefaultencoding('UTF16') 
               #line_sp_elm = line_sp_elm.replace("\xc2\xa0", " ") 
               #line_sp_elm = line_sp_elm.replace("\xa0\xa0", " ") 
               line_sp_elm = ' '.join(line_sp_elm.split())

               #if line_sp_elm.lower().strip() == 'null':
               #   return "", [], ""

               #print '<br>Insider getSheetData: ', line_sp_elm 
               #try:
               #   d = float(line_sp_elm)
               #   #print "ddddddDD:", d
               #   #line_sp_elm = str(d)
               #   #flg = 1
               #   line_sp_elm = "%.2f" % d 
               #except:
               #   pass     
                 

               #print [line_sp_elm]
               ##line_sp_elm = line_sp_elm.replace('(', " ( ") 
               ##line_sp_elm = line_sp_elm.replace(')', " ) ") 
               ##line_sp_elm = line_sp_elm.replace('/', " / ") 
               #line_sp_elm = line_sp_elm.replace("'", " ' ") 
               ##line_sp_elm = line_sp_elm.replace(' " ', ' " ') 
               ##line_sp_elm = line_sp_elm.replace(",", " , ") 
               ##line_sp_elm = line_sp_elm.replace("=", " = ") 
               ##line_sp_elm = line_sp_elm.replace(":", " : ") 
               ##line_sp_elm = line_sp_elm.replace('"', ' " ') 

               line_sp_elm = ' '.join(line_sp_elm.split())

               ############### 's no space in between but for others add space #############
               new_line_sp_elm = '' 
               ind = 0
               for e in line_sp_elm:
                   if 0:#e in ["'"]:
                      if (ind+1 < len(line_sp_elm)):
                         if line_sp_elm[ind+1]=='s':
                            # dont add space
                            new_line_sp_elm = new_line_sp_elm + e  
                         else:
                            # add space
                            new_line_sp_elm = new_line_sp_elm + ' '+ e+' '  
                      else:
                         #add space  
                         new_line_sp_elm = new_line_sp_elm +' '+e+' '  
                   else:
                      new_line_sp_elm = new_line_sp_elm + e  
                   ind = ind + 1          
               line_sp_elm = new_line_sp_elm.strip()
               #print '<br>Insider: ', line_sp_elm  
               #print "OOOO: ", line_sp_elm
               #sys.exit() 
         
               line_sp_elm_wds = line_sp_elm.split()
               chr_list = []
               chr_str = '' 
               for line_sp_elm_wd in line_sp_elm_wds:
                   type_char, words, index_ar = obj_ext.gettype(line_sp_elm_wd)
                   chr_list.append((type_char, words, index_ar))
                   chr_str += type_char+'_'
                   #print '<hr> MMMM '
                   #print 'line_sp_elm_wd: ', line_sp_elm_wd
                   #print 'type: ', type_char
                   #print 'ind: ', index_ar   

               structure_str = obj_gen_struct.generate_semantic_patt(chr_list[:])  
               chr_str = chr_str.strip('_')                
          
               
               #print '::::::::::::::::::::::::::::::::::::::::::::::' 
               #print "chr_str:",chr_str
               #print "chr_list:",chr_list
               #sys.exit() 
               g_char_dict = {}
               global_count = 0 
               for chr_elm in chr_list:
                   #print 'chr_elm: ', chr_elm 
                   i = 0 
                   for ind_elm in chr_elm[2]:
                     c_elm = chr_elm[0][i]  
                     for j in range(ind_elm[1]-ind_elm[0]):
                       g_char_dict[global_count] = (c_elm, chr_elm[1][i][j])
                       global_count += 1
                     i = i + 1
                   global_count += 1
                      
               #print line_sp_elm
               mch_d = {}
               norm_date_dict = {}
               #mch_d = self.MatchEntity(chr_list, line_sp_elm, chr_str, g_char_dict, norm_date_dict)
               #print mch_d
               #print norm_date_dict    
               #sys.exit()
               #print "norm_date_dict",norm_date_dict
               #sys.exit()
               #all_str, all_f_tup_dict_ar, mega_asset_dict, my_f_f_tup_dict = self.ParseMyDict(copy.deepcopy(mch_d), chr_str, g_char_dict, line_sp_elm, norm_date_dict, structure_str)
               #print all_f_tup_dict_ar
               #print mega_asset_dict
               #print my_f_f_tup_dict 
               #sys.exit()
               #print g_char_dict
               #print norm_date_dict 
               #print all_str
               #print 'line_sp_elm: ', line_sp_elm   
               #print 'g_char_dict: ', g_char_dict 
               #print 'chr_list: ', chr_list 
               #print chr_str 
               return line_sp_elm, chr_list, chr_str, g_char_dict      
               all_str = self.EntityDisAmbiguate(all_str, line_sp_elm, all_f_tup_dict_ar, structure_str)

               #print all_str, all_f_tup_dict_ar
               #sys.exit() 

               all_pos_list = []
               if all_str:
                 for all_pos in all_str.split(":^:"):
                     a_sp = all_pos.split(":*:")
                     all_pos_list.append(a_sp[:])

               my_data_dict = self.create_all_poss(all_pos_list[:])

               all_results_dict = self.MatchAssetsClass(my_data_dict)
               if not all_results_dict:
                  all_results_dict = self.MatchAssetsClassAdd(my_data_dict)
               #all_results_dict = {}
               t_c_dict = {}

               #for k,vs in norm_date_dict.items():
               #    r = line_sp_elm[k[0]:k[1]]
               #    line_sp_elm = line_sp_elm.replace(r, vs)
   
               #print all_f_tup_dict_ar
               #print mch_d  
               #sys.exit()
               #print all_f_tup_dict_ar 
               #print all_str
               #print norm_date_dict
               #sys.exit() 
               #print mega_asset_dict
               #sys.exit()
               return mch_d, all_str, all_results_dict, all_f_tup_dict_ar, t_c_dict,chr_list, line_sp_elm, structure_str, my_data_dict, norm_date_dict, mega_asset_dict, my_f_f_tup_dict

if __name__=='__main__':
   obj = UploadFile()
   #def getSheetData(self, line_sp_elm):
   a, b, c, d = obj.getSheetData('Next Payment 4/15/2020')
   print 'a ',a
   print 'b ', b
   print 'c ', c
   print 'd ', d
