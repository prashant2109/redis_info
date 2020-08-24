#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys
import re
#reload(sys)
#sys.setdefaultencoding('utf-8')
import GetCleanValue as GetCleanValue
Gobj = GetCleanValue.GetCleanValue()

class numbercleanup(object):
    def __init__(self, numfmt=None):
        self.numberformat = numfmt # None:default, 0:USD, 1:EUR
        pass

    def isNumber(self, istr):
        istr1 = istr.replace(' ', '')
        if istr1 and istr1[0] in ['-', '+']:
            istr1 = istr1[1:]
        res = istr1.isnumeric() if isinstance(istr1, unicode) else str(istr1).isdigit()
        return res

    def getnumfmt(self, numfmttype=''):
        numtype = numfmttype.strip()
        numfmt = 0
        if numtype.upper() in ['USD']:
            numfmt = 0
        elif numtype.upper() in ['EUR']:
            numfmt = 1
        return numfmt

    def get_special_chars_cleanup(self, value):
        val = value.strip()
        val = re.sub(r"\(\d{1,2}\)", '', val)
        val = val.replace(u'\xae', '')
        return val

    def get_special_chars_cleanup_v2(self, value):
        val = value.strip()
        val = val.replace('&amp;', '&')
        val = re.sub("&#\d+;", '', val)
        return val

    def get_numfmt_delimiter(self, numfmt=0):
        val = '.' if (not numfmt) else ','
        return val

    def get_poss_numfmt(self, value, numfmt=None):
        #print [value,  numfmt]
        def get_delim_pos(value, delimiter):
            val = value.strip()
            tlst = []
            for i, ch in enumerate(val[:]):
                if ch in [delimiter]:
                    tlst.append(i)
                pass
            return tlst

        def decimal_cleanup(value, delimiter):
            val = value.strip()
            tlst = []
            for i, ch in enumerate(val[:]):
                if ch in [delimiter]:
                    tlst.append(i)
                pass

            tmp0 = val.split(delimiter)
            tmp0 = [elm.strip() for elm in tmp0 if elm.strip()]

            flag = None
            if tlst and tmp0 and (len(tmp0[-1]) <= 2):
                flag = True
            else:
                pass
            return tlst, flag

        tmpnumfmt = 0
        delim_tlst_cma, flag_cma = decimal_cleanup(value, ',')
        delim_tlst_dec, flag_dec = decimal_cleanup(value, '.')
        #print '\n=============================='
        #print [value]
        #print [delim_tlst_cma, flag_cma]
        #print [delim_tlst_dec, flag_dec]

        if (len(delim_tlst_cma) > 1):
            tmpnumfmt = 0
        elif (len(delim_tlst_dec) > 1):
            tmpnumfmt = 1

        if (len(delim_tlst_cma) == 1):
            tmpnumfmt = 1 if flag_cma else 0
            pass
        if (len(delim_tlst_dec) == 1):
            tmpnumfmt = 0
            pass

        sysfmt = 1 if ((len(delim_tlst_cma) > 1) or (len(delim_tlst_dec) > 1)) else 0
        if 1 and (not sysfmt) and ((len(delim_tlst_cma) == 1) or (len(delim_tlst_dec) == 1)):
            if len(delim_tlst_cma) and len(delim_tlst_dec):
                tmpnumfmt = 0 if (delim_tlst_dec[0] > delim_tlst_cma[0]) else 1
            elif self.numberformat is not None:
                tmpnumfmt = self.numberformat
            elif numfmt is not None:
                tmpnumfmt = numfmt
        #print tmpnumfmt
        return tmpnumfmt

    def replace_numfmt(self, istr, numfmt=None):
        def decimal_cleanup(value, delimiter):
            val = value.strip()
            tlst = []
            for i, ch in enumerate(val[:]):
                if ch in [delimiter]:
                    tlst.append(i)
                pass

            tmp0 = val.split(delimiter)
            tmp0 = [elm.strip() for elm in tmp0 if elm.strip()]

            if tlst and tmp0 and (len(tmp0[-1]) <= 2):
                tmp1 = tmp0[:-1]
                tmp2 = tmp1 + [delimiter] + [tmp0[-1]]
                val = ''.join(tmp2)
                pass
            else:
                val = ''.join(tmp0)
                pass
            return val


        ostr = istr.strip()
        tmp_num_fmt = self.get_poss_numfmt(ostr, numfmt)

        if not tmp_num_fmt:
            ostr = ostr.replace(',', '')        # USD
        else:
            #ostr = ostr.replace('.', '')        # EUR
            ostr = decimal_cleanup(ostr, '.')
        return ostr

    def replace_numfmt_type(self, istr, tmp_num_fmt=0):
        def decimal_cleanup(value, delimiter):
            val = value.strip()
            tlst = []
            for i, ch in enumerate(val[:]):
                if ch in [delimiter]:
                    tlst.append(i)
                pass

            tmp0 = val.split(delimiter)
            tmp0 = [elm.strip() for elm in tmp0 if elm.strip()]

            if tlst and tmp0 and (len(tmp0[-1]) <= 2):
                tmp1 = tmp0[:-1]
                tmp2 = tmp1 + [delimiter] + [tmp0[-1]]
                val = ''.join(tmp2)
                pass
            else:
                val = ''.join(tmp0)
                pass
            return val

        ostr = istr.strip()
         
            
        if not tmp_num_fmt:
            ostr = ostr.replace(',', '')        # USD
        else:
            #ostr = ostr.replace('.', '')        # EUR
            ostr = decimal_cleanup(ostr, '.')
        return ostr

    def get_decimal_cleanup(self, value):
        val = value.strip()
        tlst = []
        for i, ch in enumerate(val[:]):
            if ch in ['.']:
                tlst.append(i)
            pass
        if tlst:
            tmpidx = tlst[-1]
            val = val[:tmpidx].replace('.', '') + val[tmpidx:]
            pass
        return val

    def data_cleanup_blank_number(self, value):
        val = value.strip()
        val = val.replace('NA', '')
        val = val.replace('N/A', '')
        val = val.strip()
        if val and (len(value.strip()) != len(val)):
            val = ''
        return val

    def data_cleanup_basic(self, value):
        val = value.strip()
        val = val.replace(' ', '')
        val = val.replace('US$', '')
        val = val.replace('$', '')
        val = val.replace('USD', '')
        val = val.replace('Rs.', '')
        val = val.replace('\xc2\xa5', '')
        val = val.replace('\xc2\xa0', '')
        val = val.replace('\xc2\xa3', '')
        val = val.replace('c.', '')
        val = val.replace('p.', '')
        val = val.replace('C.', '')
        val = val.replace('P.', '')
        val = val.replace('p', '')
        val = val.replace('c', '')
        val = val.replace('P', '')
        val = val.replace('C', '')
        val = self.get_special_chars_cleanup_v2(val)
        val = self.data_cleanup_blank_number(val)
        val = val.strip()
        return val

    def data_cleanup_neg(self, value):
        val = value.strip()
        val = val.replace('(', '')
        val = val.replace(')', '')
        val = val.strip()
        return val

    def data_cleanup_other(self, value, ilst):
        val = value.strip()
        for ch in ilst[:]:
            val = val.replace(ch, '')
        val = val.strip()
        return val

    def isnegvalue(self, value):
        isneg = None
        val = self.data_cleanup_basic(value)
        val = self.replace_numfmt(val)
        val = val.replace('%', '')
        if val:
            f0 = 1 if (val[0] == '(' or val[0] == '\xef\xbc\x88') else 0
            f1 = 1 if (val[-1] == ')' or val[-1] == '\xef\xbc\x89') else 0
            if f0 and f1:
                isneg = True
        return isneg

    def isminusvalue(self, value):
        isneg = None
        val = self.data_cleanup_basic(value)
        val = self.replace_numfmt(val)
        val = val.replace('%', '')
        if val:
            if (val[0] == '-'):
                isneg = True
        return isneg

    def get_number_cleanup_v2(self, value, numfmt=None):

        isneg = self.isnegvalue(value)
        isminus = self.isminusvalue(value)

        tmpval = value.strip()
        if not isneg:
            tmpval = re.sub(r"\(\d{1,2}\)", '', tmpval)
            pass

        tmpval = self.data_cleanup_basic(tmpval)
        tmpval = self.replace_numfmt(tmpval, numfmt)
        tmpval = tmpval.replace('%', '')
        if isneg and tmpval:
            tmpval = '-%s'%(tmpval)
            pass

        val = ''
        if tmpval:
            tmp_num_fmt = self.get_poss_numfmt(tmpval, numfmt)
            t = tmpval.split('.') if (not tmp_num_fmt) else tmpval.split(',')
            isok = True
            for elm in t:
                if not self.isNumber(elm):
                    isok = False
                    break
                pass
            if isok:
                val = tmpval[:]
            pass

        return val

    def __get_num_cleanup(self, inumstr, numfmt=None):
        onumstr = inumstr.strip()
        onumstr = onumstr.replace('(', '')
        onumstr = onumstr.replace(')', '')
        onumstr = self.replace_numfmt(onumstr, numfmt)
        return onumstr

    def __cleanup_currency(self, value):
        val = value.strip()
        val = val.replace('USD', '')
        val = val.replace('Rs.', '')
        val = val.replace('EUR', '')
        val = val.replace('AUD', '')
        val = val.replace('GBP', '')
        return val

    def __value_cleanup(self, value):
        alpha      = {',':1, '.':1, '':1, '-':1, '(':1, ')':1}
        num         = {'0':1,'1':1,'2':1,'3':1,'4':1, '5':1,'6':1,'7':1, '8':1, '9':1}
        
        val = value.strip()
        val = val.replace('&amp;', '&')
        val = val.replace('&#8364;', '')
        val = val.replace('&#65510;', '')
        val = val.replace('\xef\xbf\xa6', '')
        val = val.replace('\xe2\x82\xa9', '')
        val = val.replace('\xe2\x80\x83', '')
        val = val.replace(' ', '')
        val = val.rstrip('%')
        val = val.replace('USD', '')
        val = val.replace('Rs.', '')
        fnum    = 0
        for i, c in enumerate(val):
            if fnum == 1 and (c not in num and c not in alpha):
                fnum_end    = 0
                for c1 in val[i+1:]:
                    if c1 in num:
                        fnum_end =1
                        break
                if fnum_end == 1:
                    val = ''
                    break
            if c in num:
                fnum    = 1
            
        val = re.sub('\~|[a-zA-Z]+', '',val, re.I)
        val = val.replace('US$', '')
        val = val.replace('US $', '')
        val = val.replace('$', '')
        val = val.replace("'", '')
        val = val.replace('\xc2\xa5','')
        val = val.replace('\xc2\xa0','')
        val = self.__cleanup_currency(val)
        val = self.data_cleanup_blank_number(val)
        val = val.strip()
        return val

    def check_non_ascii_char(self, value):
        non_ascii = []
        tmp_li = []
        for c in value:
            if 0<ord(c)<127:
               tmp_li.append(c)
            else:
               if tmp_li:
                  non_ascii.append(''.join(tmp_li))
               tmp_li = []
        if tmp_li:
           non_ascii.append(''.join(tmp_li))
        if len(non_ascii)>1:
           return value
        #print [value, non_ascii]
        if non_ascii:
            return non_ascii[0]
        else:
            return ''

    def value_cleanup(self, value):
        val = self.__value_cleanup(value)
        return val

    def check_for_invalid_numformat(self, value):
        new_indx = []
        for cnt,v in enumerate(value):
            if v in ['.',',']:
                new_indx.append(v) 
        if re.search('\.\,\.',''.join(new_indx)):
           return ''
        return value  

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
         fnum   = form_str.format(string)
         return form_str.format(string)


    def get_value_cleanup(self, value, numfmt=None):
        try:
            tmpx    = ''.join(value.strip().split()).replace(',', '').replace('$', '')
            if tmpx:
                if '(' == tmpx[0] and ')' == tmpx[-1]:
                    tmpx    = tmpx.strip('(').strip(')')
                    tmpx    = float(tmpx)
                    return self.convert_floating_point(tmpx *-1).replace(',', '')
                else:
                    tmpx    = float(tmpx)
                    return self.convert_floating_point(tmpx).replace(',', '')
            
        except:
            pass
        if '.' in value and ',' in value and 0:
            if value.index('.') < value.index(','):
                org_value = value
                clval = Gobj.GetCleanValue(value)
                if org_value.strip().startswith('(') and org_value.strip().endswith(')'):
                    clval = '-' + clval
                elif org_value[0].strip() == '-' and '-' not in clval:
                    clval = '-' + clval
                try:
                    xxx = float(clval)
                except:
                    clval = ''
                return clval
        elif ',' in value and '.' not in value and 0:
            if len(value[value.index(',')+1:]) == 2:
                org_value = value
                clval = Gobj.GetCleanValue(value)
                if org_value.strip().startswith('(') and org_value.strip().endswith(')'):
                    clval = '-' + clval
                elif org_value[0].strip() == '-' and '-' not in clval:
                    clval = '-' + clval
                try:
                    xxx = float(clval)
                except:
                    clval = ''
                return clval
        if '.' in value and 0:
           #623.984        
           if len(value.split('.')) >= 2:
              # multiple dots
              last_sp_elm = value.split('.')[-1]
              num_count = 0
              for e in last_sp_elm:
                  if e in '0123456789':
                     num_count += 1
              if (num_count > 2):
                 value = value.replace('.', '')
 
        val = ''
        #print value
        #value = self.check_for_invalid_numformat(value)
        #print value
        if re.search('&#8211;\s\d+',value):
           value = value.replace('&#8211;','-') 
        value = value.replace('>','') 
        value = value.replace('negative','-') 
        value = value.replace('*','') 
        tmpval0 = self.__value_cleanup(value)
        #print 'tmpval0 ', [tmpval0]
        isneg = self.isnegvalue(tmpval0)
        isminus = self.isminusvalue(tmpval0)
        tmpval = value.strip()
        tmpval = self.check_non_ascii_char(tmpval)
        tmpval = self.data_cleanup_basic(tmpval)
        tmpval = self.data_cleanup_neg(tmpval)
        tmpval = self.__value_cleanup(tmpval)
        #tmpval = self.__get_num_cleanup(tmpval, numfmt)
        tmpval = self.data_cleanup_neg(tmpval)
        tmp_num_fmt = self.get_poss_numfmt(tmpval, numfmt)
        tmpval = self.replace_numfmt_type(tmpval, tmp_num_fmt)
        #tmpval = self.get_decimal_cleanup(tmpval)
        #print 'tmpval ', [tmpval]
        if tmpval:
            #tmp_num_fmt = self.get_poss_numfmt(tmpval, numfmt)
            #t = tmpval.split('.') if (not tmp_num_fmt) else tmpval.split(',')
            tstr = tmpval.strip()
            tstr = tstr.replace('.', '')
            tstr = tstr.replace(',', '')
            t = [tstr]
            isok = True
            #print [tmpval, tstr, tmp_num_fmt]
            for elm in t:
                if not self.isNumber(elm):
                    isok = False
                    break
            if isok:
                val = tmpval[:]
                if tmp_num_fmt: val = val.replace(',', '.')       # normalize number
                if isneg and (not isminus):
                    val = '-%s'%(val)
        try:
            xxx = float(val)
            val = str(xxx)
        except:
            val = ''
        return val

    def get_value_cleanup_currency(self, value, currencyfmt=''):
        numfmt = self.getnumfmt(currencyfmt)
        val = self.get_value_cleanup(value, numfmt)
        try:
            xxx = float(val)
        except:
            val = ''
        return val

    def debug(self):
        return

if __name__ == '__main__':
    obj = numbercleanup()
    x   = '\xc2\xa54,413,030'
    x = '\xc2\xa023,052'
    x = '(137,123) million~'
    x = '16.319%'
    x   = '10-extra'
    print [obj.get_value_cleanup(x)]
