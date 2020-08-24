import re

class GetCleanValue:
    def __init__(self):
        pass

    def GetCleanValue(self,num_str):
        org_num_str = num_str
        num_str = re.sub(r"&#\d+;", "", num_str)
        num_str = re.sub(r"(\.|,)$", "", num_str)
        num_str = re.sub(r"^((?:-)*[0-9]{1,3}) ([0-9]{3}(\.)[0-9]{1,2}$)", r"\1,\2", num_str)
        num_str = re.sub(r"^((?:-)*[0-9]{1,3}) ([0-9]{3}$)", r"\1,\2", num_str)
        val_str_re = re.findall(r"[0-9.,' ]+", num_str)
        #print [val_str_re]
        val_str = ""    
        if val_str_re:
           for each in val_str_re:
               if each.strip():
                  val_str = each.strip()
                  break
               
        #print "val_str: ", val_str , num_str, val_str_re
        dot_num = re.search("\.\d\d\d", val_str)
        comma_num = re.search("(?![a-z,A-Z]),\d{1,2}(?:\s|$)", val_str)
        money_num_str_3decimal= re.findall("^[0](?:\.[0-9]{3})$", val_str)
        money_num_str_USD = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[.][0-9]{1,2})?|(?:[,][0-9]{3})*(?:\.[0-9]{1,2})?)$", val_str)
        money_num_str_CHF = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[.][0-9]{1,2})?|(?:[' ][0-9]{3})*(?:\.[0-9]{1,2})?)$", val_str)
        money_num_str_EUR = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[,][0-9]{1,2})?|(?:[.][0-9]{3})*(?:,[0-9]{1,2})?)$", val_str)
        money_num_str_INR = re.findall("^[0-9]{1,2}(?:[0-9]*(?:[.][0-9]{1,2})?|(?:[,][0-9]{2})*(?:,[0-9]{3}\.[0-9]{1,2})?)$", val_str)
        #print "++"+val_str+"++"
        #print "money_num_str_USD: ", money_num_str_USD  
        if money_num_str_3decimal:
            #print '1'
            return val_str
        elif (comma_num or money_num_str_EUR):
            money_num_str = re.sub(r"\.", "", money_num_str_EUR[0])
            money_num_str = re.sub(r",", ".", money_num_str)
            return money_num_str
        #'''
        ## added from compare numbers
        elif money_num_str_EUR:
            if not float(money_num_str_EUR[0]) < 1:
               money_num_str = re.sub(r"\.", "", money_num_str_EUR[0])
            #money_num_str = re.sub(r",", ".", money_num_str)
            else:
                money_num_str = money_num_str_EUR[0]
            return money_num_str
        #'''
        elif money_num_str_CHF or money_num_str_USD or money_num_str_INR:
            money_num_str = re.sub(r"'|,| ", "", val_str)
            return money_num_str

        money_num_str_USD_3decimal = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[.][0-9]{1,3})?|(?:[,][0-9]{3})*(?:\.[0-9]{1,3})?)$", val_str)
        if money_num_str_USD_3decimal:
            money_num_str = re.sub(r"'|,", "", val_str)
            return money_num_str 

        #print "here - 1"     
        money_num_str_4decimal = re.findall("^[0-9]{1}(?:\.[0-9]{4})$", val_str)
        if money_num_str_4decimal:
            return val_str 
        return ""

    
    def GetCleanValue_flg(self, num_str, Eur_Curr):
        num_str = re.sub(r"&#\d+;", "", num_str)
        num_str = re.sub(r"(\.|,)$", "", num_str)
        num_str = re.sub(r"^((?:-)*[0-9]{1,3}) ([0-9]{3}(\.)[0-9]{1,2}$)", r"\1,\2", num_str)
        num_str = re.sub(r"^((?:-)*[0-9]{1,3}) ([0-9]{3}$)", r"\1,\2", num_str)
        val_str_re = re.findall(r"[0-9.,' ]+", num_str)
        if val_str_re:
            val_str = val_str_re[0]
        else:
            val_str = ""    
        #print val_str 
        dot_num = re.search("\.\d\d\d", val_str)
        comma_num = re.search("(?![a-z,A-Z]),\d{1,2}(?:\s|$)", val_str)
        #if comma_num==None:
         #  comma_num = ''
        money_num_str_3decimal= re.findall("^[0](?:\.[0-9]{3})$", val_str)
        money_num_str_USD = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[.][0-9]{1,2})?|(?:[,][0-9]{3})*(?:\.[0-9]{1,2})?)$", val_str)
        money_num_str_CHF = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[.][0-9]{1,2})?|(?:[' ][0-9]{3})*(?:\.[0-9]{1,2})?)$", val_str)
        money_num_str_EUR = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[,][0-9]{1,2})?|(?:[.][0-9]{3})*(?:,[0-9]{1,2})?)$", val_str)
        money_num_str_INR = re.findall("^[0-9]{1,2}(?:[0-9]*(?:[.][0-9]{1,2})?|(?:[,][0-9]{2})*(?:,[0-9]{3}\.[0-9]{1,2})?)$", val_str)
        ''' 
        print comma_num
        print money_num_str_3decimal
        print money_num_str_USD
        print money_num_str_CHF
        print money_num_str_EUR
        print money_num_str_INR   
        '''
        if money_num_str_3decimal:
        #    print '1'
            return val_str
        elif (comma_num and money_num_str_EUR):
            money_num_str = re.sub(r"\.", "", money_num_str_EUR[0])
            money_num_str = re.sub(r",", ".", money_num_str)
            return money_num_str
        elif money_num_str_EUR and Eur_Curr:
            money_num_str = re.sub(r"\.", "", money_num_str_EUR[0])
            return money_num_str
        elif money_num_str_EUR and not Eur_Curr:
            money_num_str = re.sub(r",", "", money_num_str_EUR[0])
            return money_num_str
        elif money_num_str_EUR:
            if not float(money_num_str_EUR[0]) < 1:
               money_num_str = re.sub(r"\.", "", money_num_str_EUR[0])
            #money_num_str = re.sub(r",", ".", money_num_str)
            else:
                money_num_str = money_num_str_EUR[0]
            return money_num_str
        elif money_num_str_CHF or money_num_str_USD or money_num_str_INR:
            money_num_str = re.sub(r"'|,", "", val_str)
            return money_num_str
        money_num_str_USD_3decimal = re.findall("^[0-9]{1,3}(?:[0-9]*(?:[.][0-9]{1,3})?|(?:[,][0-9]{3})*(?:\.[0-9]{1,3})?)$", val_str)
        if money_num_str_USD_3decimal:
            money_num_str = re.sub(r"'|,", "", val_str)
            return money_num_str 

        money_num_str_4decimal = re.findall("^[0-9]{1}(?:\.[0-9]{4})$", val_str)
        if money_num_str_4decimal:
            return val_str 
        return ""
 

if __name__=="__main__":
    obj = GetCleanValue()
    #val = obj.GetCleanValue('0.01 A')
    #val = obj.GetCleanValue('743,04')
    val = obj.GetCleanValue('Rs. 734,872.6')
    #val = obj.GetCleanValue_flg('333.999','')
    #val = obj.GetCleanValue_flg('Rs. 734,872.6','Rs.')
    #val = obj.GetCleanValue_flg('743,04','EUR')
    print "Clean Value: ", val  
