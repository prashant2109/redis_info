import MySQLdb, os
from TAS_Redisearch import *
db_str_glob = '172.16.20.7#root#tas123#Research_Analyser_AGRE'

def createSchema():
    rsObj.set_schema([NumericField('INDEX'), TextField('DOCID'), TextField('CATEGORY'), TextField('TAXONAME'), TextField('VALUE'), TextField('XML_REF'), TextField('REF_KEY')])

def pushToRedis(res_dict, rsObj):
    index = 0
    for doc_id, taxo_dict in res_dict.items():
        for (category, taxoname), val_list in taxo_dict.items():
            for each in val_list:
                value, xml_ref = each
                ref_key = '%s~%s'%(str(doc_id), xml_ref)
                print doc_id, taxoname, value
                rsObj.client.add_document(index, DOCID=doc_id, CATEGORY=category, TAXONAME=taxoname, VALUE=value, XML_REF=xml_ref, REF_KEY=ref_key)
                index += 1

def addToAutoSuggestionList(suggestion_list, db_name):
    rac_obj = TAS_Autocompleter(db_name, "172.16.20.220", 6380)
    rac_obj.add_to_autocomplete(suggestion_list)
    return

def addChunkData(fname, db_name):
    suggestion_list = []
    if os.path.exists(fname):
        with open(fname) as f1:
            for each in f1:
                suggestion_list.append(each.strip().split('\t')[-1])
    addToAutoSuggestionList(suggestion_list, db_name)  


def readData(fname, rsObj):
    with open(fname) as f1:
        index = 1
        for each in f1:
           doc_id, category, taxo, value, xml_ref, bbox, ref_key =  each.strip().split('\t')
           rsObj.client.add_document(index, DOCID=doc_id, CATEGORY=category, TAXONAME=taxo, VALUE=value, XML_REF=xml_ref, REF_KEY=ref_key, BBOX=bbox)
           index += 1
            

if __name__ == '__main__':
   rsObj = TAS_Redisearch("agreements_data", "172.16.20.220", 6380)
   rsObj.drop_index()
   rsObj = TAS_Redisearch("agreements_data", "172.16.20.220", 6380)
   rsObj.set_schema([NumericField('INDEX'), TextField('DOCID'), TextField('CATEGORY'), TextField('TAXONAME'), TextField('VALUE'), TextField('XML_REF'), TextField('REF_KEY'), TextField('BBOX')])
   readData('Agreements_Data_Bbox.txt', rsObj)
