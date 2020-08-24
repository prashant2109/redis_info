import json, sys, os

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class WebAPI():
    def process(self, cmd_id, ijson):
        res     = []

        if cmd_id == 6: #insert Model
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.insert_model(ijson)

        elif cmd_id == 7: #read template 
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.read_templates(ijson)

        elif cmd_id == 8: #read sheets 
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.read_sheets(ijson)
             
        elif cmd_id == 9: #read sheet data 
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.sheet_gridinfo(ijson)

        return res #json.dumps(res)


if __name__ == '__main__':
    obj = WebAPI()
    try:
        ijson   = json.loads(sys.argv[1])
        if not isinstance(ijson, dict):
                print xxxx
    except:
        cmd_id  = int(sys.argv[1])
        ijson   =  {"cmd_id":cmd_id}
        if len(sys.argv) > 2:
            tmpjson = json.loads(sys.argv[2])
            ijson.update(tmpjson)
    cmd_id  = int(ijson['cmd_id'])
    if ijson.get('PRINT') != 'Y':
        disableprint()
    res = obj.process(cmd_id, ijson)
    enableprint()
    print res
