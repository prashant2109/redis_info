import sys, json
def read_data(company_id, table_id, stype=[]):
    json_path   = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
    grid_data   = json.loads(open(json_path, 'r').read())
    ddict = grid_data.get('data', {})
    r_cs = ddict.keys()
    r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
    rc_d    = {}
    x1      = {}
    y1      = {}
    w1      = {}
    h1      = {}
    bbox_d  = {}
    tmpbbox_d   = {}
    tar = []
    for r_c in r_cs:
        row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
        rc_d.setdefault(row, {})[col]   =  ddict[r_c]
        if ddict[r_c]['ldr'] in stype:
            tar.append((r_c, ddict[r_c]['xml_ids']))
    return tar

if __name__ == '__main__':
    print read_data(sys.argv[1], sys.argv[2], sys.argv[3].split('#'))
        
            
