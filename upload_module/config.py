class Config:
    Template_storage = "/mnt/eMB_db/upload_module/model.db"
    Template_schema  = "CREATE TABLE IF NOT EXISTS Template_mgmt (template_id int(10) , template_name varchar(255), industry varchar(255), project varchar(255), user varchar(255), datetime DATETIME DEFAULT CURRENT_TIMESTAMP)"
    sheet_schema  = "CREATE TABLE IF NOT EXISTS sheet_mgmt (template_id int(10) , template_name varchar(255), sheet_id int(10), sheet_name varchar(255), user varchar(255), datetime DATETIME DEFAULT CURRENT_TIMESTAMP)"
    model_schema = "CREATE TABLE IF NOT EXISTS model_info (template_id int(10), sheet_id int(10), row int(10), col int(10), value text, taxonomy varchar(255), formular_str varchar(255), cell_alph varchar(255))"
    mapping_schema = "CREATE TABLE IF NOT EXISTS mapping_mgmt (template_id int(10), sheet_id int(10), taxonomy varchar(255), doc_id int(10), group_id int(10), row_id int(10))"
    tas_company_db = '/mnt/eMB_db/%s/%s/tas_company.db'
    company_list_db = '/mnt/eMB_db/company_info/compnay_info.db' 
    doc_wise_db = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/newvalidation_test_db/{0}/{1}.db'
