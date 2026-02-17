from src.models.mysql_connector import execute_query,update_query,update_query_last_index



def get_user_data(email_id):    
    user_data = {}
    try:  
        query = """SELECT users.user_id, users.is_active, users.payment_status, users.login_status, users.login_mode, users.user_pwd, users.email_id,
                    users.profile_image, user_role.user_role, users.city, users.country, users.pricing_category, users.first_name, users.last_name, 
                    users.email_active, users.login_count, users.country_code, users.contact_number, users.existing_pricing_key, users.gender, users.current_period_start, users.current_period_end
                    FROM users
                    INNER JOIN user_role ON user_role.role_id = users.user_role_fk
                    WHERE users.email_id = %s;"""
        values = (email_id,)
        rs = execute_query(query,values)                     
        if len(rs) > 0:                    
            user_data["is_exist"] = True
            user_data["user_id"] = rs[0]["user_id"]        
            user_data["is_active"] = rs[0]["is_active"]
            user_data["payment_status"] = rs[0]["payment_status"]
            user_data["login_status"] = rs[0]["login_status"]
            user_data["login_mode"] = rs[0]["login_mode"]
            user_data["user_pwd"] = rs[0]["user_pwd"]
            user_data["user_role"] = rs[0]["user_role"]
            user_data["email_id"] = rs[0]["email_id"]       
            user_data["city"] = rs[0]["city"]  
            user_data["country"] = rs[0]["country"]  
            user_data["first_name"] = rs[0]["first_name"]  
            user_data["last_name"] = rs[0]["last_name"]
            user_data["email_active"] = rs[0]["email_active"]
            user_data["profile_image"] = rs[0]["profile_image"]
            user_data["pricing_category"] = rs[0]["pricing_category"]
            user_data['login_count'] = rs[0]["login_count"]
            user_data['country_code'] = rs[0]["country_code"]
            user_data['contact_number'] = rs[0]["contact_number"]
            user_data['existing_pricing_key'] = rs[0]["existing_pricing_key"]
            user_data['gender'] = rs[0]["gender"]
            user_data['current_period_start'] = rs[0]["current_period_start"]
            user_data['current_period_end'] = rs[0]["current_period_end"]
        else:
            user_data["is_exist"] = False
            user_data["user_role"] = ""
            user_data["login_mode"] = ""
            user_data['email_active'] = ""
    except Exception as error:        
        user_data["is_exist"] = False
        user_data["user_role"] = ""
        print("Error in get_user_data(): ",error)        
    return user_data 

def get_sub_user_data(email_id):    
    user_data = {}
    try:  
        query = """SELECT su.sub_user_id, su.user_id, su.is_active, su.profile_image, su.payment_status, su.login_status, su.login_mode, su.user_pwd, 
                    su.email_id, su.city, su.country, ur.user_role, su.pricing_category, su.first_name, su.last_name, su.email_active, su.login_count, 
                    su.country_code, su.phone_number, su.existing_pricing_key 
                    FROM sub_users su INNER JOIN user_role ur ON ur.role_id = su.role_id 
                    WHERE su.email_id = %s;"""
        values = (email_id,)
        rs = execute_query(query,values)                     
        if len(rs) > 0:                    
            user_data["is_exist"] = True
            user_data["sub_user_id"] = rs[0]["sub_user_id"]
            user_data["user_id"] = rs[0]["user_id"]
            user_data["is_active"] = rs[0]["is_active"]
            user_data["payment_status"] = rs[0]["payment_status"]
            user_data["login_status"] = rs[0]["login_status"]
            user_data["login_mode"] = rs[0]["login_mode"]
            user_data["user_pwd"] = rs[0]["user_pwd"]
            user_data["user_role"] = rs[0]["user_role"]
            user_data["email_id"] = rs[0]["email_id"] 
            user_data["first_name"] = rs[0]["first_name"]
            user_data["city"] = rs[0]["city"]  
            user_data["country"] = rs[0]["country"]
            user_data["last_name"] = rs[0]["last_name"]
            user_data["email_active"] = rs[0]["email_active"]
            user_data["pricing_category"] = rs[0]["pricing_category"]
            user_data["profile_image"] = rs[0]["profile_image"]
            user_data['login_count'] = rs[0]["login_count"]
            user_data['country_code'] = rs[0]["country_code"]
            user_data['contact_number'] = rs[0]["phone_number"]
            user_data['existing_pricing_key'] = rs[0]["existing_pricing_key"]
        else:
            user_data["is_exist"] = False
            user_data["user_role"] = ""
            user_data["login_mode"] = ""
            user_data['email_active'] = ""
    except Exception as error:        
        user_data["is_exist"] = False
        user_data["user_role"] = ""
        print("Error in get_sub_user_data(): ",error)        
    return user_data 

def isUserExist(table,column,user_id):
    try:
        query = "select * from "+str(table)+"  where "+str(column)+" = %s"                
        values = (user_id,)        
        rs = execute_query(query,values)            
        if len(rs) > 0:                    
            return True       
        else:
            return False
    except Exception as error:
        print("Exception in isUserExist() ",error)
        return False    

def get_user_roll_id(user_role):
    roll_id = -1    
    user_data = {}
    query = "SELECT role_id FROM user_role  where user_role = %s "
    values = (user_role,)
    rs = execute_query(query,values)            
    if len(rs) > 0:                    
        roll_id = rs[0]["role_id"]    
    return roll_id

def api_json_response_format(status,message,error_code,data):
    result_json = {"success" : status,"message" : message,"error_code" : error_code,"data": data}
    return result_json
    




