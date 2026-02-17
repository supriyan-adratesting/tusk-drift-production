from flask import request,jsonify
from src import app

from flask_jwt_extended import create_access_token
from flask_jwt_extended import create_refresh_token
from flask_jwt_extended import JWTManager
from src.models.mysql_connector import execute_query,update_query,update_query_last_index

from datetime import timedelta
import jwt
import os
from functools import wraps

JWT_SECRET_KEY = os.environ.get('JWT_KEY')
JWT_ACCESS_TOKEN_EXPIRES = int(os.environ.get('JWT_ACCESS_TOKEN_EXPIRES'))
JWT_REFRESH_TOKEN_EXPIRES = int(os.environ.get('JWT_REFRESH_TOKEN_EXPIRES'))

app.config["JWT_COOKIE_SECURE"] = True
app.config["JWT_SECRET_KEY"] = JWT_SECRET_KEY  # Change this in your code!
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRES)
app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(minutes=JWT_REFRESH_TOKEN_EXPIRES)

JWTManager(app)


def token_required(f):    
    # @wraps(f)
    def decorator(*args, **kwargs):                
        if 'Authorization' in request.headers:
            bearer_token = request.headers["Authorization"].split(" ")
            if len(bearer_token) == 2 :
                if bearer_token[0] == "Bearer":
                    token = bearer_token[1]                    
                    token_auth_result = token_authentication(token)                                                                                
                    
                    if token_auth_result["status_code"] == 200:        
                        query = "SELECT access_token FROM user_token  where access_token = %s "            
                        values = (token,)
                        rs = execute_query(query,values)             
                        if len(rs) == 0: 
                            return (jsonify({"message": "Session has expired"}), 401)  
                        else:                            
                            return f(*args, **kwargs)
                
                        # return f(*args, **kwargs)
                    elif token_auth_result["status_code"] == 401:
                        return (jsonify({"message": "Invalid token. Please try again."}), 401)  
                    else:
                        return (jsonify({"message": "Invalid Token"}), 401)
            else:
                return (jsonify({"message": "Invalid Authorization"}), 401)
        else:
            return (jsonify({"message": "Invalid Authorization"}), 401)          
    return decorator

def chat_bot_token_required(f):    
    @wraps(f)
    async def decorator(*args, **kwargs):                
        if 'Authorization' in request.headers:
            bearer_token = request.headers["Authorization"].split(" ")
            if len(bearer_token) == 2 :
                if bearer_token[0] == "Bearer":
                    token = bearer_token[1]                    
                    token_auth_result = token_authentication(token)                                                                                
                    
                    if token_auth_result["status_code"] == 200:        
                        query = "SELECT access_token FROM user_token  where access_token = %s "            
                        values = (token,)
                        rs = execute_query(query,values)             
                        if len(rs) == 0: 
                            return (jsonify({"message": "Session has expired"}), 401)  
                        else:                            
                            return await f(*args, **kwargs)
                
                        # return f(*args, **kwargs)
                    elif token_auth_result["status_code"] == 401:
                        return (jsonify({"message": "Invalid token. Please try again."}), 401)  
                    else:
                        return (jsonify({"message": "Invalid Token"}), 401)
            else:
                return (jsonify({"message": "Invalid Authorization"}), 401)
        else:
            return (jsonify({"message": "Invalid Authorization"}), 401)          
    return decorator

# def get_jwt_auth_tokens(email_id):
#     access_token = create_access_token(identity=email_id,fresh=True)  
#     refresh_token = create_refresh_token(identity=email_id)  
#     return {"access_token":access_token,"refresh_token":refresh_token}
    

def get_renewal_access_token(email_id):
    access_token = create_access_token(identity=email_id)
    return access_token

def token_authentication(token):    
    token_status = 0
    try:                
        decoded_data = jwt.decode(jwt=token,key=JWT_SECRET_KEY,algorithms=["HS256"])      
        # print(decoded_data)        
        # print("Token " + token)
        if 'sub' in decoded_data:
            email_id = decoded_data["sub"]                         
            return {"email_id":email_id,"status_code":200}
    except Exception as error:
        print(error)
        if str(error) == "Signature has expired" or str(error) == "Invalid header padding":
            token_status = 401
            return {"status_code":token_status}
        else:
            token_status = -1             
    return {"status_code":token_status}


def get_jwt_access_token(user_id,email_id):
    token_result = {}
    access_token = ''
    try:                
        access_token = create_access_token(identity=email_id,fresh=True)  
        refresh_token = create_refresh_token(identity=email_id) 
        # query = "SELECT * FROM user_token  where user_id = %s "
        # values = (user_id,)        
        # rs = execute_query(query,values)
        # if len(rs) > 0:                               
        #     query = "UPDATE user_token set access_token=%s,refresh_token=%s  where user_id = %s "
        #     values = (access_token, refresh_token,user_id,)
        #     row_count = update_query(query,values)                    
        #     # token_result["access_token"] = access_token                        
        # else:
        query = "INSERT INTO user_token (user_id,access_token,refresh_token) VALUES(%s,%s,%s) "
        values = (user_id, access_token, refresh_token,)
        row_count = update_query(query,values)            
        token_result["access_token"] = access_token             
        token_result["status"] = "success"  
    except Exception as error:
        print(error)
        token_result["access_token"] = '' 
        token_result["status"] = str(error)          
    return token_result

def get_jwt_forgot_pwd_token(email_id):
    token_result = {}
    access_token = ''
    try:                
        access_token = create_access_token(identity=email_id,fresh=True,expires_delta=timedelta(seconds=86400))                     
        token_result["access_token"] = access_token             
        token_result["status"] = "success"  
    except Exception as error:
        print(error)
        token_result["access_token"] = '' 
        token_result["status"] = str(error)          
    return token_result

def get_user_token(request):
    token = ""
    if 'Authorization' in request.headers:
            bearer_token = request.headers["Authorization"].split(" ")
            if len(bearer_token) == 2 :
                if bearer_token[0] == "Bearer":
                    token = bearer_token[1]                    
                    return token_authentication(token)
                else:
                    return {"status_code":401,"status":"Bearer Token Required"}     
            else:
                return {"status_code":401,"status":"Bearer Token Required "}     
    else:
         return {"status_code":401,"status":"Authorization Required"}            
    

# def login_access_token(user_id):
#     try:        
#         query = "SELECT * FROM jwt_auth  where user_id = %s "
#         values = (user_id)
#         rs = execute_query(query,values)
#         if len(rs) > 0:
#             jwt_auth_token = get_jwt_auth_tokens(user_id)
#             access_token = jwt_auth_token["access_token"] 
#             refresh_token = jwt_auth_token["refresh_token"]                    
#             query = "UPDATE jwt_auth set access_token=%s,refresh_token=%s  where user_id = %s "
#             values = (access_token, refresh_token,user_id)
#             rc = update_query(query,values)        
#             print(rc)
#             print(access_token)
#             # token_validation(access_token)
#             return jsonify({"access_token":access_token})
#         else:
#             return jsonify({"message" : "Unautherized User"})
#     except Exception as error:
#         print(error)
#         return jsonify({"error":error})
    
# def user_signup_access_token(user_id):
#     try:
#         # query = "SELECT * FROM jwt_auth  where user_id = %s "
#         # values = (user_id)
#         # rs = execute_query(query,values)        
#         # if len(rs) > 0:                    
#         #     return jsonify({"message":"user already exist"})
#         # else:
#         jwt_auth_token = get_jwt_auth_tokens(user_id)
#         access_token = jwt_auth_token["access_token"] 
#         refresh_token = jwt_auth_token["refresh_token"]     
#         query = "INSERT INTO jwt_auth (user_id,access_token,refresh_token) VALUES(%s,%s,%s) "
#         values = (user_id, access_token, refresh_token)
#         row_count = update_query(query,values)            
#         return jsonify({"access_token":access_token})
#     except Exception as error:
#         print(error)
#         return jsonify({"error":error})
