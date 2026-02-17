from src import app
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from flask import request,jsonify
import datetime
from src.models.user_authentication import get_user_data,get_user_roll_id,api_json_response_format,isUserExist, get_sub_user_data
from src.controllers.jwt_tokens.jwt_token_required import get_user_token,get_jwt_access_token,get_jwt_forgot_pwd_token,token_authentication,get_renewal_access_token
from src.models.email.Send_email import sendgrid_mail
import os
import re
from src.controllers.payment.payment_process import create_trial_session
import time
import platform
from flask_executor import Executor
from src.models.background_task import BackgroundTask
from datetime import datetime as dt, date
from src.models.aws_resources import S3_Client
from io import BytesIO
from PyPDF2 import PdfReader
import docx2txt
import json
from  openai import OpenAI
import meilisearch
from langchain_openai import OpenAIEmbeddings
import uuid
from langchain_community.vectorstores import Meilisearch
import base64

# from src.controllers.professional.professional_process import vector_search_init

executor = Executor(app)
background_runner = BackgroundTask(executor)

s3_resume_folder_name = "professional/resume/"
s3_obj = S3_Client()

BUCKET_NAME = os.environ.get('PROMPT_BUCKET')
S3_BUCKET_NAME = os.environ.get('CDN_BUCKET')
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
g_resume_path = os.getcwd()
g_openai_completion_token_limit =int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))
g_summary_model_name = os.environ.get('SUMMARY_MODEL_NAME')
g_token_encoding_txt = os.environ.get('TOKEN_ENCODING_TEXT')
g_openai_token_limit =int(os.environ.get('OPENAI_TOKEN_LIMIT'))
WEB_APP_URI = os.environ.get('WEB_APP_URI')  
API_URI = os.environ.get('API_URI')  
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
PROFESSIONAL_TRIAL_PERIOD = os.environ.get('PROFESSIONAL_TRIAL_PERIOD')
EMPLOYER_TRIAL_PERIOD = os.environ.get('EMPLOYER_TRIAL_PERIOD')
PARTNER_TRIAL_PERIOD = os.environ.get('PARTNER_TRIAL_PERIOD')
PROFESSIONAL_BASIC_PLAN_ID = os.environ.get('PROFESSIONAL_BASIC_PLAN_ID')
EMPLOYER_BASIC_PLAN_ID = os.environ.get('EMPLOYER_BASIC_PLAN_ID')
PARTNER_BASIC_PLAN_ID = os.environ.get('PARTNER_BASIC_PLAN_ID')
PROFILE_INDEX = os.environ.get("PROFILE_INDEX")

def calculate_professional_profile_percentage(professional_id):
                    query = """
                    SELECT 
                    u.email_id, u.contact_number, u.country, u.city, u.profile_image, u.first_name, u.user_id,
                    pp.professional_resume, pp.preferences, pp.about, pp.video_url,
                    COUNT(DISTINCT pe.id) AS education_count,
                    COUNT(DISTINCT pex.id) AS experience_count,
                    COUNT(DISTINCT ps.id) AS skill_count,
                    COUNT(DISTINCT pl.id) AS language_count,
                    COUNT(DISTINCT pai.id) AS additional_info_count,
                    COUNT(DISTINCT psl.id) AS social_link_count
                FROM 
                    users u
                LEFT JOIN 
                    professional_profile pp ON u.user_id = pp.professional_id
                LEFT JOIN 
                    professional_education pe ON u.user_id = pe.professional_id
                LEFT JOIN 
                    professional_experience pex ON u.user_id = pex.professional_id
                LEFT JOIN 
                    professional_skill ps ON u.user_id = ps.professional_id
                LEFT JOIN 
                    professional_language pl ON u.user_id = pl.professional_id
                LEFT JOIN 
                    professional_additional_info pai ON u.user_id = pai.professional_id
                LEFT JOIN 
                    professional_social_link psl ON u.user_id = psl.professional_id
                WHERE 
                    u.user_id = %s
                GROUP BY 
                    u.user_id, u.email_id, u.contact_number, u.country, u.city, u.profile_image, u.first_name,
                    pp.professional_resume, pp.preferences, pp.about, pp.video_url;
                    """
                    values = (professional_id,)
                    result = execute_query(query, values)
                    value = 30
                    if result:
                        value = 30  # Base value
                        if result[0]['email_id'] is not None:                            
                            if result[0]['contact_number'] is not None:
                                if result[0]['country'] is not None:
                                    if result[0]['city'] is not None:
                                        if result[0]['profile_image'] is not None and result[0]['professional_resume'] is not None:
                                                value += 0  # Already included in base value
                                        else:
                                            if result[0]['professional_resume'] is None:
                                                value -= 5
                                            if result[0]['profile_image'] is None:
                                                value -= 5
                                    else:
                                        value -= 2 * 5
                                else:
                                    value -= 3 * 5
                            else:
                                value -= 4 * 5
                            
                        else:
                           value -= 5 * 5
                    else:
                        value -= 6 * 5

                    if result[0]['about'] is not None and result[0]['preferences'] is not None and result[0]['video_url'] is not None:
                                value += 10
                    else:
                        if result[0]['about'] is not None:
                                value += 3.33
                        if result[0]['preferences'] is not None:
                                value += 3.33
                        if result[0]['video_url'] is not None:
                                value += 3.33
                    if result[0]['education_count'] > 0 and result[0]['experience_count'] > 0 and result[0]['language_count'] > 0 and result[0]['skill_count'] > 0 and result[0]['additional_info_count'] > 0 and result[0]['social_link_count'] > 0:
                        value += 60
                    else:
                        if result[0]['education_count'] > 0:
                            value += 10
                        if result[0]['experience_count'] > 0:
                            value += 10
                        if result[0]['language_count'] > 0:
                            value += 10
                        if result[0]['skill_count'] > 0:
                            value += 10
                        if result[0]['additional_info_count'] > 0:
                            value += 10
                        if result[0]['social_link_count'] > 0:
                            value += 10
                    return {"value" : round(value), "first_name" : result[0]['first_name'], "email_id" : result[0]['email_id'], "user_id" : result[0]["user_id"]}

def user_login():         
    result_json = {}
    try:  
        username,password = "",""
        try:
            username = str(request.authorization["username"])
            password = str(request.authorization["password"])            

        except Exception as error:
            result_json = api_json_response_format(False,"Authorization required",204,{})  
            return result_json
        if username == "" or password == "":
            result_json = api_json_response_format(False,"Invalid login credentials",401,{}) 
            return result_json
        user_data = get_user_data(username)
        sub_user_data = get_sub_user_data(username)
        if user_data["is_exist"]:
            query = 'select last_login_time from users where email_id = %s'
            values = (username,)
            last_login_time = execute_query(query,values)
            if not last_login_time[0]['last_login_time'] == None:
                current_time = datetime.datetime.now()
                time_diff = current_time - last_login_time[0]['last_login_time']

                if time_diff > datetime.timedelta(hours=1):
                    query = 'update users set login_attempt = %s, last_login_time = %s   where email_id = %s'
                    values = (0,None,username,)
                    update_query(query,values)
                else:
                    remaining_time = int((datetime.timedelta(hours=1) - time_diff).total_seconds()//60)
                    result_json = api_json_response_format(False,f"Please wait for {remaining_time} minutes before retrying sign-in.",401,{})
                    return result_json


            if  user_data["login_mode"] == None or user_data["login_mode"] != "Manual":
                result_json = api_json_response_format(False,"If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple.",401,{}) 
                return result_json
            if password == user_data["user_pwd"]:
                query = 'update users set login_attempt = %s, last_login_time = %s   where email_id = %s'
                values = (0,None,username,)
                update_query(query,values)
                if user_data["email_active"] == "N":
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Email not verified"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"Login Failed - {event_properties['Email']} not Verified", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e)) 

                    
                    user_role = user_data["user_role"]
                    if user_role == "professional" or user_role == "employer" or user_role == "partner" or user_role == "employer_sub_admin":
                        token_result = get_jwt_access_token(user_data["user_id"],username) 
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]    
                            
                            result_json = api_json_response_format(False,"Please verify your email before logging in. A verification link has been sent to your registered email. If you don't see the email, please check your spam or junk folder.",401,{"user_role":user_data["user_role"],"access_token":access_token}) 
                            return result_json
                    else:
                        result_json = api_json_response_format(False,"Please verify your email before logging in. A verification link has been sent to your registered email. If you don't see the email, please check your spam or junk folder.",401,{}) 
                        return result_json

                token_result = get_jwt_access_token(user_data["user_id"],username) 
                if token_result["status"] == "success":
                    access_token =  token_result["access_token"]       
                    # query = "select user_id from users where email_id = %s"
                    # values = (username,)
                    # res = execute_query(query, values)
                    # if res:
                    #     user_id = res[0]['user_id']
                    user_role = user_data["user_role"]
                    user_id = user_data["user_id"]
                    # if user_role == "professional":
                    query = "select flag_status from users where user_id = %s"
                    values = (user_id,)
                    res = execute_query(query, values)
                    if res:
                        flag_status = res[0]['flag_status']              
                            
                    res_data = {"access_token" : access_token,"user_role":user_data["user_role"], "pricing_category" : user_data['pricing_category'], "payment_status" : user_data['payment_status'], "Registration_status": flag_status}

                    # else:
                    #     res_data = {"access_token" : access_token,"user_role":user_data["user_role"], "pricing_category" : user_data['pricing_category'], "payment_status" : user_data['payment_status']}
                    profile_percentage = calculate_professional_profile_percentage(user_data['user_id'])
                    query = "UPDATE users set profile_percentage = %s, login_status='IN',login_count = login_count + 1 where user_id = %s"
                    values = (profile_percentage['value'], user_data["user_id"],)
                    update_query(query,values) 
                    query = "DELETE FROM user_token WHERE updated_at <= DATE_SUB(NOW(), INTERVAL 2 DAY)"
                    values = ()
                    update_query(query,values) 
                    try:
                        event_properties = {    
                            '$distinct_id' : username, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),
                            'User Role' : user_data['user_role'],           
                            'Email' : username,
                            'Signin Mode' : user_data['login_mode'],
                            'Signin Status' : "Success"
                        }
                        event_name = "User Signin"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"{event_properties['Email']} has signed in Successfully.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Login successful!",0,res_data)                    
                else:
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Token Error"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"Unable to Sign in at {event_properties['Email']} because of a Token error.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,token_result["status"],401,{})                    
            else:
                query =  'select login_attempt from users where email_id= %s' 
                values = (username,)
                login_attempt = execute_query(query,values) 
                if not login_attempt[0]["login_attempt"] == 3:
                    query = 'UPDATE users SET login_attempt = login_attempt + 1 WHERE email_id = %s'
                    values = (username,)
                    update_query(query,values)
                    rem_attempts = 3 - login_attempt[0]['login_attempt']
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Incorrect Password"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"sign-in failed for {event_properties['Email']} due to an incorrect password", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,f"Sorry, that password is incorrect. {rem_attempts} attempts remaining.",401,{}) 
                else:
                    query = 'update users set last_login_time = %s where email_id = %s'
                    values = (datetime.datetime.now(),username,)
                    update_query(query,values)    
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Too many attempts"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"{event_properties['Email']} failed to signin due to too many attempts.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))                
                    result_json = api_json_response_format(False,"Too many attempts. Please try logging in again in one hour.",500,{}) 
        elif sub_user_data['is_exist']:
            query = 'select last_login_time from sub_users where email_id = %s'
            values = (username,)
            last_login_time = execute_query(query,values)
            if not last_login_time[0]['last_login_time'] == None:
                current_time = datetime.datetime.now()
                time_diff = current_time - last_login_time[0]['last_login_time']

                if time_diff > datetime.timedelta(hours=1):
                    query = 'update sub_users set login_attempt = %s, last_login_time = %s where email_id = %s'
                    values = (0, None, username,)
                    update_query(query,values)
                else:
                    remaining_time = int((datetime.timedelta(hours=1) - time_diff).total_seconds()//60)
                    result_json = api_json_response_format(False,f"Please wait for {remaining_time} minutes before retrying sign-in.",401,{})
                    return result_json
            if sub_user_data["user_pwd"] == '' or sub_user_data["user_pwd"] == None:
                try:
                    event_properties = {    
                            '$distinct_id' : username, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),
                            'User Role' : sub_user_data['user_role'],           
                            'Email' : username,
                            'Signin Mode' : sub_user_data['login_mode'],
                            'Signin Status' : "Failed",
                            'Error' : "Password not set"
                        }
                    event_name = "User Signin Failure"
                    background_runner.mixpanel_event_async(username,event_name,event_properties, f"Login failed for {event_properties['Email']} because the password is not set", user_data)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,f"Please check your email for the password setup link before signing in.",401,{})
                return result_json
            if password == sub_user_data["user_pwd"]:
                query = 'update sub_users set login_attempt = %s, last_login_time = %s where email_id = %s'
                values = (0, None, username,)
                update_query(query,values)
                if sub_user_data["email_active"] == "N":
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : sub_user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : sub_user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Email not verified"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"Sign-in failed for {event_properties['Email']} because the email is not verified", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e)) 
                    
                    user_role = user_data["user_role"]
                    if user_role == "professional" or user_role == "employer" or user_role == "partner" or user_role == "employer_sub_admin":
                        token_result = get_jwt_access_token(user_data["user_id"],username) 
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]      
                            result_json = api_json_response_format(False,"Please verify your email before logging in. A verification link has been sent to your registered email. If you don't see the email, please check your spam or junk folder.",401,{"user_role":user_data["user_role"],"access_token":access_token}) 
                            return result_json
                    else:
                        result_json = api_json_response_format(False,"Please verify your email before logging in. A verification link has been sent to your registered email. If you don't see the email, please check your spam or junk folder.",401,{}) 
                        return result_json

                token_result = get_jwt_access_token(sub_user_data["user_id"],username) 
                if token_result["status"] == "success":
                    access_token =  token_result["access_token"]                                
                    res_data = {"access_token" : access_token,"user_role":sub_user_data["user_role"], "pricing_category" : sub_user_data['pricing_category'], "payment_status" : sub_user_data['payment_status']}
                    query = "UPDATE sub_users set login_status = 'IN', login_count = login_count + 1 where sub_user_id = %s"
                    values = (sub_user_data["sub_user_id"],)
                    update_query(query,values) 
                    query = "DELETE FROM user_token WHERE updated_at <= DATE_SUB(NOW(), INTERVAL 2 DAY)"
                    values = ()
                    update_query(query,values) 
                    try:
                        event_properties = {    
                            '$distinct_id' : username, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),
                            'User Role' : sub_user_data['user_role'],           
                            'Email' : username,
                            'Signin Mode' : sub_user_data['login_mode'],
                            'Signin Status' : "Success"
                        }
                        event_name = "User Signin"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"{event_properties['Email']} Signed in Successfully.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Login successful!",0,res_data)                    
                else:
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : sub_user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : sub_user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Token Error"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"Unable to Sign in at {event_properties['Email']} because of a Token error.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,token_result["status"],401,{})                    
            else:
                query =  'select login_attempt from sub_users where email_id= %s' 
                values = (username,)
                login_attempt = execute_query(query,values) 
                if not login_attempt[0]["login_attempt"] == 3:
                    query = 'UPDATE sub_users SET login_attempt = login_attempt + 1 WHERE email_id = %s'
                    values = (username,)
                    update_query(query,values)
                    rem_attempts = 3 - login_attempt[0]['login_attempt']
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : sub_user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : sub_user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Incorrect Password"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"sign-in failed for {event_properties['Email']} due to an incorrect password", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,f"Sorry, that password is incorrect. {rem_attempts} attempts remaining.",401,{}) 
                else:
                    query = 'update sub_users set last_login_time = %s where email_id = %s'
                    values = (datetime.datetime.now(),username,)
                    update_query(query,values)    
                    try:
                        event_properties = {    
                                '$distinct_id' : username, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'User Role' : sub_user_data['user_role'],           
                                'Email' : username,
                                'Signin Mode' : sub_user_data['login_mode'],
                                'Signin Status' : "Failed",
                                'Error' : "Too many attempts"
                            }
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(username,event_name,event_properties, f"{event_properties['Email']} failed to signin due to too many attempts.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))                
                    result_json = api_json_response_format(False,"Too many attempts. Please try logging in again in one hour.",500,{})
        else:
            if not user_data['is_exist']:
                login_mode = user_data['login_mode']
                user_role = user_data['user_role']
                result_json = api_json_response_format(False,"Before signing in, please take a moment to sign up and create your account.",401,{})
            else:
                login_mode = sub_user_data['login_mode']
                user_role = sub_user_data['user_role']
                result_json = api_json_response_format(False,"Sorry, that email address is not associated with any account.",401,{})
            try:
                event_properties = {    
                        '$distinct_id' : username, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),         
                        'Email' : username,
                        'Login Mode' : login_mode,
                        'User Role' : user_role,  
                        'Signin Status' : "Failed",
                        'Error' : "Account not found"
                    }
                event_name = "User Signin Failure"
                background_runner.mixpanel_event_async(username,event_name,event_properties, f"{event_properties['Email']} Account not found. So, unable to Signin.", user_data)
            except Exception as e:  
                print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        print("Exception in user_login(): "+error)
        result_json = api_json_response_format(False,str(error),500,{})  
    finally:
        return result_json        
    
def token_mail_id():
    try:                                
        token_result = get_user_token(request)                                                        
        if token_result["status_code"] == 200:                                
            token_mail = {"email" : token_result["email_id"]}
            result_json = api_json_response_format(True,"success",0,token_mail)                
        else:
            result_json = api_json_response_format(False,token_result["status"],401,{})     
    except Exception as error:
        print("Exception in token_mail_id() ",error)
        result_json = api_json_response_format(False,str(error),500,{})            
    finally:
        return result_json

def renewal_access_token_process():            
    result_json = {}    
    try:                
        if 'Authorization' in request.headers:
            bearer_token = request.headers["Authorization"].split(" ")
            if len(bearer_token) == 2 :
                if bearer_token[0] == "Bearer":
                    req_token = bearer_token[1]                                                                                                                                           
                    query = "SELECT refresh_token FROM user_token  where access_token = %s "            
                    values = (req_token,)
                    rs = execute_query(query,values)             
                    if len(rs) > 0: 
                        refresh_token = rs[0]["refresh_token"]   
                        token_auth_result = token_authentication(refresh_token)                    
                        if token_auth_result["status_code"] == 200:
                            email_id = token_auth_result["email_id"]
                            access_token = get_renewal_access_token(email_id)  
                            query = "UPDATE user_token set access_token=%s where access_token = %s "
                            values = (access_token,req_token,)
                            update_query(query,values) 
                            res_data = {"access_token" : access_token}
                            result_json = api_json_response_format(True,"token updated",0,res_data)                                                                                  
                        elif token_auth_result["status_code"] == 401:
                            return (jsonify({"message": "Session has expired"}), 401)   
                    else:
                        return (jsonify({"message": "Session has expired"}), 401)                      
            else:
                result_json = api_json_response_format(False,"Invalid Authorization",401,{})  
        else:
            result_json = api_json_response_format(False,"Invalid Authorization",401,{})
        return result_json     
        
    except Exception as error:        
        print("Exception in renewal_access_token_process() ",error)
        result_json = api_json_response_format(False,str(error),500,{})  
        return result_json           
    
    
def forgot_password():
    try:     
                                     
        req_data = request.get_json() 
        if 'email_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json        
        user_email_id = req_data['email_id']          
        user_data = get_user_data(user_email_id)
        sub_user_data = get_sub_user_data(user_email_id)
        if user_data["is_exist"]:  
            if user_data["login_mode"] == "Manual": 
                token_result = get_jwt_forgot_pwd_token(str(user_data["user_pwd"])+"######"+str(user_email_id)) 
                if token_result["status"] == "success":                
                    access_token = token_result["access_token"] 
                    subject = "Forgot password"                    
                    recipients = [user_email_id]                    
                    index = open(os.getcwd()+"/templates/Forget_Password.html",'r').read()
                    index = index.replace("{{link}}",API_URI+"/reset_password_token_validation?eded128fdf05f4c0a2e29d2b121e5c710b9bd54dbcd98a64a9b02ab6e2564115="+access_token)
                    body = index
                    manual_gen_link = API_URI+"/reset_password_token_validation?eded128fdf05f4c0a2e29d2b121e5c710b9bd54dbcd98a64a9b02ab6e2564115="+access_token
                    print(f"Manually genarated link: {manual_gen_link}")
                    sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Forgot Password")
                    result_json = api_json_response_format(True,"A link has been sent to your registered email. Please follow the link to reset your password. If you don't see the email, please check your spam or junk folder.",0,{})   
                    try:
                        event_properties = {    
                            '$distinct_id' : user_email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'is_email_sent' : "Yes",
                            'Email' : user_email_id
                        }
                        event_name = "Forgot Password"
                        background_runner.mixpanel_event_async(user_email_id,event_name,event_properties, f"{event_properties['Email']} selected Forgot Password.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    return result_json
                else:
                    try:
                        event_properties = {    
                            '$distinct_id' : user_email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'is_email_sent' : "No",
                            'Email' : user_email_id,
                            'Error' : "Token error"
                        }
                        event_name = "Forgot Password Error"
                        background_runner.mixpanel_event_async(user_email_id,event_name,event_properties, f"{event_properties['email']} is unable to change the forgotten password due to token error.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,token_result["status"],401,{})  
            else:
                try:
                    event_properties = {    
                        '$distinct_id' : user_email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'is_email_sent' : "No",
                        'Email' : user_email_id,
                        'Error' : "User signed up via social account."
                    }
                    event_name = "Forgot Password Error"
                    background_runner.mixpanel_event_async(user_email_id,event_name,event_properties, f"Forgot Password error: {event_properties['Email']} was registered via a social account", user_data)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple.",401,{}) 
        elif sub_user_data["is_exist"]:  
            if sub_user_data["login_mode"] == "Manual": 
                token_result = get_jwt_forgot_pwd_token(str(sub_user_data["user_pwd"])+"######"+str(user_email_id)) 
                if sub_user_data['user_pwd'] == '' or sub_user_data['user_pwd'] == None:
                    return api_json_response_format(False,f"You haven't set up your password yet. Please check your email for the link to create your password.",401,{})
                if token_result["status"] == "success":                
                    access_token = token_result["access_token"] 
                    subject = "Forgot password"                    
                    recipients = [user_email_id]                    
                    index = open(os.getcwd()+"/templates/Forget_Password.html",'r').read()
                    index = index.replace("{{link}}",API_URI+"/reset_password_token_validation?eded128fdf05f4c0a2e29d2b121e5c710b9bd54dbcd98a64a9b02ab6e2564115="+access_token)
                    body = index
                    sendgrid_mail(SENDER_EMAIL, recipients, subject, body, "Forgot Password")
                    result_json = api_json_response_format(True,"A link has been sent to your registered email. Please follow the link to reset your password. If you don't see the email, please check your spam or junk folder.",0,{})   
                    try:
                        event_properties = {    
                            '$distinct_id' : user_email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'is_email_sent' : "Yes",
                            'Email' : user_email_id
                        }
                        event_name = "Forgot Password"
                        background_runner.mixpanel_event_async(user_email_id,event_name,event_properties, f"{event_properties['Email']} Chose Forgot Password.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    return result_json
                else:
                    try:
                        event_properties = {    
                            '$distinct_id' : user_email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'is_email_sent' : "No",
                            'Email' : user_email_id,
                            'Error' : "Token error"
                        }
                        event_name = "Forgot Password Error"
                        background_runner.mixpanel_event_async(user_email_id,event_name,event_properties, f"{event_properties['Email']}cannot change the forgotten password due to a token error.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,token_result["status"],401,{})  
            else:
                try:
                    event_properties = {    
                        '$distinct_id' : user_email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'is_email_sent' : "No",
                        'Email' : user_email_id,
                        'Error' : "User signed up via social account."
                    }
                    event_name = "Forgot Password Error"
                    background_runner.mixpanel_event_async(user_email_id,event_name,event_properties,  f"Forgot Password error: {event_properties['Email']} was registered via a social account", user_data)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple.",401,{}) 
        else:
            try:
                event_properties = {    
                    '$distinct_id' : user_email_id, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'is_email_sent' : "No",
                    'Email' : user_email_id,
                    'Error' : "User account not exists."
                }
                event_name = "Forgot Password Error"
                background_runner.mixpanel_event_async(user_email_id,event_name,event_properties, f"Forgot password error came because of {event_properties['Email']} not exists.", user_data)
            except Exception as e:  
                print("Error in mixpanel_event_log : %s",str(e))
            if not user_data['is_exist']:
                result_json = api_json_response_format(False,"Our records show that the user does not exist. Please signup to use the platform",401,{})
            elif not sub_user_data['is_exist']:
                result_json = api_json_response_format(False,"Our records show that the user does not associates with any profile.", 401, {})           
    except Exception as error:
        print(error)
        result_json = api_json_response_format(False,str(error),500,{})            
    finally:
        return result_json


def update_password():
    result_json = {}
    try:                              
        req_data = request.get_json() 
        if 'email_id' not in req_data:
            result_json = api_json_response_format(False,"Please enter a valid email ID",204,{})  
            return result_json
        if 'user_pwd' not in req_data:
            result_json = api_json_response_format(False,"Please enter a password",204,{})  
            return result_json
        email_id = req_data['email_id']  
        user_pwd = req_data['user_pwd']                    
        user_data = get_user_data(email_id)
        sub_user_data = get_sub_user_data(email_id)
        if user_data["is_exist"]:  
            if user_data["login_mode"] == "Manual":   
                query = 'delete from user_token where user_id = %s'
                values = (user_data["user_id"],)
                update_query(query, values)           
                query = 'update users set user_pwd = %s where email_id = %s'
                values = (user_pwd, email_id,)
                row_count = update_query(query, values) 
                if row_count > 0:
                    try:
                        event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Password updation status' : "Success",
                            'Email' : email_id
                        }
                        event_name = "Password Updation"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Password updated Successfully for {event_properties['Email']}", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Your password has been updated successfully!",0,{})
                else:
                    try:
                        event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Password updation status' : "Failure",
                            'Email' : email_id,
                            'Error' : "Error in updating user password."
                        }
                        event_name = "Password Updation Error"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Password not updated for {event_properties['Email']}", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your password. We request you to retry.",500,{})
            else:
                try:
                    event_properties = {    
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Password updation status' : "Failure",
                        'Email' : email_id,
                        'Error' : "Error in updating user password. User signed up via social account."
                    }
                    event_name = "Password Updation Error"
                    background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Unable to update the {event_properties['Email']} password because, the user signed up via social account.", user_data)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple.",401,{})      
        elif sub_user_data["is_exist"]:  
            if sub_user_data["login_mode"] == "Manual":   
                query = 'delete from user_token where user_id = %s'
                values = (sub_user_data["sub_user_id"],)
                update_query(query, values)           
                query = 'update sub_users set user_pwd = %s where email_id = %s'
                values = (user_pwd, email_id,)
                row_count = update_query(query, values) 
                if row_count > 0:
                    try:
                        event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Password updation status' : "Success",
                            'Email' : email_id
                        }
                        event_name = "Password Updation"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Password Updated Successfully for {event_properties['Email']}", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Your password has been updated successfully!",0,{})
                else:
                    try:
                        event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Password updation status' : "Failure",
                            'Email' : email_id,
                            'Error' : "Error in updating user password."
                        }
                        event_name = "Password Updation Error"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"The password is not getting updating for {event_properties['Email']}.", user_data)
                    except Exception as e:  
                        print("Error in mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your password. We request you to retry.",500,{})
            else:
                try:
                    event_properties = {    
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Password updation status' : "Failure",
                        'Email' : email_id,
                        'Error' : "Error in updating user password. User signed up via social account."
                    }
                    event_name = "Password Updation Error"
                    background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"unable to update the {event_properties['Email']} password because, the user signed up via social account.", user_data)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple.",401,{})      
        else:
            try:
                event_properties = {    
                    '$distinct_id' : email_id, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'Password updation status' : "Failure",
                    'Email' : email_id,
                    'Error' : "Error in updating user password. User account not exists."
                }
                event_name = "Password Updation Error"
                background_runner.mixpanel_event_async(email_id,event_name,event_properties, "User account not exists so, unable to update the password", user_data)
            except Exception as e:  
                print("Error in mixpanel_event_log : %s",str(e))
            if not user_data['is_exist']:
                result_json = api_json_response_format(False,"Our records show that the user does not exist. Please signup to use the platform",401,{})
            elif not sub_user_data['is_exist']:
                result_json = api_json_response_format(False,"Our records show that the user does not associates with any profile.", 401, {})
            # result_json = api_json_response_format(False,"Our records show that the user does not exist. Please signup to use the platform",401,{})      
    except Exception as error:
        print("logout error "+str(error))
    finally:        
        return result_json

def user_logout():
    result_json = {}
    try:
        token_result = get_user_token(request)  
        if token_result["status_code"] == 200:                                
            login_status = 'OUT'
            user_data = get_user_data(token_result["email_id"]) 
            sub_user_data = get_sub_user_data(token_result["email_id"])
            if user_data['is_exist']:
                user_id = user_data["user_id"]
                query = 'update users set login_status = %s where user_id = %s'
            elif sub_user_data['is_exist']:
                user_id = sub_user_data["sub_user_id"]
                query = 'update sub_users set login_status = %s where sub_user_id = %s'
                user_data = sub_user_data
            values = (login_status, user_id,)
            query = 'update user_token set access_token = %s, refresh_token = %s where user_id = %s'
            values = ("", "", user_id,)
            update_query(query, values)
            try:
                event_properties = {    
                        '$distinct_id' : user_data["email_id"], 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),
                        'User Role' : user_data['user_role'],           
                        'Email' : user_data["email_id"],
                        'Signin Mode' : user_data['login_mode'],
                        'Sign out Status' : "Success"
                    }
                event_name = "User Sign out"
                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, f"{event_properties['Email']} Signed out Successfully", user_data)
            except Exception as e:  
                print("Error in mixpanel_event_log : %s",str(e))         
    except Exception as error:
        print("logout error "+str(error))
    finally:
        result_json = api_json_response_format(True,"Signed out sucessfully",0,{})
        return result_json
            

def check(email_id):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    if(re.fullmatch(regex, email_id)):
        return True
    else:
        return False

class DocumentWrapper:
    def __init__(self, document):
        self.document = document
        self.page_content = json.dumps(document)  # Serialize the document to a JSON string
        self.metadata = document  # Use the document itself as metadata
        self.id = str(uuid.uuid4())  # Generate a unique ID for each document

    def to_dict(self):
        return self.document
    
def vector_search_init(professional_id):
    try:
        profile = get_profile_search(professional_id)
        print("Loaded {} documents".format(len(profile)))

        index_name = PROFILE_INDEX

        # Store documents in Meilisearch
        embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")
        embedders = {
            "adra": {
                "source": "userProvided",
                "dimensions": 1536,
            }
        }
        embedder_name = "adra"
        wrapped_documents = [DocumentWrapper(doc) for doc in profile]
        for doc in wrapped_documents:
            if not hasattr(doc, 'id') or doc.id is None:
                raise ValueError(f"Document {doc} is missing an 'id' field.")
        vector_store = Meilisearch.from_documents(documents=wrapped_documents, 
                                                  embedding=embeddings,
                                                  embedders=embedders,
                                                  embedder_name=embedder_name,
                                                  index_name=index_name)
        print("Documents successfully stored in Meilisearch.")

    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as error:
        print(f"An error occurred while storing documents in Meilisearch: {error}")

def replace_empty_values(data):
    for item in data:
        for key, value in item.items():
            if value == 'N/A' or value == None:
                item[key] = ''
    return data

def replace_empty_values1(data):
    if data == 'N/A' or data == None:
        data = ''
    return data

def get_profile_search(professional_id):
    try:      
        profiles = []   
        query = '''
            SELECT 
                u.first_name,
                u.last_name,
                u.email_id,
                u.contact_number,
                u.country,
                u.state,
                u.city,
                p.about,
                p.preferences,
                p.video_url,
                p.expert_notes,
                pe.id AS experience_id,
                pe.company_name,
                pe.job_title,
                pe.start_month AS experience_start_month,
                pe.start_year AS experience_start_year,
                pe.end_month AS experience_end_month,
                pe.end_year AS experience_end_year,
                pe.job_description,
                pe.job_location,
                ed.id AS education_id,
                ed.institute_name,
                ed.degree_level,
                ed.specialisation,
                ed.start_month AS education_start_month,
                ed.start_year AS education_start_year,
                ed.end_month AS education_end_month,
                ed.end_year AS education_end_year,
                ed.institute_location,
                ps.id AS skill_id,
                ps.skill_name,
                ps.skill_level,
                pl.id AS language_id,
                pl.language_known,
                pl.language_level,
                pai.id AS additional_info_id,
                pai.title AS additional_info_title,
                pai.description AS additional_info_description,
                psl.id AS social_link_id,
                psl.title AS social_link_title,
                psl.url AS social_link_url,
                u2.profile_image
            FROM users AS u
            LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id
            LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id
            LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id
            LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id
            LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id
            LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id
            LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id
            LEFT JOIN users AS u2 ON u.user_id = u2.user_id
            WHERE u.user_id = %s
        '''

        values = (professional_id,)
        profile_result = execute_query(query, values)

        profile_dict = {
            'professional_id': professional_id,
            'first_name': '',
            'last_name': '',
            'email_id': '',
            'contact_number': '',
            'city': '',
            'about': '',
            'preferences': '',
            'video_url': '',
            'expert_notes': '',
            'education': [],
            'skills': [],
            'experience': [],
            'languages': [],
            'social_links': [],
            'additional_info': []
        }

        if profile_result:
            profile_dict['first_name'] = replace_empty_values1(profile_result[0]['first_name'])
            profile_dict['last_name'] = replace_empty_values1(profile_result[0]['last_name'])
            profile_dict['email_id'] = replace_empty_values1(profile_result[0]['email_id'])
            profile_dict['contact_number'] = replace_empty_values1(profile_result[0]['contact_number'])
            profile_dict['city'] = replace_empty_values1(profile_result[0]['city'])
            profile_dict['about'] = replace_empty_values1(profile_result[0]['about'])

            education_data = {}
            skills_data = {}
            experience_data = {}
            languages_data = {}
            social_links_data = {}
            additional_info_data = {}
            education_ids = set(row['education_id'] for row in profile_result)
            skill_ids = set(row['skill_id'] for row in profile_result)
            language_ids = set(row['language_id'] for row in profile_result)
            experience_ids = set(row['experience_id'] for row in profile_result)
            social_link_ids = set(row['social_link_id'] for row in profile_result)
            additional_info_ids = set(row['additional_info_id'] for row in profile_result)

            MAX_EDUCATION_ENTRIES = len(education_ids)
            MAX_SKILLS_ENTRIES = len(skill_ids)
            MAX_LANGUAGES_ENTRIES = len(language_ids)
            MAX_EXPERIENCE_ENTRIES = len(experience_ids)
            MAX_SOCIAL_LINKS_ENTRIES = len(social_link_ids)
            MAX_ADDITIONAL_INFO_ENTRIES = len(additional_info_ids)

            for row in profile_result:
                # Education
                if len(profile_dict['education']) < MAX_EDUCATION_ENTRIES and row['education_id'] not in {edu['id'] for edu in profile_dict['education']}:
                    education_data = {
                        'id': row['education_id'],
                        'institute_name': replace_empty_values1(row['institute_name']),
                        'degree_level': replace_empty_values1(row['degree_level']),
                        'specialisation': replace_empty_values1(row['specialisation']),
                        'education_start_month': replace_empty_values1(row['education_start_month']),
                        'education_start_year': replace_empty_values1(row['education_start_year']),
                        'education_end_month': replace_empty_values1(row['education_end_month']),
                        'education_end_year': replace_empty_values1(row['education_end_year']),
                        'institute_location': replace_empty_values1(row['institute_location'])
                    }
                    profile_dict['education'].append(education_data)

                # Skills
                if len(profile_dict['skills']) < MAX_SKILLS_ENTRIES and row['skill_id'] not in {skill['id'] for skill in profile_dict['skills']}:
                    skills_data = {
                        'id': row['skill_id'],
                        'skill_name': replace_empty_values1(row['skill_name']),
                        'skill_level': replace_empty_values1(row['skill_level'])
                    }
                    profile_dict['skills'].append(skills_data)

                # Experience
                if len(profile_dict['experience']) < MAX_EXPERIENCE_ENTRIES and row['experience_id'] not in {exp['id'] for exp in profile_dict['experience']}:
                    experience_data = {
                        'id': row['experience_id'],
                        'company_name': row['company_name'],
                        'job_title': row['job_title'],
                        'experience_start_month': replace_empty_values1(row['experience_start_month']),
                        'experience_start_year': replace_empty_values1(row['experience_start_year']),
                        'experience_end_month': replace_empty_values1(row['experience_end_month']),
                        'experience_end_year': replace_empty_values1(row['experience_end_year']),
                        'job_description': replace_empty_values1(row['job_description']),
                        'job_location': replace_empty_values1(row['job_location'])
                    }
                    profile_dict['experience'].append(experience_data)

                # Languages
                if len(profile_dict['languages']) < MAX_LANGUAGES_ENTRIES and row['language_id'] not in {lang['id'] for lang in profile_dict['languages']}:
                    languages_data = {
                        'id': row['language_id'],
                        'language_known': replace_empty_values1(row['language_known']),
                        'language_level': replace_empty_values1(row['language_level'])
                    }
                    profile_dict['languages'].append(languages_data)

                # Social Links
                if len(profile_dict['social_links']) < MAX_SOCIAL_LINKS_ENTRIES and row['social_link_id'] not in {link['id'] for link in profile_dict['social_links']}:
                    social_links_data = {
                        'id': replace_empty_values1(row['social_link_id']),
                        'title': replace_empty_values1(row['social_link_title']),
                        'url': replace_empty_values1(row['social_link_url'])
                    }
                    profile_dict['social_links'].append(social_links_data)

                # Additional Info
                if len(profile_dict['additional_info']) < MAX_ADDITIONAL_INFO_ENTRIES and row['additional_info_id'] not in {info['id'] for info in profile_dict['additional_info']}:
                    additional_info_data = {
                        'id': replace_empty_values1(row['additional_info_id']),
                        'title': replace_empty_values1(row['additional_info_title']),
                        'description': replace_empty_values1(row['additional_info_description'])
                    }
                    profile_dict['additional_info'].append(additional_info_data)

            profiles.append(format_profile(profile_dict))
        return profiles
    except Exception as error:
        print("Error:", error)
        return (False, str(error), 500, {})
    
def format_profile(profile_data):
    def convert_date(date_obj):
        if isinstance(date_obj, date):
            return date_obj.strftime("%Y-%m-%d")  # Or any other date format you prefer
        return date_obj

    profile = {
        "id": profile_data['professional_id'],
        "About": profile_data['about'],
        "Additional_Information": profile_data['additional_info'],
        "Candidate_Name": f"{profile_data['first_name']} {profile_data['last_name']}",
        "Contact_Information": {
            "Address": profile_data['city'],
            "Email": profile_data['email_id'],
            "Phone_Number": profile_data['contact_number']
        },
        "Education": [
            {
                "CGPA/Percentage": edu['degree_level'],
                "Degree": edu['degree_level'],
                "End_Month": edu['education_end_month'],
                "End_Year": edu['education_end_year'],
                "Institute_Name": edu['institute_name'],
                "Is_Pursuing": False if edu['education_end_year'] else True,
                "Location": edu['institute_location'],
                "Major": edu['specialisation'],
                "Start_Month": edu['education_start_month'],
                "Start_Year": edu['education_start_year'],
                "University": ""
            }
            for edu in profile_data['education']
        ],
        "Languages": [{"Language": "", "Language_Level": ""} for _ in range(5)],
        "Personal_Details": {            
            "Father's_Name": "",
            "Gender": "",
            "Nationality": ""
        },
        "Skills": [
            {"Skill": skill['skill_name'], "Skill_Level": skill['skill_level']} 
            for skill in profile_data['skills']
        ],
        "Social_Links": {
            "LinkedIn": ""
        },
        "Work_Experience": [
            {
                "Currently_Working": False if exp['experience_end_year'] else True,
                "End_Month": exp['experience_end_month'],
                "End_Year": exp['experience_end_year'],
                "Job_Description": exp['job_description'],
                "Job_Title": exp['job_title'],
                "Organization_Name": exp['company_name'],
                "Start_Month": exp['experience_start_month'],
                "Start_Year": exp['experience_start_year'],
                "Work_Location": exp['job_location']
            }
            for exp in profile_data['experience']
        ]
    }
    return profile

def professional_details_update(file):
    s3_pro = s3_obj.get_s3_client()   
    s3_pro.upload_fileobj(file, S3_BUCKET_NAME, s3_resume_folder_name+file.filename)

# def professional_register(request):    
#     result_json = {}  
#     try:
#         # req_data = request.get_json()
#         req_data = request.form        
#         last_name = ""
#         if 'last_name' in req_data:
#             last_name = req_data['last_name']
#         if 'first_name' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json        
#         if 'email_id' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'contact_number' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'country_code' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'user_pwd' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json        
#         if 'is_age_verified' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json  
#         if 'country' not in req_data or 'city' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json  
#         if 'years_of_experience' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         # if 'gender' not in req_data:
#         #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#         #     return result_json
#         if 'functional_specification' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'sector' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'industry_sector' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'job_type' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'location_preference' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'mode_of_communication' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         # contact_number = None
#         # if 'contact_number' in req_data:
#         # if 'country_code' in req_data:
#         # else:
#         #     country_code = None

#         # first_name = req_data['first_name']                
#         # email_id = req_data['email_id']
#         # user_pwd = req_data['user_pwd']
#         # city = req_data['city']
#         # country = req_data['country']
#         # contact_number = req_data['contact_number']
#         # country_code = req_data['country_code']
#         # is_age_verified = req_data['is_age_verified']

#         first_name = req_data.get('first_name')
#         email_id = req_data.get('email_id')
#         user_pwd = req_data.get('user_pwd')
#         city = req_data.get('city')
#         country = req_data.get('country')
#         contact_number = req_data.get('contact_number')
#         country_code = req_data.get('country_code')
#         is_age_verified = req_data.get('is_age_verified')
#         years_of_experience = req_data.get('years_of_experience')
#         if 'gender' in req_data:
#             gender = req_data.get('gender')
#         else:
#             gender = None
#         functional_specification = req_data.get('functional_specification')
#         sector = req_data.get('sector')
#         industry_sector = req_data.get('industry_sector')
#         job_type = req_data.get('job_type')
#         location_preference = req_data.get('location_preference')
#         mode_of_communication = req_data.get('mode_of_communication')
#         willing_to_relocate = req_data.get('willing_to_relocate')
        
#         file = request.files['file']

#         if check(email_id):
#             pass
#         else:
#             result_json = api_json_response_format(False,"Invalid email id",204,{})
#             return result_json

#         user_data = get_user_data(email_id)
#         if user_data["is_exist"]:
#             if user_data["user_role"] == "employer":
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already registered as employer.",409,{})  
#                 return result_json
#             elif user_data["user_role"] == "partner":
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already registered as partner.",409,{})  
#                 return result_json
#             else:
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
#                 return result_json
#         sub_user_data = get_sub_user_data(email_id)
#         if sub_user_data["is_exist"]:
#             if sub_user_data["user_role"] == "employer_sub_admin" or sub_user_data["user_role"] == "recruiter":
#                 result_json = api_json_response_format(False, "It appears that an account with this email address already associated with us.",409,{})  
#                 return result_json
#             else:
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
#                 return result_json
#         if not is_age_verified == "Y":
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})  
#             return result_json
#         created_at = datetime.datetime.now()
#         if file:
#             professional_details_update(file)
#             # output = professional_details_update(file)
#             # print(output['error_code'])    
#         # if output['error_code'] != 0:
#         #     result_json = api_json_response_format(False, output['message'], 204, {}) 
#         #     return result_json
#         query = "insert into users (first_name,last_name,user_role_fk,email_id,profile_image,country_code,contact_number,pricing_category, payment_status, user_pwd,login_mode,is_active,country,city,created_at,gender) values (%s,%s,(select role_id from user_role where user_role = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#         values = (first_name,last_name,'professional', email_id,'default_profile_picture.png',country_code,contact_number,'Basic', 'trialing', user_pwd,'Manual','Y',country,city,created_at,gender)  
#         row_count = update_query(query, values)        
#         if row_count > 0:
#             user_data = get_user_data(email_id)
#             user_id = user_data["user_id"]
#             current_date = dt.today()
#             file_name = file.filename
#             formatted_date = current_date.strftime("%Y/%m/%d")
#             query = "insert into professional_profile(professional_id, years_of_experience,functional_specification,sector,industry_sector,job_type,location_preference,mode_of_communication,willing_to_relocate, professional_resume, upload_date, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#             values = (user_id, years_of_experience,functional_specification,sector,industry_sector,job_type,location_preference,mode_of_communication,willing_to_relocate, file_name, formatted_date, created_at,) 
#             row_count = update_query(query, values)
#             if row_count > 0:
#                 # result_json = api_json_response_format(True,"Professional Account created successfully",0,{})
#                 token_result = get_jwt_access_token(user_id,email_id)                
#                 if token_result["status"] == "success":
#                     access_token =  token_result["access_token"]     
#                     subject = "Email Verification"
#                     recipients = [email_id]                    
#                     index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
#                     # index = open("/home/applied-sw07/Documents/1_Second-Careers_source_files/Sept_30/2ndcareers-back-end/second_careers_project/templates/Email_verification.html",'r').read()
#                     index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(user_id))
#                     body = index
#                     sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Professional Email Verification")                          
#                     res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
#                     create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
#                     try:
#                         user_properties = {
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),                
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'City': city,
#                             'Country': country,
#                             'Signup Mode' : 'Manual',
#                             'User plan' :  user_data['pricing_category']                             
#                         }

#                         event_properties = {    
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'City': city,
#                             'Country': country,
#                             'Signup Mode' : 'Manual',
#                             'User plan' :  user_data['pricing_category']         
#                         }
#                         event_name = "Professional Profile Creation"
#                         background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Professional Profile created", user_data)
#                         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Professional Profile created", user_data)
#                     except Exception as e:  
#                         print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                     background_runner.get_professional_details(user_data['user_id'])
#                     result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)    
#                 else:
#                     try:
#                         event_properties = {    
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Signup Mode' : 'Manual',
#                             'Error' : 'Token error ' + str(token_result["stauts"])            
#                         }
#                         event_name = "Professional Profile Error"
#                         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Professional Profile unable to create because of token error", user_data)
#                     except Exception as e:  
#                         print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e)) 
#                     result_json = api_json_response_format(False,token_result["stauts"],401,{})   
#             else:
#                 query = "delete from users where email_id = %s"
#                 values = (email_id,)  
#                 update_query(query, values)
#                 try:
#                     event_properties = {    
#                         '$distinct_id' : email_id, 
#                         '$time': int(time.mktime(dt.now().timetuple())),
#                         '$os' : platform.system(),
#                         'First Name'    : first_name,
#                         'Last Name'     : last_name,
#                         'Email'         : email_id,
#                         'Signup Mode' : 'Manual',
#                         'Error' : 'Data Base error. Record not inserted'            
#                     }
#                     event_name = "Professional Profile Error"
#                     background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Professional Profile data is missing so unable to create Professional Profile", user_data)
#                 except Exception as e:  
#                     print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                 result_json = api_json_response_format(False,"Sorry! We are unable to create your account. We request you to retry.",500,{})
#         else:
#             try:
#                 event_properties = {    
#                     '$distinct_id' : email_id, 
#                     '$time': int(time.mktime(dt.now().timetuple())),
#                     '$os' : platform.system(),
#                     'First Name'    : first_name,
#                     'Last Name'     : last_name,
#                     'Email'         : email_id,
#                     'Signup Mode' : 'Manual',
#                     'Error' : 'Data Base error. Record not inserted'            
#                 }
#                 event_name = "Professional Profile Error"
#                 background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Professional Profile data is missing so unable to create Professional Profile", user_data)
#             except Exception as e:  
#                 print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
#             result_json = api_json_response_format(False,"Sorry! We are unable to create your account. We request you to retry.",500,{})        
        
#     except Exception as error:
#         result_json = api_json_response_format(False,str(error),500,{})  
#     finally:
#         return result_json


def is_valid_base64(s: str) -> bool:
    try:
        # Try decoding (with validation=True to reject invalid chars)
        base64.b64decode(s, validate=True)
        return True
    except Exception:
        return False

def about_you_data(request):
    try:
        print("success")

        # result_json = "success"
        email_id = request.args.get("email_id")



        # data = request.get_json()
        # email_id = data.get("email_id")
        # first_name = request.args.get("first_name")
        # last_name = request.args.get("last_name")
        # city = request.args.get("first_name")
        # contact_number = request.args.get("first_name")
        # gender = request.args.get("first_name")
        # user_datas = {}
        # if email_id:
        #     user_datas["email_id"] = email_id
        # if first_name:
        #     user_datas["first_name"] = first_name
        # if last_name:
        #     user_datas["last_name"] = last_name
        # if city:
        #     user_datas["city"] = city
        # if contact_number:
        #     user_datas["contact_number"] = contact_number
        # if gender:
        #     user_datas["gender"] = gender

        user_values = {}
        if email_id:
            decoded_bytes = base64.b64decode(email_id)
            decoded_str = decoded_bytes.decode("utf-8")
            if is_valid_base64(email_id):
                decoded_str = base64.b64decode(email_id).decode("utf-8")
                # print("Decoded:", decoded_str)
            else:
                print("Invalid base64 string")

            user_values["email_id"] = decoded_str
            query = "select first_name, last_name, contact_number, city, country, gender, country_code from users where email_id = %s"
            values = (decoded_str,)
            res = execute_query(query, values)
            if res:
                # data = res[0]
                if decoded_str:
                    user_values["email_id"] = decoded_str
                if res[0]["first_name"]:
                    user_values["first_name"] = res[0]["first_name"]
                if res[0]["last_name"]:
                    user_values["last_name"] = res[0]["last_name"]
                if res[0]["city"]:
                    user_values["city"] = res[0]["city"]
                if res[0]["country"]:
                    user_values["country"] = res[0]["country"]
                if res[0]["contact_number"]:
                    user_values["contact_number"] = res[0]["contact_number"]
                if res[0]["gender"]:
                    user_values["gender"] = res[0]["gender"]
                if res[0]["country_code"]:
                    user_values["country_code"] = res[0]["country_code"]
                if user_values:
                    result_json = api_json_response_format(True, "Data fetched successfully", 200, user_values)
                else:
                    result_json = api_json_response_format(True, "Data not fetched successfully", 200, user_values)
            else:
                 result_json = api_json_response_format(True, "No records found", 200, user_values)
            
        else:
            result_json = api_json_response_format(True, "email is not provided", 200, user_values)

    except Exception as error:
        result_json = api_json_response_format(False,str(error),500,{})  
    finally:
        return result_json
    
def your_career_story_data(request):
    try:
        
        email_id = request.args.get("email_id")
        user_values = {}
        if email_id:
            # user_datas["email_id"] = email_id

            decoded_bytes = base64.b64decode(email_id)
            decoded_str = decoded_bytes.decode("utf-8")
            if is_valid_base64(email_id):
                decoded_str = base64.b64decode(email_id).decode("utf-8")
                # print("Decoded:", decoded_str)
            else:
                print("Invalid base64 string")

            user_values = get_user_data(decoded_str)
            user_id = user_values["user_id"]


            # query = "select user_id from users where email_id = %s"
            # values = (decoded_str,)
            # res = execute_query(query,values)
        
            if not user_id: 
                result_json = api_json_response_format(True, "No records found", 200, user_values)
                return result_json
                
            user_values["email_id"] = decoded_str
            query = "select years_of_experience, industry_sector, sector, functional_specification, designation, professional_resume from professional_profile where professional_id = %s"
            values = (user_id,)
            res = execute_query(query, values)
            if res:
                # data = res[0]
                if decoded_str:
                    user_values["email_id"] = decoded_str
                if res[0]["years_of_experience"]:
                    user_values["years_of_experience"] = res[0]["years_of_experience"]
                if res[0]["industry_sector"]:
                    user_values["industry_sector"] = res[0]["industry_sector"]
                if res[0]["sector"]:
                    user_values["sector"] = res[0]["sector"]
                if res[0]["functional_specification"]:
                    user_values["functional_specification"] = res[0]["functional_specification"]
                if res[0]["designation"]:
                    user_values["designation"] = res[0]["designation"]
                if res[0]["professional_resume"]:
                    user_values["file"] = res[0]["professional_resume"]
                
                if res:
                    result_json = api_json_response_format(True, "Data fetched successfully", 200, user_values)
                else:
                    result_json = api_json_response_format(True, "Data not fetched successfully", 200, user_values)
            else:
                result_json = api_json_response_format(True, "No records found", 200, user_values)
            
        else:
            result_json = api_json_response_format(True, "email is not provided", 200, user_values)

        result_json = api_json_response_format(True, "Data fetched successfully", 200, user_values)
    except Exception as error:
        result_json = api_json_response_format(False,str(error),500,{})  
    finally:
        return result_json


def delete_file(request):
    # result_json = {}
    data = request.get_json()
    if not data or "email_id" not in data:
        return api_json_response_format(False, "email_id is required", 400, {})
    email_id = data["email_id"]

    if not data or "file" not in data:
        return api_json_response_format(False,"file_name is required", 400,{})

    file_name = data["file"]

    try:
        s3_pro = s3_obj.get_s3_client()   
        deleted_file = s3_pro.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_resume_folder_name+file_name)
        print("deleted_file", deleted_file)
        # query = "select * from users where email_id = %s"
        # values = (email_id,)
        # res = execute_query(query, values)
        # if res:
        #     user_id = res[0]["user_id"]

        user_data = get_user_data(email_id)
        user_id = user_data["user_id"]
        query = "update professional_profile set professional_resume = '' where  professional_id = %s"
        values = (user_id,)
        rowcount = update_query(query, values)
        if rowcount > 0:
            return api_json_response_format(True, f"{file_name} deleted successfully", 200,{})
        else:
            return api_json_response_format(True, f"{file_name} deleted successfully", 404,{})
        # return api_json_response_format(True, f"{file_name} deleted successfully from S3", 200, {})

    except Exception as e:
        print("Error in deleting resume", e)
        return api_json_response_format(False, f"Error deleting {file_name}: {str(e)}", 404,{})



def mixpanel_task(email_id,user_data, event_name, add_event_properties=None, message=""):
    try:
        user_id = user_data["user_id"]
        query = "select * from professional_profile where professional_id = %s"
        values = (user_id,)
        res = execute_query(query, values)
        if res:
            prof_data = res[0]
        event_properties = {    
            '$distinct_id' : email_id, 
            '$time': int(time.mktime(dt.now().timetuple())),
            '$os' : platform.system(),
            'Email'         : email_id,
            'Signup Mode' : 'Manual' , 
            # 'functional_specification': add_event_properties["functional_specification"],
            # 'Gender':add_event_properties["Gender"],
            # 'location_preference':add_event_properties["location_preference"],
            # 'mode_of_communication':add_event_properties["mode_of_communication"],
            # 'professional_industry_sector': industry_sector,
            # 'professional_profile_percentage': professional_profile_percentage,
            # 'professional_sector':sector,
            # 'willing_to_relocate':willing_to_relocate,
            # 'work_type':job_type,
            # 'years_of_experience': years_of_experience    
        }

        user_properties = {}


        if event_name == "New User Signup":
            user_properties = {
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),                
                            # 'First Name'    : first_name,
                            # 'Last Name'     : last_name,
                            'Email'         : email_id,
                            # 'City': city,
                            # 'Country': country,
                            # 'Signup Mode' : 'Manual',
                            'Signup Mode' : user_data['login_mode'],
                            'User plan' :  user_data['pricing_category'],
                            'User_role' : user_data['user_role']
                        }
            
            
        elif event_name == "About You":
            raw_country = user_data.get('country', '').strip().lower()

            if raw_country in ["india", "in"]:
                country = "India"

            elif raw_country in ["united states", "us", "usa", "united states of america"]:
                country = "United States"

            else:
                country = "Global"

            user_properties = {    
                'City':user_data.get('city', ''),
                'Country': country,
                'first_name': user_data.get('first_name', ''),
                'last_name': user_data.get('last_name', ''),
                'Gender': user_data.get('gender', '')
                }
        elif event_name == 'Your Career Story':
            user_properties = {    
            'Functional Specification' : prof_data.get('functional_specification', ''),
            'Professional Industry sector': prof_data.get('industry_sector', ''),
            'Years of Experience': prof_data.get('years_of_experience', ''),
            'Professional sector': prof_data.get('sector', ''),
        }
        elif event_name == 'Your Next chapter':
            query = "select profile_percentage from users where email_id = %s"
            values = (email_id,)
            res = execute_query(query, values)
            profile_percentage = res[0]["profile_percentage"]
            user_properties = {    
                    'Location Preference' : prof_data.get('location_preference', ''),
                    'Mode of communication': prof_data.get('mode_of_communication', ''),
                    'Professional Profile percentage': profile_percentage,
                    'Willing to Relocate': prof_data.get('willing_to_relocate', ''),
                    'Work type' :  prof_data.get('job_type', ''),
                }
            
        elif event_name == 'New Employer Signup':
            user_properties = {
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),                
                                    # 'First Name'    : first_name,
                                    # 'Last Name'     : last_name,
                                    'Email'         : email_id,
                                    # 'Sector' : sector,
                                    # 'Organization Type' : organization_type,
                                    # "Employer's Title" : title,
                                    # 'Website' : website, 
                                    # 'City': city,
                                    # 'Country': country,
                                    'Signup Mode' : user_data['login_mode'],
                                    'User plan' :  user_data['pricing_category'],
                                    'Organization Name' : add_event_properties.get('Organization Name'),
                                    'User_role' : user_data['user_role']
                                    }
            
        elif event_name == 'Employer Account Details':
            user_properties = {   
                                'First Name'    : user_data.get('first_name', ''),
                                'Last Name'     : user_data.get('last_name', ''),
                                'Email'         : email_id,
                                'Sector' : add_event_properties.get('Sector',''),
                                'Organization Type' : add_event_properties.get('Organization Type',''),
                                "Employer's Title" : add_event_properties.get("Employer's Title",''),
                                'Website' : add_event_properties.get('website',''), 
                                'City': user_data.get('city', ''),
                                'Country':  user_data.get('country', ''),
                                'Signup Mode' : 'Manual',
                                'User plan' :  user_data['pricing_category']
                            }
            
        elif event_name == 'New Partner Signup':
            user_properties = {    
                                        '$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'Email'         : email_id,
                                        'Company Name' : add_event_properties.get('Organization Name',''),
                                        'Signup Mode' : user_data['login_mode'],
                                        # 'User plan' :  user_data['pricing_category']       
                                        'User_role' : user_data['user_role']
                                    }
            
        elif event_name == 'Partner Account Details':
            user_properties = {    
                            'First Name'    : user_data.get('first_name', ''),
                            'Last Name'     : user_data.get('last_name', ''),
                            'Email'         : email_id,
                            'Sector' : add_event_properties.get('Sector',''),
                            'Partner Type' : add_event_properties.get('Organization Type',''),
                            'Website' : add_event_properties.get('Website',''), 
                            'City': user_data.get('city', ''),
                            'Country': user_data.get('country', ''),
                            'Signup Mode' : user_data['login_mode'],
                            'User plan' :  user_data['pricing_category']
                        }

        else:
            print("Enter a valid Event name")
        # elif event_name == "Professional Profile Error":
        #     event_properties['Error'] = 'Token error ' + str(token_result["status"])  
        #     background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Professional Profile unable to create because of token error", user_data)
        # elif event_name == "Professional Profile Error":
        #     background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Professional Profile data is missing so unable to create Professional Profile", user_data)
        if add_event_properties:
            event_properties.update(add_event_properties)

        if user_properties:
            background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
        background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
    except Exception as e:  
        print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))

def professional_register(request):
    try:
        result_json = {}
        # req_data = request.get_json()
        # req_data = request.form
        # flag = request.form.get("flag", "")
        # error_status = None
        if request.is_json:
            req_data = request.get_json()
        else:
            req_data = request.form

        flag = req_data.get("flag")
        if not flag:
            result_json = api_json_response_format(False,"Please fill in all the flag fields.",204,{})  
            return result_json

        if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
        email_id = req_data.get('email_id')
        if check(email_id):
            pass
        else:
            result_json = api_json_response_format(False,"Invalid email id",204,{})
            return result_json
        user_data = get_user_data(email_id)
        created_at = datetime.datetime.now()
        if user_data["is_exist"]:
            if user_data["user_role"] == "employer":
                result_json = api_json_response_format(False,"It appears that an account with this email address already registered as employer.",409,{})  
                return result_json
            elif user_data["user_role"] == "partner":
                result_json = api_json_response_format(False,"It appears that an account with this email address already registered as partner.",409,{})  
                return result_json
            # elif user_data["user_role"] == "professional" and flag == "":
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json
            # else:
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json
        sub_user_data = get_sub_user_data(email_id)
        if sub_user_data["is_exist"]:
            if sub_user_data["user_role"] == "employer_sub_admin" or sub_user_data["user_role"] == "recruiter":
                result_json = api_json_response_format(False, "It appears that an account with this email address already associated with us.",409,{})  
                return result_json
            # else:
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json

        if flag == "new_user_signup":
            
            event_name = "New User Signup"
            email_id = req_data.get('email_id')
            if not req_data.get('email_id'):
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            login_mode = req_data.get('login_mode')
            is_age_verified = req_data.get('is_age_verified')
            # if not email_id or str(email_id).strip() == "" or str(email_id).strip().lower() == "null":
            #     result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
            #     return result_json

            # if 'login_mode' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # login_mode = req_data.get('login_mode')
            if login_mode == '' or login_mode == None:
                result_json = api_json_response_format(False,"Something went wrong.",204,{})  
                return result_json
            if login_mode == "Manual":
                if 'user_pwd' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json  
                user_pwd = req_data.get('user_pwd')
            else:
                user_pwd = None
            if is_age_verified is None or str(is_age_verified).strip().lower() in ["", "null"]:
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json

            # if 'is_age_verified' not in req_data and is_age_verified:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'login_mode' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            
            # email_id = req_data.get('email_id')
            # user_pwd = req_data.get('user_pwd')
            # is_age_verified = req_data.get('is_age_verified')
            # login_mode = req_data.get('login_mode')
            if not is_age_verified == "Y":
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})  
                return result_json
            query = "select email_id from users where email_id = %s"
            values = (email_id,)
            res = execute_query(query, values)
            if not res:

                query = "insert into users (email_id, user_pwd, user_role_fk, created_at, flag_status, login_mode, pricing_category) values (%s,%s,(select role_id from user_role where user_role = %s), %s, %s, %s, %s)"
                values = (email_id, user_pwd,'professional',created_at, 'new_user_signup', login_mode, 'Basic')  
                row_count = update_query(query, values)
                if row_count >0:
                    user_data = get_user_data(email_id)
                    user_id = user_data["user_id"]
                    if user_id:
                        user_id = user_id
                        query = "insert into professional_profile(professional_id,created_at) values (%s,%s)"
                        values = (user_id, created_at)
                        res = update_query(query, values)
                    else:
                        result_json = api_json_response_format(False,"No record found.",409,{})
                    query = "select flag_status from users where user_id = %s"
                    values = (user_id,)
                    res = execute_query(query, values)
                    if res:
                        flag_status = res[0]['flag_status']

                    if login_mode == "Manual":
                        token_result = get_jwt_access_token(user_id,email_id)                
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]     
                            subject = "Email Verification"
                            recipients = [email_id]                    
                            index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
                            # index = open("/home/adrasw-sam/Documents/2nddevsource/2nd/2ndcareers-back-end/second_careers_project/templates/Email_verification.html",'r').read()
                            # (don't_enable) -> index = open("/home/applied-sw07/Documents/1_Second-Careers_source_files/Sept_30/2ndcareers-back-end/second_careers_project/templates/Email_verification.html",'r').read()
                            index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(user_id))
                            body = index
                            sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Professional Email Verification")                         
                            query = "Update users set profile_image = %s, payment_status = %s, is_active = %s where email_id = %s"
                            values = ('default_profile_picture.png', 'trialing', 'Y',email_id)  
                            res = update_query(query, values) 
                            res_data = {"access_token" : access_token,"user_role":user_data["user_role"], "registration_status":flag_status}
                            # create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
                            add_event_properties = {    
                            'email_id' : email_id,
                            'User plan' :  user_data['pricing_category'],
                            # 'City': "",
                            # 'Country': ""
                        }
                        

                        mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Professional Profile created")
                        background_runner.get_professional_details(user_id)
                        result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data) 
                    else:
                        token_result = get_jwt_access_token(user_id,email_id)                
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]     
                            query = "Update users set profile_image = %s, payment_status = %s, is_active = %s, email_active = %s where email_id = %s"
                            values = ('default_profile_picture.png', 'trialing', 'Y', 'Y', email_id)  
                            res = update_query(query, values) 
                            res_data = {"access_token" : access_token,"user_role":user_data["user_role"], "registration_status":flag_status}
                            # create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
                            add_event_properties = {    
                            'email_id' : email_id,
                            'User plan' : user_data['pricing_category'],
                            # 'City': "",
                            # 'Country': ""
                        }

                        mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Professional Profile created")
                        background_runner.get_professional_details(user_id)
                        result_json = api_json_response_format(True,"Registered successfully.",0,res_data) 
                else:
                    try:
                        add_event_properties = {    
    
                            'Error' : 'Token error ' + str(token_result["stauts"]),
                            # 'User plan' :  user_data['pricing_category'],
                            'City': "",
                            'Country': ""
                        }
                        mixpanel_task(email_id, user_data, event_name, add_event_properties, f"{email_id} Professional Profile unable to create because of token error")
                    except Exception as e:  
                        print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e)) 
                    result_json = api_json_response_format(False,token_result["stauts"],401,{})   
            else:
                result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
                return result_json  
            
        elif flag == "about_you":
            event_name = "About You"
            last_name = ""
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'last_name' in req_data:
                last_name = req_data['last_name']
            # if 'first_name' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json        
            # if 'contact_number' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'country' not in req_data or 'city' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json 
            # if 'gender' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'country_code' not in req_data:
            #     result_json = api_json_response_format(False, "Please fill in all the required fields.", 204,{})
            #     return result_json
            
            first_name = req_data.get('first_name')
            city = req_data.get('city')
            country = req_data.get('country')
            contact_number = req_data.get('contact_number')
            country_code = req_data.get('country_code')
            if 'gender' in req_data:
                gender = req_data.get('gender')
            else:
                gender = None

            # first_name = req_data.get("first_name")

            if first_name is None or str(first_name).strip() == "" or str(first_name).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if city is None or str(city).strip() == "" or str(city).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if country is None or str(country).strip() == "" or str(country).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if contact_number is None or str(contact_number).strip() == "" or str(contact_number).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if country_code is None or str(country_code).strip() == "" or str(country_code).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            query = "Update users set first_name = %s,last_name = %s, country_code = %s,contact_number = %s, gender = %s, city = %s, country = %s, created_at = %s, flag_status = %s where email_id = %s"
            values = (first_name,last_name, country_code,contact_number,gender, city, country, created_at, 'about_you', email_id)  
            row_count = update_query(query, values)
            
            # result_json = api_json_response_format(True,"users about us inserted",204,{})  
            # return result_json  
        # elif flag == "about_you_edit":
        #     event_name = "About you edited"
        #     if 'email_id' not in req_data:
                # result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                # return result_json  
        
        elif flag == "your_career_story":
            event_name = "Your Career Story"
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'years_of_experience' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'functional_specification' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'sector' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'industry_sector' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'designation' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            email_id = req_data.get('email_id')
            years_of_experience = req_data.get('years_of_experience')
            functional_specification = req_data.get('functional_specification')
            # functional_specification = req_data.get('functional_specification', [])
            # if not isinstance(functional_specification, list):
            #     # return jsonify({'success': False,'message': 'functional_specification must be an array'}), 400
            #     result_json = api_json_response_format(False, 'functional_specification must be an array', 400, {})
            #     return result_json
            sector = req_data.get('sector')
            industry_sector = req_data.get('industry_sector')
            # designation = req_data.get('designation')
            user_data = get_user_data(email_id)
            user_id = user_data['user_id']
            if years_of_experience is None or str(years_of_experience).strip() == "" or str(years_of_experience).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if functional_specification is None or str(functional_specification).strip() == "" or str(functional_specification).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            
            if sector is None or str(sector).strip() == "" or str(sector).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if industry_sector is None or str(industry_sector).strip() == "" or str(industry_sector).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            # if designation is None or str(designation).strip() == "" or str(designation).strip().lower() == "null":
            #     result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
            #     return result_json
            if 'file' not in request.files:
                file = None
                # file_name = None
                query = "select professional_resume from professional_profile where professional_id = %s"
                values = (user_id,)
                res = execute_query(query, values)
                file_name = res[0]["professional_resume"] if res else None
            else:
                file = request.files['file']
                if file.filename == "":   # empty filename = no file uploaded
                    file = None
                    file_name = None
                else:
                    file_name = file.filename

            # if 'file' not in request.files:
            #     return {"msg": "No file part in request"}, 400

            # file = request.files['file']
            # if file.filename == "":
            #     return {"msg": "No selected file"}, 400

                # return {"filename": file.filename}

            if file:
                professional_details_update(file)
            # query = "Update users set years_of_experience = %s,gender = %s, functional_specification = %s,industry_sector = %s, job_type = %s , location_preference = %s, mode_of_communication = %s, created_at = %s where email_id = %s"
            # values = (years_of_experience,gender, functional_specification,industry_sector,job_type, location_preference, mode_of_communication, created_at, email_id)  
            # row_count = update_query(query, values)


            user_id = user_data["user_id"]
            query = "select professional_id from professional_profile where professional_id = %s"
            values = (user_id,)
            res = execute_query(query, values)
            # row_count = len(res)
            # if not res:
                # query = "insert into professional_profile(professional_id, years_of_experience,functional_specification,sector,industry_sector, designation, professional_resume,created_at) values (%s,%s,%s,%s,%s,%s,%s,%s)"
                # values = (user_id,years_of_experience,functional_specification,sector,industry_sector,designation,file_name,created_at) 
                # row_count = update_query(query, values)
                # query = "update professional_profile set years_of_experience"
            if res:
                query = "update professional_profile set years_of_experience = %s,functional_specification = %s,sector = %s,industry_sector = %s, professional_resume = %s, created_at = %s where professional_id = %s"
                values = (years_of_experience,functional_specification,sector,industry_sector,file_name,created_at, user_id) 
                row_count = update_query(query, values)
            query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
            values2 = ('your_career_story', user_id)   # Example: set flag_status=1
            row_count2 = update_query(query2, values2)
            
        elif flag == "your_next_chapter":
            event_name = "Your Next chapter"
            # is_age_verified = req_data.get('is_age_verified')
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'job_type' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'location_preference' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'mode_of_communication' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'willing_to_relocate' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'reference' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            email_id = req_data.get('email_id')
            job_type = req_data.get('job_type', [])
            if isinstance(job_type, list):
                job_type = ",".join(job_type)
            # job_type = json.dumps(job_type)
            location_preference = req_data.get('location_preference')
            user_id = user_data["user_id"]
            mode_of_communication = req_data.get('mode_of_communication')
            willing_to_relocate = req_data.get('willing_to_relocate')
            # reference = req_data.get('reference')
            if job_type is None or str(job_type).strip() == "" or str(job_type).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if location_preference is None or str(location_preference).strip() == "" or str(location_preference).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if mode_of_communication is None or str(mode_of_communication).strip() == "" or str(mode_of_communication).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if willing_to_relocate is None or str(willing_to_relocate).strip() == "" or str(willing_to_relocate).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            token_result = get_jwt_access_token(user_id,email_id)                
            if token_result["status"] == "success":
                access_token =  token_result["access_token"]  
            query = "update professional_profile set job_type = %s,willing_to_relocate = %s, location_preference = %s,mode_of_communication = %s, created_at = %s where professional_id = %s"
            values = (job_type,willing_to_relocate, location_preference,mode_of_communication, created_at, user_id) 
            row_count = update_query(query, values)
            user_data = get_user_data(email_id)
            username = user_data["email_id"]
            query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
            values2 = ('your_next_chapter', user_id)   # Example: set flag_status=1
            row_count2 = update_query(query2, values2)
            query = "update users set completed_status = %s where email_id = %s"
            values = ("Y",email_id)
            res = update_query(query, values)
            background_runner.get_professional_details(user_id)
            
            # query = "Update users set profile_image = %s,pricing_category = %s, payment_status = %s, is_active = %s where email_id = %s"
            # values = ('default_profile_picture.png','Basic', 'trialing', 'Y',email_id)  
            # res = update_query(query, values)
            #query = "insert into users (first_name,last_name,user_role_fk,email_id,profile_image,country_code,contact_number,pricing_category, payment_status, user_pwd,login_mode,is_active,country,city,created_at,gender) values (%s,%s,(select role_id from user_role where user_role = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#         values = (first_name,last_name,'professional', email_id,'default_profile_picture.png',country_code,contact_number,'Basic', 'trialing', user_pwd,'Manual','Y',country,city,created_at,gender)  
#         row_count = update_query(query, values)   
#           query = update users  
            # query = "Select completed_status from users where email_id = %s"
            # values = (email_id,)
            # res = execute_query(query, values)
            # if res[0]["completed_status"] == "N":
            #     result_json = api_json_response_format(False,"Please verify email.",204,{})  
            #     return result_json
            
            # else:
            #     result_json = api_json_response_format(True,"Profile completed successfully.",204,{})  
            #     return result_json
        
        else:
            result_json = api_json_response_format(False,"Invalid flag was written.",400,{})  
            return result_json
        
        if row_count > 0:
            user_data = get_user_data(email_id)
            user_id = user_data["user_id"]
            current_date = dt.today()
            
            formatted_date = current_date.strftime("%Y/%m/%d")
            if flag == "your_career_story" or flag == "your_next_chapter":
                query = "Update professional_profile set upload_date = %s, created_at = %s where professional_id = %s"
                values = (formatted_date, created_at, user_id)
                row_count = update_query(query, values)
            
            # if row_count > 0:
            #     if flag == "about_you":
            #         add_event_properties = {    
            #         'City':city,
            #         'Country': country,
            #         'first_name': first_name,
            #         'last_name': last_name,
            #         'Gender': gender
            #         }
            #     elif flag == 'your_career_story':
            #         add_event_properties = {    
            #         'Functional Specification' : functional_specification,
            #         'Professional Industry sector': industry_sector,
            #         'Years of Experience': years_of_experience,
            #         'Professional sector': sector,
            #         'User plan' :  "",

            #         # 'City': "",
            #         # 'Country': ""
            #     }
            #     elif flag == 'your_next_chapter':
            #         query = "select professional_profile_percentage from users where email_id = %s"
            #         values = (email_id,)
            #         res = execute_query(query, values)
            #         professional_profile_percentage = res[0]["professional_profile_percentage"]
            #         add_event_properties = {    
            #                 'location_preference' : location_preference,
            #                 'mode_of_communication': mode_of_communication,
            #                 'professional_profile_percentage': professional_profile_percentage,
            #                 'willing_to_relocate': sector,
            #                 'User plan' :  "",
                            
            #                 # 'City': "",
            #                 # 'Country': ""
            #             }
            #         query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
            #         values2 = ('completed', user_id)   # Example: set flag_status=1
            #         row_count2 = update_query(query2, values2)

            #     elif flag == 'new_user_signup':
            #         print("new user signup activated")
                # elif flag== "your_career_story" or flag == "your_next_chapter" or flag == "new_user_signup":

                #     if flag != "new_user_signup":
                #         mixpanel_task(email_id, user_data, event_name, message=f"{email_id} Professional Profile created")
                #     # result_json = api_json_response_format(True,"Professional Account created successfully",0,{})
                #         if flag == 'your_next_chapter':

                #     result_json = api_json_response_format(True,"Your Professional Account details were added successfully",0,{})

                #         # if flag == 'your_next_chapter':
                #         #     query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
                #         #     values2 = ('completed', user_id)   # Example: set flag_status=1
                #         #     row_count2 = update_query(query2, values2)
                #     mixpanel_task(email_id,user_data, event_name,add_event_properties, f"{email_id} Professional Profile created")
                    # result_json = api_json_response_format(True,"Your Professional Account details were added successfully",0,{})
                            
            if row_count > 0:
                if flag == "about_you":
                    raw_country = user_data.get('country', '').strip().lower()

                    if raw_country in ["india", "in"]:
                        country = "India"

                    elif raw_country in ["united states", "us", "usa", "united states of america"]:
                        country = "United States"

                    else:
                        country = "Global"
                    add_event_properties = {    
                    'City':city,
                    'Country': country,
                    'first_name': first_name,
                    'last_name': last_name
                    }
                    mixpanel_task(email_id,user_data, event_name,add_event_properties, f"{email_id} Professional Profile created")
                    result_json = api_json_response_format(True,"Professional Account created successfully",0,{})
                elif flag== "your_career_story" or flag == "your_next_chapter" or flag == "new_user_signup":

                    if flag != "new_user_signup":
                        mixpanel_task(email_id, user_data, event_name, message=f"{email_id} Professional Profile created")
                    # result_json = api_json_response_format(True,"Professional Account created successfully",0,{})
                        if flag == 'your_next_chapter':
                            query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
                            values2 = ('completed', user_id)   # Example: set flag_status=1
                            row_count2 = update_query(query2, values2)
                    res_data = {"access_token": access_token if flag == 'your_next_chapter' else None}
                    result_json = api_json_response_format(True,"Your Professional Account details were added successfully",0,res_data)
                        

                else:
                    try:
                        add_event_properties = {    

                            'Error' : 'Token error ' + str(token_result["stauts"]),
                            # 'User plan' :  user_data['pricing_category'],
                            'City': "",
                            'Country': ""
                        }
                        event_name = "Professional Profile Error"
                        mixpanel_task(email_id, user_data, event_name, add_event_properties, f"{email_id} Professional Profile unable to create because of token error")
                        # error_status = "Y"
                    except Exception as e:  
                        print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e)) 
                # if error_status != "Y" and flag != 'new_user_signup':
                #     mixpanel_task(email_id,user_data, event_name,add_event_properties, f"{email_id} Professional Profile created")
                #     result_json = api_json_response_format(True,"Your Professional Account details were added successfully",0,{})
                    
            else:
                query = "delete from users where email_id = %s"
                values = (email_id,)  
                update_query(query, values)
                try:

                    add_event_properties = {    
                        'Error' : 'Data Base error. Record not inserted'  
                    }
                    event_name = "Professional Profile Error"
                    mixpanel_task(email_id, user_data, event_name, add_event_properties, f"{email_id} Professional Profile data is missing so unable to create Professional Profile")
                except Exception as e:  
                    print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                result_json = api_json_response_format(False,"Sorry! We are unable to create your account. We request you to retry.",500,{})
        else:
            try:
                add_event_properties = {    
                        'Error' : 'Data Base error. Record not inserted'  
                    }
                event_name = "Professional Profile Error"
                mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Professional Profile data is missing so unable to create Professional Profile")
            except Exception as e:  
                print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
            result_json = api_json_response_format(False,"Sorry! We are unable to create your account. We request you to retry.",500,{})        
        
        # return "Successfully Registered"
    except Exception as error:
        result_json = api_json_response_format(False,str(error),500,{})  
        print(result_json)
    finally:
        return result_json


# def employer_register():
#     try:
#         req_data = request.get_json()        
#         if 'first_name' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'last_name' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'organization_name' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'title' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'sector' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json  
#         if 'email_id' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json  
#         if 'website' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'country' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json  
#         if 'city' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json 
#         if 'user_pwd' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json       
#         if 'contact_number' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'country_code' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json          
#         first_name = req_data['first_name']
#         last_name = req_data['last_name']
#         organization_name = req_data['organization_name']
#         title =req_data['title']
#         organization_type = ""
#         if 'organization_type' in req_data:
#             organization_type = req_data['organization_type']  
#         sector = req_data['sector']
#         email_id = req_data['email_id']
#         website = req_data['website'] 
#         country = req_data['country']
#         city = req_data['city']
#         user_pwd = req_data['user_pwd']
#         contact_number = req_data['contact_number']
#         country_code = req_data['country_code']
#         # contact_number = None
#         # if 'contact_number' in req_data:
#         #     contact_number = req_data['contact_number']  
#         # country_code = None
#         # if 'country_code' in req_data:
#         #     country_code = req_data['country_code']      
#         login_mode = 'Manual'        
#         user_role = 'employer'
#         user_data = get_user_data(email_id)
#         if user_data["is_exist"]:
#             if user_data["user_role"] == "professional":
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already registered as professional.",409,{})  
#                 return result_json
#             elif user_data["user_role"] == "partner":
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already registered as partner.",409,{})  
#                 return result_json
#             else:
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
#                 return result_json
#         sub_user_data = get_sub_user_data(email_id)
#         if sub_user_data["is_exist"]:
#             if sub_user_data["user_role"] == "employer_sub_admin" or sub_user_data["user_role"] == "recruiter":
#                 result_json = api_json_response_format(False, "It appears that an account with this email address already associated with us.",409,{})  
#                 return result_json
#             else:
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
#                 return result_json
#         created_at = datetime.datetime.now()
#         query = 'insert into users (user_role_fk,first_name,last_name,email_id,profile_image,country_code,contact_number,country,city,user_pwd,login_mode,is_active,payment_status,created_at) values ((select role_id from user_role where user_role = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
#         values = (user_role,first_name,last_name,email_id,'default_profile_picture_employer.png',country_code,contact_number,country,city,user_pwd,login_mode,'Y','unpaid',created_at,)
#         row_count = update_query(query,values)
#         print(query)
#         print(values)
#         print(row_count)
#         if row_count > 0:
#             user_data = get_user_data(email_id)
#             employer_id = user_data["user_id"]
#             query = 'insert into employer_profile (employer_id,designation,company_name,employer_type,sector,website_url,created_at) values (%s,%s,%s,%s,%s,%s,%s)'
#             values = (employer_id, title, organization_name,organization_type,sector,website,created_at,)
#             row_count_emp_profile = update_query(query,values)
#             query = 'insert into hiring_team(employer_id,first_name,last_name,designation,country_code,contact_number,email_id,created_at) values (%s,%s,%s,%s,%s,%s,%s,%s)'
#             values = (employer_id, first_name,last_name,title,country_code,contact_number,email_id,created_at,)
#             row_count_hiring_table = update_query(query,values) 
#             query = 'insert into user_plan_details(user_id,user_role_fk,user_plan,no_of_jobs,total_jobs,created_at) values (%s,%s,%s,%s,%s,%s)'
#             values = (employer_id, 2,'trialing',0,0,created_at,)
#             user_plan_table = update_query(query,values)
#             if row_count_emp_profile > 0 and row_count_hiring_table > 0: #and user_plan_table > 0:
#                 token_result = get_jwt_access_token(employer_id,email_id)                
#                 if token_result["status"] == "success":
#                     access_token =  token_result["access_token"]                                     
#                     subject = "Email Verification"
#                     recipients = [email_id] 
#                     print(os.getcwd()+"/templates/Email_verification.html")                   
#                     index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
#                     index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(employer_id))
#                     body = index
#                     sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Employer Email Verification")                          
#                     res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
#                     # create_trial_session(email_id,EMPLOYER_TRIAL_PERIOD,EMPLOYER_BASIC_PLAN_ID,"Basic")
#                     try:
#                         user_properties = {
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),                
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Organization Name' : organization_name,
#                             'Sector' : sector,
#                             'Organization Type' : organization_type,
#                             "Employer's Title" : title,
#                             'Website' : website, 
#                             'City': city,
#                             'Country': country,
#                             'Signup Mode' : 'Manual',
#                             'User plan' :  user_data['pricing_category']
#                         }

#                         event_properties = {    
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Organization Name' : organization_name,
#                             'Sector' : sector,
#                             'Organization Type' : organization_type,
#                             "Employer's Title" : title,
#                             'Website' : website, 
#                             'City': city,
#                             'Country': country,
#                             'Signup Mode' : 'Manual',
#                             'User plan' :  user_data['pricing_category']       
#                         }
#                         event_name = "Employer Profile Creation"
#                         background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
#                         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile created", user_data)
#                     except Exception as e:  
#                         print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                     background_runner.get_employer_details(employer_id)
#                     result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)     
#                 else:
#                     try:
#                         event_properties = {    
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Signup Mode' : 'Manual',
#                             'Error' : 'Token error ' + str(token_result["stauts"])            
#                         }
#                         event_name = "Employer Profile Error"
#                         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} profile not created because of Token error", user_data)
#                     except Exception as e:  
#                         print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                     result_json = api_json_response_format(False,token_result["stauts"],401,{})                   
#             else:
#                 query = "delete from users where email_id = %s"
#                 values = (email_id,)  
#                 update_query(query, values)
#                 try:
#                     event_properties = {    
#                         '$distinct_id' : email_id, 
#                         '$time': int(time.mktime(dt.now().timetuple())),
#                         '$os' : platform.system(),
#                         'First Name'    : first_name,
#                         'Last Name'     : last_name,
#                         'Email'         : email_id,
#                         'Signup Mode' : 'Manual',
#                         'Error' : 'Data Base error. Record not inserted'          
#                     }
#                     event_name = "Employer Profile Error"
#                     background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Record not found so, unable to create {event_properties['Email']} Profile.", user_data)
#                 except Exception as e:  
#                     print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                 result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})
#         else:
#             try:
#                 event_properties = {    
#                     '$distinct_id' : email_id, 
#                     '$time': int(time.mktime(dt.now().timetuple())),
#                     '$os' : platform.system(),
#                     'First Name'    : first_name,
#                     'Last Name'     : last_name,
#                     'Email'         : email_id,
#                     'Signup Mode' : 'Manual',
#                     'Error' : 'Data Base error. Record not inserted'          
#                 }
#                 event_name = "Employer Profile Error"
#                 background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Record not found so, unable to create {event_properties['Email']} Profile.", user_data)
#             except Exception as e:  
#                 print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
#             result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})            
#     except Exception as error:
#         print(error)
#         result_json = api_json_response_format(False,str(error),500,{}) 
#     finally:
#         return result_json

def employer_register():
    try:
        result_json = {}
        # req_data = request.get_json()
        # req_data = request.form
        # flag = request.form.get("flag", "")
        # error_status = None
        if request.is_json:
            req_data = request.get_json()

        flag = req_data.get("flag")
        if not flag:
            result_json = api_json_response_format(False,"Please fill in all the flag fields.",204,{})  
            return result_json

        if 'email_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        email_id = req_data.get('email_id')
        if check(email_id):
            pass
        else:
            result_json = api_json_response_format(False,"Invalid email id",204,{})
            return result_json
        
        # login_mode = 'Manual'        
        user_role = 'employer'
        user_data = get_user_data(email_id)
        created_at = datetime.datetime.now()

        if user_data["is_exist"]:
            if user_data["user_role"] == "professional":
                result_json = api_json_response_format(False,"It appears that an account with this email address already registered as professional.",409,{})  
                return result_json
            elif user_data["user_role"] == "partner":
                result_json = api_json_response_format(False,"It appears that an account with this email address already registered as partner.",409,{})  
                return result_json
            # elif user_data["user_role"] == "employer" and flag == "":
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json
            # else:
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json
        sub_user_data = get_sub_user_data(email_id)
        if sub_user_data["is_exist"]:
            if sub_user_data["user_role"] == "employer_sub_admin" or sub_user_data["user_role"] == "recruiter":
                result_json = api_json_response_format(False, "It appears that an account with this email address already associated with us.",409,{})  
                return result_json
            # else:
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json
        created_at = datetime.datetime.now()
        if flag == "new_user_signup":
            event_name = 'New Employer Signup'
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'organization_name' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            if 'login_mode' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            login_mode = req_data.get('login_mode')
            if login_mode == '' or login_mode == None:
                    result_json = api_json_response_format(False,"Something went wrong.",204,{})  
                    return result_json
            if login_mode == "Manual":
                if 'user_pwd' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json  
                user_pwd = req_data.get('user_pwd')
            else:
                user_pwd = None
            
            organization_name = req_data.get('organization_name')
            if organization_name is None or str(organization_name).strip() == "" or str(organization_name).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            query = "select email_id from users where email_id = %s"
            values = (email_id,)
            res = execute_query(query, values)
            if not res:
                query = 'insert into users (user_role_fk,email_id,user_pwd,login_mode,flag_status,created_at) values ((select role_id from user_role where user_role = %s),%s,%s,%s,%s,%s)'
                values = (user_role,email_id,user_pwd,login_mode,'new_user_signup',created_at,)
                row_count = update_query(query,values)
                if row_count > 0 :
                    user_data = get_user_data(email_id)
                    user_id = user_data["user_id"]
                    
                    query = "select flag_status from users where user_id = %s"
                    values = (user_id,)
                    res = execute_query(query, values)
                    if res:
                        flag_status = res[0]['flag_status']

                    if user_id:
                        query = "insert into employer_profile(employer_id, company_name, created_at) values (%s, %s, %s)"
                        values = (user_id, organization_name, created_at)
                        row_count_emp_profile = update_query(query, values)
                        
                    else:
                        result_json = api_json_response_format(False,"No record found.",409,{})
                        return result_json
                
                    employer_id = user_data["user_id"]

                    if login_mode == "Manual":

                        # query = 'insert into employer_profile (employer_id,designation,company_name,employer_type,sector,website_url,created_at) values (%s,%s,%s,%s,%s,%s,%s)'
                        # values = (employer_id, title, organization_name,organization_type,sector,website,created_at,)
                        # row_count_emp_profile = update_query(query,values)
                        query = 'insert into hiring_team(employer_id,email_id,created_at) values (%s,%s,%s)'
                        values = (employer_id,email_id,created_at)
                        row_count_hiring_table = update_query(query,values) 
                        query = 'insert into user_plan_details(user_id,user_role_fk,user_plan,no_of_jobs,total_jobs,created_at) values (%s,%s,%s,%s,%s,%s)'
                        values = (employer_id, 2,'trialing',0,0,created_at,)
                        user_plan_table = update_query(query,values)
                        if row_count_emp_profile > 0 and row_count_hiring_table > 0: #and user_plan_table > 0:
                            token_result = get_jwt_access_token(user_id,email_id)                
                            if token_result["status"] == "success":
                                access_token =  token_result["access_token"]                                     
                                subject = "Email Verification"
                                recipients = [email_id] 
                                print(os.getcwd()+"/templates/Email_verification.html")                   
                                index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
                                index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(employer_id))
                                body = index
                                sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Employer Email Verification")
                                query = "Update users set profile_image = %s, payment_status = %s, is_active = %s where email_id = %s"
                                values = ('default_profile_picture_employer.png', 'unpaid', 'Y',email_id)  
                                res = update_query(query, values)                           
                                res_data = {"access_token" : access_token,"user_role":user_role, "registration_status":flag_status}
                                # create_trial_session(email_id,EMPLOYER_TRIAL_PERIOD,EMPLOYER_BASIC_PLAN_ID,"Basic")
                                try:
                                    add_event_properties = {    
                                        '$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        # 'First Name'    : first_name,
                                        # 'Last Name'     : last_name,
                                        'Email'         : email_id,
                                        'Organization Name' : organization_name,
                                        # 'Sector' : sector,
                                        # 'Organization Type' : organization_type,
                                        # "Employer's Title" : title,
                                        # 'Website' : website, 
                                        # 'City': city,
                                        # 'Country': country,
                                        'Signup Mode' : 'Manual',
                                        'User plan' :  user_data['pricing_category']       
                                    }
                                    # event_name = "Employer Profile Creation"
                                    mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Employer Profile created")
                                    # background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
                                    # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile created", user_data)
                                    
                                except Exception as e:  
                                    print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                                # background_runner.get_employer_details(employer_id)
                                # result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)     
                            # else:
                            #     try:
                            #         event_properties = {    
                            #             '$distinct_id' : email_id, 
                            #             '$time': int(time.mktime(dt.now().timetuple())),
                            #             '$os' : platform.system(),
                            #             'First Name'    : first_name,
                            #             'Last Name'     : last_name,
                            #             'Email'         : email_id,
                            #             'Signup Mode' : 'Manual',
                            #             'Error' : 'Token error ' + str(token_result["stauts"])            
                            #         }
                            #         event_name = "Employer Profile Error"
                            #         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} profile not created because of Token error", user_data)
                            #     except Exception as e:  
                            #         print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                            #     result_json = api_json_response_format(False,token_result["stauts"],401,{})       
                            background_runner.get_employer_details(employer_id)
                            result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)     
               
                    else:
                        query = 'insert into hiring_team(employer_id,email_id,created_at) values (%s,%s,%s)'
                        values = (employer_id,email_id,created_at)
                        row_count_hiring_table = update_query(query,values) 
                        query = 'insert into user_plan_details(user_id,user_role_fk,user_plan,no_of_jobs,total_jobs,created_at) values (%s,%s,%s,%s,%s,%s)'
                        values = (employer_id, 2,'trialing',0,0,created_at,)
                        user_plan_table = update_query(query,values)
                        if row_count_emp_profile > 0 and row_count_hiring_table > 0: #and user_plan_table > 0:
                            token_result = get_jwt_access_token(employer_id,email_id)                
                            if token_result["status"] == "success":
                                access_token =  token_result["access_token"]                                        
                                # query = "Update users set email_active = %s where email_id = %s"
                                # values = ('Y', email_id)  
                                # res = update_query(query, values)           
                                query = "Update users set profile_image = %s, payment_status = %s, is_active = %s, email_active = %s where email_id = %s"
                                values = ('default_profile_picture_employer.png', 'unpaid', 'Y','Y',email_id)  
                                res = update_query(query, values)    
                                res_data = {"access_token" : access_token,"user_role":user_data["user_role"], "registration_status":flag_status}
                                # create_trial_session(email_id,EMPLOYER_TRIAL_PERIOD,EMPLOYER_BASIC_PLAN_ID,"Basic")
                                try:
                                    add_event_properties = {    
                                        '$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'Email'         : email_id,
                                        'Signup Mode' : user_data['login_mode'],
                                        'User plan' :  user_data['pricing_category'],
                                        'Organization Name' : organization_name       
                                    }
                                    # event_name = "Employer Profile Creation"
                                    mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Employer Profile created")
                                    # background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
                                    # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile created", user_data)
                                    
                                except Exception as e:  
                                    print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        background_runner.get_employer_details(employer_id)
                        result_json = api_json_response_format(True,"Your account has been created successfully.",0,res_data)     
                else:
                    try:
                        add_event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),
                            'First Name'    : first_name,
                            'Last Name'     : last_name,
                            'Email'         : email_id,
                            'Signup Mode' : user_data['login_mode'],
                            'Organization Name' : organization_name,
                            'Error' : 'Token error ' + str(token_result["stauts"])            
                        }
                        # event_name = "Employer Profile Error"
                        mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{add_event_properties['Email']} profile not created because of Token error")
                        # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} profile not created because of Token error", user_data)
                    except Exception as e:  
                        print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                    result_json = api_json_response_format(False,token_result["stauts"],401,{})   

            else:
                result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
                return result_json      
            
        elif flag == "employer_account_details":
            event_name = 'Employer Account Details'
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json  
            # if 'first_name' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'last_name' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'sector' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'designation' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'industry' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'website' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'country' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'city' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json       
            # if 'contact_number' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'country_code' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            email_id = req_data.get('email_id')
            first_name = req_data.get('first_name')
            last_name = req_data.get('last_name')
            # organization_type = ""
            organization_type = req_data.get('organization_type')
            designation = req_data.get('designation')
            sector = req_data.get('industry')
            website = req_data.get('website')
            country = req_data.get('country')
            city = req_data.get('city')
            # user_pwd = req_data.get('user_pwd')
            contact_number = req_data.get('contact_number')
            country_code = req_data.get('country_code')
            if first_name is None or str(first_name).strip() == "" or str(first_name).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if last_name is None or str(last_name).strip() == "" or str(last_name).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if sector is None or str(sector).strip() == "" or str(sector).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if designation is None or str(designation).strip() == "" or str(designation).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if website is None or str(website).strip() == "" or str(website).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if city is None or str(city).strip() == "" or str(city).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if country is None or str(country).strip() == "" or str(country).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if contact_number is None or str(contact_number).strip() == "" or str(contact_number).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if country_code is None or str(country_code).strip() == "" or str(country_code).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if organization_type is None or str(organization_type).strip() == "" or str(organization_type).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            query = "update users set first_name = %s, last_name = %s,country_code = %s,contact_number = %s,country = %s,city = %s,created_at = %s where email_id = %s"
            values = (first_name,last_name,country_code,contact_number,country,city,created_at,email_id,)
            row_count = update_query(query,values)
        else:
            result_json = api_json_response_format(False,"Enter a valid flag.",204,{})  
            return result_json
        
        if row_count > 0:
                
                user_data = get_user_data(email_id)
                employer_id = user_data["user_id"]
                if flag == "employer_account_details":
                    query = 'update employer_profile set designation = %s, employer_type = %s, sector = %s, website_url = %s, created_at = %s where employer_id = %s'
                    values = (designation,organization_type,sector,website,created_at,employer_id) 
                    row_count_emp_profile = update_query(query,values)
                    query = 'update hiring_team set first_name = %s,last_name = %s,designation = %s,country_code = %s,contact_number = %s,email_id = %s,created_at = %s where employer_id = %s'
                    values = (first_name,last_name,designation,country_code,contact_number,email_id,created_at,employer_id)
                    row_count_hiring_table = update_query(query,values) 
                    if row_count_emp_profile > 0 and row_count_hiring_table > 0: #and user_plan_table > 0:
                        query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
                        values2 = ('employer_account_details', employer_id)   # Example: set flag_status=1
                        row_count2 = update_query(query2, values2)
                        token_result = get_jwt_access_token(employer_id,email_id)                
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]   
                            if flag == "employer_account_details":       
                                query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
                                values2 = ('completed', employer_id)   # Example: set flag_status=1
                                row_count2 = update_query(query2, values2)             
                            query = "select flag_status from users where user_id = %s"
                            values = (employer_id,)
                            res = execute_query(query, values)
                            if res:
                                flag_status = res[0]['flag_status']                                        
                            res_data = {"access_token" : access_token,"user_role":user_role, "Registration_status": flag_status}
                            
                            try:
                                add_event_properties = {    
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'First Name'    : first_name,
                                    'Last Name'     : last_name,
                                    'Email'         : email_id,
                                    'Sector' : sector,
                                    'Organization Type' : organization_type,
                                    "Employer's Title" : designation,
                                    'Website' : website, 
                                    'City': city,
                                    'Country': country,
                                    'Signup Mode' : 'Manual',
                                    'User plan' :  user_data['pricing_category']       
                                }
                                # event_name = "Employer Profile Creation"
                                mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Employer Profile created")
                                # organization_name, organization_type, sector,designation, website
                                # background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
                                # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile created", user_data)
                            except Exception as e:  
                                print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                            background_runner.get_employer_details(employer_id)
                            result_json = api_json_response_format(True,"Your account has been updated.",0,res_data)     
                        else:
                            try:
                                add_event_properties = {    
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'First Name'    : first_name,
                                    'Last Name'     : last_name,
                                    'Email'         : email_id,
                                    'Signup Mode' : 'Manual',
                                    'Error' : 'Token error ' + str(token_result["stauts"])            
                                }
                                event_name = "Employer Profile Error"
                                background_runner.mixpanel_event_async(email_id,event_name,add_event_properties, f"{add_event_properties['Email']} profile not created because of Token error", user_data)
                            except Exception as e:  
                                print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                            result_json = api_json_response_format(False,token_result["stauts"],401,{})                   
                    else:
                        query = "delete from users where email_id = %s"
                        values = (email_id,)  
                        update_query(query, values)
                        try:
                            add_event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'First Name'    : first_name,
                                'Last Name'     : last_name,
                                'Email'         : email_id,
                                'Signup Mode' : 'Manual',
                                'Error' : 'Data Base error. Record not inserted'          
                            }
                            event_name = "Employer Profile Error"
                            background_runner.mixpanel_event_async(email_id,event_name,add_event_properties, f"Record not found so, unable to create {add_event_properties['Email']} Profile.", user_data)
                        except Exception as e:  
                            print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})
        else:
            try:
                event_properties = {    
                    '$distinct_id' : email_id, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),
                    'First Name'    : first_name,
                    'Last Name'     : last_name,
                    'Email'         : email_id,
                    'Signup Mode' : 'Manual',
                    'Error' : 'Data Base error. Record not inserted'          
                }
                event_name = "Employer Profile Error"
                background_runner.mixpanel_event_async(email_id,event_name,add_event_properties, f"Record not found so, unable to create {add_event_properties['Email']} Profile.", user_data)
            except Exception as e:  
                print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
            result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})
                

    except Exception as error:
        print(error)
        result_json = api_json_response_format(False,str(error),500,{}) 
    finally:
        return result_json

# def partner_register():
#     try:
#         req_data = request.get_json()        
#         if 'first_name' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'last_name' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'company_name' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         # if 'website' not in req_data:
#         #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#         #     return result_json
#         if 'title' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'email_id' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json    
#         if 'country' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json  
#         if 'city' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json 
#         if 'partner_type' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'sector' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'user_pwd' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'contact_number' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json
#         if 'country_code' not in req_data:
#             result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#             return result_json               
#         # contact_number = None
#         # if 'contact_number' in req_data:
#         #     contact_number = req_data['contact_number']
#         # country_code = None
#         # if 'country_code' in req_data:
#         #     country_code = req_data['country_code'] 
#         first_name = req_data['first_name']
#         last_name = req_data['last_name']
#         company_name = req_data['company_name']
#         if 'website' in req_data:
#             website = req_data['website']
#         else: 
#             website = ''
#         title =req_data['title']
#         email_id = req_data['email_id']
#         country = req_data['country']
#         city = req_data['city']
#         partner_type = req_data['partner_type']
#         sector = req_data['sector']
#         user_pwd = req_data['user_pwd']
#         contact_number = req_data['contact_number']
#         country_code = req_data['country_code']
#         login_mode = 'Manual'        
#         user_role = 'partner'
#         user_data = get_user_data(email_id)
#         if user_data["is_exist"]:
#             if user_data["user_role"] == "professional":
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already registered as professional.",409,{})  
#                 return result_json
#             elif user_data["user_role"] == "employer":
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already registered as employer.",409,{})  
#                 return result_json
#             else:
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
#                 return result_json
#         sub_user_data = get_sub_user_data(email_id)
#         if sub_user_data["is_exist"]:
#             if sub_user_data["user_role"] == "employer_sub_admin" or sub_user_data["user_role"] == "recruiter":
#                 result_json = api_json_response_format(False, "It appears that an account with this email address already associated with us.",409,{})  
#                 return result_json
#             else:
#                 result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
#                 return result_json
#         created_at = datetime.datetime.now()
#         query = 'insert into users (user_role_fk,first_name,last_name,email_id,profile_image,country,city,country_code,contact_number,user_pwd,login_mode,is_active,created_at) values ((select role_id from user_role where user_role = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
#         values = (user_role,first_name,last_name,email_id,'default_profile_picture_partner.png',country,city,country_code,contact_number,user_pwd,login_mode,'Y',created_at,)
#         row_count = update_query(query,values)
#         if row_count > 0:
#             user_data = get_user_data(email_id)
#             partner_id = user_data["user_id"]
#             query = 'insert into partner_profile (partner_id,designation,company_name,partner_type,sector,website_url,created_at) values (%s,%s,%s,%s,%s,%s,%s)'
#             values = (partner_id, title, company_name,partner_type,sector,website,created_at,)
#             row_count_partner_profile = update_query(query,values)
#             query = 'insert into partner_team(partner_id,first_name,last_name,designation,country_code,contact_number,email_id,created_at) values (%s,%s,%s,%s,%s,%s,%s,%s)'
#             values = (partner_id, first_name,last_name,title,country_code,contact_number,email_id,created_at,)
#             row_count_partner_table = update_query(query,values)
#             query = 'insert into user_plan_details(user_id,user_role_fk,user_plan,no_of_jobs,total_jobs,created_at) values (%s,%s,%s,%s,%s,%s)'
#             values = (partner_id, 6,'trialing',0,0,created_at,)
#             user_plan_table = update_query(query,values)
#             if row_count_partner_profile > 0 and row_count_partner_table:
#                 token_result = get_jwt_access_token(partner_id,email_id)                
#                 if token_result["status"] == "success":
#                     access_token =  token_result["access_token"]                                     
#                     subject = "Email Verification"
#                     recipients = [email_id] 
#                     print(os.getcwd()+"/templates/Email_verification.html")                   
#                     index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
#                     index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(partner_id))
#                     body = index
#                     sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Partner Email Verification")                          
#                     res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
#                     # create_trial_session(email_id,PARTNER_TRIAL_PERIOD,PARTNER_BASIC_PLAN_ID,"Basic")
#                     try:
#                         user_properties = {
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),                
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Company Name' : company_name,
#                             'Sector' : sector,
#                             'Partner Type' : partner_type,
#                             'Website' : website, 
#                             'City': city,
#                             'Country': country,
#                             'Signup Mode' : 'Manual',
#                             'User plan' :  user_data['pricing_category']
#                         }

#                         event_properties = {    
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Company Name' : company_name,
#                             'Sector' : sector,
#                             'Partner Type' : partner_type,
#                             'Website' : website, 
#                             'City': city,
#                             'Country': country,
#                             'Signup Mode' : 'Manual',
#                             'User plan' :  user_data['pricing_category']
#                         }
#                         event_name = "Partner Profile Creation"
#                         background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile Created", user_data)
#                         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile Created", user_data)
#                     except Exception as e:  
#                         print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                     background_runner.get_partner_details(partner_id)
#                     result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)     
#                 else:
#                     try:
#                         event_properties = {    
#                             '$distinct_id' : email_id, 
#                             '$time': int(time.mktime(dt.now().timetuple())),
#                             '$os' : platform.system(),
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Signup Mode' : 'Manual',
#                             'Error' : 'Token error ' + str(token_result["stauts"])            
#                         }
#                         event_name = "Partner Profile Error"
#                         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} profile Error due to token issue", user_data)
#                     except Exception as e:  
#                         print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                     result_json = api_json_response_format(False,token_result["stauts"],401,{})                  
#             else:
#                 query = "delete from users where email_id = %s"
#                 values = (email_id,)  
#                 update_query(query, values)
#                 try:
#                     event_properties = {    
#                         '$distinct_id' : email_id, 
#                         '$time': int(time.mktime(dt.now().timetuple())),
#                         '$os' : platform.system(),
#                         'First Name'    : first_name,
#                         'Last Name'     : last_name,
#                         'Email'         : email_id,
#                         'Signup Mode' : 'Manual',
#                         'Error' : 'Data Base error. Record not inserted'
#                     }
#                     event_name = "Partner Profile Error"
#                     background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Record not Found So, unable to create {event_properties['Email']} profile", user_data)
#                 except Exception as e:  
#                     print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
#                 result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})
#         else:
#             try:
#                 event_properties = {    
#                     '$distinct_id' : email_id, 
#                     '$time': int(time.mktime(dt.now().timetuple())),
#                     '$os' : platform.system(),
#                     'First Name'    : first_name,
#                     'Last Name'     : last_name,
#                     'Email'         : email_id,
#                     'Signup Mode' : 'Manual',
#                     'Error' : 'Data Base error. Record not inserted'
#                 }
#                 event_name = "Partner Profile Error"
#                 background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Record not Found So, unable to create {event_properties['Email']} profile", user_data)
#             except Exception as e:  
#                 print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
#             result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})            
#     except Exception as error:
#         result_json = api_json_response_format(False,str(error),500,{}) 
#     finally:
#         return result_json
    
def partner_register():
    try:
        result_json = {}
        if request.is_json:
                req_data = request.get_json()

        flag = req_data.get("flag")
        if not flag:
            result_json = api_json_response_format(False,"Please fill in all the flag fields.",204,{})  
            return result_json

        if 'email_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        email_id = req_data.get('email_id')
        if check(email_id):
            pass
        else:
            result_json = api_json_response_format(False,"Invalid email id",204,{})
            return result_json
        
        # login_mode = 'Manual'        
        user_role = 'partner'
        user_data = get_user_data(email_id)
        created_at = datetime.datetime.now()
        if user_data["is_exist"]:
            if user_data["user_role"] == "professional":
                result_json = api_json_response_format(False,"It appears that an account with this email address already registered as professional.",409,{})  
                return result_json
            elif user_data["user_role"] == "employer":
                result_json = api_json_response_format(False,"It appears that an account with this email address already registered as employer.",409,{})  
                return result_json
            # else:
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json
        sub_user_data = get_sub_user_data(email_id)
        if sub_user_data["is_exist"]:
            if sub_user_data["user_role"] == "employer_sub_admin" or sub_user_data["user_role"] == "recruiter":
                result_json = api_json_response_format(False, "It appears that an account with this email address already associated with us.",409,{})  
                return result_json
            # else:
            #     result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
            #     return result_json
        if flag == "new_user_signup":
            event_name = 'New Partner Signup'
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'login_mode' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            login_mode = req_data.get('login_mode')
            if login_mode == '' or login_mode == None:
                    result_json = api_json_response_format(False,"Something went wrong.",204,{})  
                    return result_json
            if login_mode == "Manual":
                if 'user_pwd' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json  
                user_pwd = req_data.get('user_pwd')
            else:
                user_pwd = None
            
            organization_name = req_data.get('organization_name')
            if organization_name is None or str(organization_name).strip() == "" or str(organization_name).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            query = "select email_id from users where email_id = %s"
            values = (email_id,)
            res = execute_query(query, values)
            if not res:
                query = 'insert into users (user_role_fk,email_id,user_pwd,login_mode,flag_status,created_at) values ((select role_id from user_role where user_role = %s),%s,%s,%s,%s,%s)'
                values = (user_role,email_id,user_pwd,login_mode,'new_user_signup',created_at,)
                row_count = update_query(query,values)
                if row_count > 0 :
                    user_data = get_user_data(email_id)
                    user_id = user_data["user_id"]
                    
                    query = "select flag_status from users where user_id = %s"
                    values = (user_id,)
                    res = execute_query(query, values)
                    if res:
                        flag_status = res[0]['flag_status']

                    if user_id:
                        query = "insert into partner_profile(partner_id, company_name, created_at) values (%s, %s, %s)"
                        values = (user_id, organization_name, created_at)
                        row_count_partner_profile = update_query(query, values)
                        
                    else:
                        result_json = api_json_response_format(False,"No record found.",409,{})
                        return result_json
                
                    partner_id = user_data["user_id"]

                    if login_mode == "Manual":

                        # query = 'insert into employer_profile (employer_id,designation,company_name,employer_type,sector,website_url,created_at) values (%s,%s,%s,%s,%s,%s,%s)'
                        # values = (employer_id, title, organization_name,organization_type,sector,website,created_at,)
                        # row_count_emp_profile = update_query(query,values)
                        query = 'insert into partner_team(partner_id,email_id,created_at) values (%s,%s,%s)'
                        values = (partner_id,email_id,created_at)
                        row_count_partner_table = update_query(query,values) 
                        query = 'insert into user_plan_details(user_id,user_role_fk,user_plan,no_of_jobs,total_jobs,created_at) values (%s,%s,%s,%s,%s,%s)'
                        values = (partner_id, 6,'trialing',0,0,created_at)
                        user_plan_table = update_query(query,values)
                        if row_count_partner_profile > 0 and row_count_partner_table: #and user_plan_table > 0:
                            token_result = get_jwt_access_token(partner_id,email_id)                
                            if token_result["status"] == "success":
                                access_token =  token_result["access_token"]                                     
                                subject = "Email Verification"
                                recipients = [email_id] 
                                print(os.getcwd()+"/templates/Email_verification.html")                   
                                index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
                                # index = open("/home/adrasw-sam/Documents/2nddevsource/2nd/2ndcareers-back-end/second_careers_project/templates/Email_verification.html", 'r').read()
                                index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(partner_id))
                                body = index
                                sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Partner Email Verification")                          
                                query = "Update users set profile_image = %s, payment_status = %s, is_active = %s where email_id = %s"
                                values = ('default_profile_picture_partner.png', 'unpaid', 'Y',email_id)  
                                res = update_query(query, values)                     
                                res_data = {"access_token" : access_token,"user_role":user_data["user_role"], "registration_status":flag_status}
                                # create_trial_session(email_id,PARTNER_TRIAL_PERIOD,PARTNER_BASIC_PLAN_ID,"Basic")
                                try:
                                    add_event_properties = {    
                                        '$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'Email'         : email_id,
                                        'Organization Name' : organization_name,
                                        'Signup Mode' : 'Manual',
                                        'User plan' :  user_data['pricing_category']       
                                    }
                                    # event_name = "Employer Profile Creation"
                                    mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Employer Profile created")
                                    # background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
                                    # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile created", user_data)
                                    
                                except Exception as e:  
                                    print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
                                # background_runner.get_employer_details(employer_id)
                                # result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)     
                            # else:
                            #     try:
                            #         event_properties = {    
                            #             '$distinct_id' : email_id, 
                            #             '$time': int(time.mktime(dt.now().timetuple())),
                            #             '$os' : platform.system(),
                            #             'First Name'    : first_name,
                            #             'Last Name'     : last_name,
                            #             'Email'         : email_id,
                            #             'Signup Mode' : 'Manual',
                            #             'Error' : 'Token error ' + str(token_result["stauts"])            
                            #         }
                            #         event_name = "Employer Profile Error"
                            #         background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} profile not created because of Token error", user_data)
                            #     except Exception as e:  
                            #         print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                            #     result_json = api_json_response_format(False,token_result["stauts"],401,{})       
                            background_runner.get_partner_details(partner_id)
                            result_json = api_json_response_format(True,"Your account has been created successfully. A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)     
               
                    else:
                        query = 'insert into partner_team(partner_id,email_id,created_at) values (%s,%s,%s)'
                        values = (partner_id,email_id,created_at)
                        row_count_partner_table = update_query(query,values) 
                        query = 'insert into user_plan_details(user_id,user_role_fk,user_plan,no_of_jobs,total_jobs,created_at) values (%s,%s,%s,%s,%s,%s)'
                        values = (partner_id, 6,'trialing',0,0,created_at)
                        user_plan_table = update_query(query,values)
                        if row_count_partner_profile > 0 and row_count_partner_table > 0: #and user_plan_table > 0:
                            token_result = get_jwt_access_token(partner_id,email_id)                
                            if token_result["status"] == "success":
                                access_token =  token_result["access_token"]                                  
                                # query = "Update users set email_active = %s where email_id = %s"
                                # values = ('Y', email_id)  
                                # res = update_query(query, values)     
                                query = "Update users set profile_image = %s, payment_status = %s, is_active = %s, email_active = %s where email_id = %s"
                                values = ('default_profile_picture_partner.png', 'unpaid', 'Y', 'Y', email_id)  
                                res = update_query(query, values)                             
                                res_data = {"access_token" : access_token,"user_role":user_role, "registration_status":flag_status}
                                # create_trial_session(email_id,EMPLOYER_TRIAL_PERIOD,EMPLOYER_BASIC_PLAN_ID,"Basic")
                                try:
                                    add_event_properties = {    
                                        '$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'Email'         : email_id,
                                        'Signup Mode' : user_data['login_mode'],
                                        'User plan' :  user_data['pricing_category']       
                                    }
                                    # event_name = "Employer Profile Creation"
                                    mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Partner Profile created")
                                    # background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
                                    # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile created", user_data)
                                    
                                except Exception as e:  
                                    print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        background_runner.get_partner_details(partner_id)
                        result_json = api_json_response_format(True,"Your account has been created successfully.",0,res_data)     
                else:
                    try:
                        add_event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),
                            'Email'         : email_id,
                            'Signup Mode' : user_data['login_mode'],
                            'Error' : 'Token error ' + str(token_result["stauts"])            
                        }
                        # event_name = "Employer Profile Error"
                        mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{add_event_properties['Email']} profile not created because of Token error")
                        # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} profile not created because of Token error", user_data)
                    except Exception as e:  
                        print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
                    result_json = api_json_response_format(False,token_result["stauts"],401,{})   

            else:
                result_json = api_json_response_format(False,"It appears that an account with this email address already exists. Please sign in using your existing credentials.",409,{})  
                return result_json      
        elif flag == "partner_account_details":
            event_name = 'Partner Account Details'
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json  
            # if 'first_name' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'last_name' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'sector' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'designation' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'industry' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'website' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'country' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            # if 'city' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json       
            # if 'contact_number' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'country_code' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json  
            email_id = req_data.get('email_id')
            first_name = req_data.get('first_name')
            last_name = req_data.get('last_name')
            # organization_type = ""
            partner_type = req_data.get('partner_type')
            designation = req_data.get('designation')
            sector = req_data.get('industry')
            website = req_data.get('website')
            country = req_data.get('country')
            city = req_data.get('city')
            # user_pwd = req_data.get('user_pwd')
            contact_number = req_data.get('contact_number')
            country_code = req_data.get('country_code')
            if first_name is None or str(first_name).strip() == "" or str(first_name).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if last_name is None or str(last_name).strip() == "" or str(last_name).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if sector is None or str(sector).strip() == "" or str(sector).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if designation is None or str(designation).strip() == "" or str(designation).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if website is None or str(website).strip() == "" or str(website).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if city is None or str(city).strip() == "" or str(city).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if country is None or str(country).strip() == "" or str(country).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if contact_number is None or str(contact_number).strip() == "" or str(contact_number).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if country_code is None or str(country_code).strip() == "" or str(country_code).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            if partner_type is None or str(partner_type).strip() == "" or str(partner_type).strip().lower() == "null":
                result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
                return result_json
            query = "update users set first_name = %s, last_name = %s,country_code = %s,contact_number = %s,country = %s,city = %s,created_at = %s where email_id = %s"
            values = (first_name,last_name,country_code,contact_number,country,city,created_at,email_id)
            row_count = update_query(query,values)
        else:
            result_json = api_json_response_format(False,"Enter a valid flag.",204,{})  
            return result_json
        
        if row_count > 0:
                
                user_data = get_user_data(email_id)
                partner_id = user_data["user_id"]
                if flag == "partner_account_details":
                    query = 'update partner_profile set designation = %s, partner_type = %s, sector = %s, website_url = %s, created_at = %s where partner_id = %s'
                    values = (designation,partner_type,sector,website,created_at,partner_id) 
                    row_count_partner_profile = update_query(query,values)
                    query = 'update partner_team set first_name = %s,last_name = %s,designation = %s,country_code = %s,contact_number = %s,email_id = %s,created_at = %s where partner_id = %s'
                    values = (first_name,last_name,designation,country_code,contact_number,email_id,created_at,partner_id)
                    row_count_partner_table = update_query(query,values) 
                    if row_count_partner_profile > 0 and row_count_partner_table > 0: #and user_plan_table > 0:
                        query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
                        values2 = ('partner_account_details', partner_id)   # Example: set flag_status=1
                        row_count2 = update_query(query2, values2)
                        token_result = get_jwt_access_token(partner_id,email_id)                
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]   
                            if flag == "partner_account_details":       
                                query2 = "UPDATE users SET flag_status = %s WHERE user_id = %s"
                                values2 = ('completed', partner_id)   # Example: set flag_status=1
                                row_count2 = update_query(query2, values2)             
                            query = "select flag_status from users where user_id = %s"
                            values = (partner_id,)
                            res = execute_query(query, values)
                            if res:
                                flag_status = res[0]['flag_status']                                        
                            res_data = {"access_token" : access_token,"user_role":user_role, "Registration_status": flag_status}
                            
                            try:
                                add_event_properties = {    
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'First Name'    : first_name,
                                    'Last Name'     : last_name,
                                    'Email'         : email_id,
                                    'Sector' : sector,
                                    'Organization Type' : partner_type,
                                    "Employer's Title" : designation,
                                    'Website' : website, 
                                    'City': city,
                                    'Country': country,
                                    'Signup Mode' : user_data['login_mode'],
                                    'User plan' :  user_data['pricing_category']       
                                }
                                # event_name = "Employer Profile Creation"
                                mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{email_id} Employer Profile created")
                                # organization_name, organization_type, sector,designation, website
                                # background_runner.mixpanel_user_async(email_id,user_properties, f"{event_properties['Email']} Profile created", user_data)
                                # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} Profile created", user_data)
                            except Exception as e:  
                                print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
                            background_runner.get_partner_details(partner_id)
                            result_json = api_json_response_format(True,"Your account has been updated.",0,res_data)     
                        else:
                            try:
                                add_event_properties = {    
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'First Name'    : first_name,
                                    'Last Name'     : last_name,
                                    'Email'         : email_id,
                                    'Signup Mode' : user_data['login_mode'],
                                    'Error' : 'Token error ' + str(token_result["stauts"])            
                                }
                                # event_name = "Partner Profile Error"
                                mixpanel_task(email_id, user_data, event_name,add_event_properties, f"{add_event_properties['Email']} profile not created because of Token error")
                                # background_runner.mixpanel_event_async(email_id,event_name,add_event_properties, f"{add_event_properties['Email']} profile not created because of Token error", user_data)
                            except Exception as e:  
                                print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
                            result_json = api_json_response_format(False,token_result["stauts"],401,{})                   
                    else:
                        query = "delete from users where email_id = %s"
                        values = (email_id,)  
                        update_query(query, values)
                        try:
                            add_event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'First Name'    : first_name,
                                'Last Name'     : last_name,
                                'Email'         : email_id,
                                'Signup Mode' : user_data['login_mode'],
                                'Error' : 'Data Base error. Record not inserted'          
                            }
                            # event_name = "Partner Profile Error"
                            mixpanel_task(email_id, user_data, event_name,add_event_properties,f"Record not found so, unable to create {add_event_properties['Email']} Profile.")
                            # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Record not found so, unable to create {event_properties['Email']} Profile.", user_data)
                        except Exception as e:  
                            print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})
                else:
                    result_json = result_json
        else:
            try:
                add_event_properties = {    
                    '$distinct_id' : email_id, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),
                    'First Name'    : first_name,
                    'Last Name'     : last_name,
                    'Email'         : email_id,
                    'Signup Mode' : user_data['login_mode'],
                    'Error' : 'Data Base error. Record not inserted'          
                }
                # event_name = "Partner Profile Error"
                mixpanel_task(email_id, user_data, event_name,add_event_properties,f"Record not found so, unable to create {add_event_properties['Email']} Profile.")
                # background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Record not found so, unable to create {event_properties['Email']} Profile.", user_data)
            except Exception as e:  
                print("Error in Partner Profile Creation mixpanel_user_and_event_log : %s",str(e))
            result_json = api_json_response_format(False,"Sorry, we encountered an issue while creating your account. Please try again.",500,{})
        

    except Exception as error:
            result_json = api_json_response_format(False,str(error),500,{})  
            print(result_json)
    finally:
        return result_json

def reset_password_validation():
    try:
        if 'eded128fdf05f4c0a2e29d2b121e5c710b9bd54dbcd98a64a9b02ab6e2564115' not in request.args:
            result_json = api_json_response_format(False,"Invalid Link",401,{})  
            return result_json
        reset_token = request.args.get('eded128fdf05f4c0a2e29d2b121e5c710b9bd54dbcd98a64a9b02ab6e2564115')         
        token_auth_result = token_authentication(reset_token)                    
        if token_auth_result["status_code"] == 200:
            token_list = token_auth_result["email_id"].split("######")
            if len(token_list) > 1:
                user_data = get_user_data(token_list[1])
                if not user_data['is_exist']:
                    user_data = get_sub_user_data(token_list[1])
                if token_list[0] == user_data["user_pwd"]:                
                    token_result = get_jwt_access_token(user_data["user_id"],user_data["email_id"]) 
                    if token_result["status"] == "success":  
                        access_token = token_result["access_token"] 
                        url_redirect = WEB_APP_URI+"/reset_password?token="+access_token
                        return url_redirect 
                    else:
                        result_json = api_json_response_format(False,token_result["status"],401,{}) 
                elif user_data["user_pwd"] == '' or user_data['user_pwd'] == None:
                    url_redirect = WEB_APP_URI+"?message=You haven't set up your password yet, please check your email for the password setup link."  
                    return url_redirect
                else:
                    url_redirect = WEB_APP_URI+"?message=Reset password already used."  
                    return url_redirect 
    except Exception as error:
        print("Exception in reset_password_validation() : ",error)        
        url_redirect = WEB_APP_URI+"?message=Reset password already used."  
        return url_redirect 
        

    
    
def email_verification():
    mail_content = open(os.getcwd()+"/templates/emailFailure.html",'r').read()
    # mail_content = open("/home/adrasw-sam/Documents/2nddevsource/2nd/2ndcareers-back-end/second_careers_project/templates/emailFailure.html", 'r').read()
    try:
        if 'f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b' not in request.args:
            result_json = api_json_response_format(False,"Invalid Link",401,{})  
            return result_json
        user_id = request.args.get('f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b')  
        
        query = ''
        count_query = 'select count(user_id) as count from users where user_id = %s'
        value = (user_id,)
        id_count_dict = execute_query(count_query, value)
        if id_count_dict:
            if id_count_dict[0]['count'] > 0:
                query = 'select email_active, email_id, first_name, user_role_fk from users where user_id = %s'
            else:
                query = 'select count(sub_user_id) as count from sub_users where sub_user_id = %s'
                value = (user_id,)
                sub_user_id_dict = execute_query(query, value)
                if sub_user_id_dict:
                    if sub_user_id_dict[0]['count'] > 0:
                        query = 'select email_active, email_id, first_name, user_role_fk from sub_users where sub_user_id = %s'
                    else:
                        return mail_content
        value = (user_id,)
        email_active = execute_query(query, value)
        user_role = email_active[0]['user_role_fk']
        if email_active[0]["email_active"] == 'Y':
            mail_content = open(os.getcwd()+"/templates/email_already_verified.html",'r').read()
        else:
            query = 'update users set email_active = %s where user_id = %s'
            value = ('Y', user_id,)
            row_count = update_query(query, value)
            if row_count > 0:
                index = open(os.getcwd()+"/templates/emailSuccess.html",'r').read()
                # index = open("/home/adrasw-sam/Documents/2nddevsource/2nd/2ndcareers-back-end/second_careers_project/templates/emailSuccess.html",'r').read()
                index = index.replace("{{link}}",WEB_APP_URI)
                
                mail_content = index
                #if user_role is professional only the welcome should go
                if user_role == 3:
                    first_name = email_active[0].get("first_name") or ""
                    if first_name:
                        subject = f"Welcome to Second Careers, {first_name}! Let's get you started."
                    else:
                        subject = "Welcome to our project! Let's get you started."
                    subject = subject.replace("{{first_name}}", first_name)
                    recipients = email_active[0]["email_id"]

                    welcome_index = open(os.getcwd()+"/templates/welcome_message.html",'r').read()
                    # welcome_index = open("/home/adrasw-sam/Documents/2nddevsource/2nd/2ndcareers-back-end/second_careers_project/templates/welcome_message.html", 'r').read()
                    welcome_index = welcome_index.replace("{{First Name}}",first_name)
                    body = welcome_index
                    sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Professional Welcome Mail")
                try:
                    event_properties = {    
                        '$distinct_id' : email_active[0]['email_id'], 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'is_email_verified' : "Yes",
                        'Email' : email_active[0]['email_id']
                    }
                    event_name = "Email Verified"
                    background_runner.mixpanel_event_async(email_active[0]['email_id'],event_name,event_properties,f"{event_properties['Email']} Verified Successfully", user_data=None)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
        # else:
        #     mail_content = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
    except Exception as error:
        print(error)        
        # result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return mail_content

def update_professional_account_details():
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                req_data = request.get_json()
                if 'first_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json                
                if 'last_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json 
                if 'contact_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'contact_code' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'country' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'city' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'email_id'  in req_data:
                    user_email = req_data["email_id"]
                    if user_email != user_data["email_id"]:
                        result_json = api_json_response_format(False,"Please enter a valid email ID.",204,{})  
                        return result_json

                user_id = user_data["user_id"]
                first_name = req_data["first_name"]
                last_name = req_data["last_name"]     
                # contact_number = None
                # if 'contact_number' in req_data:
                #     contact_number = req_data['contact_number']
                # country_code = None
                # if 'country_code' in req_data:
                #     country_code = req_data['country_code']
                contact_number = req_data['contact_number']
                country_code = req_data['country_code']
                country = req_data["country"]
                city = req_data["city"]                
                query = 'update users set first_name = %s,last_name = %s,country_code=%s, contact_number = %s, country = %s, city = %s where user_id = %s'
                values = (first_name, last_name, country_code, contact_number, country, city, user_id,)
                row_count = update_query(query,values)                
                # query = 'insert into professional_profile (professional_id) values(%s)'
                # values = (user_id)
                # row_count = update_query(query,values)

                if row_count > 0:
                    result_json = api_json_response_format(True,"Your account has been updated successfully!",0,{})
                else:
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your account. We request you to retry.",500,{})
                
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_social_media_account_info():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])                        
            if user_data["is_exist"]:
                professional_id = user_data["user_id"]                                   
                query = 'select first_name, last_name, email_id, country_code, contact_number, country, state, city,login_mode from users where user_id=%s'
                values = (professional_id,)
                profile_data_set = execute_query(query, values) 
                result_json = api_json_response_format(True,"User details displayed successfully",0,profile_data_set)                    
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

    

def resend_email():
    result_json = {}
    try:
        req_data = request.get_json() 
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200: 
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the fields(status)",204,{})
                return result_json
            email_id = req_data['email_id']     

            user_data = get_user_data(email_id)
            query = 'select email_active from users where user_id = %s'
            user_id = user_data["user_id"]

            if not user_data["is_exist"]:
                user_data = get_sub_user_data(email_id)
                query = 'select email_active from sub_users where sub_user_id = %s'
                user_id = user_data["sub_user_id"]
            if user_data['is_exist']:
                value = (user_id,)
                email_active = execute_query(query, value)
                if not email_active[0]["email_active"] == 'Y':
                    token_result = get_jwt_access_token(user_id,email_id)                
                    if token_result["status"] == "success":
                        access_token =  token_result["access_token"]                                     
                        subject = "Email Verification"
                        recipients = [email_id] 
                        print(os.getcwd()+"/templates/Email_verification.html")                   
                        index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
                        index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(user_id))
                        body = index
                        sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Resend Verification Email")                          
                        res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
                        try:
                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email resend status' : "Success",
                                'Email' : email_id
                            }
                            event_name = "Resend Email"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"{event_properties['Email']} resent successfully", user_data)
                        except Exception as e:  
                            print("Error in mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(True," A verification link has been sent to your registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)     
                    else:
                        try:
                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email resend status' : "Failure",
                                'Email' : email_id,
                                'Error' : "Error in resending email."
                            }
                            event_name = "Resend Email Error"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"The resent {event_properties['Email']} was not sent correctly", user_data)
                        except Exception as e:  
                            print("Error in mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False,token_result["stauts"],401,{})    
                else:
                    token_result = get_jwt_access_token(user_id,email_id)                
                    if token_result["status"] == "success":
                        access_token =  token_result["access_token"]
                        res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
                        try:
                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email resend status' : "Failure",
                                'Email' : email_id,
                                'Error' : "Error in resending email. User email already verified."
                            }
                            event_name = "Resend Email Error"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Resending the email failed because the {event_properties['Email']} is already verified", user_data)
                        except Exception as e:  
                            print("Error in mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(True," Your email already verified. Please signin to proceed using the platform",0,res_data)
                    else:
                        try:
                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email resend status' : "Failure",
                                'Email' : email_id,
                                'Error' : "Error in resending email. User email already verified."
                            }
                            event_name = "Resend Email Error"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Resending the email failed because {event_properties['Email']} is already verified", user_data)
                        except Exception as e:  
                            print("Error in mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(True," Your email already verified. Please signin to proceed using the platform",0,{})
            else:
                try:
                    event_properties = {    
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Email resend status' : "Failure",
                        'Email' : email_id,
                        'Error' : "Error in resending email. User account not exists."
                    }
                    event_name = "Resend Email Error"
                    background_runner.mixpanel_event_async(email_id,event_name,event_properties, f"Failed to resend the email because {event_properties['Email']} account does not exist", user_data)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"User profile Not Found",204,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def redirect_email():
    result_json = {}
    try:        
        req_data = request.get_json() 
        if 'email_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(status)",204,{})
            return result_json
        email_id = req_data['email_id']

        query = ''
        count_query = 'select count(user_id) as count from users where email_id = %s'
        value = (email_id,)
        id_count_dict = execute_query(count_query, value)
        if id_count_dict:
            if id_count_dict[0]['count'] > 0:
                query = 'select user_id from users where email_id = %s'
            else:
                query = 'select count(sub_user_id) as count from sub_users where email_id = %s'
                value = (email_id,)
                sub_user_id_dict = execute_query(query, value)
                if sub_user_id_dict:
                    if sub_user_id_dict[0]['count'] > 0:
                        query = 'select sub_user_id as user_id from sub_users where email_id = %s'

        # query = 'select user_id from users where email_id=%s'
        values = (email_id,)
        data_set = execute_query(query, values) 
        employer_id =  data_set[0]["user_id"]
        subject = f"Email Verification for {email_id}"
        recipients = ["info@2ndcareers.com"] 
        print(os.getcwd()+"/templates/Email_verification.html")                   
        index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
        index = index.replace("{{link}}",API_URI+"/email_verification?f89d6b6960453241bc5b09b4d0d8ad86d53769e051473350c2bf94e39077967b="+str(employer_id))
        body = index
        sendgrid_mail(SENDER_EMAIL,recipients,subject,body,"Redirect Email")                          
        # create_trial_session(email_id,EMPLOYER_TRIAL_PERIOD,EMPLOYER_BASIC_PLAN_ID,"Basic")
        result_json = api_json_response_format(True,"A verification link has been sent to your registered email. Please verify to proceed using the platform.",0,{})              
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
