from src import app
import os
import json
import string
import random
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from flask import jsonify
import datetime
from authlib.integrations.flask_client import OAuth
from src.controllers.jwt_tokens import jwt_token_required as jwt_token
from src.models.user_authentication import get_user_data,isUserExist,get_user_roll_id,api_json_response_format, get_sub_user_data

from flask_executor import Executor
from src.models.background_task import BackgroundTask
from src.controllers.payment.payment_process import create_trial_session
from src.controllers.authentication.manual.authentication_process import calculate_professional_profile_percentage
#from dotenv import load_dotenv
from datetime import datetime as dt
import time
import platform
from urllib.parse import urlencode

#home_dir = "/home"
#load_dotenv(home_dir+"/.env")
CONF_URL = os.environ.get('CONF_URL')

GOOGLE_CLIENT_ID_SIGNUP = os.environ.get('GOOGLE_CLIENT_ID_SIGNUP')
GOOGLE_CLIENT_SECRET_SIGNUP = os.environ.get('GOOGLE_CLIENT_SECRET_SIGNUP')
WEB_REDIRECT_URI_SIGNUP = os.environ.get('WEB_REDIRECT_URI_SIGNUP')

GOOGLE_CLIENT_ID_SIGNIN = os.environ.get('GOOGLE_CLIENT_ID_SIGNIN')
GOOGLE_CLIENT_SECRET_SIGNIN = os.environ.get('GOOGLE_CLIENT_SECRET_SIGNIN')
WEB_REDIRECT_URI_SIGNIN = os.environ.get('WEB_REDIRECT_URI_SIGNIN')
PROFESSIONAL_TRIAL_PERIOD = os.environ.get('PROFESSIONAL_TRIAL_PERIOD')
PROFESSIONAL_BASIC_PLAN_ID = os.environ.get('PROFESSIONAL_BASIC_PLAN_ID')

WEB_APP_URI = os.environ.get('WEB_APP_URI')

app.secret_key = os.environ.get('SECRET_KEY')

oauth_web_singnup = OAuth(app)
oauth_web_singnin = OAuth(app)
executor = Executor(app)
background_runner = BackgroundTask(executor)

oauth_web_singnup.register(
                name='googleweb',
                client_id=GOOGLE_CLIENT_ID_SIGNUP,
                client_secret=GOOGLE_CLIENT_SECRET_SIGNUP,
                server_metadata_url=CONF_URL,                
                client_kwargs={
                    'scope': 'openid email profile'                                          
                }
            )

oauth_web_singnin.register(
                name='googleweb',
                client_id=GOOGLE_CLIENT_ID_SIGNIN,
                client_secret=GOOGLE_CLIENT_SECRET_SIGNIN,
                server_metadata_url=CONF_URL,                
                client_kwargs={
                    'scope': 'openid email profile'                                          
                }
            )
    

def id_generator(size=5, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def id_generator(size=5, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def get_redirect_url_signup():
    try: 
        l_redirect_uri = oauth_web_singnup.googleweb.authorize_redirect(WEB_REDIRECT_URI_SIGNUP)
        return l_redirect_uri
    except Exception as error:
        print(error)
        return jsonify({"error ":error})
    
def get_redirect_url_signin():
    try:
        l_redirect_uri = oauth_web_singnin.googleweb.authorize_redirect(WEB_REDIRECT_URI_SIGNIN)            
        return l_redirect_uri
    except Exception as error:
        print(error)
        return jsonify({"error ":error})
    
# def get_google_profile_signup(auth_code):
    result_json = {}  
    last_name,first_name = "",""
    try:
        oauth_web_singnup.authorization_code = auth_code 
        response = oauth_web_singnup.googleweb.authorize_access_token()  
        google_data = str(response)
        google_data = google_data.replace("'", '"')
        google_data = google_data.replace("True", '"True"')        
        json_object = json.loads(google_data)
        usr_info = json_object["userinfo"]        
        email_id = str(usr_info["email"])          
        if 'given_name' in usr_info:
            first_name = str(usr_info['given_name'])
        
        if 'family_name' in usr_info:
            last_name = str(usr_info['family_name'])

        
        
        access_token = str(json_object["access_token"])
        user_data = get_user_data(email_id)       
        
        if user_data["is_exist"]: 
                
                user_id = user_data["user_id"]
                if user_data["login_mode"] == "Linked-in":
                    url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                    return url_redirect 
                if user_data["login_mode"] == "Apple_Login":
                    url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                    return url_redirect               
                if user_data["login_mode"] == "Manual":
                    url_redirect = WEB_APP_URI+"?message=If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple."                    
                    return url_redirect
                
                flag_user_exist = isUserExist("professional_profile","professional_id",user_id)
                if flag_user_exist:                                 
                    url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                    return url_redirect
                else:
                    token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                    if token_result["status"] == "success":
                        access_token =  token_result["access_token"]  
                        # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?token="+str(access_token)                  
                        url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?token="+str(access_token)           
                        print(url_redirect)
                        return url_redirect                     
                    else:
                        result_json = api_json_response_format(False,token_result["stauts"],401,{})   
                        print("Something went wrong in get_google_profile_signup user already exist token error ",token_result["status"])
                        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
                        return url_redirect                                                
                
        else:
                roll_id = get_user_roll_id("professional")#oauth state variable replace here
                created_at = datetime.datetime.now()
                query = "INSERT INTO users (user_role_fk ,first_name,last_name,email_id,login_mode,login_status,login_count,email_active,profile_image,is_active,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                values = (roll_id,first_name,last_name,email_id,"Google",'IN',1,'Y','default_profile_picture.png','Y',created_at,)
                row_count = update_query(query,values)
                
                if row_count > 0:
                    user_data = get_user_data(email_id)    
                    user_id = user_data["user_id"]
                    user_role = user_data["user_role"]
                    token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                    query = "insert into professional_profile(professional_id, created_at) values (%s,%s)"
                    values = (user_id, created_at,) 
                    profile_row_count = update_query(query, values)
                    if token_result["status"] == "success":
                        access_token =  token_result["access_token"]  
                        res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
                        # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?token="+str(access_token)
                        url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?token="+str(access_token)
                        try:
                            user_properties = {
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),                
                                'First Name'    : first_name,
                                'Last Name'     : last_name,
                                'Email'         : email_id,
                                'Signup Mode' : 'Google',
                                'User plan' :  user_data['pricing_category']                             
                            }

                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'First Name'    : first_name,
                                'Last Name'     : last_name,
                                'Email'         : email_id,
                                'Signup Mode' : 'Google',
                                'User plan' :  user_data['pricing_category']       
                            }
                            event_name = "Professional Profile Creation"
                            background_runner.mixpanel_user_async(email_id,user_properties)
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                        except Exception as e:  
                            print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        print("Google signup process success")
                        create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
                        return url_redirect                         
                    else:
                        result_json = api_json_response_format(False,token_result["stauts"],401,{})  
                        try:
                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'First Name'    : first_name,
                                'Last Name'     : last_name,
                                'Email'         : email_id,
                                'Signup Mode' : 'Google',
                                'Error' : 'Token error ' + str(token_result["stauts"]),
                                'User plan' :  user_data['pricing_category']
                            }
                            event_name = "Professional Profile Creation Error"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                        except Exception as e:  
                            print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        print("Something went wrong in get_google_profile_signup while new user creation token error ",token_result["status"])
                        # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
                        url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
                        return url_redirect
                else:
                    try:
                        event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),
                            'First Name'    : first_name,
                            'Last Name'     : last_name,
                            'Email'         : email_id,
                            'Signup Mode' : 'Google',
                            'Error' : 'Data Base error. Record not inserted',
                            'User plan' :  user_data['pricing_category']         
                        }
                        event_name = "Professional Profile Creation Error"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                    except Exception as e:  
                        print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                    result_json = api_json_response_format(False,"Could not signup google",500,{})
                    print("Something went wrong in Could not signup google account  ")                     
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
                    return url_redirect

    except Exception as error:        
        print("exception in get_google_profile_signup process "+str(error))
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect     
    


def get_google_profile_signup(auth_code):
    result_json = {}  
    last_name,first_name = "",""
    try:
        oauth_web_singnup.authorization_code = auth_code 
        response = oauth_web_singnup.googleweb.authorize_access_token()  
        google_data = str(response)
        google_data = google_data.replace("'", '"')
        google_data = google_data.replace("True", '"True"')        
        json_object = json.loads(google_data)
        usr_info = json_object["userinfo"]        
        email_id = str(usr_info["email"])          
        if 'given_name' in usr_info:
            first_name = str(usr_info['given_name'])
        
        if 'family_name' in usr_info:
            last_name = str(usr_info['family_name'])

        access_token = str(json_object["access_token"])
        user_data = get_user_data(email_id)       
        
        if user_data["is_exist"]:
            user_id = user_data["user_id"]
            if user_data["login_mode"] == "Linked-in":
                url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                return url_redirect 
            if user_data["login_mode"] == "Apple_Login":
                url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                return url_redirect               
            if user_data["login_mode"] == "Manual":
                url_redirect = WEB_APP_URI+"?message=If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple."                    
                return url_redirect
            
            flag_user_exist = isUserExist("professional_profile","professional_id",user_id)
            if flag_user_exist:                                 
                url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                return url_redirect
            else:
                token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                if token_result["status"] == "success":
                    access_token =  token_result["access_token"]  
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?token="+str(access_token)   
                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?token="+str(access_token)                        
                    print(url_redirect)
                    return url_redirect                     
                else:
                    result_json = api_json_response_format(False,token_result["stauts"],401,{})   
                    print("Something went wrong in get_google_profile_signup user already exist token error ",token_result["status"])
                    url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
                    return url_redirect                                                
        else:
            try:
                # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?first_name="+first_name+"&&last_name="+last_name+"&&email_id="+email_id+"&&mode="+"Google"
                url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?first_name="+first_name+"&&last_name="+last_name+"&&email_id="+email_id+"&&mode="+"Google"
                user_properties = {
                    '$distinct_id' : email_id, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),                
                    'First Name'    : first_name,
                    'Last Name'     : last_name,
                    'Email'         : email_id,
                    'Signup Mode' : 'Google'                           
                }
                event_properties = {    
                    '$distinct_id' : email_id, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),
                    'First Name'    : first_name,
                    'Last Name'     : last_name,
                    'Email'         : email_id,
                    'Signup Mode' : 'Google'   
                }
                message = f"{email_id} Professional Profile Created"
                event_name = "Professional Profile Creation"
                background_runner.mixpanel_user_async(email_id,user_properties, message, user_data)
                background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
                return url_redirect
            except Exception as e:  
                try:
                    event_properties = {    
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),
                        'First Name'    : first_name,
                        'Last Name'     : last_name,
                        'Email'         : email_id,
                        'Signup Mode' : 'Google'
                    }
                    message = f"Unable to create Professional profile for {email_id}"
                    event_name = "Professional Profile Creation Error"
                    background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                except Exception as e:  
                    print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                # print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."
                url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."
                return url_redirect                         
    except Exception as error:        
        print("exception in get_google_profile_signup process "+str(error))
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect     

def get_google_profile_signin(auth_code):
    try:
        oauth_web_singnin.authorization_code = auth_code 
        response = oauth_web_singnin.googleweb.authorize_access_token()  
        google_data = str(response)
        google_data = google_data.replace("'", '"')
        google_data = google_data.replace("True", '"True"')        
        json_object = json.loads(google_data)
        usr_info = json_object["userinfo"]        
        email_id = str(usr_info["email"])    
        user_name = str(usr_info["name"])        
        access_token = str(json_object["access_token"])
        user_data = get_user_data(email_id)
        if user_data["is_exist"]:
            query = "update users set email_active = %s where user_id = %s"
            user_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(email_id)
            if user_data["is_exist"]:
              query = "update sub_users set email_active = %s where sub_user_id = %s"
              user_id = user_data["user_id"]
        if not user_data["is_exist"]:
            user_data = get_sub_user_data(email_id)
            if not user_data["is_exist"]:
                    try:
                        event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : email_id,
                                'Signin Status' : "Failed",
                                'Error' : "Account not found"
                            }
                        message = f"User sign-in failed because the account associated with {email_id} was not found."
                        event_name = "User Signin Failure"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in User Signin Failure mixpanel_event_log : %s",str(e))
                    url_redirect = WEB_APP_URI+"?message=Before signing in, please take a moment to sign up and create your account."
                    return url_redirect
            query = "update sub_users set email_active = %s where sub_user_id = %s"
            user_id = user_data["sub_user_id"]
        if not user_data["is_exist"]: 
            try:
                event_properties = {    
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),           
                        'Email' : email_id,
                        'Signin Status' : "Failed",
                        'Error' : "Account not found"
                    }
                message = f"{email_id} Signin Failure"
                event_name = "User Signin Failure"
                background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
            except Exception as e:  
                print("Error in User Signin Failure mixpanel_event_log : %s",str(e)) 
            url_redirect = WEB_APP_URI+"?message=Before signing in, please take a moment to sign up and create your account."                
            return url_redirect
        else:   
                if user_data['login_mode'] == 'Manual' and user_data['email_active'] == 'N':
                    # query = "update users set email_active = %s where user_id = %s"
                    values = ('Y', user_id,)
                    mail_status_updation = update_query(query, values)
                # user_id = user_data["user_id"]
                user_role = user_data["user_role"]                
                usr_tbl,usr_column,profile_exist_url,profile_not_exist_url = "","","",""
                token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                access_token =  token_result["access_token"]  
                if user_role == "professional":
                    query = "select flag_status from users where user_id = %s"
                    values = (user_id,)
                    res = execute_query(query, values)
                    if res:
                        flag_status = res[0]['flag_status']  
                        registration_status = flag_status
                    else:
                        registration_status = None
                    if registration_status:
                        params = {
                                "token": access_token or "",
                                "role": user_role or "",
                                "email_id": email_id or "",
                                "registration_status": registration_status or "",
                            }
                        profile_exist_url = f"{WEB_APP_URI}?{urlencode(params)}"
                    else:
                        profile_exist_url = WEB_APP_URI+"?token="+str(access_token)+"&role="+user_role   
                else:
                    profile_exist_url = WEB_APP_URI+"?token="+str(access_token)+"&role="+user_role
                if user_role == "professional":
                    usr_tbl = "professional_profile"
                    usr_column = "professional_id"
                    # profile_not_exist_url = WEB_APP_URI+"/role_selection/professional_signup/social_media?token="+str(access_token)
                    profile_not_exist_url = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?token="+str(access_token)
                elif  user_role == "employer":
                    usr_tbl = "employer_profile"
                    usr_column = "employer_id"
                    # if user_data["email_active"] == "N":
                    #     try:
                    #         event_properties = {    
                    #                 '$distinct_id' : email_id, 
                    #                 '$time': int(time.mktime(dt.now().timetuple())),
                    #                 '$os' : platform.system(),
                    #                 'User Role' : user_data['user_role'],           
                    #                 'Email' : email_id,
                    #                 'Signin Mode' : user_data['login_mode'],
                    #                 'Signin Status' : "Failed",
                    #                 'Error' : "Email not verified",
                    #                 'User plan' :  user_data['pricing_category']
                    #             }
                    #         event_name = "User Signin Failure"
                    #         background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                    #     except Exception as e:  
                    #         print("Error in User Signin Failure mixpanel_event_log : %s",str(e)) 
                    #     url_redirect = WEB_APP_URI+"?message=Please verify your email before logging in. A verification link has been sent to your registered email. If you don't see the email, please check your spam or junk folder."
                    #     return url_redirect
                    profile_not_exist_url = WEB_APP_URI+"?message=Before signing in, please take a moment to sign up and create your account."  
                elif  user_role == "partner":
                    usr_tbl = "partner_profile"
                    usr_column = "partner_profile"
                    # if user_data["email_active"] == "N":
                    #     try:
                    #         event_properties = {    
                    #                 '$distinct_id' : email_id, 
                    #                 '$time': int(time.mktime(dt.now().timetuple())),
                    #                 '$os' : platform.system(),
                    #                 'User Role' : user_data['user_role'],           
                    #                 'Email' : email_id,
                    #                 'Signin Mode' : user_data['login_mode'],
                    #                 'Signin Status' : "Failed",
                    #                 'Error' : "Email not verified",
                    #                 'User plan' :  user_data['pricing_category']
                    #             }
                    #         event_name = "User Signin Failure"
                    #         background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                    #     except Exception as e:  
                    #         print("Error in User Signin Failure mixpanel_event_log : %s",str(e)) 
                    #     url_redirect = WEB_APP_URI+"?message=Please verify your email before logging in. A verification link has been sent to your registered email. If you don't see the email, please check your spam or junk folder."
                    #     return url_redirect
                    profile_exist_url = WEB_APP_URI+"?token="+str(access_token) # partner success path
                    profile_not_exist_url = WEB_APP_URI+"?message=Before signing in, please take a moment to sign up and create your account." 
                elif user_role == "employer_sub_admin" or user_role == "recruiter":
                    usr_tbl = "sub_users"
                    usr_column = "sub_user_id"
                    profile_not_exist_url = WEB_APP_URI+"?message=Before signing in, please take a moment to sign up and create your account."
                if not usr_tbl == "":
                    flag_user_exist = isUserExist(usr_tbl,usr_column,user_id)
                    if flag_user_exist:                
                        token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                        access_token =  token_result["access_token"] 
                        if usr_tbl == 'sub_users':
                            query = "update sub_users set login_status = %s, login_count = login_count + 1 where sub_user_id = %s"
                            values = ('IN', user_id,)
                        else:
                            profile_percentage = calculate_professional_profile_percentage(user_id)
                            query = "update users set profile_percentage = %s, login_status = %s,login_count = login_count + 1 where user_id = %s"
                            values = (profile_percentage['value'], 'IN',user_id,)
                        update_query(query,values) 
                        res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
                        try:
                            event_properties = {    
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'User Role' : user_data['user_role'],           
                                    'Email' : email_id,
                                    'Signin Mode' : user_data['login_mode'],
                                    'Signin Status' : "Success",
                                    'User plan' :  user_data['pricing_category']
                                }
                            message = f"{email_id} signin Successfully"
                            event_name = "User Signin Success"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in User Signin Success mixpanel_event_log : %s",str(e))
                        url_redirect = profile_exist_url
                        # url_redirect = WEB_APP_URI+"?token="+str(access_token)
                        return url_redirect
                    else:
                        token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]  
                            try:
                                event_properties = {    
                                        '$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'User Role' : user_data['user_role'],           
                                        'Email' : email_id,
                                        'Signin Mode' : user_data['login_mode'],
                                        'Signin Status' : "Failed",
                                        'Error' : "Account not found",
                                        'User plan' :  user_data['pricing_category']
                                    }
                                message = f"{email_id} signin Failure"
                                event_name = "User Signin Failure"
                                background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                            except Exception as e:  
                                print("Error in User Signin Failure mixpanel_event_log : %s",str(e))
                            url_redirect = profile_not_exist_url
                            res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}                        
                            return url_redirect                     
                        else:
                            result_json = api_json_response_format(False,token_result["stauts"],401,{})  
                            try:
                                event_properties = {    
                                        '$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'User Role' : user_data['user_role'],           
                                        'Email' : email_id,
                                        'Signin Mode' : user_data['login_mode'],
                                        'Signin Status' : "Failed",
                                        'Error' : "Token Error",
                                        'User plan' :  user_data['pricing_category']
                                    }
                                message =f"{email_id} Signin Failure"
                                event_name = "User Signin Failure"
                                background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                            except Exception as e:  
                                print("Error in User Signin Failure mixpanel_event_log : %s",str(e))  
                            # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Before signing in, please take a moment to sign up and create your account."  
                            url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Before signing in, please take a moment to sign up and create your account."  
                            return url_redirect 
                else:
                    url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
                    return url_redirect 
                #     url_redirect = WEB_APP_URI+"?message=Before signing in, please take a moment to  create your account."                                
                # return url_redirect
    except Exception as error:        
        print("Exception in get_google_profile_signin() : "+str(error)) 
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect 
