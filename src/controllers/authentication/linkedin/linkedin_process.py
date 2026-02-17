from src import app
from linkedin import linkedin
import requests
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from flask import jsonify, request
from flask_oauthlib.client import OAuth
from src.models.user_log.log_management import log_msg_auto
from src.controllers.authentication.manual.authentication_process import calculate_professional_profile_percentage
import os
import json
#from dotenv import load_dotenv
from pathlib import Path
import datetime
from src.controllers.jwt_tokens import jwt_token_required as jwt_token
from src.models.user_authentication import get_user_data,isUserExist,get_user_roll_id,api_json_response_format, get_sub_user_data
from flask_executor import Executor
from src.models.background_task import BackgroundTask
from src.controllers.payment.payment_process import create_trial_session
import time
import platform
from datetime import datetime as dt
from urllib.parse import urlencode

APPLICATON_KEY,APPLICATON_SECRET,RETURN_URL,SCOPE,WEB_CONFIG_URL,LI_PROFILE_API_ENDPOINT,BASE_URL,ACCESS_TOKEN_URL,AUTHORIZE_URL = "","","","","","","","",""

try:
    #home_dir = "/home"
    #print(home_dir)
    #load_dotenv(home_dir+"/.env")
    APPLICATON_KEY = os.environ.get('LINKEDIN_APPLICATION_KEY')
    APPLICATON_SECRET = os.environ.get('LINKEDIN_APPLICATION_SECRET')
    RETURN_URL_SIGNUP = os.environ.get('LINKEDIN_RETURN_URL_SIGNUP')
    RETURN_URL_SIGNIN = os.environ.get('LINKEDIN_RETURN_URL_SIGNIN')
    PROFESSIONAL_TRIAL_PERIOD = os.environ.get('PROFESSIONAL_TRIAL_PERIOD')
    PROFESSIONAL_BASIC_PLAN_ID = os.environ.get('PROFESSIONAL_BASIC_PLAN_ID')

    WEB_CONFIG_URL = os.environ.get('LINKEDIN_WEB_CONFIG_URL') #"https://www.linkedin.com/oauth/.well-known/openid-configuration"    
    SCOPE = os.environ.get('LINKEDIN_SCOPE').split(",")
    LI_PROFILE_API_ENDPOINT = os.environ.get('LINKEDIN_PROFILE_API_ENDPOINT')
    BASE_URL = os.environ.get('LINKEDIN_BASE_URL')
    ACCESS_TOKEN_URL = os.environ.get('LINKEDIN_ACCESS_TOKEN_URL')
    AUTHORIZE_URL = os.environ.get('LINKEDIN_AUTHORIZE_URL') 
    WEB_APP_URI = os.environ.get('WEB_APP_URI')        
    
except Exception as error:
    print("Read Exception "+str(error))    

# scope_val = str(os.environ.get('LINKEDIN_SCOPE'))

authentication_signup = linkedin.LinkedInAuthentication(
    APPLICATON_KEY,
    APPLICATON_SECRET,
    RETURN_URL_SIGNUP,
    SCOPE
)

authentication_signin = linkedin.LinkedInAuthentication(
    APPLICATON_KEY,
    APPLICATON_SECRET,
    RETURN_URL_SIGNIN,
    SCOPE
)
oauth = OAuth(app)

linkedin_data = oauth.remote_app(
    'linkedin',
    consumer_key = APPLICATON_KEY,
    consumer_secret = APPLICATON_SECRET,
    request_token_params = {'scope': SCOPE},
    base_url = BASE_URL,
    request_token_url = None,
    access_token_method = 'POST',
    access_token_url = ACCESS_TOKEN_URL,
    authorize_url = AUTHORIZE_URL
)

executor = Executor(app)
background_runner = BackgroundTask(executor)

def get_redirect_url_signup(user_role):    
    try: 
        authorization_url = authentication_signup.authorization_url           
        return authorization_url
    except Exception as error:
        result_json = api_json_response_format(False,str(error),500,{})  
        return result_json
    
def get_redirect_url_signin(user_role):    
    try: 
        authorization_url = authentication_signin.authorization_url           
        return authorization_url
    except Exception as error:
        result_json = api_json_response_format(False,str(error),500,{})  
        return result_json

def get_linkedin_profile_signup(auth_code):
    result_json = {}
    last_name,first_name = "",""
    try:    
        user_role = "professional"    
        authentication_signup.authorization_code = auth_code                     
        api_access_token = authentication_signup.get_access_token().access_token                
        profile_data = requests.get(LI_PROFILE_API_ENDPOINT, headers={'Authorization': 'Bearer ' + api_access_token}).json()                    
        if 'given_name' in profile_data:
            first_name = str(profile_data['given_name'])
        if 'family_name' in profile_data:
            last_name = str(profile_data['family_name'])         
        if 'email' in profile_data:
            email_id = profile_data['email']        
            user_data = get_user_data(email_id) 
            if user_data["is_exist"]:                                                             
                    user_id = user_data["user_id"]           
                    if user_data["login_mode"] == "Google":
                        url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."
                        print(url_redirect)
                        return url_redirect
                    if user_data["login_mode"] == "Apple_Login":
                        url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                        return url_redirect
                    if user_data["login_mode"] == "Manual":
                        url_redirect = WEB_APP_URI+"?message=If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple."
                        print(url_redirect)
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
                            print("Something went wrong in get_linkedin_profile_signup user already exist token error ")
                            print(token_result["status"])
                            url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
                            return url_redirect
            else:
                try:
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?first_name="+first_name+"&&last_name="+last_name+"&&email_id="+email_id+"&&mode="+"Linked-in"
                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?first_name="+first_name+"&&last_name="+last_name+"&&email_id="+email_id+"&&mode="+"Linked-in"
                    user_properties = {
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),                
                        'First Name'    : first_name,
                        'Last Name'     : last_name,
                        'Email'         : email_id,
                        'Signup Mode' : 'Linked-In'
                    }

                    event_properties = {    
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),
                        'First Name'    : first_name,
                        'Last Name'     : last_name,
                        'Email'         : email_id,
                        'Signup Mode' : 'Linked-In'     
                    }
                    message = f"{email_id} Professional Profile Created"
                    event_name = "Professional Profile Creation"
                    background_runner.mixpanel_user_async(email_id,user_properties, message, user_data)
                    background_runner.mixpanel_event_async(email_id,event_name,event_properties,message, user_data)
                    create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
                    return url_redirect
                except Exception as e:  
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."
                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."
                    try:
                        event_properties = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),
                            'First Name'    : first_name,
                            'Last Name'     : last_name,
                            'Email'         : email_id,
                            'Signup Mode' : 'Linked-In'         
                        }
                        message = f"unable to create Professional Profile for {email_id}"
                        event_name = "Professional Profile Creation Error"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))  
                    # print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))   
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."
                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."
                    return url_redirect
        else:
            # result_json = api_json_response_format(False,"Email ID not found",401,{}) 
            print("email not found in linked in get profile process")
            # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
            url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
            return url_redirect 
    except Exception as error:
        # result_json = api_json_response_format(False,str(error),500,{}) 
        print("Exception in get_linkedin_profile_signup process ",error)
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect 

# def get_linkedin_profile_signup(auth_code):
    result_json = {}
    last_name,first_name = "",""
    try:    
        user_role = "professional"    
        authentication_signup.authorization_code = auth_code                     
        api_access_token = authentication_signup.get_access_token().access_token                
        profile_data = requests.get(LI_PROFILE_API_ENDPOINT, headers={'Authorization': 'Bearer ' + api_access_token}).json()                    
        if 'given_name' in profile_data:
            first_name = str(profile_data['given_name'])
        if 'family_name' in profile_data:
            last_name = str(profile_data['family_name'])         
        if 'email' in profile_data:
            email_id = profile_data['email']        
            user_data = get_user_data(email_id) 
            if user_data["is_exist"]:                                                             
                    user_id = user_data["user_id"]           
                    if user_data["login_mode"] == "Google":
                        url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."
                        print(url_redirect)
                        return url_redirect
                    if user_data["login_mode"] == "Apple_Login":
                        url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                        return url_redirect
                    if user_data["login_mode"] == "Manual":
                        url_redirect = WEB_APP_URI+"?message=If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple."
                        print(url_redirect)
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
                            print("Something went wrong in get_linkedin_profile_signup user already exist token error ")
                            print(token_result["status"])
                            url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
                            return url_redirect
            else:
                    roll_id = get_user_roll_id(user_role)                                    
                    created_at = datetime.datetime.now()  
                    query = "insert into users (email_id,user_role_fk,first_name,last_name,login_mode,login_status,login_count,email_active,profile_image,is_active,created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    values = (profile_data['email'],roll_id,first_name,last_name,'Linked-in','IN',1,'Y','default_profile_picture.png','Y',created_at,) 
                    row_count = update_query(query,values)    
                                  
                    user_data = get_user_data(email_id)    
                    if row_count > 0:                    
                        user_id = user_data["user_id"]
                        user_role = user_data["user_role"]
                        query = "insert into professional_profile(professional_id, created_at) values (%s,%s)"
                        values = (user_id, created_at,) 
                        profile_row_count = update_query(query, values)
                        token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]        
                            res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
                            try:
                                user_properties = {
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),                
                                    'First Name'    : first_name,
                                    'Last Name'     : last_name,
                                    'Email'         : email_id,
                                    'Signup Mode' : 'Linked-In',
                                    'User plan' :  user_data['pricing_category']
                                }

                                event_properties = {    
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'First Name'    : first_name,
                                    'Last Name'     : last_name,
                                    'Email'         : email_id,
                                    'Signup Mode' : 'Linked-In',
                                    'User plan' :  user_data['pricing_category']      
                                }
                                event_name = "Professional Profile Creation"
                                background_runner.mixpanel_user_async(email_id,user_properties)
                                background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                            except Exception as e:  
                                print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))   
                            result_json = api_json_response_format(True,"Linkedin signup successfully",0,res_data)                     
                            # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?token="+str(access_token)
                            url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?token="+str(access_token)
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
                                    'Signup Mode' : 'Linked-In',
                                    'Error' : 'Token error ' + str(token_result["stauts"]),
                                    'User plan' :  user_data['pricing_category']           
                                }
                                event_name = "Professional Profile Creation Error"
                                background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                            except Exception as e:  
                                print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))  
                            print("Something went wrong in get_linkedin_profile_signup while new user creation token error ")
                            print(token_result["status"])
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
                                'Signup Mode' : 'Linked-In',
                                'Error' : 'Data Base error. Record not inserted',
                                'User plan' :  user_data['pricing_category']    
                            }
                            event_name = "Professional Profile Creation Error"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties)
                        except Exception as e:  
                            print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        result_json = api_json_response_format(False,"Could not update user in linkedin",500,{})                         
                        print("Something went wrong in Could not signup linked in account  ")
                        # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
                        url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
                        return url_redirect 
        else:
            result_json = api_json_response_format(False,"email ID not found",401,{}) 
            print("email not found in linked in get profile process")
            # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
            url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
            return url_redirect 
    except Exception as error:
        result_json = api_json_response_format(False,str(error),500,{}) 
        print("exception in get_linkedin_profile_signup process ",error)
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect 
    
def get_linkedin_profile_signin(auth_code):    
    try:    
       
        authentication_signin.authorization_code = auth_code                     
        api_access_token = authentication_signin.get_access_token().access_token                
        profile_data = requests.get(LI_PROFILE_API_ENDPOINT, headers={'Authorization': 'Bearer ' + api_access_token}).json()            
        if 'email' in profile_data:
            email_id = profile_data['email']        
            user_data = get_user_data(email_id)
            
            if not user_data["is_exist"]:
                # query = "update users set email_active = %s where user_id = %s"
                # user_id = user_data["user_id"]
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
            else:
                user_id = user_data['user_id']
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
                            # profile_exist_url = WEB_APP_URI+"?token="+str(access_token)+"&role="+str(user_role)+"&email_id="+str(email_id)+"&registration_status="+str(registration_status) 
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
                        usr_column = "partner_id"
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
                                message = f"{email_id} Signin Successfully."
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
                                    message = f"User sign-in failed because the account associated with {email_id} was not found."
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
                                    message = f"User sign-in failed due to a token error for {email_id}"
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
            
        else:            
            url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
            return url_redirect                       
    except Exception as error:        
        print(f"Error in get_linkedin_profile_signin() for email_id: {email_id}, error: {error}")
        print(error)
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect 
        
    
def get_access_token():
    response = linkedin_data.authorized_response()
    return response
