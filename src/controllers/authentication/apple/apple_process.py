import os
import jwt
import requests
import datetime
from flask import request
from src.models.user_authentication import get_user_data,isUserExist,get_user_roll_id,api_json_response_format, get_sub_user_data
from src import app
from flask_executor import Executor
from src.models.background_task import BackgroundTask
from src.controllers.jwt_tokens import jwt_token_required as jwt_token
from src.controllers.payment.payment_process import create_trial_session
from src.models.mysql_connector import update_query
from src.controllers.authentication.manual.authentication_process import calculate_professional_profile_percentage
from datetime import datetime as dt
import time
import platform
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from urllib.parse import urlencode

APPLE_CLIENT_ID = os.environ.get('APPLE_CLIENT_ID')
APPLE_TEAM_ID = os.environ.get('APPLE_TEAM_ID')
APPLE_KEY_ID = os.environ.get('APPLE_KEY_ID')
APPLE_REDIRECT_URI_SIGNUP = os.environ.get('APPLE_REDIRECT_URI_SIGNUP')
APPLE_REDIRECT_URI_SIGNIN = os.environ.get('APPLE_REDIRECT_URI_SIGNIN')
PRIVATE_KEY_PATH = os.environ.get('PRIVATE_KEY_PATH')
APPLE_PRIVATE_KEY = os.environ.get('APPLE_PRIVATE_KEY')
WEB_APP_URI = os.environ.get('WEB_APP_URI')        
PROFESSIONAL_TRIAL_PERIOD = os.environ.get('PROFESSIONAL_TRIAL_PERIOD')
PROFESSIONAL_BASIC_PLAN_ID = os.environ.get('PROFESSIONAL_BASIC_PLAN_ID')

executor = Executor(app)
background_runner = BackgroundTask(executor)

def generate_apple_client_secret():
    headers = {
        "kid": APPLE_KEY_ID,
        "alg": "ES256"
    }
    claims = {
        "iss": APPLE_TEAM_ID,
        "iat": datetime.datetime.now(),
        "exp": datetime.datetime.now() + datetime.timedelta(days=180),
        "aud": "https://appleid.apple.com",
        "sub": APPLE_CLIENT_ID
    }
    client_secret = jwt.encode(claims, APPLE_PRIVATE_KEY, algorithm="ES256", headers=headers)
    return client_secret

def get_redirect_url_signup(user_role):
    try:
        authorization_url = (
            f"https://appleid.apple.com/auth/authorize?response_type=code"
            f"&client_id={APPLE_CLIENT_ID}"
            f"&redirect_uri={APPLE_REDIRECT_URI_SIGNUP}"
            f"&scope=name email"
            f"&response_mode=form_post"
        )
        return authorization_url
    except Exception as error:
        return api_json_response_format(False, str(error), 500, {})

def get_redirect_url_signin(user_role):
    try:
        authorization_url = (
            f"https://appleid.apple.com/auth/authorize?response_type=code"
            f"&client_id={APPLE_CLIENT_ID}"
            f"&redirect_uri={APPLE_REDIRECT_URI_SIGNIN}"
            f"&scope=name email"
            f"&response_mode=form_post"
        )
        return authorization_url
    except Exception as error:
        return api_json_response_format(False, str(error), 500, {})

def get_apple_profile(auth_code, redirect_uri):
    try:
        client_secret = generate_apple_client_secret()
        token_url = "https://appleid.apple.com/auth/token"
        data = {
            "client_id": APPLE_CLIENT_ID,
            "client_secret": client_secret,
            "code": auth_code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri
        }
        response = requests.post(token_url, data=data)
        token_response = response.json()
        id_token = token_response.get("id_token")
        user_info = jwt.decode(id_token, "", options={"verify_signature": False})
        return user_info
    except Exception as error:
        print(f"Error in get_apple_profile: {error}")
        return None

def apple_signup():
    redirect_url = get_redirect_url_signup()
    return redirect_url

def apple_signin():
    redirect_url = get_redirect_url_signin()
    return redirect_url

def apple_signup_callback(code):
    result_json = {}
    try:
        auth_code = request.form.get("code")
        user_info = get_apple_profile(auth_code, APPLE_REDIRECT_URI_SIGNUP)
        print(user_info)
        user_role = "professional"  
        if user_info:
            print("user_info", user_info)
            email_id = user_info.get("email")
            mail_arr = email_id.split("@")
            if len(mail_arr) > 0 and mail_arr[1] == 'privaterelay.appleid.com':
                email_id = ''
            print(email_id)
            # first_name = user_info.get("name", {}).get("firstName", "")
            # last_name = user_info.get("name", {}).get("lastName", "")
            user_data = get_user_data(email_id)           
            print("user_data", user_data)
            if user_data["is_exist"]:                                                             
                if user_data["login_mode"] == "Google":
                    url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."
                    print(url_redirect)
                    print("Google")
                    return url_redirect
                if user_data["login_mode"] == "Linked-in":
                    url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                    print("Linked-in")
                    return url_redirect

                if user_data["login_mode"] == "Manual":
                    url_redirect = WEB_APP_URI+"?message=If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple."
                    print("Manual")
                    print(url_redirect)
                    return url_redirect
                user_id = user_data["user_id"]
                flag_user_exist = isUserExist("professional_profile","professional_id",user_id)
                if flag_user_exist:                           
                    url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                    print("flag existing users")
                    return url_redirect                    
                else:
                    token_result = jwt_token.get_jwt_access_token(user_id,email_id) 
                    if token_result["status"] == "success":
                        access_token =  token_result["access_token"]  
                        # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?token="+str(access_token)         
                        url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?token="+str(access_token)                           
                        print(url_redirect)
                        print("not flag existing users")
                        return url_redirect                     
                    else:
                        result_json = api_json_response_format(False,token_result["stauts"],401,{})  
                        # print("Something went wrong in get_linkedin_profile_signup user already exist token error ")
                        print(token_result["status"])
                        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
                        return url_redirect
            else:
                try:
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?first_name=''&&last_name=''&&email_id="+email_id+"&&mode="+"Apple"

                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?first_name=''&&last_name=''&&email_id="+email_id+"&&mode="+"Apple_Login"
                    user_properties = {
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),                
                        'Email'         : email_id,
                        'Signup Mode' : 'Apple_Login'                             
                    }

                    event_properties = {    
                        '$distinct_id' : email_id, 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),
                        'Email'         : email_id,
                        'Signup Mode' : 'Apple_Login'
                        # 'User plan' :  user_data['pricing_category']
                    }
                    message = f"Professional Profile created for {email_id}"
                    event_name = "Professional Profile Creation"
                    print("new users message", message)
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
                            'Email'         : email_id,
                            'Signup Mode' : 'Apple_Login'
                        }
                        message = f"Unable to create Professional Profile for {email_id}"
                        print("unable to Professional", message)
                        event_name = "Professional Profile Creation Error"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                    except Exception as e:
                        print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                    # result_json = api_json_response_format(True,"Apple signed up successfully",0,res_data)
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."
                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."
                    return url_redirect 
        else:
            # result_json = api_json_response_format(False,"Profile info not found",401,{}) 
            print("Profile info not found in Apple_Login signup process")
            # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
            url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
            return url_redirect 
    except Exception as error:
        # result_json = api_json_response_format(False,str(error),500,{}) 
        print("exception in apple_signup_callback process ",error)
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect

# def apple_signup_callback(code):
    result_json = {}
    try:
        auth_code = request.form.get("code")
        user_info = get_apple_profile(auth_code, APPLE_REDIRECT_URI_SIGNUP)
        print(user_info)
        user_role = "professional"  
        if user_info:
            email_id = user_info.get("email")
            mail_arr = email_id.split("@")
            if len(mail_arr) > 0 and mail_arr[1] == 'privaterelay.appleid.com':
                email_id = ''
            print(email_id)
            # first_name = user_info.get("name", {}).get("firstName", "")
            # last_name = user_info.get("name", {}).get("lastName", "")
            user_data = get_user_data(email_id)           
            if user_data["is_exist"]:                                                             
                    if user_data["login_mode"] == "Google":
                        url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."
                        print(url_redirect)
                        return url_redirect
                    if user_data["login_mode"] == "Linked-in":
                        url_redirect = WEB_APP_URI+"?message=It appears that an account with this email address already exists. Please sign in using your existing credentials."                    
                        return url_redirect
                    if user_data["login_mode"] == "Manual":
                        url_redirect = WEB_APP_URI+"?message=If you have signed up with LinkedIn or Google or Apple, please sign in through LinkedIn or Google or Apple."
                        print(url_redirect)
                        return url_redirect
                    user_id = user_data["user_id"]
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
                            # print("Something went wrong in get_linkedin_profile_signup user already exist token error ")
                            print(token_result["status"])
                            url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
                            return url_redirect
            else:
                roll_id = get_user_roll_id(user_role)                                    
                created_at = datetime.datetime.now()  
                query = "insert into users (email_id,user_role_fk,login_mode,login_status,login_count,email_active,profile_image,is_active,created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                values = (email_id,roll_id,'Apple_Login','IN',1,'Y','default_profile_picture.png','Y',created_at,) 
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
                                'Email'         : email_id,
                                'Signup Mode' : 'Apple_Login',
                                'User plan' :  user_data['pricing_category']                               
                            }

                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'Email'         : email_id,
                                'Signup Mode' : 'Apple_Login',
                                'User plan' :  user_data['pricing_category']
                            }
                            message = "Professional Profile Created"
                            event_name = "Professional Profile Creation"
                            background_runner.mixpanel_user_async(email_id,user_properties, message, user_data)
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        result_json = api_json_response_format(True,"Apple_Login signed up successfully",0,res_data)
                        # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?token="+str(access_token)
                        url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?token="+str(access_token)
                        create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
                        return url_redirect 
                    else:
                        try:
                            event_properties = {    
                                '$distinct_id' : email_id, 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),
                                'Email'         : email_id,
                                'Signup Mode' : 'Apple_Login',
                                'Error' : 'Token error ' + str(token_result["stauts"]),
                                'User plan' :  user_data['pricing_category']         
                            }
                            message = "Professional Profile not Created because of Token error"
                            event_name = "Professional Profile Creation Error"
                            background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                        result_json = api_json_response_format(False,token_result["stauts"],401,{}) 
                        background_runner.send_session_data_auto_async(user_id,email_id,result_json)  
                        print("Something went wrong in apple_signup_callback while new user creation token error ")
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
                            'Email'         : email_id,
                            'Signup Mode' : 'Apple_Login',
                            'Error' : 'Data Base error. Record not inserted',
                            'User plan' :  user_data['pricing_category']
                        }
                        message = "Professional Profile not Created due to Database Error"
                        event_name = "Professional Profile Creation Error"
                        background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in Professional Profile Creation mixpanel_user_and_event_log : %s",str(e))
                    result_json = api_json_response_format(False,"Could not update user in Apple_Login sign-in",500,{})                         
                    print("Something went wrong in Apple_Login sign-in")
                    # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
                    url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
                    return url_redirect 
        else:
            result_json = api_json_response_format(False,"Profile info not found",401,{}) 
            print("Profile info not found in Apple signup process")
            # url_redirect = WEB_APP_URI+"/role_selection/professional_signup/social_media?message=Something went wrong. Please try again."  
            url_redirect = WEB_APP_URI+"/role_selection/professional_signup/new_user_signup?message=Something went wrong. Please try again."  
            return url_redirect 
    except Exception as error:
        result_json = api_json_response_format(False,str(error),500,{}) 
        print("exception in apple_signup_callback process ",error)
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect
    
def apple_signin_callback(code):
    try:
        auth_code = request.form.get("code")
        user_info = get_apple_profile(auth_code, APPLE_REDIRECT_URI_SIGNIN)
        if user_info:
            email_id = user_info.get("email")
            print(email_id)
            user_data = get_user_data(email_id) 
            query = "update users set email_active = %s where user_id = %s"
            if user_data["is_exist"]:
                # user_id = user_data["user_id"]
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
                    message = f"User sign-in failed because the account associated with {email_id} was not found"
                    event_name = "User Signin Failure"
                    background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                except Exception as e:  
                    print("Error in mixpanel_event_log : %s",str(e))
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
                                message = f"{email_id} Signin Failure"
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
                                event_name = f"{email_id} Signin Failure"
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
        print("Error in apple_signin_callback()")
        print(error)
        url_redirect = WEB_APP_URI+"?message=Something went wrong. Please try again."  
        return url_redirect 

