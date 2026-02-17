# from flask import Flask,request

# import app.models.mysql_connector as db_con
import datetime
from flask import jsonify, request, redirect, url_for, session
from src import app
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from src.models.user_authentication import get_user_data,isUserExist,api_json_response_format
from src.controllers.jwt_tokens.jwt_token_required import get_user_token
from src.models.aws_resources import S3_Client
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import boto3
import random
from datetime import datetime as dt
import time
import platform
from flask_executor import Executor
from src.models.background_task import BackgroundTask

executor = Executor(app)
background_runner = BackgroundTask(executor)
S3_BUCKET_NAME = os.environ.get('CDN_BUCKET')

s3_obj = S3_Client()

s3_partner_learning_folder_name = "partner/learning-doc/"
s3_partner_cover_pic_folder_name = "partner/cover-pic/"
s3_picture_folder_name = "partner/profile-pic/"
# s3_learning_picture_folder_name = ""
# s3_attachment_folder_name = ""

def partner_signup():         
    try:
        print("partner sign up")
        return "success"
    except Exception as error:
        print(error)
        return {"error ":error}

def replace_empty_values1(data):
    if data == 'N/A' or data == None:
        data = ''
    return data

def replace_empty_values(data):
    for item in data:
        for key, value in item.items():
            if value == 'N/A' or value == None:
                item[key] = ''
    return data

def get_partner_team_details():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            partner_id = user_data["user_id"]             
            if user_data["user_role"] == "partner":
                if isUserExist("partner_profile","partner_id",partner_id):
                    if isUserExist("partner_team", "partner_id", partner_id):                                                       
                        query = 'select * from partner_team where partner_id = %s'
                        values = (partner_id,)
                        about_data = execute_query(query, values)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': f"User {user_data['email_id']}'s team details displayed successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Team Details", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Team Details",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Partner Team Details, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(about_data))
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': f"User {user_data['email_id']}'s team details fetched successfully. No records found"}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Team Details", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Team Details",event_properties,  temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Partner Team Details, {str(e)}")
                        result_json = api_json_response_format(False,"No records found for this id",204,{})
                else:                        
                    result_json = api_json_response_format(False,"Partner profile Not Found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching partner team details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Team Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Team Details Error",event_properties,  temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Partner Team Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def update_partner_team_details():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            partner_id = user_data["user_id"]
            if user_data["user_role"] == "partner":                                        
                req_data = request.get_json()
                if 'first_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'last_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'designation' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'contact_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json 
                if 'country_code' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json              
                first_name = req_data['first_name']
                last_name = req_data['last_name']
                partner_designation = req_data["designation"]
                contact_number = req_data['contact_number']
                country_code = req_data['country_code']
                if isUserExist("partner_profile","partner_id",partner_id):
                    if isUserExist("partner_team", "partner_id", partner_id): 
                        query = 'update partner_team set first_name = %s, last_name = %s, designation = %s, country_code = %s, contact_number = %s where partner_id = %s'
                        values = (first_name, last_name, partner_designation, country_code, contact_number, partner_id,)
                        row_count = update_query(query,values)
                        query = 'update users set first_name = %s, last_name = %s, country_code = %s, contact_number = %s where user_id = %s'
                        values = (first_name, last_name, country_code, contact_number, partner_id,)
                        update_users_tbl = update_query(query, values)
                        if row_count > 0:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'First Name' : first_name,
                                            'Last Name' : last_name,
                                            'Designation' : partner_designation,
                                            'Message': 'Partner team details updated successfully'}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Partner Team Updation", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Partner Team Updation",event_properties,  temp_dict.get('Message'), user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Partner Team Updation, {str(e)}")
                            background_runner.get_partner_details(partner_id)
                            result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                        else:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'First Name' : first_name,
                                            'Last Name' : last_name,
                                            'Designation' : partner_designation,
                                            'Message': "Unable to update partner team details."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Partner Team Updation Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Partner Team Updation Error",event_properties,  temp_dict.get('Message'), user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Partner Team Updation Error, {str(e)}")
                            result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
                    else:
                        result_json = api_json_response_format(False,"No records found for this employer",204,{})
                else:
                     result_json = api_json_response_format(False,"Partner profile Not Found",204,{})                  
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update partner team details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Team Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Team Updation Error",event_properties,  temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Partner Team Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_partner_company_details():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":                                        
                partner_id = user_data["user_id"]
                if isUserExist("partner_profile","partner_id",partner_id):                                
                    query = 'select website_url, partner_type from partner_profile where partner_id = %s'
                    values = (partner_id,)
                    company_data = execute_query(query, values) 
                    query = "select city, country from users where user_id = %s"
                    values = (partner_id,)
                    location_data = execute_query(query, values)
                    location_dict = {"location" : location_data}
                    company_data[0].update(location_dict)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Partner company details fetched successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Partner Company Details", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Partner Company Details",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Partner Company Details, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(company_data))
                else:
                    result_json = api_json_response_format(False,"Partner profile Not Found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to fetch partner company details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Company Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Company Details Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Partner Company Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def update_partner_company_details():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":                                        
                req_data = request.get_json()
                if 'website_url' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                # if 'sector' not in req_data:
                #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                #     return result_json
                if 'partner_type' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'country' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'city' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json    
                partner_id = user_data["user_id"]
                website_url = req_data["website_url"]
                # sector = req_data["sector"]
                partner_type = req_data["partner_type"]
                country = req_data["country"]
                city = req_data["city"]
                if isUserExist("partner_profile","partner_id",partner_id): 
                    query = 'update partner_profile set website_url = %s, partner_type = %s where partner_id = %s'
                    values = (website_url, partner_type, partner_id,)
                    data_row_count = update_query(query,values)
                    query = 'update users set country = %s, city = %s where user_id = %s'
                    values = (country, city, partner_id,)
                    location_row_count = update_query(query,values)
                    if data_row_count > 0 and location_row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Partner Type' : partner_type,
                                        'Website' : website_url,
                                        'Message': 'Partner company details updated successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Company Updation", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Company Updation",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Partner Company Updation, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Partner Type' : partner_type,
                                        'Website' : website_url,
                                        'Message': "Unable to update partner company details."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Company Updation Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Company Updation Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Partner Company Updation Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                else:                        
                    result_json = api_json_response_format(False,"Partner profile Not Found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update partner company details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Company Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Company Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Partner Company Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def get_partner_profile_dashboard_data():         
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":  
                partner_id =  user_data['user_id']
                query = "SELECT pt.partner_id,pt.first_name, pt.last_name, pt.email_id, pt.country_code, pt.contact_number,pp.id,pt.designation, pp.company_name, pp.company_description, pp.partner_type, pp.sector, pp.website_url, u.country, u.state, u.city, u.profile_image, u.pricing_category FROM partner_team pt JOIN users u ON pt.partner_id = u.user_id JOIN partner_profile pp ON pt.partner_id = pp.partner_id WHERE pp.partner_id =  %s"
                values = (partner_id,)
                profile_result = execute_query(query, values) 
                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                s3_pic_key = s3_picture_folder_name+str(profile_image_name)

                profile_dict = {
                    'company_name' : replace_empty_values1(profile_result[0]['company_name']),
                    'company_description' : replace_empty_values1(profile_result[0]['company_description']),
                    'partner_type' : replace_empty_values1(profile_result[0]['partner_type']),
                    'sector' : replace_empty_values1(profile_result[0]['sector']),
                    'website_url' : replace_empty_values1(profile_result[0]['website_url']),
                    'city': replace_empty_values1(profile_result[0]['city']),
                    'state': replace_empty_values1(profile_result[0]['state']),
                    'country': replace_empty_values1(profile_result[0]['country']),
                    'profile_image': s3_pic_key,
                    'pricing_category' : profile_result[0]['pricing_category'],
                    'partner_team' : {}
                }
                partner_team_set = set()
                partner_team_list = []
                for pt in profile_result:
                    if pt['id'] is not None:
                        pt_tuple = (pt['first_name'], pt['last_name'], pt['designation'], pt['email_id'], pt['country_code'], pt['contact_number'])
                        if pt_tuple not in partner_team_set:
                            partner_team_set.add(pt_tuple)
                            partner_team_list.append({
                                'first_name': replace_empty_values1(profile_result[0]['first_name']),
                                'last_name': replace_empty_values1(profile_result[0]['last_name']),  
                                'designation': replace_empty_values1(profile_result[0]['designation']),                                        
                                'email_id': replace_empty_values1(profile_result[0]['email_id']),
                                'country_code': replace_empty_values1(profile_result[0]['country_code']),
                                'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                                'partner_id' : profile_result[0]['partner_id'],
                            })

                profile_dict['partner_team'] = partner_team_list
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']}'s profile details fetched successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Partner Profile Details", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Partner Profile Details",event_properties, temp_dict.get('Message'), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Partner Profile Details, {str(e)}")
                result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict) 
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching {user_data['email_id']}'s profile details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Profile Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Profile Details Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Partner Profile Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json


def check_file_in_s3(bucket_name, key):
    # s3_client = boto3.client('s3')
    s3_client = s3_obj.get_s3_client()
    try:
        s3_client.get_object(Bucket=bucket_name, Key=s3_partner_cover_pic_folder_name + key)
        print("File found.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("File not found.")
            return False
        else:
            raise e

def get_additional_jobs_count(user_id):
    try:
        query = "select post_count from partner_additional_posts where user_id = %s and post_status = %s"
        values = (user_id, 'opened',)
        count_result = execute_query(query, values)
        count = 0
        if count_result:
            for row in count_result:
                count = count + int(row['post_count'])
    except Exception as error:
        print(f"Error in get_additional_jobs_count function, Error: {error}")        
        count = 0        
    finally:        
        return count

#def create_post():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            req_data = request.form
            key_id = 0
            if 'key_id' in req_data:
                key_id = int(req_data['key_id'])
            if 'title' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'short_description' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            if 'detailed_description' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'url' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'post_status' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'attached_file' not in request.files:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            if 'cover_pic' not in request.files:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            title = req_data['title']
            url = req_data['url']
            if user_data["user_role"] == "partner":                                        
                cover_pic = request.files['cover_pic']
                cover_pic_filename = ""
                attachment_filename = ""
                if cover_pic.filename != '':
                            try:                                                                                  
                                cover_pic_filename = cover_pic.filename
                                s3_client = s3_obj.get_s3_client()
                                if req_data['is_image_changed'] == 'Y':
                                    pdf_present = check_file_in_s3(S3_BUCKET_NAME, cover_pic_filename)
                                    if pdf_present == True:
                                        num = str(random.randint(10000, 99999))
                                        txt = cover_pic_filename.split('.')
                                        cover_pic_filename = txt[0] + '_' + num + '.' + txt[len(txt) - 1]
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(cover_pic, S3_BUCKET_NAME, s3_partner_cover_pic_folder_name + cover_pic_filename)                        
                            except Exception as error:
                                try:
                                    event_properties = {    
                                            '$distinct_id' : user_data["email_id"], 
                                            '$time': int(time.mktime(dt.now().timetuple())),
                                            '$os' : platform.system(),          
                                            'Email' : user_data["email_id"],
                                            'Post Title' : title,
                                            'URL' : url,
                                            'Post Created Status' : "Failure",
                                            'Error' : "Unsupported file format Error " + str(error)
                                        }
                                    # partner post
                                    message = f"{user_data['email_id']} post creation failed due to unsupported file format"
                                    event_name = "Partner Post Creation Error "
                                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                except Exception as e:  
                                    print("Error in partner create post mixpanel_event_log : %s",str(e))
                                print("Error in uploading cover picture.",error)                                    
                                result_json = api_json_response_format(False,"Could not upload profile picture ",500,{})
                if 'attached_file' in request.files:            
                    if request.files['attached_file'] is not None :
                                    attachment = request.files['attached_file']
                                    if((attachment.filename.endswith(".pdf") or attachment.filename.endswith(".docx"))):
                                        s3_pro = s3_obj.get_s3_client()   
                                        s3_pro.upload_fileobj(attachment, S3_BUCKET_NAME, s3_partner_learning_folder_name+attachment.filename)
                                    else:
                                        try:
                                            event_properties = {    
                                                    '$distinct_id' : user_data["email_id"], 
                                                    '$time': int(time.mktime(dt.now().timetuple())),
                                                    '$os' : platform.system(),          
                                                    'Email' : user_data["email_id"],
                                                    'Post Title' : title,
                                                    'URL' : url,
                                                    'Post Created Status' : "Failure",
                                                    'Error' : "Unsupported file format Error"
                                                }
                                            message = f"{user_data['email_id']} post creation failed due to unsupported file format"
                                            event_name = "Partner Post Creation Error"
                                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                        except Exception as e:  
                                            print("Error in partner create post mixpanel_event_log : %s",str(e))
                                        result_json = api_json_response_format(False,"Unsupported file format",500,{})
                                        return result_json
                                    attachment_filename = attachment.filename
                # title = req_data['title']
                # short_description = req_data['short_description']
                detailed_description = req_data['detailed_description']
                attached_file = attachment_filename
                # url = req_data['url']
                image = cover_pic_filename
                post_status = req_data['post_status']
                partner_id = user_data["user_id"]

                # posts_left_query = 'SELECT COUNT(l.id) AS opened_posts_count FROM learning l LEFT JOIN users u ON l.partner_id = u.user_id WHERE u.user_id = %s AND (l.post_status = %s OR l.post_status = %s);'
                # values = (partner_id, 'opened', 'paused',)
                # opened_posts_dict = execute_query(posts_left_query, values)
                # get_total_posts = 'select * from user_plan_details where user_id = %s'
                # values = (partner_id,)
                # total_posts_dict = execute_query(get_total_posts, values)
                # total_posts = 0 
                # if total_posts_dict:
                #     additional_posts_count = int(total_posts_dict[0]['additional_jobs_count'])
                #     total_posts = total_posts_dict[0]['total_jobs'] + additional_posts_count
                # opened_posts = 0
                # if len(opened_posts_dict) > 0:
                #     opened_posts = opened_posts_dict[0]['opened_jobs_count']
                # posts_left = total_posts - opened_posts

                if isUserExist("learning","id",key_id):
                    query = 'select * from user_plan_details where user_id = %s'
                    values = (partner_id,)
                    total_jobs = execute_query(query, values)
                    if len(total_jobs) > 0:
                        rem_jobs = total_jobs[0]['no_of_jobs']
                    # count = get_additional_jobs_count(user_data['user_id'])
                    # rem_jobs = rem_jobs + count
                    # if posts_left == 0:
                    if rem_jobs == 0:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Post Title' : title,
                                    'URL' : url,
                                    'Post Created Status' : "Failure",
                                    'Error' : "Plan Expired Error"
                                }
                            message = f"{user_data['email_id']} post creation error due to an expired plan"
                            event_name = "Partner Post Creation Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in partner create post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new ads.",201,{})
                        return result_json 
                    query = 'update learning set partner_id =%s,title=%s, detailed_description=%s, attached_file=%s, url=%s, image=%s, post_status=%s, is_active=%s where id=%s'
                    values = (partner_id,title, detailed_description, attached_file, url, image, post_status, 'Y', key_id,)
                    data = update_query(query, values)
                elif key_id == 0 or key_id == None:
                    created_at = datetime.datetime.now()
                    query = 'select no_of_jobs from user_plan_details where user_id = %s'
                    values = (partner_id,)
                    total_jobs = execute_query(query, values)
                    if len(total_jobs) > 0:
                        rem_jobs = total_jobs[0]['no_of_jobs']
                    # count = get_additional_jobs_count(user_data['user_id'])
                    # rem_jobs = rem_jobs + count
                    # if posts_left == 0:
                    if rem_jobs == 0:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Post Title' : title,
                                    'URL' : url,
                                    'Post Created Status' : "Failure",
                                    'Error' : "Plan Expired Error"
                                }
                            message = f"{user_data['email_id']} post creation error due to an expired plan"
                            event_name = "Partner Post Creation Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in partner create post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new ads.",201,{})
                        return result_json                   
                    query = 'insert into learning (partner_id,title, detailed_description, attached_file, url, image, post_status, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    values = (partner_id,title, detailed_description, attached_file, url, image, post_status, 'Y', created_at,)
                    data = update_query(query, values)
                else:
                    result_json = api_json_response_format(False,"Learning post Not Found",204,{})
                    return result_json
                if data > 0 :
                    query = 'UPDATE user_plan_details SET no_of_jobs = no_of_jobs - 1 WHERE user_id = %s'
                    values = (partner_id,)
                    rs = update_query_last_index(query, values)
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Post Title' : title,
                                'URL' : url,
                                'Post Created Status' : "Success"
                            }
                        message = f"{user_data['email_id']} Post created successfully"
                        event_name = "Partner Post Creation"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in partner create post mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Post has been created successfully!",0,{})
                    # if opened_posts > 4:
                    #     get_recent_post_id = "SELECT id from learning where partner_id = %s and post_status = 'opened' ORDER BY created_at DESC LIMIT 1"
                    #     get_recent_post_id_values = (partner_id,)
                    #     recent_post_id_dict = execute_query(get_recent_post_id, get_recent_post_id_values)
                    #     if recent_post_id_dict:
                    #         recent_post_id = recent_post_id_dict[0]['id']
                    #     else:
                    #         recent_post_id = 0
                    #     if recent_post_id != 0:
                    #         query = "INSERT INTO partner_additional_posts (user_id, post_id, post_status) VALUES (%s,%s,%s)"
                    #         values = (partner_id, recent_post_id, 'opened',)
                else:
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Post Title' : title,
                                'URL' : url,
                                'Post Created Status' : "Failure",
                                'Error' : "Data Base Error"
                            }
                        message = f"Partner Post Creation Error for {user_data['email_id']} due to Data Base Error"
                        event_name = "Partner Post Creation Error"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in partner create post mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                try:
                    event_properties = {    
                            '$distinct_id' : user_data["email_id"], 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Email' : user_data["email_id"],
                            'Post Title' : title,
                            'URL' : url,
                            'Post Created Status' : "Failure",
                            'Error' : "Unauthorized User Error"
                        }
                    message = f"Partner Post Creation Error for {user_data['email_id']} due to Unauthorized User Error"
                    event_name = "Partner Post Creation Error"
                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                except Exception as e:  
                    print("Error in partner create post mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            try:
                event_properties = {    
                        '$distinct_id' : user_data["email_id"], 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Email' : user_data["email_id"],
                        'Post Title' : title,
                        'URL' : url,
                        'Post Created Status' : "Failure",
                        'Error' : "Token Error"
                    }
                message = f"Partner Post Creation Error for {user_data['email_id']} due to Token Error"
                event_name = "Partner Post Creation Error"
                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
            except Exception as e:  
                print("Error in partner create post mixpanel_event_log : %s",str(e))
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            event_properties = {    
                    '$distinct_id' : user_data["email_id"], 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'Email' : user_data["email_id"],
                    'Post Created Status' : "Failure",
                    'Error' : str(error)
                }
            # to find doubt
            message = f"Partner Post Creation Error for {user_data['email_id']} due to {event_properties['Error']}"
            event_name = "Partner Post Creation Error"
            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
        except Exception as e:  
            print("Error in partner create post mixpanel_event_log : %s",str(e))
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 
    
def update_learning_post_days(data):
    try:
        get_partner_id_query = 'select partner_id from learning where id = %s'
        values = (data['last_index'],)
        partner_id_details = execute_query(get_partner_id_query, values)
        if partner_id_details:
            partner_id = partner_id_details[0]['partner_id']
        else:
            partner_id = 0
        query = 'insert into learning_post_details (post_id, partner_id, post_status) values (%s, %s, %s)'
        values = (data['last_index'], partner_id, 'opened',)
        insert_data = update_query_last_index(query, values)
        if insert_data:
            print("Learning post details inserted into learning_post_details table successfully.")
        else:
            print("Error in inserting learning post details into learning_post_details table.")
    except Exception as error:
        print(f"Error in update_learning_post_days function, Error: {error}")        
    finally:        
        return 1
    
def create_post():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            req_data = request.form
            key_id = 0
            if 'key_id' in req_data:
                key_id = int(req_data['key_id'])
            if 'title' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'short_description' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            if 'detailed_description' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'url' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'post_status' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'attached_file' not in request.files:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            if 'cover_pic' not in request.files:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            title = req_data['title']
            url = req_data['url']
            if user_data["user_role"] == "partner":                                        
                cover_pic = request.files['cover_pic']
                cover_pic_filename = ""
                attachment_filename = ""
                if cover_pic.filename != '':
                            try:                                                                                  
                                cover_pic_filename = cover_pic.filename
                                s3_client = s3_obj.get_s3_client()
                                if req_data['is_image_changed'] == 'Y':
                                    pdf_present = check_file_in_s3(S3_BUCKET_NAME, cover_pic_filename)
                                    if pdf_present == True:
                                        num = str(random.randint(10000, 99999))
                                        txt = cover_pic_filename.split('.')
                                        cover_pic_filename = txt[0] + '_' + num + '.' + txt[len(txt) - 1]
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(cover_pic, S3_BUCKET_NAME, s3_partner_cover_pic_folder_name + cover_pic_filename)                        
                            except Exception as error:
                                try:
                                    event_properties = {    
                                            '$distinct_id' : user_data["email_id"], 
                                            '$time': int(time.mktime(dt.now().timetuple())),
                                            '$os' : platform.system(),          
                                            'Email' : user_data["email_id"],
                                            'Post Title' : title,
                                            'URL' : url,
                                            'Post Created Status' : "Failure",
                                            'Error' : "Unsupported file format Error " + str(error)
                                        }
                                    # partner post
                                    message = f"{user_data['email_id']} post creation failed due to unsupported file format"
                                    event_name = "Partner Post Creation Error "
                                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                except Exception as e:  
                                    print("Error in partner create post mixpanel_event_log : %s",str(e))
                                print("Error in uploading cover picture.",error)                                    
                                result_json = api_json_response_format(False,"Could not upload profile picture ",500,{})
                if 'attached_file' in request.files:            
                    if request.files['attached_file'] is not None :
                                    attachment = request.files['attached_file']
                                    if((attachment.filename.endswith(".pdf") or attachment.filename.endswith(".docx"))):
                                        s3_pro = s3_obj.get_s3_client()   
                                        s3_pro.upload_fileobj(attachment, S3_BUCKET_NAME, s3_partner_learning_folder_name+attachment.filename)
                                    else:
                                        try:
                                            event_properties = {    
                                                    '$distinct_id' : user_data["email_id"], 
                                                    '$time': int(time.mktime(dt.now().timetuple())),
                                                    '$os' : platform.system(),          
                                                    'Email' : user_data["email_id"],
                                                    'Post Title' : title,
                                                    'URL' : url,
                                                    'Post Created Status' : "Failure",
                                                    'Error' : "Unsupported file format Error"
                                                }
                                            message = f"{user_data['email_id']} post creation failed due to unsupported file format"
                                            event_name = "Partner Post Creation Error"
                                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                        except Exception as e:  
                                            print("Error in partner create post mixpanel_event_log : %s",str(e))
                                        result_json = api_json_response_format(False,"Unsupported file format",500,{})
                                        return result_json
                                    attachment_filename = attachment.filename
                # title = req_data['title']
                # short_description = req_data['short_description']
                detailed_description = req_data['detailed_description']
                attached_file = attachment_filename
                # url = req_data['url']
                image = cover_pic_filename
                post_status = req_data['post_status']
                partner_id = user_data["user_id"]

                # posts_left_query = 'SELECT COUNT(l.id) AS opened_posts_count FROM learning l LEFT JOIN users u ON l.partner_id = u.user_id WHERE u.user_id = %s AND (l.post_status = %s OR l.post_status = %s);'
                # values = (partner_id, 'opened', 'paused',)
                # opened_posts_dict = execute_query(posts_left_query, values)
                # opened_posts = 0
                # if len(opened_posts_dict) > 0:
                #     opened_posts = opened_posts_dict[0]['opened_posts_count']

                # posted_count_query = 'select count(id) as count from learning where partner_id = %s and post_status != %s'
                # values = (partner_id, 'drafted',)
                # posted_count_list = execute_query(posted_count_query, values)
                # posted_count = 0
                # if posted_count_list:
                #     posted_count = posted_count_list[0]['count']

                get_total_posts = 'select * from user_plan_details where user_id = %s '
                values = (partner_id,)
                total_posts_dict = execute_query(get_total_posts, values)
                total_posts = 0 
                posts_left = 0
                if total_posts_dict:
                    additional_posts_count = int(total_posts_dict[0]['additional_jobs_count'])
                    total_posts = total_posts_dict[0]['total_jobs']
                    posts_left = total_posts_dict[0]['no_of_jobs']

                # posts_left = total_posts - posted_count
                created_at = datetime.datetime.now()
                data = {"row_count": -1}
                if isUserExist("learning","id",key_id):
                    # query = 'select * from user_plan_details where user_id = %s'
                    # values = (partner_id,)
                    # total_jobs = execute_query(query, values)
                    # if len(total_jobs) > 0:
                    #     rem_jobs = total_jobs[0]['no_of_jobs']
                    # count = get_additional_jobs_count(user_data['user_id'])
                    # rem_jobs = rem_jobs + count
                    # if posts_left == 0:
                    if posts_left == 0:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Post Title' : title,
                                    'URL' : url,
                                    'Post Created Status' : "Failure",
                                    'Error' : "Plan Expired Error"
                                }
                            message = f"{user_data['email_id']} post creation error due to an expired plan"
                            event_name = "Partner Post Creation Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in partner create post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new ads.",201,{})
                        return result_json 
                    query = 'update learning set partner_id =%s,title=%s, detailed_description=%s, attached_file=%s, url=%s, image=%s, post_status=%s, is_active=%s, created_at = %s where id=%s'
                    values = (partner_id,title, detailed_description, attached_file, url, image, post_status, 'Y', created_at, key_id,)
                    data = update_query_last_index(query, values)
                    data.update({'last_index': key_id})
                    # update_learning_post_days(data)
                elif key_id == 0 or key_id == None:
                    # query = 'select no_of_jobs from user_plan_details where user_id = %s'
                    # values = (partner_id,)
                    # total_jobs = execute_query(query, values)
                    # if len(total_jobs) > 0:
                    #     rem_jobs = total_jobs[0]['no_of_jobs']
                    # count = get_additional_jobs_count(user_data['user_id'])
                    # rem_jobs = rem_jobs + count
                    # if posts_left == 0:
                    if posts_left == 0:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Post Title' : title,
                                    'URL' : url,
                                    'Post Created Status' : "Failure",
                                    'Error' : "Plan Expired Error"
                                }
                            message = f"{user_data['email_id']} post creation error due to an expired plan"
                            event_name = "Partner Post Creation Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in partner create post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new ads.",201,{})
                        return result_json                   
                    query = 'insert into learning (partner_id,title, detailed_description, attached_file, url, image, post_status, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    values = (partner_id,title, detailed_description, attached_file, url, image, post_status, 'Y', created_at,)
                    data = update_query_last_index(query, values)
                    # update_learning_post_days(data)
                else:
                    result_json = api_json_response_format(False,"Learning post Not Found",204,{})
                    return result_json
                if data["row_count"] > 0:
                    query = 'UPDATE user_plan_details SET no_of_jobs = no_of_jobs - 1 WHERE user_id = %s'
                    values = (partner_id,)
                    rs = update_query_last_index(query, values)
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Post Title' : title,
                                'URL' : url,
                                'Post Created Status' : "Success"
                            }
                        message = f"{user_data['email_id']} Post created successfully"
                        event_name = "Partner Post Creation"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in partner create post mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Post has been created successfully!",0,{})
                    # if opened_posts > 4:
                    #     get_recent_post_id = "SELECT id from learning where partner_id = %s and post_status = 'opened' ORDER BY created_at DESC LIMIT 1"
                    #     get_recent_post_id_values = (partner_id,)
                    #     recent_post_id_dict = execute_query(get_recent_post_id, get_recent_post_id_values)
                    #     if recent_post_id_dict:
                    #         recent_post_id = recent_post_id_dict[0]['id']
                    #     else:
                    #         recent_post_id = 0
                    #     if recent_post_id != 0:
                    #         query = "INSERT INTO partner_additional_posts (user_id, post_id, post_status) VALUES (%s,%s,%s)"
                    #         values = (partner_id, recent_post_id, 'opened',)
                else:
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Post Title' : title,
                                'URL' : url,
                                'Post Created Status' : "Failure",
                                'Error' : "Data Base Error"
                            }
                        message = f"Partner Post Creation Error for {user_data['email_id']} due to Data Base Error"
                        event_name = "Partner Post Creation Error"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in partner create post mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                try:
                    event_properties = {    
                            '$distinct_id' : user_data["email_id"], 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Email' : user_data["email_id"],
                            'Post Title' : title,
                            'URL' : url,
                            'Post Created Status' : "Failure",
                            'Error' : "Unauthorized User Error"
                        }
                    message = f"Partner Post Creation Error for {user_data['email_id']} due to Unauthorized User Error"
                    event_name = "Partner Post Creation Error"
                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                except Exception as e:  
                    print("Error in partner create post mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            try:
                event_properties = {    
                        '$distinct_id' : user_data["email_id"], 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Email' : user_data["email_id"],
                        'Post Title' : title,
                        'URL' : url,
                        'Post Created Status' : "Failure",
                        'Error' : "Token Error"
                    }
                message = f"Partner Post Creation Error for {user_data['email_id']} due to Token Error"
                event_name = "Partner Post Creation Error"
                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
            except Exception as e:  
                print("Error in partner create post mixpanel_event_log : %s",str(e))
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            event_properties = {    
                    '$distinct_id' : user_data["email_id"], 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'Email' : user_data["email_id"],
                    'Post Created Status' : "Failure",
                    'Error' : str(error)
                }
            # to find doubt
            message = f"Partner Post Creation Error for {user_data['email_id']} due to {event_properties['Error']}"
            event_name = "Partner Post Creation Error"
            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
        except Exception as e:  
            print("Error in partner create post mixpanel_event_log : %s",str(e))
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

# def create_post():
#     result_json = {}
#     try:
#         token_result = get_user_token(request)                                        
#         if token_result["status_code"] == 200:                                
#             user_data = get_user_data(token_result["email_id"])
#             req_data = request.form
#             key_id = 0
            
#             if user_data["user_role"] == "partner":                                        
#                 cover_pic = request.files['cover_pic']
#                 cover_pic_filename = ""
#                 attachment_filename = ""
#                 if cover_pic.filename != '':
#                             try:                                                                                  
#                                 cover_pic_filename = cover_pic.filename
#                                 s3_client = s3_obj.get_s3_client()
#                                 if req_data['is_image_changed'] == 'Y':
#                                     pdf_present = check_file_in_s3(S3_BUCKET_NAME, cover_pic_filename)
#                                     if pdf_present == True:
#                                         num = str(random.randint(10000, 99999))
#                                         txt = cover_pic_filename.split('.')
#                                         cover_pic_filename = txt[0] + '_' + num + '.' + txt[len(txt) - 1]
#                                     s3_pro = s3_obj.get_s3_client()                            
#                                     s3_pro.upload_fileobj(cover_pic, S3_BUCKET_NAME, s3_partner_cover_pic_folder_name + cover_pic_filename)                        
#                             except Exception as error:
                                
#                                 print("Error in uploading cover picture.",error)                                    
#                                 result_json = api_json_response_format(False,"Could not upload profile picture ",500,{})
#                 if 'attached_file' in request.files:            
#                     if request.files['attached_file'] is not None :
#                                     attachment = request.files['attached_file']
#                                     if((attachment.filename.endswith(".pdf") or attachment.filename.endswith(".docx"))):
#                                         s3_pro = s3_obj.get_s3_client()   
#                                         s3_pro.upload_fileobj(attachment, S3_BUCKET_NAME, s3_partner_learning_folder_name+attachment.filename)
#                                     else:
                                        
#                                         result_json = api_json_response_format(False,"Unsupported file format",500,{})
#                                         return result_json
#                                     attachment_filename = attachment.filename
                
#                 detailed_description = req_data['detailed_description']
#                 attached_file = attachment_filename
#                 image = cover_pic_filename
#                 post_status = req_data['post_status']
#                 partner_id = user_data["user_id"]

#                 posts_left_query = 'SELECT COUNT(l.id) AS opened_posts_count FROM learning l LEFT JOIN users u ON l.partner_id = u.user_id WHERE u.user_id = %s AND (l.post_status = %s OR l.post_status = %s);'
#                 values = (partner_id, 'opened', 'paused',)
#                 opened_posts_dict = execute_query(posts_left_query, values)
#                 get_total_posts = 'select * from user_plan_details where user_id = %s'
#                 values = (partner_id,)
#                 total_posts_dict = execute_query(get_total_posts, values)
#                 total_posts = 0 
#                 if total_posts_dict:
#                     additional_posts_count = int(total_posts_dict[0]['additional_jobs_count'])
#                     total_posts = total_posts_dict[0]['total_jobs'] + additional_posts_count
#                 opened_posts = 0
#                 if len(opened_posts_dict) > 0:
#                     opened_posts = opened_posts_dict[0]['opened_jobs_count']
#                 posts_left = total_posts - opened_posts

#                 if isUserExist("learning","id",key_id):
#                     if posts_left == 0:
#                         result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new ads.",201,{})
#                         return result_json 
#                     query = 'update learning set partner_id =%s,title=%s, detailed_description=%s, attached_file=%s, url=%s, image=%s, post_status=%s, is_active=%s where id=%s'
#                     values = (partner_id,title, detailed_description, attached_file, url, image, post_status, 'Y', key_id,)
#                     data = update_query(query, values)
#                 elif key_id == 0 or key_id == None:
#                     if posts_left == 0:
#                         result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new ads.",201,{})
#                         return result_json                   
#                     query = 'insert into learning (partner_id,title, detailed_description, attached_file, url, image, post_status, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'
#                     values = (partner_id,title, detailed_description, attached_file, url, image, post_status, 'Y', created_at,)
#                     data = update_query_last_index(query, values)
#                 else:
#                     result_json = api_json_response_format(False,"Learning post Not Found",204,{})
#                     return result_json
#                 if data > 0 :
#                     query = 'UPDATE user_plan_details SET no_of_jobs = no_of_jobs - 1 WHERE user_id = %s'
#                     values = (partner_id,)
#                     rs = update_query_last_index(query, values)
#                     result_json = api_json_response_format(True,"Post has been created successfully!",0,{})
#                 else:
#                     result_json = api_json_response_format(True,"Sorry, we encountered an issue. Please try again.",500,{})
#             else:
#                 result_json = api_json_response_format(False,"Unauthorized User",401,{})
#         else:
#             result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
#     except Exception as error:
#         print(error)        
#         result_json = api_json_response_format(False,str(error),500,{})        
#     finally:        
#         return result_json 
   
def edit_partner_post():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            req_data = request.form
            if 'key_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'title' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'short_description' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            # if 'attached_file' not in request.files:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            if 'detailed_description' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'url' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'post_status' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'cover_pic' not in request.files:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            
            key_id = int(req_data['key_id'])
            title = req_data['title']
            url = req_data['url']
            if user_data["user_role"] == "partner":                                        
                cover_pic = request.files['cover_pic']
                cover_pic_filename = ""
                attachment_filename = ""
                if cover_pic.filename != '':
                            try:                                                                                  
                                cover_pic_filename = cover_pic.filename
                                s3_client = s3_obj.get_s3_client()
                                if req_data['is_image_changed'] == 'Y':
                                    pdf_present = check_file_in_s3(S3_BUCKET_NAME, cover_pic_filename)
                                    if pdf_present == True:
                                        num = str(random.randint(10000, 99999))
                                        txt = cover_pic_filename.split('.')
                                        cover_pic_filename = txt[0] + '_' + num + '.' + txt[len(txt) - 1]
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(cover_pic, S3_BUCKET_NAME, s3_partner_cover_pic_folder_name + cover_pic_filename)                        
                            except Exception as error:
                                try:
                                    event_properties = {    
                                            '$distinct_id' : user_data["email_id"], 
                                            '$time': int(time.mktime(dt.now().timetuple())),
                                            '$os' : platform.system(),          
                                            'Email' : user_data["email_id"],
                                            'Post Title' : title,
                                            'URL' : url,
                                            'Post Edited Status' : "Failure",
                                            'Error' : "Unsupported file format Error " + str(error)
                                        }
                                    message = message = f"Unable to edit Partner Post for {user_data['email_id']} due to Unsupported file format Error"
                                    event_name = "Edit Partner Post Error"
                                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                except Exception as e:  
                                    print("Error in partner edit post mixpanel_event_log : %s",str(e))
                                print("Error in uploading cover picture.",error)                                    
                                result_json = api_json_response_format(False,"Could not upload profile picture ",500,{})
                if 'attached_file' in request.files:            
                    if request.files['attached_file'] is not None :
                                    attachment = request.files['attached_file']
                                    if((attachment.filename.endswith(".pdf") or attachment.filename.endswith(".docx"))):
                                        s3_pro = s3_obj.get_s3_client()   
                                        s3_pro.upload_fileobj(attachment, S3_BUCKET_NAME, s3_partner_learning_folder_name+attachment.filename)
                                    else:
                                        try:
                                            event_properties = {    
                                                    '$distinct_id' : user_data["email_id"], 
                                                    '$time': int(time.mktime(dt.now().timetuple())),
                                                    '$os' : platform.system(),          
                                                    'Email' : user_data["email_id"],
                                                    'Post Title' : title,
                                                    'URL' : url,
                                                    'Post Edited Status' : "Failure",
                                                    'Error' : "Unsupported file format Error"
                                                }
                                            message = f"Unable to edit Partner Post for {user_data['email_id']} due to Unsupported file format Error"
                                            event_name = "Edit Partner Post Error"
                                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                        except Exception as e:  
                                            print("Error in partner edit post mixpanel_event_log : %s",str(e))
                                        result_json = api_json_response_format(False,"Unsupported file format",500,{})
                                        return result_json
                                    attachment_filename = attachment.filename
                # title = req_data['title']
                # short_description = req_data['short_description']
                detailed_description = req_data['detailed_description']
                attached_file = attachment_filename
                # url = req_data['url']
                image = cover_pic_filename
                post_status = req_data['post_status']
                partner_id = user_data["user_id"]
                if isUserExist("learning","id",key_id):
                    query = 'update learning set partner_id =%s,title=%s, detailed_description=%s, attached_file=%s, url=%s, image=%s, post_status=%s, is_active=%s where id=%s'
                    values = (partner_id,title, detailed_description, attached_file, url, image, post_status, 'Y', key_id,)
                    data = update_query(query, values)
                else:
                    result_json = api_json_response_format(False,"Learning post Not Found",204,{})
                    return result_json
                if data > 0 :
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Post Title' : title,
                                'URL' : url,
                                'Post Edited Status' : "Success"
                            }
                        message = f"{user_data['email_id']} Post successfully edited"
                        event_name = "Edit Partner Post"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in partner edit post mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Post has been edited successfully!",0,{})
                else:
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Post Title' : title,
                                'URL' : url,
                                'Post Edited Status' : "Failure",
                                'Error' : "Data Base Error"
                            }
                        message = f"Unable to edit Partner Post for {user_data['email_id']} due to Data Base Error"
                        event_name = "Edit Partner Post Error"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in partner edit post mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(True,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                try:
                    event_properties = {    
                            '$distinct_id' : user_data["email_id"], 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Email' : user_data["email_id"],
                            'Post Title' : title,
                            'URL' : url,
                            'Post Edited Status' : "Failure",
                            'Error' : "Unauthorized User Error"
                        }
                    message = f"Unable to edit Partner Post for {user_data['email_id']} due to Unauthorized User Error"
                    event_name = "Edit Partner Post Error"
                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                except Exception as e:  
                    print("Error in partner edit post mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            try:
                event_properties = {    
                        '$distinct_id' : user_data["email_id"], 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Email' : user_data["email_id"],
                        'Post Title' : title,
                        'URL' : url,
                        'Post Edited Status' : "Failure",
                        'Error' : "Token Error"
                    }
                message = f"Unable to edit Partner Post for {user_data['email_id']} due to Token Error"
                event_name = "Edit Partner Post Error"
                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
            except Exception as e:  
                print("Error in partner edit post mixpanel_event_log : %s",str(e))
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            event_properties = {    
                    '$distinct_id' : user_data["email_id"], 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'Email' : user_data["email_id"],
                    'Post Edited Status' : "Failure",
                    'Error' : str(error)
                }
            message = f"Unable to edit Partner Post for {user_data['email_id']}"
            event_name = "Edit Partner Post Error"
            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
        except Exception as e:  
            print("Error in partner edit post mixpanel_event_log : %s",str(e))
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def draft_post():
    result_json = {}
    try:
        key_id = 0
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200: 
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "partner":                               
                req_data = request.form
                file = request.files
                title = ""
                short_description = ""
                detailed_description = ""
                url = ""
                post_status = ""
                cover_pic = ""
                cover_pic_filename = ""
                attachment_filename = ""
                if 'key_id' in req_data:
                    key_id = req_data['key_id']
                if 'title' in req_data:
                    title = req_data['title']
                if 'short_description' in req_data:
                    short_description = req_data['short_description']
                if 'detailed_description' in req_data:
                    detailed_description = req_data['detailed_description']
                if 'url' in req_data:
                    url = req_data['url']
                if 'post_status' in req_data:
                    post_status = req_data['post_status']         
                if 'cover_pic' in file:                                    
                    cover_pic = request.files['cover_pic']
                    if not cover_pic == "": 
                        cover_pic_filename = cover_pic.filename
                        if cover_pic.filename != '':
                            try:                                                                                  
                                s3_pro = s3_obj.get_s3_client()                            
                                s3_pro.upload_fileobj(cover_pic, S3_BUCKET_NAME, s3_partner_cover_pic_folder_name+cover_pic_filename)                        
                            except Exception as error:
                                print("Error in uploading cover picture.",error)                                    
                                result_json = api_json_response_format(False,"could not upload profile picture ",500,{})
                if 'attached_file' in file:                                      
                    attachment = request.files['attached_file']
                    if not attachment == "": 
                        attachment_filename = attachment.filename              
                        if request.files['attached_file'] is not None and  request.files['attached_file'] == '':
                                        attachment = request.files['attached_file']
                                        if((attachment.filename.endswith(".pdf") or attachment.filename.endswith(".docx"))):
                                            s3_pro = s3_obj.get_s3_client()   
                                            s3_pro.upload_fileobj(attachment, S3_BUCKET_NAME, s3_partner_learning_folder_name+attachment.filename)
                                        else:
                                            try:
                                                event_properties = {    
                                                        '$distinct_id' : user_data["email_id"], 
                                                        '$time': int(time.mktime(dt.now().timetuple())),
                                                        '$os' : platform.system(),          
                                                        'Email' : user_data["email_id"],
                                                        'Post Title' : title,
                                                        'URL' : url,
                                                        'Post Created Status' : "Failure",
                                                        'Error' : "Unsupported file format Error"
                                                    }
                                                message = f"Partner Post Draft Error for {user_data['email_id']} due to Unsupported file format Error"
                                                event_name = "Partner Post Draft Error"
                                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                            except Exception as e:  
                                                print("Error in partner draft post mixpanel_event_log : %s",str(e))
                                            result_json = api_json_response_format(False,"Unsupported file format",500,{})
                                            return result_json
                partner_id = user_data["user_id"]
                if not key_id == 0 or not key_id == None:
                    if isUserExist("learning","id",key_id):
                        query = 'update learning set title = %s, short_description = %s, detailed_description = %s, attached_file = %s, url = %s, image = %s, post_status = %s where id = %s'
                        values = (title, short_description, detailed_description, attachment_filename, url, cover_pic_filename, post_status, key_id,)
                        val = update_query(query, values)
                        if val > 0:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Post Title' : title,
                                        'URL' : url,
                                        'Post Drafted Status' : "Success",
                                        'Message' : f"The post {title} has been drafted successfully."
                                    }
                                event_name = "Partner Post Draft"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, event_properties.get('message'), user_data)
                            except Exception as e:  
                                print("Error in partner draft post mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(True,"Your draft has been saved successfully",0,{})
                        else:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Post Title' : title,
                                        'URL' : url,
                                        'Post Drafted Status' : "Failure",
                                        'Message' : "An error occured while drafting the job."
                                    }
                                event_name = "Partner Post Draft Error"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, event_properties.get('message'), user_data)
                            except Exception as e:  
                                print("Error in partner draft post mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                    else:
                        created_at = datetime.datetime.now()                        
                        query = 'insert into learning (partner_id,title, short_description, detailed_description, attached_file, url, image, post_status, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                        values = (partner_id,title, short_description, detailed_description, attachment_filename, url, cover_pic_filename, post_status, 'Y', created_at,)
                        data = update_query(query, values)
                        if data > 0:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Post Title' : title,
                                        'URL' : url,
                                        'Post Drafted Status' : "Success"
                                    }
                                message = "An error occured while drafting the job."
                                event_name = "Partner Post Draft"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, event_properties.get('message'), user_data)
                            except Exception as e:  
                                print("Error in partner draft post mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(True,"Your draft has been saved successfully",0,{})
                        else:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Post Title' : title,
                                        'URL' : url,
                                        'Post Drafted Status' : "Failure"
                                    }
                                message = "An error occured while drafting the job."
                                event_name = "Partner Post Draft Error"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                            except Exception as e:  
                                print("Error in partner draft post mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                else:
                    created_at = datetime.datetime.now()                        
                    query = 'insert into learning (partner_id,title, short_description, detailed_description, attached_file, url, image, post_status, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    values = (partner_id,title, short_description, detailed_description, attachment_filename, url, cover_pic_filename, post_status, 'Y', created_at,)
                    data = update_query(query, values)
                    if data > 0:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Post Title' : title,
                                    'URL' : url,
                                    'Post Drafted Status' : "Success"
                                }
                            message = "An error occured while drafting the job."
                            event_name = "Partner Post Draft"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in partner draft post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(True,"Your draft has been saved successfully",0,{})
                    else:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Post Title' : title,
                                    'URL' : url,
                                    'Post Drafted Status' : "Failure"
                                }
                            message = "An error occured while drafting the job."
                            event_name = "Partner Post Draft Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in partner draft post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            event_properties = {    
                    '$distinct_id' : user_data["email_id"], 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'Email' : user_data["email_id"],
                    'Post Drafted Status' : "Failure",
                    'Error' : str(error)
                }
            message = "An error occured while drafting the job."
            event_name = "Partner Post Draft Error"
            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
        except Exception as e:  
            print("Error in partner draft post mixpanel_event_log : %s",str(e))
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def get_post():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":                                        
                partner_id = user_data["user_id"]
                req_data = request.get_json()
                if 'key_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                key_id = req_data['key_id']
                if isUserExist("learning","id",key_id):                                
                    query = 'select * from learning where id = %s and partner_id = %s'
                    values = (key_id, partner_id,)
                    data = execute_query(query, values)
                    if len(data) > 0:
                        image_name = s3_partner_cover_pic_folder_name + data[0]['image']
                        attached_file = s3_partner_learning_folder_name + data[0]['attached_file']
                        data[0].update({"image" : image_name})
                        data[0].update({"attached_file" : attached_file})
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Title' : data[0]['title'],
                                        'Post Status' : data[0]['post_status'],
                                        'Message': f"User {user_data['email_id']}'s partner post details fetched successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Post", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Post",event_properties, message, user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Get Partner Post, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(data))
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': "No posts found"}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Post", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Post",event_properties, temp_dict.get('message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Get Partner Post, {str(e)}")
                        result_json = api_json_response_format(True,"No records found",0,{})
                else:
                    result_json = api_json_response_format(False,"Record not found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in fetching partner post."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Post Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Post Error",event_properties,temp_dict.get('message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Get Partner Post Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def get_partner_home_view():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":                                        
                req_data = request.get_json()
                if 'post_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                cost_to_extend_ad = int(os.environ.get('COST_TO_EXTEND_AD'))
                cost_per_ad = int(os.environ.get('COST_PER_AD'))
                partner_id = user_data["user_id"]
                status = req_data["post_status"]
                learning_post_list = []  # Initialize an array to store job statistics
                query = "SELECT DISTINCT pp.company_name, u.profile_image, l.id AS learning_id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count, GREATEST(l.days_left - DATEDIFF(CURDATE(), l.created_at), 0) AS days_left FROM partner_profile pp LEFT JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.partner_id = %s AND l.post_status = %s ORDER BY l.id DESC"
                values = (partner_id,status,)
                learning_post_list = execute_query(query, values)
                # posts_left_query = 'select no_of_jobs, user_plan, total_jobs + additional_jobs_count as total_jobs from user_plan_details where user_id = %s'
                # values = (partner_id,)
                # rs = execute_query(posts_left_query, values)
                # posts_left = 0
                # total_posts = 0
                # posts_left_query = 'SELECT COUNT(jp.id) AS opened_jobs_count FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE COALESCE(u.user_id, su.sub_user_id) IN %s AND (jp.job_status = %s OR jp.job_status = %s);'
                # values = (partner_id, 'opened', 'paused',)
                # opened_posts_dict = execute_query(posts_left_query, values)
                # opened_jobs = 0
                # if len(opened_posts_dict) > 0:
                #     opened_jobs = opened_posts_dict[0]['opened_jobs_count']

                # posted_count_query = 'select count(id) as count from learning where partner_id = %s and post_status != %s'
                # values = (partner_id, 'drafted',)
                # posted_count_list = execute_query(posted_count_query, values)
                # posted_count = 0
                # if posted_count_list:
                #     posted_count = posted_count_list[0]['count']

                get_total_posts = 'select * from user_plan_details where user_id = %s'
                values = (partner_id,)
                total_posts_dict = execute_query(get_total_posts, values)
                total_posts = 0
                posts_left = 0
                if total_posts_dict:
                    additional_posts_count = int(total_posts_dict[0]['additional_jobs_count'])
                    total_posts = total_posts_dict[0]['total_jobs']
                    posts_left = total_posts_dict[0]['no_of_jobs']
                
                # posts_left = total_posts - posted_count
                # if len(rs) > 0:
                #     posts_left = rs[0]['no_of_jobs']
                #     total_posts = rs[0]['total_jobs']
                #     if rs[0]['user_plan'] == 'Basic' and posts_left == 0:
                # if posts_left == 0:
                #         notification_msg = f"You've reached your ad limit. You can add additional ads by clicking on create post button."
                #         query = "select count(id) as count from user_notifications where notification_msg = %s and user_id = %s"
                #         values = (notification_msg,partner_id,)
                #         id_count = execute_query(query,values)
                #         if len(id_count) > 0:
                #             if id_count[0]["count"] == 0:
                #                 query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                #                 created_at = datetime.datetime.now()
                #                 values = (partner_id,f"You've reached your ad limit. You can add additional ads by clicking on create post button.",created_at,)
                #                 update_query(query,values)
                    # if rs[0]['user_plan'] == 'Basic':
                    #     total_posts = 1
                    # elif rs[0]['user_plan'] == 'Premium':
                    #     total_posts = 5
                    # elif rs[0]['user_plan'] == 'Platinum':
                    #     total_posts = 10
                new_list = []
                for l in learning_post_list:
                    query = 'update learning set calc_day = %s where id = %s'
                    values = (l['days_left'], l['learning_id'])
                    updation = update_query(query, values)
                    if l['days_left'] <= 5:
                        notification_msg = f"Your ad {l['title']} will expire soon. If you’d like to keep it active, you can extend the validity of the ad."
                        query = "select count(id) as count from user_notifications where notification_msg = %s and user_id = %s"
                        values = (notification_msg,partner_id,)
                        id_count = execute_query(query,values)
                        if len(id_count) >0:
                            if id_count[0]["count"] == 0:
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.datetime.now()                  
                                values = (partner_id,f"Your ad {l['title']} will expire soon. If you’d like to keep it active, you can extend the validity of the ad.",created_at,)
                                update_query(query,values)
                    if l['days_left'] == 0:
                        query = 'SELECT id, title, created_at, DATEDIFF(NOW(), created_at) AS days_since_creation FROM learning WHERE id = %s'
                        values = (l['learning_id'],)
                        date_created = execute_query(query, values)
                        if len(date_created) > 0:
                            if date_created[0]['days_since_creation'] >= 100:
                                notification_msg = f"Your ad {date_created[0]['title']} has expired. If you'd like to reopen it and continue attracting candidates, you can extend the validity at any time."
                                query = "select count(id) as count from user_notifications where notification_msg = %s and user_id = %s"
                                values = (notification_msg,partner_id,)
                                id_count = execute_query(query,values)
                                if len(id_count) >0:
                                    if id_count[0]["count"] == 0:
                                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                        created_at = datetime.datetime.now()                    
                                        values = (partner_id,f"Your ad {date_created[0]['title']} has expired. If you'd like to reopen it and continue attracting candidates, you can extend the validity at any time.",created_at,)
                                        update_query(query,values)
                    s3_pic_key = s3_picture_folder_name + l['profile_image']
                    s3_cover_pic_key = s3_partner_cover_pic_folder_name+l['image']
                    s3_attached_file_key = s3_partner_learning_folder_name+l['attached_file']
                    l.update({"profile_image" : s3_pic_key})
                    l.update({"image": s3_cover_pic_key})   
                    l.update({"attached_file": s3_attached_file_key})                      
                    new_list.append(l)      
                if learning_post_list == []:
                    query = 'select welcome_count from partner_profile where partner_id = %s'
                    values = (partner_id,)
                    count = execute_query(query, values)
                    if count[0]['welcome_count'] == 0:
                        result_list = [{"post_list" : new_list},
                                       {"posts_left" : posts_left},
                                       {"cost_per_ad" : cost_per_ad},
                                       {"cost_to_extend_ad" : cost_to_extend_ad},
                                       {"total_posts" : total_posts}]
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Posts Left' : posts_left,
                                        'Total Posts' : total_posts,
                                        'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Home Page", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Home Page",event_properties, temp_dict.get('message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Get Partner Home Page, {str(e)}")
                        result_json = api_json_response_format(True, "No records found", 200, result_list)
                        query = 'update partner_profile set welcome_count = %s where partner_id = %s'
                        values = (1, partner_id,)
                        temp = update_query(query, values)
                    else:
                        result_list = [{"post_list" : new_list},
                                       {"posts_left" : posts_left},
                                       {"cost_per_ad" : cost_per_ad},
                                       {"cost_to_extend_ad" : cost_to_extend_ad},
                                       {"total_posts" : total_posts}]
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Posts Left' : posts_left,
                                        'Total Posts' : total_posts,
                                        'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Home Page", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Home Page",event_properties, temp_dict.get('message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Get Partner Home Page, {str(e)}")
                        result_json = api_json_response_format(True, "No records found", 0, result_list)
                else:
                    result_list = [{"post_list" : new_list},
                                       {"posts_left" : posts_left},
                                       {"cost_per_ad" : cost_per_ad},
                                       {"cost_to_extend_ad" : cost_to_extend_ad},
                                       {"total_posts" : total_posts}]
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Posts Left' : posts_left,
                                    'Total Posts' : total_posts,
                                    'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Home Page",event_properties, temp_dict.get('message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Partner Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, result_list)                 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching user {user_data['email_id']}'s home dashboard details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Home Page Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Home Page Error",event_properties, temp_dict.get('message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Get Partner Home Page Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def status_update_posted():
    result_json = {}
    try:   
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":                                        
                req_data = request.get_json()   
                id = req_data["id"] 
                post_status = req_data["post_status"]              
                if isUserExist("learning","id",id): 
                    # if post_status == "Opened" or post_status == "opened":
                    #     is_active = "Y"
                    # else:
                    #     is_active = "N"
                    query = 'update learning set post_status = %s where id = %s'
                    values = (post_status,id,)
                    row_count = update_query(query,values)
                    query = 'select title from learning where id = %s'
                    values = (id,)
                    post_title = execute_query(query,values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Post Title' : post_title[0]['title'],
                                        'Post Status' : post_status,
                                        'Message': f"Status of '{post_title[0]['title']}' updated to {post_status}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Post Status Updation", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Post Status Updation",event_properties, temp_dict.get('message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Post Status Updation, {str(e)}")
                        result_json = api_json_response_format(True,"Status has been updated successfully.",0,{})
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Post Title' : post_title[0]['title'],
                                        'Post Status' : post_status,
                                        'Message': f"Error in updating the status of '{post_title[0]['title']}'."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Post Status Updation Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Post Status Updation Error",event_properties, temp_dict.get('message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Post Status Updation Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with update your post. We request you to retry.",500,{})
                else:                        
                    result_json = api_json_response_format(False,"Post not found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in updating partner post status."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Post Status Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Post Status Updation Error",event_properties, temp_dict.get('message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Post Status Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def close_posted():
    result_json = {}
    try:   
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":                                        
                req_data = request.get_json()   
                id = req_data["id"] 
                post_status = req_data["post_status"] 
                is_referral_received = req_data["is_referral_received"]
                total_referral_received = req_data["total_referrals_received"]
                feedback = req_data["feedback"]
                query = 'select title from learning where id = %s'
                values = (id,)
                post_title = execute_query(query,values)
                if isUserExist("learning","id",id): 
                    query = 'update learning set post_status = %s,feedback = %s,is_active = %s, is_referrals_received = %s, total_referrals_received = %s where id = %s'
                    values = (post_status,feedback,"N",is_referral_received,total_referral_received,id,)
                    row_count = update_query(query,values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Post Title' : post_title[0]['title'],
                                        'Post Status' : post_status,
                                        'Is Referral Received' : is_referral_received,
                                        'Total Referral Received' : total_referral_received,
                                        'Feedback' : feedback,
                                        'Message': f"Status of '{post_title[0]['title']}' updated to {post_status}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Close Post", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Close Post",event_properties, temp_dict.get('message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Partner Close Post, {str(e)}")
                        result_json = api_json_response_format(True,"Status has been updated successfully.",0,{})
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Post Title' : post_title[0]['title'],
                                        'Post Status' : post_status,
                                        'Is Referral Received' : is_referral_received,
                                        'Total Referral Received' : total_referral_received,
                                        'Feedback' : feedback,
                                        'Message': f"An error occured, failed to update the status of '{post_title[0]['title']}'."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Close Post Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Close Post Error",event_properties, temp_dict.get('message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Partner Close Post Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with update your post. We request you to retry.",500,{})
                else:                        
                    result_json = api_json_response_format(False,"Post not found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in updating partner post status."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Close Post Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Close Post Error",event_properties, temp_dict.get('message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Partner Close Post Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def delete_learning_post():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner": 
                partner_id = user_data["user_id"]                                       
                req_data = request.get_json()
                if 'key_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                key_id = req_data["key_id"]
                query = 'select title from learning where id = %s'
                values = (key_id,)
                post_title = execute_query(query,values)
                if isUserExist("partner_profile","partner_id",partner_id):
                    if isUserExist("learning","id",key_id): 
                        query = 'delete from learning where id = %s'
                        values = (key_id,)
                        post_deletion = update_query(query,values)
                        if post_deletion > 0 :
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'Post Title' : post_title[0]['title'],
                                            'Message': f"Post '{post_title[0]['title']}' deleted successfully."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Partner Delete Post", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Partner Delete Post",event_properties, temp_dict.get('message'), user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Partner Delete Post, {str(e)}")
                            result_json = api_json_response_format(True,"The post has been deleted successfully!",0,{})
                        else:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'Post Title' : post_title[0]['title'],
                                            'Message': f"An error occured while deleting the post '{post_title[0]['title']}'."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Partner Delete Post Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Partner Delete Post Error",event_properties, temp_dict.get('message'), user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Partner Delete Post Error, {str(e)}")
                            result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                    else:                        
                        result_json = api_json_response_format(False,"Job not found",204,{})
                else:                        
                    result_json = api_json_response_format(False,"Partner profile Not Found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in deleting partner post."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Partner Delete Post Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Partner Delete Post Error",event_properties, temp_dict.get('message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Partner Delete Post Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def get_ad_posts():
    try:
        token_result = get_user_token(request)  
        req_data = request.get_json()                                           
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":
                if 'ad_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json  
                ad_status = req_data['ad_status']  
                partner_id = user_data['user_id']

                if ad_status == "drafted":
                    query = """
                    SELECT 
                        id, title, short_description, detailed_description, image, attached_file, url, created_at 
                    FROM 
                        learning
                    WHERE 
                        (post_status = 'drafted') and partner_id = %s ORDER BY created_at DESC """
                else:
                    query = """
                        SELECT 
                        id, title, short_description, detailed_description, image, attached_file, url, created_at 
                    FROM 
                        learning
                    WHERE 
                        (post_status IN ('opened', 'paused', 'closed')) and partner_id = %s ORDER BY created_at DESC """
                values = (partner_id,)
                ad_list = execute_query(query, values)
                
                for obj in ad_list:
                    image_name = s3_partner_cover_pic_folder_name + obj['image']
                    attached_file = s3_partner_learning_folder_name + obj['attached_file']
                    obj.update({"image" : image_name})
                    obj.update({"attached_file" : attached_file}) 

                if len(ad_list) > 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Partner ad posts fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Ad Post", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Ad Post",event_properties, temp_dict.get('message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Partner Ad Post, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, ad_list)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Partner ad posts fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Ad Post", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Ad Post",event_properties, temp_dict.get('message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Partner Ad Post, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, {})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching partner ad posts."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Get Partner Ad Post Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Get Partner Ad Post Error",event_properties, temp_dict.get('message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Get Partner Ad Post Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_payment_status_partner():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "partner":  
                partner_id = user_data["user_id"]
                email_id = user_data["email_id"]
                query = 'select payment_status from users where user_id = %s'
                values = (partner_id,)
                ps = execute_query(query,values)
                if len(ps) >0:
                    payment_status = ps[0]["payment_status"]
                    if payment_status =="canceled" or payment_status =="incomplete":
                        result_json = api_json_response_format(False,f"Trial period of {email_id} has ended. please Subcribe to continue.",300,{})   
                    else:
                        result_json = api_json_response_format(False,f"Trial period",0,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 
