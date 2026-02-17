import os
from src import app
from src.models.mysql_connector import execute_query,update_query,update_query_last_index,view_execute_query
from src.models.aws_resources import S3_Client
from flask import jsonify, request, redirect, url_for, session
import json
from src.controllers.jwt_tokens.jwt_token_required import get_user_token,get_jwt_access_token
from src.controllers.professional.professional_process import show_percentage
from src.models.user_authentication import get_user_data,isUserExist,api_json_response_format
from datetime import datetime,date, timedelta
from flask_executor import Executor
from src.models.background_task import BackgroundTask
from meilisearch import Client
from  openai import OpenAI
import base64
import hmac
import hashlib
import urllib.parse
from pathlib import Path
from src.controllers.employer.employer_process import store_in_meilisearch

import pandas as pd
import numpy as np
import pyexcel as pe
import ast
from tempfile import NamedTemporaryFile

executor = Executor(app)
background_runner = BackgroundTask(executor)

s3_picture_folder_name = "professional/profile-pic/"
s3_intro_video_folder_name = "professional/profile-video/"
s3_resume_folder_name = "professional/resume/"
s3_cover_letter_folder_name = "professional/cover-letter/"
s3_sector_image_folder_name = "sector-image/"
s3_employer_picture_folder_name = "employer/logo/"
s3_partner_picture_folder_name = "partner/profile-pic/"
s3_partner_learning_folder_name = "partner/learning-doc/"
s3_partner_cover_pic_folder_name = "partner/cover-pic/"
s3_employeer_logo_folder_name = "employer/logo/"
S3_BUCKET_NAME = os.environ.get('CDN_BUCKET')
s3_community_cover_pic_folder_name = "2ndcareers/cover-pic/"
s3_trailing_cover_pic_folder_name = "2ndcareers/trailing-pic/"

master_key = os.environ.get("MEILISEARCH_MASTER_KEY")
meilisearch_url = os.environ.get("MEILISEARCH_URL")
meilisearch_professional_index = os.environ.get("MEILISEARCH_PROFESSIONAL_INDEX")
meilisearch_employer_index = os.environ.get("MEILISEARCH_EMPLOYER_INDEX")
meilisearch_partner_index = os.environ.get("MEILISEARCH_PARTNER_INDEX")
meilisearch_job_index = os.environ.get("MEILISEARCH_JOB_INDEX")
meilisearch_admin_job_index = os.environ.get("MEILISEARCH_ADMIN_JOB_INDEX")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
g_summary_model_name = os.environ.get('SUMMARY_MODEL_NAME')
g_token_encoding_txt = os.environ.get('TOKEN_ENCODING_TEXT')
g_openai_token_limit =int(os.environ.get('OPENAI_TOKEN_LIMIT'))
g_openai_completion_token_limit =int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))
BUCKET_NAME = os.environ.get('PROMPT_BUCKET')
s3_obj = S3_Client()
SECRET = "2ndcareers"
DISCOURSE_URL = "https://2ndcareers-community.discourse.group/session/sso_login"

MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
MEILISEARCH_ADMIN_JOB_INDEX =  os.environ.get("MEILISEARCH_ADMIN_JOB_INDEX")

def get_company_names():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200: 
            req_data = request.get_json()                                  
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":    
                professional_id = req_data['professional_id']
                query = "SELECT DISTINCT e.employer_id, e.company_name FROM employer_profile e LEFT JOIN users u ON e.employer_id = u.user_id WHERE u.email_active = %s"
                values = ("Y",)
                company_names_list = execute_query(query, values)                       
                if len(company_names_list) > 0:
                    result_json = api_json_response_format(True,"Company names fetched successfully!",0,company_names_list) 
                else:
                    result_json = api_json_response_format(True,"The candidate has not applied for any jobs.",0,[]) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception in get_job_list ",error)           
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_sub_users():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200: 
            req_data = request.get_json()                                  
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":    
                employer_id = req_data['employer_id']
                query = "SELECT first_name, last_name, title, email_id, CASE WHEN role_id = 9 THEN 'ADMIN' ELSE 'RECRUITER' END AS role, phone_number, created_at FROM sub_users WHERE user_id = %s"
                values = (employer_id,)
                sub_users_dict = execute_query(query, values)
                emp_details_query = "select u.first_name, u.last_name, u.email_id, ep.designation as title, u.contact_number as phone_number, u.created_at from users u left join employer_profile ep on u.user_id = ep.employer_id where u.user_id = %s"
                emp_details_values = (employer_id,)
                emp_details = execute_query(emp_details_query, emp_details_values)
                if emp_details:
                    emp_details[0].update({'role' : 'OWNER'})
                else:
                    emp_details = []
                sub_users_dict.append(emp_details[0])
                if len(sub_users_dict) > 0:
                    result_json = api_json_response_format(True,"Sub-users details fetched successfully!",0,sub_users_dict) 
                else:
                    result_json = api_json_response_format(True,"Sub-users not exists.",0,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception in get_job_list ",error)           
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_job_list():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()  
            if 'employer_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # if 'company_name' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            #     return result_json
            employer_id = req_data['employer_id']
            # company_name = req_data['company_name']
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                get_sub_users_values = (employer_id,)
                sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                sub_users_list = []
                if sub_users_dict:
                    sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                sub_users_list.append(employer_id,)
                query = """SELECT jp.id, jp.job_title, COALESCE(e.employer_id, su.sub_user_id) AS employer_id FROM job_post jp 
                           LEFT JOIN employer_profile e ON jp.employer_id = e.employer_id 
                           LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                           WHERE jp.employer_id IN %s and jp.job_status = 'opened'"""
                values = (tuple(sub_users_list),)
                posted_job_list = execute_query(query, values)                       
                if len(posted_job_list) > 0:
                    result_json = api_json_response_format(True,"Posted job names fetched successfully!",0,posted_job_list) 
                else:
                    result_json = api_json_response_format(False,"No jobs have been posted by this company yet.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception in get_job_list ",error)           
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_list():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()  
            if 'job_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            job_id = req_data['job_id']
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":    
                query = 'select professional_id from job_activity where job_id = %s'
                values = (job_id,)
                professional_id_list = execute_query(query, values)
                professional_details_list = []
                if len(professional_id_list) > 0:
                    for professional in professional_id_list:
                        id = professional['professional_id']
                        query = 'select user_id, first_name, last_name from users where user_id = %s'
                        values = (id,)
                        professional_detail = execute_query(query, values)
                        professional_details_list.append(professional_detail[0])
                    result_json = api_json_response_format(True,"Candidates details fetched successfully!",0,professional_details_list)
                else:
                    result_json = api_json_response_format(True,"No applicants have applied for this job yet.",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception in get_job_list ",error)           
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def send_expert_notes():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()  
            if 'expert_notes' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'professional_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'is_clear' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            professional_id = req_data['professional_id']
            professional_id = int(professional_id.split("-")[2])
            user_data = get_user_data(token_result["email_id"]) 
            is_clear = req_data['is_clear']           
            if user_data["user_role"] == "admin":
                if is_clear == 'N':
                    query = 'update professional_profile set expert_notes = %s, show_to_employer = %s where professional_id = %s'
                    values = (req_data['expert_notes'], 'N', professional_id,)
                    update_expert_notes = update_query(query, values)
                    if update_expert_notes > 0:
                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                        created_at = datetime.now()                    
                        values = (professional_id, "Expert note has been updated, you can check the same in your profile", created_at)
                        update_query(query,values)
                        result_json = api_json_response_format(True,"Expert_notes updated successfully!",0,{})
                    else:
                        result_json = api_json_response_format(False,"No jobs have been posted by the employer.",204,{}) 
                else: 
                    query = 'update professional_profile set expert_notes = %s, show_to_employer = %s where professional_id = %s'
                    values = (None, 'N', professional_id,)
                    update_expert_notes = update_query(query, values)                       
                    result_json = api_json_response_format(True,"Expert notes cleared successfully!",0,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception ",error)           
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

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

def format_date(year,month):
    if year == None or month == None:
        return ""
    else:
        date = year + '-' + month
        return date

def create_dict1(row):
    return {row['type']: row['name']}

# def fetch_filter_params():
#     data1 = []
#     query = """
#     SELECT 'skill' AS type, skill_name AS name FROM filter_skills WHERE is_active = %s
#     UNION
#     SELECT 'country' AS type, country AS name FROM filter_location WHERE is_active = %s
#     UNION
#     SELECT 'plan' AS type, plan AS name FROM filter_plan WHERE is_active = %s
#     """
#     values = ('Y','Y','Y',) 
#     result = execute_query(query, values)

#     for row in result:
#         data1.append(create_dict1(row))

#     merged_data = {
#     'skill': [],
#     'location': [],
#     'plan': []
#     }

#     for row in result:
#         param_type = row['type']
#         name = row['name'].strip()

#         if param_type in merged_data:
#             merged_data[param_type].append(name)

#         if param_type == 'country':
#             merged_data['location'].append(name)
#     return merged_data

# def fetch_filter_params():
#     data1 = []
#     query = """
#     SELECT 'skill' AS type, skill_name AS name FROM filter_skills WHERE is_active = %s
#     UNION
#     SELECT 'country' AS type, country AS name FROM filter_location WHERE is_active = %s
#     UNION
#     SELECT 'industry_sector' AS type, sector_name AS name FROM filter_sectors WHERE is_active = %s
#     UNION
#     SELECT 'plan' AS type, plan AS name FROM filter_plan WHERE is_active = %s
#     """
#     values = ('Y','Y','Y','Y',) 
#     result = execute_query(query, values)

#     for row in result:
#         data1.append(create_dict1(row))

#     merged_data = {
#     'skill': [],
#     'location': [],
#     'plan': [],
#     'industry_sector' : []
#     }

#     for row in result:
#         param_type = row['type']
#         name = row['name'].strip()

#         if param_type in merged_data:
#             merged_data[param_type].append(name)

#         if param_type == 'country':
#             merged_data['location'].append(name)
#     merged_data.update({'gender' : ['Male', 'Female']})
#     merged_data.update({'functional_specification' : ['Sales & Marketing', 'Human Resources', 'Technology & Technology Management', 'Finance & Accounting', 'C Suite and Board', 'Others']})
#     merged_data.update({'job_type' : ['Full-time', 'Part-time', 'Contract', 'Volunteer']})
#     merged_data.update({'willing_to_relocate' : ['Yes', 'No']})
#     merged_data.update({'work_location_preference' : ['Remote', 'Hybrid', 'In-person']})
#     merged_data.update({'mode_of_communication' : ['Email', 'Whatsapp', 'Text message', 'Phone', 'Others']})
#     merged_data.update({'sectors' : ['Academic', 'Corporate', 'Non-profit', 'Startup', 'Others']})
    
#     return merged_data

def fetch_filter_params():

    ACTIVE_FLAG = 'Y'

    # Fetch all filters from main filter tables
    query = """
        SELECT 'skill' AS type, skill_name AS name FROM filter_skills WHERE is_active = %s
        UNION
        SELECT 'country' AS type, country AS name FROM filter_location WHERE is_active = %s
        UNION
        SELECT 'plan' AS type, plan AS name FROM filter_plan WHERE is_active = %s
    """
    values = (ACTIVE_FLAG,) * 3
    base_filters = execute_query(query, values)

    # Initialize dictionary for base filters
    merged_data = {
        'skill': [],
        'location': [],
        'plan': []
    }

    # Process base filter results
    for row in base_filters:
        param_type = row['type']
        name = row['name'].strip()
        if param_type == 'country':
            merged_data['location'].append(name)
        elif param_type in merged_data:
            merged_data[param_type].append(name)

    # Fetch dynamic filters from admin_filters table
    admit_query = "SELECT filter_name, filter_value FROM admin_filters"
    admit_result = execute_query(admit_query, ())

    # Merge admit_filter results
    for row in admit_result:
        filter_name = row['filter_name']
        filter_value = row['filter_value']

        # Convert JSON/text array to Python list if needed
        if isinstance(filter_value, str):
            try:
                filter_value = json.loads(filter_value)
            except json.JSONDecodeError as e:
                print(e)
                filter_value = [filter_value]

        merged_data[filter_name] = filter_value

    return merged_data

def fetch_filter_params_partner():
    data1 = []
    query = """
    SELECT 'partner_type' AS type, partner_type AS name FROM filter_partner_type WHERE is_active = %s
    UNION
    SELECT 'sector' AS type, sector_name AS name FROM filter_sectors WHERE is_active = %s
    UNION
    SELECT 'country' AS type, country AS name FROM filter_location WHERE is_active = %s
    UNION
    SELECT 'plan' AS type, plan AS name FROM filter_plan WHERE is_active = %s
    """
    values = ('Y','Y','Y','Y',) 
    result = execute_query(query, values)

    for row in result:
        data1.append(create_dict1(row))

    merged_data = {
    'partner_type' : [],
    'sector' : [],
    'location': [],
    'plan': []
    }

    for row in result:
        param_type = row['type']
        name = row['name'].strip()

        if param_type in merged_data:
            merged_data[param_type].append(name)

        if param_type == 'country':
            merged_data['location'].append(name)
    return merged_data

def fetch_employer_filter_params():
    data1 = []
    query = """
    SELECT 'sector' AS type, sector_name AS name FROM filter_sectors WHERE is_active = %s
    UNION
    SELECT 'country' AS type, country AS name FROM filter_location WHERE is_active = %s
    UNION
    SELECT 'plan' AS type, plan AS name FROM filter_plan WHERE is_active = %s
    """
    values = ('Y','Y','Y',) 
    result = execute_query(query, values)

    for row in result:
        data1.append(create_dict1(row))

    merged_data = {
    'sector': [],
    'location': [],
    'plan': []
    }

    for row in result:
        param_type = row['type']
        name = row['name'].strip()

        if param_type in merged_data:
            merged_data[param_type].append(name)

        if param_type == 'country':
            merged_data['location'].append(name)
    return merged_data

def fetch_job_filter_params():
    data1 = []
    ACTIVE_FLAG = 'Y'  # Constant for is_active condition

    query = """
    SELECT 'skill' AS type, skill_name AS name FROM filter_skills WHERE is_active = %s
    UNION
    SELECT 'sector' AS type, sector_name AS name FROM filter_sectors WHERE is_active = %s
    UNION
    SELECT 'specialisation' AS type, specialisation_name AS name FROM filter_specialisation WHERE is_active = %s
    UNION
    SELECT 'workplace_type' AS type, workplace_type AS name FROM filter_workplace WHERE is_active = %s
    UNION
    SELECT 'job_type' AS type, job_type AS name FROM filter_jobtype WHERE is_active = %s
    UNION
    SELECT 'country' AS type,country AS name FROM filter_location WHERE is_active = %s
    UNION
    SELECT 'schedule' AS type, schedule AS name FROM filter_schedule WHERE is_active = %s
    UNION
    SELECT 'plan' AS type, plan AS name FROM filter_plan WHERE is_active = %s
    """
    values = (ACTIVE_FLAG,) * 7 + (ACTIVE_FLAG,)  # Repeat ACTIVE_FLAG for each parameter
    result = execute_query(query, values)

    for row in result:
        data1.append(create_dict1(row))

    merged_data = {
    'skill': [],
    'sector': [],
    'specialisation': [],
    'workplace_type': [],
    'job_type': [],
    'location': [],
    'schedule': [],
    'plan' : []
    }
    # Process the query result
    for row in result:
        # Extract the type and name from each row
        param_type = row['type']
        name = row['name'].strip()

        # Add the name to the corresponding list in the merged_data dictionary
        if param_type in merged_data:
            merged_data[param_type].append(name)

        # For 'country' type, store the concatenated city and country
        if param_type == 'country':
            merged_data['location'].append(name)
    return merged_data
  
def individual_professional_detail():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            req_data = request.get_json()
            if 'professional_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",500,{})
            professional_id = str(req_data['professional_id'])
            professional_id = int(professional_id.split("-")[2])
            user_data = get_user_data(token_result["email_id"])      
            if user_data["user_role"] == "admin":
                if isUserExist("professional_profile","professional_id",professional_id):
                    query = "SELECT u.first_name, u.last_name, u.email_id, u.dob, u.country_code, u.contact_number, u.country, u.gender, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, p.professional_id,p.professional_resume,p.years_of_experience, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate,p.expert_notes,p.about,p.preferences, p.video_url, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') THEN 0 ELSE pe.end_year END DESC, CASE WHEN (pe.end_month IS NULL OR pe.end_month = '') THEN 0 ELSE pe.end_month END DESC, CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') OR (pe.end_month IS NULL OR pe.end_month = '') THEN pe.created_at END DESC"                
                    values = (professional_id,)
                    profile_result = execute_query(query, values) 

                    if len(profile_result) > 0:                              
                        profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                        intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                        resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                    else:
                        result_json = api_json_response_format(True,"No records found",0,{})     
                        return result_json          
                    s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                    if intro_video_name != '':
                        s3_video_key = s3_intro_video_folder_name + str(intro_video_name)  
                    else:
                        s3_video_key = ""
                    if resume_name != '':     
                        s3_resume_key = s3_resume_folder_name + str(resume_name)
                    else:
                        s3_resume_key = ""
                    query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                    values = (professional_id, 3,)
                    recommended_jobs_id = execute_query(query, values)
                    recommended_jobs_list = []
                    if len(recommended_jobs_id) > 0:
                        for id in recommended_jobs_id:
                            query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country , jp.city , 
                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                            COALESCE(u.country, su.country) AS company_country, 
                            COALESCE(u.city, su.city) AS company_city, 
                            COALESCE(u.email_id, su.email_id) AS email_id,
                            COALESCE(u.is_active, '') AS is_active,
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_name, su.company_name) AS company_name
                            FROM job_post AS jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                            values = (id['job_id'],)
                            detail = execute_query(query, values)
                            if len(detail) > 0:
                                txt = detail[0]['sector']
                                txt = txt.replace(", ", "_")
                                txt = txt.replace(" ", "_")
                                sector_name = txt + ".png"
                                detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                detail[0].update({'profile_image' : img_key})
                                recommended_jobs_list.append(detail[0])
                    else:
                        recommended_jobs_list = []
                    profile_dict = {
                        'first_name': replace_empty_values1(profile_result[0]['first_name']),
                        'last_name': replace_empty_values1(profile_result[0]['last_name']),
                        'professional_id' : '2C-PR-'+ str(profile_result[0]['professional_id']),                                        
                        'email_id': replace_empty_values1(profile_result[0]['email_id']),
                        'dob': replace_empty_values1(profile_result[0]['dob']),
                        'country_code': replace_empty_values1(profile_result[0]['country_code']),
                        'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                        'city': replace_empty_values1(profile_result[0]['city']),
                        'profile_percentage' : replace_empty_values1(profile_result[0]['profile_percentage']),
                        'state': replace_empty_values1(profile_result[0]['state']),
                        'country': replace_empty_values1(profile_result[0]['country']),
                        'pricing_category' : profile_result[0]['pricing_category'],
                        'is_active' : profile_result[0]['is_active'],
                        'payment_status' : profile_result[0]['payment_status'],
                        'profile_image': s3_pic_key,
                        'video_name': s3_video_key,
                        'resume_name': s3_resume_key,
                        'expert_notes' : replace_empty_values1(profile_result[0]['expert_notes']),
                        'about': replace_empty_values1(profile_result[0]['about']),
                        'preferences': replace_empty_values1(profile_result[0]['preferences']),
                        'experience': {},
                        'education': {},
                        'skills': {},
                        'languages': {},
                        'additional_info': {},
                        'social_link': {},
                        'job_list' : {},
                        'recommended_jobs' : recommended_jobs_list,
                        'gender' : replace_empty_values1(profile_result[0]['gender']),
                        'years_of_experience' : replace_empty_values1(profile_result[0]['years_of_experience']),
                        'functional_specification' : replace_empty_values1(profile_result[0]['functional_specification']),
                        'sector' : replace_empty_values1(profile_result[0]['sector']),
                        'industry_sector' : replace_empty_values1(profile_result[0]['industry_sector']),
                        'job_type' : replace_empty_values1(profile_result[0]['job_type']),
                        'location_preference' : replace_empty_values1(profile_result[0]['location_preference']),
                        'mode_of_communication' : replace_empty_values1(profile_result[0]['mode_of_communication']),
                        'willing_to_relocate' : replace_empty_values1(profile_result[0]['willing_to_relocate'])
                    }

                    # Grouping experience data
                    experience_set = set()
                    experience_list = []
                    for exp in profile_result:
                        if exp['experience_id'] is not None:
                            start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                            end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                            exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                            if exp_tuple not in experience_set:
                                experience_set.add(exp_tuple)
                                experience_list.append({
                                    'id': exp['experience_id'],
                                    'company_name': replace_empty_values1(exp['company_name']),
                                    'job_title': replace_empty_values1(exp['job_title']),
                                    'start_date': start_date,                                
                                    'end_date': end_date,                                
                                    'job_description': replace_empty_values1(exp['job_description']),
                                    'job_location': replace_empty_values1(exp['job_location'])
                                })

                    profile_dict['experience'] = experience_list

                    # Grouping education data
                    education_set = set()
                    education_list = []
                    for edu in profile_result:
                        if edu['education_id'] is not None:
                            start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                            end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                            edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                        start_date, end_date, edu['institute_location'])
                            if edu_tuple not in education_set:
                                education_set.add(edu_tuple)
                                education_list.append({
                                    'id': edu['education_id'],
                                    'institute_name': replace_empty_values1(edu['institute_name']),
                                    'degree_level': replace_empty_values1(edu['degree_level']),
                                    'specialisation': replace_empty_values1(edu['specialisation']),
                                    'start_date': start_date,                                
                                    'end_date': end_date,
                                    'institute_location': replace_empty_values1(edu['institute_location'])
                                })

                    profile_dict['education'] = education_list

                    # Grouping skills data
                    skills_set = set()
                    skills_list = []
                    for skill in profile_result:
                        if skill['skill_id'] is not None:
                            skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                            if skill_tuple not in skills_set:
                                skills_set.add(skill_tuple)
                                skills_list.append({
                                    'id': skill['skill_id'],
                                    'skill_name': replace_empty_values1(skill['skill_name']),
                                    'skill_level': replace_empty_values1(skill['skill_level'])
                                })

                    profile_dict['skills'] = skills_list

                    # Grouping languages data
                    languages_set = set()
                    languages_list = []
                    for lang in profile_result:
                        if lang['language_id'] is not None:
                            lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                            if lang_tuple not in languages_set:
                                languages_set.add(lang_tuple)
                                languages_list.append({
                                    'language_known': replace_empty_values1(lang['language_known']),
                                    'id':  lang['language_id'],
                                    'language_level': replace_empty_values1(lang['language_level'])
                            })

                    profile_dict['languages'] = languages_list
                    # Grouping additional info data
                    additional_info_set = set()
                    additional_info_list = []
                    for info in profile_result:
                        if info['additional_info_id'] is not None:
                            info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                            if info_tuple not in additional_info_set:
                                additional_info_set.add(info_tuple)
                                additional_info_list.append({
                                'id': info['additional_info_id'],
                                'title': replace_empty_values1(info['additional_info_title']),
                                'description': replace_empty_values1(info['additional_info_description'])
                            })

                    profile_dict['additional_info'] = additional_info_list
                    # Grouping social link data
                    social_link_set = set()
                    social_link_list = []
                    for link in profile_result:
                        if link['social_link_id'] is not None:
                            link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                            if link_tuple not in social_link_set:
                                social_link_set.add(link_tuple)
                                social_link_list.append({
                                'title': replace_empty_values1(link['social_link_title']),
                                'id':  link['social_link_id'],
                                'url': replace_empty_values1(link['social_link_url'])
                            })

                    profile_dict['social_link'] = social_link_list
                    
                    #Get recommended jobs for the professional
                    query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                    values = (3,professional_id,)
                    job_id_list = execute_query(query, values)
                    job_detail_list = []
                    if len(job_id_list) > 0:
                        for job_id in job_id_list:
                            id = job_id['job_id']
                            query = 'select employer_id, job_title, country, city from job_post where id = %s'
                            values = (id,)
                            job_detail = execute_query(query, values)

                            query = 'select company_name from employer_profile where employer_id = %s'
                            values = (job_detail[0]['employer_id'],)
                            company_name = execute_query(query, values)

                            if len(job_detail) > 0 and len(company_name) > 0:
                                job_detail_dict = {"job_title" : job_detail[0]['job_title'],
                                                    "country" : job_detail[0]['country'],
                                                    "city" : job_detail[0]['city'],
                                                    "company_name" : company_name[0]['company_name']}
                            else:
                                job_detail_dict = {}
                            job_detail_list.append(job_detail_dict)
                        profile_dict['job_list'] = job_detail_list
                    else:
                        profile_dict['job_list'] = []
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
                else:
                    result_json = api_json_response_format(False,"User profile not found",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def admin_professional_dashboard():
    try:
        result_json = {}
        key = ''
        param = ''
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()
            if 'professional_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
                return result_json
            if 'page_number' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'key' in req_data:
                key = req_data['key']
            if 'param' in req_data:
                 param = req_data['param']
            user_data = get_user_data(token_result["email_id"])
            professional_id = req_data['professional_id']
            page_number = req_data['page_number']
             
            offset = (page_number - 1) * 10
            if user_data["user_role"] == "admin":
                total_query = "SELECT COUNT(*) AS total_count FROM professional_profile pp JOIN users u ON pp.professional_id = u.user_id WHERE u.email_active = 'Y' AND u.user_role_fk = 3"
                values = ()
                total_count = execute_query(total_query,values)[0]['total_count']
                filter_parameters = fetch_filter_params()
                if key == 'sort':
                    if param == 'by_date':
                        query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.created_at as user_created_at, u.gender, p.professional_id, p.years_of_experience, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.expert_notes, p.created_at as posted_at, le.job_title, le.created_at, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id > %s AND u.email_active = 'Y' ORDER BY user_created_at LIMIT 10 OFFSET %s"
                    elif param == 'asc':
                        query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.created_at as user_created_at, u.gender, p.professional_id, p.years_of_experience, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.expert_notes, p.created_at as posted_at, le.job_title, le.created_at, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id > %s AND u.email_active = 'Y' ORDER BY u.first_name ASC LIMIT 10 OFFSET %s"
                    else:
                        query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.created_at as user_created_at, u.gender, p.professional_id, p.years_of_experience, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.expert_notes, p.created_at as posted_at, le.job_title, le.created_at, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id > %s AND u.email_active = 'Y' ORDER BY u.first_name DESC LIMIT 10 OFFSET %s"
                else:
                    query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.created_at as user_created_at, u.gender, p.professional_id, p.years_of_experience, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.expert_notes, p.created_at as posted_at, le.job_title, le.created_at, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id > %s AND u.email_active = 'Y' ORDER BY p.professional_id DESC LIMIT 10 OFFSET %s"
                values = (professional_id, offset,)
                candidates_desc = execute_query(query, values)
                first_id = 0
                if len(candidates_desc) > 0:
                    for obj in candidates_desc:
                        query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                        values = (obj['professional_id'], 3,)
                        recommended_jobs_id = execute_query(query, values)
                        recommended_jobs_list = []
                        if len(recommended_jobs_id) > 0:
                            for id in recommended_jobs_id:
                                if isUserExist("job_post", "id", id['job_id']):
                                    query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                    COALESCE(u.country, su.country) AS company_country, 
                                    COALESCE(u.city, su.city) AS company_city, 
                                    COALESCE(u.email_id, su.email_id) AS email_id,
                                    COALESCE(u.is_active, '') AS is_active,
                                    COALESCE(ep.sector, su.sector) AS sector, 
                                    COALESCE(ep.company_name, su.company_name) AS company_name
                                    FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                    values = (id['job_id'],)
                                    detail = execute_query(query, values)
                                    if len(detail) > 0:
                                        txt = detail[0]['sector']
                                        txt = txt.replace(", ", "_")
                                        txt = txt.replace(" ", "_")
                                        sector_name = txt + ".png"
                                        detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                        img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                        detail[0].update({'profile_image' : img_key})
                                        recommended_jobs_list.append(detail[0])
                        else:
                            recommended_jobs_list = []
                        obj.update({'recommended_jobs' : recommended_jobs_list})
                        obj.update({'professional_id' : "2C-PR-" + str(obj['professional_id'])})
                        profile_image_name = replace_empty_values1(obj['profile_image'])
                        s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                        obj.update({'profile_image' : s3_pic_key})
                    first_id = int((candidates_desc[0]['professional_id'].split("-"))[2])
                    if isUserExist("professional_profile","professional_id",first_id):
                        query = "SELECT u.first_name, u.last_name, u.email_id, u.dob, u.country_code, u.contact_number, u.pricing_category, u.profile_percentage, u.is_active, u.payment_status, u.country, u.state, u.city, u.gender, p.professional_id, p.years_of_experience, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.professional_resume,p.expert_notes,p.about,p.preferences, p.video_url, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') THEN 0 ELSE pe.end_year END DESC, CASE WHEN (pe.end_month IS NULL OR pe.end_month = '') THEN 0 ELSE pe.end_month END DESC, CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') OR (pe.end_month IS NULL OR pe.end_month = '') THEN pe.created_at END DESC"                
                        values = (first_id,)
                        profile_result = execute_query(query, values) 

                        if len(profile_result) > 0:                              
                            profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                            intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                            resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                        else:
                            result_json = api_json_response_format(True,"No records found",0,{})     
                            return result_json          
                        s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                        if intro_video_name == '':
                            s3_video_key = ""
                        else:
                            s3_video_key = s3_intro_video_folder_name + str(intro_video_name)
                        if resume_name == '':    
                            s3_resume_key = ""
                        else:          
                            s3_resume_key = s3_resume_folder_name + str(resume_name)

                        query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                        values = (first_id, 3,)
                        recommended_jobs_id = execute_query(query, values)
                        recommended_jobs_list = []
                        if len(recommended_jobs_id) > 0:
                            for id in recommended_jobs_id:
                                if isUserExist("job_post", "id", id['job_id']):
                                    query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                    COALESCE(u.country, su.country) AS company_country, 
                                    COALESCE(u.city, su.city) AS company_city, 
                                    COALESCE(u.email_id, su.email_id) AS email_id,
                                    COALESCE(u.is_active, '') AS is_active,
                                    COALESCE(ep.sector, su.sector) AS sector, 
                                    COALESCE(ep.company_name, su.company_name) AS company_name
                                    FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                    values = (id['job_id'],)
                                    detail = execute_query(query, values)
                                    if len(detail) > 0:
                                        txt = detail[0]['sector']
                                        txt = txt.replace(", ", "_")
                                        txt = txt.replace(" ", "_")
                                        sector_name = txt + ".png"
                                        detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                        img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                        detail[0].update({'profile_image' : img_key})
                                        recommended_jobs_list.append(detail[0])
                        else:
                            recommended_jobs_list = []
                        profile_dict = {
                            'first_name': replace_empty_values1(profile_result[0]['first_name']),
                            'last_name': replace_empty_values1(profile_result[0]['last_name']),
                            'professional_id' : "2C-PR-" + str(profile_result[0]['professional_id']),                                        
                            'email_id': replace_empty_values1(profile_result[0]['email_id']),
                            'dob': replace_empty_values1(profile_result[0]['dob']),
                            'country_code': replace_empty_values1(profile_result[0]['country_code']),
                            'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                            'city': replace_empty_values1(profile_result[0]['city']),
                            'profile_percentage': replace_empty_values1(profile_result[0]['profile_percentage']),
                            'state': replace_empty_values1(profile_result[0]['state']),
                            'country': replace_empty_values1(profile_result[0]['country']),
                            'pricing_category' : profile_result[0]['pricing_category'],
                            'is_active' : profile_result[0]['is_active'],
                            'payment_status' : profile_result[0]['payment_status'],
                            'profile_image': s3_pic_key,
                            'video_name': s3_video_key,
                            'resume_name': s3_resume_key,
                            'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                            'about': replace_empty_values1(profile_result[0]['about']),
                            'preferences': replace_empty_values1(profile_result[0]['preferences']),
                            'experience': {},
                            'education': {},
                            'skills': {},
                            'languages': {},
                            'additional_info': {},
                            'social_link': {},
                            'job_list' : {},
                            'filter_parameters' : filter_parameters,
                            'recommended_jobs' : recommended_jobs_list,
                            'canidates_description' : {},
                            'gender' : replace_empty_values1(profile_result[0]['gender']),
                            'years_of_experience' : replace_empty_values1(profile_result[0]['years_of_experience']),
                            'functional_specification' : replace_empty_values1(profile_result[0]['functional_specification']),
                            'sector' : replace_empty_values1(profile_result[0]['sector']),
                            'industry_sector' : replace_empty_values1(profile_result[0]['industry_sector']),
                            'job_type' : replace_empty_values1(profile_result[0]['job_type']),
                            'location_preference' : replace_empty_values1(profile_result[0]['location_preference']),
                            'mode_of_communication' : replace_empty_values1(profile_result[0]['mode_of_communication']),
                            'willing_to_relocate' : replace_empty_values1(profile_result[0]['willing_to_relocate'])
                        }

                        # Grouping experience data
                        experience_set = set()
                        experience_list = []
                        for exp in profile_result:
                            if exp['experience_id'] is not None:
                                start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                                end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                                exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                                if exp_tuple not in experience_set:
                                    experience_set.add(exp_tuple)
                                    experience_list.append({
                                        'id': exp['experience_id'],
                                        'company_name': replace_empty_values1(exp['company_name']),
                                        'job_title': replace_empty_values1(exp['job_title']),
                                        'start_date': start_date,                                
                                        'end_date': end_date,                                
                                        'job_description': replace_empty_values1(exp['job_description']),
                                        'job_location': replace_empty_values1(exp['job_location'])
                                    })

                        profile_dict['experience'] = experience_list

                        # Grouping education data
                        education_set = set()
                        education_list = []
                        for edu in profile_result:
                            if edu['education_id'] is not None:
                                start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                                end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                                edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                            start_date, end_date, edu['institute_location'])
                                if edu_tuple not in education_set:
                                    education_set.add(edu_tuple)
                                    education_list.append({
                                        'id': edu['education_id'],
                                        'institute_name': replace_empty_values1(edu['institute_name']),
                                        'degree_level': replace_empty_values1(edu['degree_level']),
                                        'specialisation': replace_empty_values1(edu['specialisation']),
                                        'start_date': start_date,                                
                                        'end_date': end_date,
                                        'institute_location': replace_empty_values1(edu['institute_location'])
                                    })

                        profile_dict['education'] = education_list

                        # Grouping skills data
                        skills_set = set()
                        skills_list = []
                        for skill in profile_result:
                            if skill['skill_id'] is not None:
                                skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                                if skill_tuple not in skills_set:
                                    skills_set.add(skill_tuple)
                                    skills_list.append({
                                        'id': skill['skill_id'],
                                        'skill_name': replace_empty_values1(skill['skill_name']),
                                        'skill_level': replace_empty_values1(skill['skill_level'])
                                    })

                        profile_dict['skills'] = skills_list

                        # Grouping languages data
                        languages_set = set()
                        languages_list = []
                        for lang in profile_result:
                            if lang['language_id'] is not None:
                                lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                                if lang_tuple not in languages_set:
                                    languages_set.add(lang_tuple)
                                    languages_list.append({
                                        'language_known': replace_empty_values1(lang['language_known']),
                                        'id':  lang['language_id'],
                                        'language_level': replace_empty_values1(lang['language_level'])
                                })

                        profile_dict['languages'] = languages_list
                        # Grouping additional info data
                        additional_info_set = set()
                        additional_info_list = []
                        for info in profile_result:
                            if info['additional_info_id'] is not None:
                                info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                                if info_tuple not in additional_info_set:
                                    additional_info_set.add(info_tuple)
                                    additional_info_list.append({
                                    'id': info['additional_info_id'],
                                    'title': replace_empty_values1(info['additional_info_title']),
                                    'description': replace_empty_values1(info['additional_info_description'])
                                })

                        profile_dict['additional_info'] = additional_info_list
                        # Grouping social link data
                        social_link_set = set()
                        social_link_list = []
                        for link in profile_result:
                            if link['social_link_id'] is not None:
                                link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                                if link_tuple not in social_link_set:
                                    social_link_set.add(link_tuple)
                                    social_link_list.append({
                                    'title': replace_empty_values1(link['social_link_title']),
                                    'id':  link['social_link_id'],
                                    'url': replace_empty_values1(link['social_link_url'])
                                })

                        profile_dict['social_link'] = social_link_list
                        
                        #Get recommended jobs for the professional
                        query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                        values = (3,first_id,)
                        job_id_list = execute_query(query, values)
                        job_detail_list = []
                        if len(job_id_list) > 0:
                            for job_id in job_id_list:
                                id = job_id['job_id']
                                if isUserExist("job_post", "id", id):
                                    query = 'select employer_id, job_title, country, city from job_post where id = %s'
                                    values = (id,)
                                    job_detail = execute_query(query, values)
                                    query = 'select company_name from employer_profile where employer_id = %s'
                                    values = (job_detail[0]['employer_id'],)
                                    company_name = execute_query(query, values)
                                else:
                                    job_detail = [{"job_title" : "","country" : "", "city" : ""}]
                                    company_name = [{'company_name': ""}]

                                if len(job_detail) > 0 and len(company_name) > 0:
                                    job_detail_dict = {"job_title" : job_detail[0]['job_title'],
                                                        "country" : job_detail[0]['country'],
                                                        "city" : job_detail[0]['city'],
                                                        "company_name" : company_name[0]['company_name']}
                                else:
                                    job_detail_dict = {}
                                job_detail_list.append(job_detail_dict)
                            profile_dict['job_list'] = job_detail_list
                        else:
                            profile_dict['job_list'] = []
                        profile_dict['canidates_description'] = candidates_desc
                        profile_dict.update({'total_count' : total_count})
                        result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
                    else:
                        profile_dict = {'filter_parameters' : filter_parameters}
                        profile_dict.update({'job_list' : []})
                        profile_dict.update({'canidates_description' : []})
                        profile_dict.update({'total_count' : 0})
                        profile_dict.update({'recommended_jobs' : []})
                        result_json = api_json_response_format(True,"User not found",0,profile_dict)                    
                else:
                    profile_dict = {'filter_parameters' : filter_parameters}
                    profile_dict.update({'job_list' : []})
                    profile_dict.update({'canidates_description' : []})
                    profile_dict.update({'total_count' : 0})
                    profile_dict.update({'recommended_jobs' : []})
                    result_json = api_json_response_format(True,"No records found.",0,profile_dict)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_details():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()  
            if 'job_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            job_id = req_data['job_id']
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                query = """WITH JobApplicants AS (
                            SELECT 
                                professional_id 
                            FROM 
                                job_activity 
                            WHERE 
                                job_id = %s),
                        LatestExperience AS (
                            SELECT 
                                pe.professional_id, 
                                pe.id AS experience_id, 
                                pe.start_month, 
                                pe.start_year, 
                                pe.job_title, 
                                pe.created_at, 
                                ROW_NUMBER() OVER (
                                    PARTITION BY pe.professional_id 
                                    ORDER BY 
                                        CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, 
                                        CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, 
                                        pe.created_at DESC 
                                ) AS rn 
                            FROM 
                                professional_experience AS pe
                            WHERE 
                                pe.professional_id IN (SELECT professional_id FROM JobApplicants)
                        )
                        SELECT 
                            u.first_name, 
                            u.last_name, 
                            u.country, 
                            u.state, 
                            u.city,
                            u.profile_percentage,
                            p.professional_id, 
                            le.job_title, 
                            u.profile_image,
                            u.pricing_category,
                            u.is_active,
                            u.payment_status
                        FROM 
                            users AS u
                        LEFT JOIN 
                            professional_profile AS p ON u.user_id = p.professional_id
                        LEFT JOIN 
                            LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                        WHERE 
                            u.user_role_fk IN (SELECT role_id FROM user_role WHERE user_role = %s)
                            AND p.professional_id IN (SELECT professional_id FROM JobApplicants) AND p.professional_id NOT IN (
                        SELECT professional_id 
                        FROM sc_recommendation 
                        WHERE job_id = %s 
                        AND user_role_id = %s
                        AND u.email_active = %s
                    )"""
                values = (job_id,"professional", job_id, 2,"Y",)
                row_count = execute_query(query, values)
                if len(row_count) > 0:
                    for i in row_count:
                        check_applied_status_query = "select count(id) as count from job_activity where professional_id = %s and job_id = %s"
                        check_applied_status_values = (i['professional_id'], job_id,)
                        check_applied_status = execute_query(check_applied_status_query, check_applied_status_values)
                        if check_applied_status:
                            if check_applied_status[0]['count'] > 0:
                                applied_status_flag = "applied"
                            else:
                                applied_status_flag = "not_applied"
                        else:
                            applied_status_flag = "not_applied"
                        mod_id = '2C-PR-'+ str(i['professional_id'])
                        i.update({"applied_status" : applied_status_flag})
                        i.update({'profile_image' : s3_picture_folder_name + str(i['profile_image'])})
                        i.update({'professional_id' : mod_id})
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,row_count)
                else:
                    result_json = api_json_response_format(False, "There are currently no applicants for this job. Please try again later.",201,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})                           
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def make_recommendation():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()                        
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'professional_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'description' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'employer_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'flag' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'applied_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                flag = req_data['flag']
                applied_status = ""
                if req_data['applied_status'] != "":
                    applied_status = req_data['applied_status']
                if req_data['employer_id'] != "":
                    employer_id = req_data['employer_id']
                job_id = req_data['job_id']
                professional_id = int(req_data['professional_id'].split("-")[2])
                description = req_data['description']
                success_msg = ''
                id = ''
                notification_msg = ''
                error_msg = ''
                if flag == 'professional':
                    success_msg = "The job has been recommended to the professional!"
                    notification_msg = "We have a job recommendation for you. Click on the recommended tab to view the recommendation."
                    error_msg = 'This job has already been recommended to the professional'
                    id = professional_id
                    role_id = 3
                else:
                    success_msg = "The professional has been recommended for the job!"
                    notification_msg = "We have a profile recommendation for the job you posted"
                    error_msg = 'The professional has already been recommended for this job!'
                    id = employer_id
                    role_id = 2

                query = 'select count(id) from sc_recommendation where job_id = %s and professional_id = %s and user_role_id = %s'
                values = (job_id, professional_id, role_id,)
                entry_row_count = execute_query(query, values)
                if entry_row_count[0]['count(id)'] > 0:
                    result_json = api_json_response_format(False,error_msg,201,{})
                else:
                    if role_id == 2:
                        created_at = datetime.now()
                        query = 'insert into sc_recommendation(job_id, professional_id, user_role_id, description, created_at) values(%s,%s,%s,%s,%s)'
                        values = (job_id, professional_id, role_id, description, created_at,)
                        insert_status = update_query_last_index(query, values)
                        if applied_status == 'not_applied':
                            invited_jobs_query = 'insert into invited_jobs(job_id, professional_id, employer_id, employer_feedback, invite_mode, is_invite_sent) values (%s,%s,%s,%s,%s,%s)' 
                            values = (job_id, professional_id, employer_id, description, 'from 2ndC', 'N',)
                            invited_jobs_insert_status = update_query_last_index(invited_jobs_query, values)
                        
                        if insert_status['row_count'] > 0:
                            created_at = datetime.now()  
                            query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                            values = (id, notification_msg, created_at,)
                            update_query(query,values)
                            background_runner.admin_recmnd_email(job_id, professional_id,role_id)
                            result_json = api_json_response_format(True,success_msg,0,{})
                        else:
                            result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.",500,{})
                    else:
                        query = 'select count(id) from job_activity where job_id = %s and professional_id = %s'
                        values = (job_id, professional_id,)
                        job_status = execute_query(query, values)

                        if job_status[0]['count(id)'] > 0:
                            result_json = api_json_response_format(False, 'The job was already applied by the professional', 500, {})
                            return result_json
                        created_at = datetime.now()
                        query = 'insert into sc_recommendation(job_id, professional_id, user_role_id, description, created_at) values(%s,%s,%s,%s,%s)'
                        values = (job_id, professional_id, role_id, description, created_at,)
                        insert_status = update_query_last_index(query, values)
                        if insert_status['row_count'] > 0:
                            created_at = datetime.now()  
                            query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                            values = (id, notification_msg, created_at,)
                            update_query(query,values)
                            background_runner.admin_recmnd_email(job_id, id,role_id)
                            result_json = api_json_response_format(True,success_msg,0,{})
                        else:
                            result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.",500,{})
                    # if insert_status['row_count'] > 0:
                    #     flag_updation_query = "update sc_recommendation set flag = flag + 1 where professional_id = %s"
                    #     flag_updation_values = (id,)
                    #     update_query(flag_updation_query,flag_updation_values)
                        # query = 'select flag, created_at from sc_recommendation where professional_id = %s and user_role_id = %s'
                        # values = (professional_id, role_id,)
                        # existing_row_count = execute_query(query, values)
                        # if len(existing_row_count) > 0:
                        #     if existing_row_count[0]['flag'] < 6:
                                # background_runner.admin_recmnd_email(job_id, id,role_id)
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})                           
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def delete_recommendation():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()                        
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'professional_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json   
                if 'role_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json 
                job_id = req_data['job_id']
                role_id = req_data['role_id']
                professional_id = int(req_data['professional_id'].split("-")[2])
                query = 'delete from sc_recommendation where job_id = %s and professional_id = %s and user_role_id = %s'
                values = (job_id, professional_id, role_id,)
                row_count = update_query(query, values)

                query = 'delete from invited_jobs where job_id = %s and professional_id = %s and invite_mode = %s'
                values = (job_id, professional_id, 'from 2ndC',)
                row_count_1 = update_query(query, values)
                if row_count > 0:
                    result_json = api_json_response_format(True,"The professional has been removed from the recommendation!",0,{})
                else:
                    result_json = api_json_response_format(False, "Please try again later.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})                           
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def individual_job_details():
    try:
        req_data = request.get_json()                        
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"job_id required",204,{})  
            return result_json
        job_id = req_data['job_id'] 
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:            
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                if isUserExist("job_post","id",job_id):
                    query = '''SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.specialisation, jp.timezone, jp.additional_info, jp.skills, jp.job_status, jp.custom_notes, jp.country as job_country, jp.state as job_state, jp.city as job_city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, 
                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                    COALESCE(u.country, su.country) AS company_country, 
                    COALESCE(u.city, su.city) AS company_city, 
                    COALESCE(u.email_id, su.email_id) AS company_email, 
                    COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                    COALESCE(ep.sector, su.sector) AS sector, 
                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                    COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                    COALESCE(ep.website_url, su.website_url) AS website_url
                    FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s'''
                    values = (job_id,)
                    job_details_data_set = execute_query(query,values)
                    
                    query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                    values = (job_id, 2,)
                    recommended_professional_id = execute_query(query, values)
                    recommended_professional_list = []
                    if len(recommended_professional_id) > 0:
                        for id in recommended_professional_id:
                            query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, u.is_active, u.payment_status, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                            values = (id['professional_id'],)
                            detail = execute_query(query, values)
                            if len(detail) > 0:
                                mod_id = "2C-PR-" + str(detail[0]['professional_id'])
                                detail[0].update({'professional_id' : mod_id})
                                if detail[0]['profile_image'] != "":
                                    detail[0].update({'profile_image' : s3_picture_folder_name + str(detail[0]['profile_image'])})
                                else:
                                    detail[0].update({'profile_image' : ''})
                                recommended_professional_list.append(detail[0])
                    else:
                        recommended_professional_list = []

                    query = 'select employer_id from job_post where id = %s'
                    values = (job_id,)
                    employer_id = execute_query(query, values)
                    if len(employer_id) > 0:
                        if employer_id[0]['employer_id'] < 500000:
                            query = 'SELECT e.employer_id, e.employer_type, e.sector, e.company_name, e.website_url, e.company_description, u.country, u.city FROM employer_profile AS e LEFT JOIN users AS u ON e.employer_id = u.user_id WHERE e.employer_id = %s'
                            values = (employer_id[0]['employer_id'],)
                            employer_details = execute_query(query, values)
                        else:
                            query = 'SELECT su.sub_user_id as employer_id, su.employer_type, su.sector, su.company_name, su.website_url, su.company_description, su.country, su.city FROM sub_users AS su WHERE su.sub_user_id = %s'
                            values = (employer_id[0]['employer_id'],)
                            employer_details = execute_query(query, values)
                    else:
                        employer_details = []

                    if len(employer_details) > 0:
                        if employer_id[0]['employer_id'] < 500000:
                            query = 'select profile_image from users where user_id = %s'
                        else:
                            query = 'select profile_image from sub_users where sub_user_id = %s'
                        values = (employer_id[0]['employer_id'],)
                        profile_pic = execute_query(query, values)
                        job_details_data_set[0].update({'profile_image' : s3_employer_picture_folder_name + str(profile_pic[0]['profile_image'])})
                        job_details_data_set[0].update({'employer_id' : employer_details[0]['employer_id']})
                        job_details_data_set[0].update({'employer_type' : employer_details[0]['employer_type']})
                        job_details_data_set[0].update({'sector' : employer_details[0]['sector']})
                        job_details_data_set[0].update({'company_name' : employer_details[0]['company_name']})
                        job_details_data_set[0].update({'country' : employer_details[0]['country']})
                        job_details_data_set[0].update({'city' : employer_details[0]['city']})
                        job_details_data_set[0].update({'company_description' : employer_details[0]['company_description']})
                        job_details_data_set[0].update({'website_url' : employer_details[0]['website_url']})
                        job_details_data_set[0].update({'recommended_professionals' : recommended_professional_list})
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(job_details_data_set))
                else:
                    result_json = api_json_response_format(False,"Job not found",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 
    
def admin_jobs_dashboard():
    try:
        profile = {}
        key = ''
        param = ''
        req_data = request.get_json()
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["is_exist"]:
                if user_data["user_role"] == "admin":
                    if 'job_id' not in req_data:
                        result_json = api_json_response_format(False,"job_id required",204,{})  
                        return result_json
                    if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"page number required",204,{})  
                        return result_json
                    if 'key' in req_data:
                        key = req_data['key']
                    if 'param' in req_data:
                        param = req_data['param']
                    job_id = req_data["job_id"]
                    page_number = req_data["page_number"]
                    offset = (page_number - 1) * 10
                    # total_query = "SELECT COUNT(*) AS total_count FROM job_post where job_status != %s"
                    # values = ("drafted",)

                    total_query = "SELECT COUNT(*) AS total_count FROM job_post jp left join users u on u.user_id = jp.employer_id left join sub_users su on su.sub_user_id = jp.employer_id WHERE jp.job_status != %s AND COALESCE(u.email_active, su.email_active) = 'Y'"
                    values = ("drafted",)
                    total_count_dict = execute_query(total_query,values)
                    total_count = 0
                    if total_count_dict:
                        total_count = total_count_dict[0]['total_count']
                    first_job_id = 0
                    if key == 'sort':
                        if param == 'by_date':
                            query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.specialisation, jp.responsibilities, jp.additional_info, jp.skills, jp.country as job_country, jp.state, jp.city as job_city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.custom_notes, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, 
                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                            COALESCE(u.country, su.country) AS company_country, 
                            COALESCE(u.city, su.city) AS company_city, 
                            COALESCE(u.email_id, su.email_id) AS company_email, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(ep.website_url, su.website_url) AS website_url
                            FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status != %s AND jp.id > %s AND COALESCE(u.email_active, su.email_active) = 'Y' ORDER BY jp.created_at LIMIT 10 OFFSET %s"""
                        elif param == 'asc':
                            query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.specialisation, jp.responsibilities, jp.additional_info, jp.skills, jp.country as job_country, jp.state, jp.city as job_city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.custom_notes, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, 
                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                            COALESCE(u.country, su.country) AS company_country, 
                            COALESCE(u.city, su.city) AS company_city, 
                            COALESCE(u.email_id, su.email_id) AS company_email, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(ep.website_url, su.website_url) AS website_url
                            FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status != %s AND jp.id > %s AND COALESCE(u.email_active, su.email_active) = 'Y' ORDER BY jp.job_title ASC LIMIT 10 OFFSET %s"""
                        else:
                            query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.specialisation, jp.responsibilities, jp.additional_info, jp.skills, jp.country as job_country, jp.state, jp.city as job_city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.custom_notes, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, 
                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                            COALESCE(u.country, su.country) AS company_country, 
                            COALESCE(u.city, su.city) AS company_city, 
                            COALESCE(u.email_id, su.email_id) AS company_email, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(ep.website_url, su.website_url) AS website_url
                            FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status != %s AND jp.id > %s AND COALESCE(u.email_active, su.email_active) = 'Y' ORDER BY jp.job_title DESC LIMIT 10 OFFSET %s"""
                    else:
                        query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.specialisation, jp.responsibilities, jp.additional_info, jp.skills, jp.country as job_country, jp.state, jp.city as job_city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.custom_notes, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, 
                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                            COALESCE(u.country, su.country) AS company_country, 
                            COALESCE(u.city, su.city) AS company_city, 
                            COALESCE(u.email_id, su.email_id) AS company_email, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(ep.website_url, su.website_url) AS website_url
                        FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status != %s AND jp.id > %s AND COALESCE(u.email_active, su.email_active) = 'Y' ORDER BY jp.id DESC LIMIT 10 OFFSET %s"""
                    values_job_details = ('drafted',job_id,offset,)
                    job_details = replace_empty_values(execute_query(query_job_details, values_job_details))
                    data = fetch_job_filter_params()
                    if len(job_details) > 0:
                        first_job_id = job_details[0]['id']
                        for job in job_details:
                            query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                            values = (job['id'], 2,)
                            prof_id_list = execute_query(query, values)
                            temp_list = []
                            for j in prof_id_list:
                                query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, u.is_active, u.payment_status, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                                values = (j['professional_id'],)
                                info = execute_query(query, values)
                                if len(info) > 0:
                                    mod_id = "2C-PR-" + str(info[0]['professional_id'])
                                    if info[0]['profile_image'] != '':
                                        s3_pic_key = s3_picture_folder_name + str(info[0]['profile_image'])
                                    else:
                                        s3_pic_key = ''
                                    info[0].update({'profile_image' : s3_pic_key})
                                    info[0].update({'professional_id' : mod_id})
                                    temp_list.append(info[0])
                                # j.update({"recommended_professional" : temp_list})
                            job.update({'profile_image' : s3_employer_picture_folder_name + str(job['profile_image'])})
                            job.update({'sector' : job['sector']})
                            job.update({'employer_type' : job['employer_type']})
                            job.update({"recommended_professional" : temp_list})
                        job_details_dict = {'job_details': job_details}
                        profile.update(job_details_dict)
                        profile.update(data)
                        profile.update({'total_count' : total_count})
                        result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                    else:
                        profile.update(data)
                        job_details_dict = {'job_details': []}
                        profile.update({'total_count' : 0})
                        # profile.update({'job_type' : })

                        profile.update(job_details_dict)
                        result_json = api_json_response_format(True, "No jobs found!", 0, profile)
                else:
                    result_json = api_json_response_format(False, "Unauthorized user", 401, {})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def admin_custom_notes():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(job_id)",204,{})  
            return result_json
        if 'custom_notes' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(custom_notes)",204,{})  
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":  
                job_id = req_data['job_id']
                if isUserExist("job_post", "id", job_id):
                    custom_notes = req_data['custom_notes']
                    query = "update job_post set custom_notes = %s WHERE id = %s"
                    values = (custom_notes, job_id,)
                    update_status = update_query(query, values)
                    if update_status > 0:
                        result_json = api_json_response_format(True,"Custom notes has been added successfully!",0,{})
                    else:
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                else:
                    result_json = api_json_response_format(False,"Invalid job id",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def admin_job_filter_results():
    try:
        data = []
        profile = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                specialisation = req_data["specialisation"]
                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                # skills_list = ["Agile Methodologies", "Algorithm Design", "Analytics", "Application Programming Interfaces (APIs)", "Budgeting", "Business Strategy", "Change Management", "Conflict Resolution", "Contract Management Skills", "Data Analysis", "Database Design", "Debugging", "Direct Sales", "Earned Value Management", "Financial Management", "Human Resource Management", "Keyword Research", "Leadership Skills", "Market Research", "Marketing Skills", "Metrics and KPIs", "Mobile Application Development", "Negotiation", "Operations Management", "Organizational Development", "Presentation", "Process Improvement", "Product Knowledge", "Project Management", "Quality Assurance (QA)", "Recruiting", "Revenue Expansion", "Risk Assessment", "SaaS Knowledge", "Sales and Budget Forecasting", "Salesforce", "Strategic Planning", "Supply Chain Management", "Talent Management", "Team Leadership", "Upselling"]
                # specialisation_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite And Board"]
                # sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # schedule_list = ["Fixed", "Flexible", "Monday to Friday", "Weekend only"]
                ACTIVE_FLAG = 'Y'

                # Step 1 — Get base filters (skills, sectors, country)
                query = """
                    SELECT 'skill' AS type, skill_name AS name FROM filter_skills WHERE is_active = %s
                    UNION
                    SELECT 'industry_sector' AS type, sector_name AS name FROM filter_sectors WHERE is_active = %s
                    UNION
                    SELECT 'country' AS type, country AS name FROM filter_location WHERE is_active = %s
                """
                values = (ACTIVE_FLAG,) * 3
                base_filters = execute_query(query, values)

                # Step 2 — Create list arrays from base filters
                skills_list = [row['name'] for row in base_filters if row['type'] == 'skill']
                # sectors_list = [row['name'] for row in base_filters if row['type'] == 'industry_sector']
                country_list = [row['name'] for row in base_filters if row['type'] == 'country']

                # Step 3 — Get specific admin filters only
                admin_query = """
                    SELECT filter_name, filter_value 
                    FROM admin_filters 
                    WHERE filter_name IN ('schedule_list', 'functional_specification', industry_sector)
                """
                admin_result = execute_query(admin_query, ())

                # Step 4 — Parse and assign lists
                schedule_list = []
                specialisation_list = []
                sectors_list = []
                for row in admin_result:
                    filter_name = row['filter_name']
                    filter_value = row['filter_value']

                    if isinstance(filter_value, str):
                        try:
                            filter_value = json.loads(filter_value)
                        except json.JSONDecodeError:
                            try:
                                filter_value = ast.literal_eval(filter_value)
                            except Exception:
                                filter_value = [filter_value]

                    if filter_name == 'schedule_list':
                        schedule_list = filter_value
                    elif filter_name == 'functional_specification':
                        specialisation_list = filter_value
                    elif filter_name == 'industry_sector':
                        sectors_list = filter_value

                plan = req_data['plan']
                offset = (page_number - 1) * 10
                
                query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.job_status, jp.custom_notes, jp.country as job_country, jp.state as job_state, jp.city as job_city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.timezone,
                COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                COALESCE(u.country, su.country) AS company_country, 
                COALESCE(u.city, su.city) AS company_city, 
                COALESCE(u.email_id, su.email_id) AS company_email, 
                COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                COALESCE(ep.sector, su.sector) AS sector, 
                COALESCE(ep.company_description, su.company_description) AS company_description, 
                COALESCE(ep.company_name, su.company_name) AS company_name, 
                COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                COALESCE(ep.website_url, su.website_url) AS website_url
                FROM job_post jp 
                LEFT JOIN users u ON jp.employer_id = u.user_id 
                LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id 
                LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status != 'drafted' """
                    # FROM 
                    #     job_post jp 
                    # JOIN 
                    #     users u ON jp.employer_id = u.user_id 
                    # WHERE 
                    #     (jp.job_status = 'Opened') """
                    
                conditions = []
                values_job_details = []

                if req_data.get('skills'):
                    if "Others" in req_data.get('skills'):
                        skill_conditions = ["jp.skills Not Like %s" for _ in skills_list]
                        conditions.append(f"({' OR '.join(skill_conditions)})")
                        for skill in skills_list:
                            values_job_details.append(f"%{skill}%")
                    else:
                        skill_conditions = ["jp.skills LIKE %s" for _ in req_data['skills']]
                        conditions.append(f"({' OR '.join(skill_conditions)})")
                        for skill in req_data['skills']:
                            values_job_details.append(f"%{skill}%")
  
                if req_data['specialisation']:
                    if "Others" in req_data['specialisation']:
                        conditions.append("jp.specialisation NOT IN %s")
                        values_job_details.append(tuple(specialisation_list),)
                    else:
                        conditions.append("jp.specialisation IN %s")
                        values_job_details.append(tuple(specialisation),)

                if req_data['workplace_type']:
                    conditions.append("jp.workplace_type IN %s")
                    values_job_details.append(tuple(req_data['workplace_type'],))

                if req_data['job_type']:
                    conditions.append("jp.job_type IN %s")
                    values_job_details.append(tuple(req_data['job_type'],))
                
                if req_data['job_status']:
                    conditions.append("jp.job_status IN %s")
                    values_job_details.append(tuple(req_data['job_status'],))

                if req_data['plan']:
                    conditions.append("COALESCE(u.pricing_category, su.pricing_category) IN %s")
                    values_job_details.append(tuple(req_data['plan'],))

                if req_data['work_schedule']:
                    if "Others" in req_data['work_schedule']:
                        conditions.append("jp.work_schedule NOT IN %s")
                        values_job_details.append(tuple(schedule_list),)
                    else:
                        conditions.append("jp.work_schedule IN %s")
                        values_job_details.append(tuple(req_data['work_schedule']),)

                # if country:
                #     if "Others" in country:
                #         conditions.append("jp.country NOT IN %s")
                #         values_job_details.append(tuple(country_list),)
                #     else:
                #         conditions.append("jp.country IN %s")
                #         values_job_details.append(tuple(country),)

                if country:
                    conditions.append("jp.country IN %s")
                    values_job_details.append(tuple(country),)
                
                if city:
                    conditions.append("jp.city IN %s")
                    values_job_details.append(tuple(city),)

                if req_data['sector']:
                    if "Others" in req_data['sector']:
                        conditions.append("COALESCE(ep.sector, su.sector) NOT IN %s")
                        # conditions.append("su.sector NOT IN %s")
                        # values_job_details.append(tuple(sectors_list),)
                        values_job_details.append(tuple(sectors_list),)
                    else:
                        conditions.append("COALESCE(ep.sector, su.sector) IN %s")
                        # conditions.append("su.sector IN %s")
                        # values_job_details.append(tuple(req_data['sector']),)
                        values_job_details.append(tuple(req_data['sector']),)

                if conditions:
                    if len(conditions) == 1:
                        query_job_details += " AND " + conditions[0]
                    else:
                        query_job_details += " AND (" + " AND ".join(conditions) + ")"
                # base_query = "FROM job_post jp JOIN users u ON jp.employer_id = u.user_id LEFT JOIN job_activity ja ON jp.id = ja.job_id WHERE (jp.job_status = 'Opened') AND ja.job_id IS NULL"
                # total_count_query = "SELECT COUNT(*) AS total_count " + base_query
                # total_count = execute_query(total_count_query, values_job_details)[0]['total_count']
                query_job_details += " ORDER BY jp.id DESC "
                new_query = query_job_details + "LIMIT 10 OFFSET %s"
                val_detail = values_job_details
                query = "SELECT COUNT(*) AS total_count FROM (" + query_job_details + ") AS subquery"
                values = (val_detail)
                total_count = execute_query(query, values)
                if len(total_count) > 0:
                    total_count = total_count[0]['total_count']
                else:
                    total_count = 0
                values_job_details.append(offset,)
                job_details = replace_empty_values(execute_query(new_query, values_job_details))
                if len(job_details) > 0:
                    for job in job_details:
                            query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                            values = (job['id'], 2,)
                            prof_id_list = execute_query(query, values)
                            temp_list = []
                            for j in prof_id_list:
                                query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, u.is_active, u.payment_status, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                                values = (j['professional_id'],)
                                info = execute_query(query, values)
                                if len(info) > 0:
                                    mod_id = "2C-PR-" + str(info[0]['professional_id'])
                                    info[0].update({'professional_id' : mod_id})
                                    if info[0]['profile_image'] != '':
                                        s3_pic_key = s3_picture_folder_name + str(info[0]['profile_image'])
                                    else:
                                        s3_pic_key = ''
                                    info[0].update({'professional_profile_image' : s3_pic_key})
                                    temp_list.append(info[0])
                                # j.update({"recommended_professional" : temp_list})
                            job.update({'profile_image' : s3_employer_picture_folder_name + str(job['profile_image'])})
                            job.update({'sector' : job['sector']})
                            job.update({'employer_type' : job['employer_type']})
                            job.update({"recommended_professional" : temp_list})


                    # job_id = job_details[0]['id']
                    # query = 'select professional_id from sc_recommendation where job_id = %s'
                    # values = (job_id,)
                    # recommended_professional_id = execute_query(query, values)
                    # recommended_professional_list = []
                    # if len(recommended_professional_id) > 0:
                    #     for id in recommended_professional_id:
                    #         query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                    #         values = (id['professional_id'],)
                    #         detail = execute_query(query, values) 
                    #         recommended_professional_list.append(detail[0])
                    # else:
                    #     recommended_professional_list = []
                else:
                    result_json = api_json_response_format(True, "No records found", 0, profile)
                    return result_json
                # recommended_professional_dict = {'recommended_professionals' : recommended_professional_list}
                # profile.update(recommended_professional_dict)               
                job_details_dict = {'job_details': job_details}
                profile.update(job_details_dict)
                profile.update({'total_count' : total_count})
                data = fetch_job_filter_params()
                profile.update(data)
                  
                if job_details == "" or job_details == []:
                    result_json = api_json_response_format(True, "No records found", 0, profile)
                else:
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def admin_professional_filter_results():
    try:
        result_json = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                page_number = req_data["page_number"]
                offset = (page_number - 1) * 10
                location = req_data["location"]
                skill_value = req_data["skills"]
                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                skill_list = ["Agile Methodologies", "Algorithm Design", "Analytics", "Application Programming Interfaces (APIs)", "Budgeting", "Business Strategy", "Change Management", "Conflict Resolution", "Contract Management Skills", "Data Analysis", "Database Design", "Debugging", "Direct Sales", "Earned Value Management", "Financial Management", "Human Resource Management", "Keyword Research", "Leadership Skills", "Market Research", "Marketing Skills", "Metrics and KPIs", "Mobile Application Development", "Negotiation", "Operations Management", "Organizational Development", "Presentation", "Process Improvement", "Product Knowledge", "Project Management", "Quality Assurance (QA)", "Recruiting", "Revenue Expansion", "Risk Assessment", "SaaS Knowledge", "Sales and Budget Forecasting", "Salesforce", "Strategic Planning", "Supply Chain Management", "Talent Management", "Team Leadership", "Upselling"]
                # industry_sector_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # sector_list = ["Academic", "Corporate", "Non-profit", "Startup"]
                functional_specification_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite and Board"]
                query_view = """CREATE OR REPLACE VIEW view_job_details AS
                    WITH LatestExperience AS (
                        SELECT
                            pe.professional_id,
                            pe.id AS experience_id,
                            pe.start_month,
                            pe.start_year,
                            pe.job_title,
                            pe.job_description,
                            pe.created_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY pe.professional_id
                                ORDER BY
                                    COALESCE(pe.start_year, 0) DESC,
                                    COALESCE(pe.start_month, 0) DESC,
                                    pe.created_at DESC
                            ) AS rn
                        FROM professional_experience pe
                    ),
                    SearchableExperience AS (
                        SELECT
                            le.professional_id,
                            le.job_title,
                            le.job_description
                        FROM LatestExperience le
                        WHERE le.rn <= 3
                    )
                    SELECT
                        u.user_id,
                        u.first_name,
                        u.last_name,
                        u.email_id,
                        u.country,
                        u.state,
                        u.city,
                        u.profile_percentage,
                        u.pricing_category,
                        u.is_active,
                        u.payment_status,
                        u.gender,
                        p.professional_id,
                        p.about,
                        p.sector,
                        p.industry_sector,
                        p.functional_specification,
                        p.job_type,
                        p.location_preference,
                        p.willing_to_relocate,
                        p.mode_of_communication,
                        p.expert_notes,
                        p.created_at AS posted_at,
                        COALESCE(le.job_title, '') AS job_title,
                        COALESCE(le.job_description, '') AS job_description,
                        COALESCE(le.created_at, '') AS experience_created_at,
                        ps.skill_name,
                        pai.description AS skill_description,
                        u2.profile_image,
                        pai.description,
                        ped.specialisation,
                        pl.language_known
                    FROM users u
                    LEFT JOIN professional_profile p ON u.user_id = p.professional_id
                    LEFT JOIN professional_education ped ON u.user_id = ped.professional_id
                    LEFT JOIN professional_additional_info pai ON u.user_id = pai.professional_id
                    LEFT JOIN professional_language pl ON u.user_id = pl.professional_id
                    LEFT JOIN LatestExperience le ON p.professional_id = le.professional_id AND le.rn = 1
                    LEFT JOIN users u2 ON u.user_id = u2.user_id
                    LEFT JOIN professional_skill ps ON u.user_id = ps.professional_id
                    LEFT JOIN SearchableExperience se ON p.professional_id = se.professional_id
                    WHERE u.user_role_fk = %s AND u.email_active = %s;

                """
                values = (3, 'Y')
                view_execute_query(query_view, values)


                values_job_details = []
                conditions = []

                if req_data['skills']:
                    if "Others" in req_data['skills']:
                        conditions.append("skill_name NOT IN %s")
                        values_job_details.append(tuple(skill_list),)
                    else:
                        conditions.append("skill_name IN %s")
                        values_job_details.append(tuple(skill_value),)
                
                # if req_data['functional_specification']:
                #     if "Others" in req_data['functional_specification']:
                #         conditions.append("p.functional_specification NOT IN %s")
                #         values_job_details.append(tuple(functional_specification_list),)
                #     else:
                #         conditions.append("p.functional_specification IN %s")
                #         values_job_details.append(tuple(req_data['functional_specification']),)
                if req_data["functional_specification"]:
                    raw_specs = req_data["functional_specification"]
                    specs = []
                    for item in raw_specs:
                        # Split comma-separated values like "Sales, Business Development"
                        parts = [s.strip() for s in item.split(",") if s.strip()]
                        specs.extend(parts)

                    # Remove duplicates if needed
                    specs = list(set(specs))
                    # If "Others" exists
                    if "Others" in specs:
                        # Exclude only provided specifications
                        exclude_specs = [s for s in specs if s != "Others"]

                        if exclude_specs: 
                            condition_str = " AND ".join([f"NOT FIND_IN_SET(%s, functional_specification)" for _ in exclude_specs])
                            conditions.append(f"({condition_str})")
                            values_job_details.extend(exclude_specs)

                    else:
                        # Match any of the provided specifications
                        condition_str = " OR ".join([f"FIND_IN_SET(%s, functional_specification)" for _ in specs])
                        conditions.append(f"({condition_str})")
                        values_job_details.extend(specs)

                if country:
                    conditions.append("country IN %s")
                    values_job_details.append(tuple(country),)
                
                if city:
                    conditions.append("city IN %s")
                    values_job_details.append(tuple(city),)
                # if country:
                #     if "Others" in country:
                #         conditions.append("u.country NOT IN %s")
                #         values_job_details.append(tuple(country_list),)
                #     else:
                #         conditions.append("u.country IN %s")
                #         values_job_details.append(tuple(country),)
                if 'profile_percentage' in req_data and req_data['profile_percentage']:
                    percentage_range = req_data['profile_percentage']
                    if percentage_range == "0-30":
                        conditions.append("profile_percentage BETWEEN 0 AND 30")
                    elif percentage_range == "30-60":
                        conditions.append("profile_percentage BETWEEN 30 AND 60")
                    elif percentage_range == "60-100":
                        conditions.append("profile_percentage BETWEEN 60 AND 100")
                    elif percentage_range == "30-100":
                        conditions.append("profile_percentage BETWEEN 30 AND 100")
                if req_data['plan']:
                    conditions.append("pricing_category IN %s")
                    values_job_details.append(tuple(req_data['plan'],))
                if req_data['gender']:
                    conditions.append("gender IN %s")
                    values_job_details.append(tuple(req_data['gender'],))
                if req_data['industry_sector']:
                    conditions.append("industry_sector IN %s")
                    values_job_details.append(tuple(req_data['industry_sector'],))
                if req_data['sector']:
                    conditions.append("sector IN %s")
                    values_job_details.append(tuple(req_data['sector'],))
                if req_data['job_type']:
                    conditions.append("job_type IN %s")
                    values_job_details.append(tuple(req_data['job_type'],))
                if req_data['willing_to_relocate']:
                    conditions.append("willing_to_relocate IN %s")
                    values_job_details.append(tuple(req_data['willing_to_relocate'],))
                if req_data['mode_of_communication']:
                    conditions.append("mode_of_communication IN %s")
                    values_job_details.append(tuple(req_data['mode_of_communication'],))
                if req_data['location_preference']:
                    conditions.append("location_preference IN %s")
                    values_job_details.append(tuple(req_data['location_preference'],))

                # if 'search_text' in req_data and req_data['search_text']:
                #     search_term = f"%{req_data['search_text']}%"
                #     search_conditions = """
                #         (first_name LIKE %s
                #         OR last_name LIKE %s
                #         OR email_id LIKE %s
                #         OR CONCAT(first_name, ' ', last_name) LIKE %s
                #         OR user_id LIKE %s
                #         OR city LIKE %s
                #         OR country LIKE %s
                #         OR about LIKE %s
                #         OR sector LIKE %s
                #         OR industry_sector LIKE %s
                #         OR functional_specification LIKE %s
                #         OR job_type LIKE %s
                #         OR location_preference LIKE %s
                #         OR mode_of_communication LIKE %s
                #         OR skill_name LIKE %s
                #         OR specialisation LIKE %s
                #         OR language_known LIKE %s
                #         OR description LIKE %s
                #         OR job_title LIKE %s
                #         OR job_description LIKE %s)
                #     """
                #     conditions.append(search_conditions)
                #     # Add the same search term for all placeholders
                #     values_job_details.extend([search_term] * 20)

                # where_clause = ""
                 # Search text filter
                if req_data.get('search_text'):
                    search_term = f"%{req_data['search_text']}%"
                    search_fields = [
                        "first_name", "last_name", "email_id", "CONCAT(first_name, ' ', last_name)", "user_id",
                        "city", "country", "about", "sector", "industry_sector", "functional_specification","pricing_category",
                        "job_type", "location_preference", "mode_of_communication", "skill_name", "specialisation",
                        "language_known", "description", "job_title", "job_description"
                    ]
                    search_conditions = " OR ".join([f"{field} LIKE %s" for field in search_fields])
                    conditions.append(f"({search_conditions})")
                    values_job_details.extend([search_term] * len(search_fields))

                # Combine all conditions
                where_clause = " AND ".join(conditions)
                if where_clause:
                    where_clause = " AND (" + where_clause + ")"

                query_job_details = f"""
                    SELECT *
                    FROM (
                        SELECT vd.*,
                            ROW_NUMBER() OVER (PARTITION BY vd.user_id ORDER BY vd.professional_id DESC) AS rn
                        FROM view_job_details vd
                        WHERE 1=1 {where_clause}
                    ) AS sub
                    WHERE sub.rn = 1
                    ORDER BY professional_id DESC
                    LIMIT 10 OFFSET %s
                """

                values_job_details.append(offset)
                count_query = f"""
                    SELECT COUNT(DISTINCT user_id) AS total_count
                    FROM view_job_details
                    WHERE 1=1 {where_clause}
                """
                count_values = values_job_details[:-1]
                count_result = execute_query(count_query, count_values)

                total_count = count_result[0]['total_count'] if count_result else 0
                prof_details = replace_empty_values(
                    execute_query(query_job_details, values_job_details)
                )

                # if conditions:
                #     if len(conditions) == 1:
                #         query_job_details += conditions[0]
                #     else:
                #         query_job_details += " (" + " AND ".join(conditions) + ")"
                #         # where_clause = " AND (" + " AND ".join(conditions) + ")"

                # query_job_details += " GROUP BY u.user_id, u.first_name, u.email_id, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, p.professional_id, u.gender, p.about, p.sector, p.industry_sector, p.functional_specification, p.job_type, p.location_preference, p.willing_to_relocate, p.mode_of_communication, p.expert_notes, p.created_at, le.job_title, le.created_at, u2.profile_image ORDER BY p.professional_id DESC "
                # new_query = query_job_details + " LIMIT 10 OFFSET %s"
                # val_detail = values_job_details
                # query = "SELECT subquery.user_id, COUNT(*) AS total_count FROM (" + query_job_details + ") AS subquery GROUP BY subquery.user_id"
                # values = (val_detail)
                # total_count = execute_query(query, values)
                # if len(total_count) > 0:
                #     total_count = len(total_count)
                # else:
                #     total_count = 0
                # # query_job_details += join_query
                # values_job_details.append(offset,)
                # prof_details = replace_empty_values(execute_query(new_query, values_job_details))
                # total_count = len(prof_details)
                if len(prof_details) > 0:
                    mod_id = '2C-PR-'+ str(prof_details[0]['professional_id'])
                    first_id = prof_details[0]['professional_id']
                    if isUserExist("professional_profile","professional_id",first_id):
                            query = """SELECT 
                                            u.first_name, 
                                            u.last_name, 
                                            u.email_id, 
                                            u.dob, 
                                            u.country_code, 
                                            u.contact_number, 
                                            u.country, 
                                            u.state, 
                                            u.city, 
                                            u.profile_percentage,
                                            u.pricing_category, 
                                            u.is_active,
                                            u.gender,
                                            u.payment_status,
                                            p.professional_id,
                                            p.professional_resume,
                                            p.expert_notes,
                                            p.about,
                                            p.preferences,
                                            p.video_url, 
                                            p.years_of_experience,
                                            p.functional_specification,
                                            p.sector, 
                                            p.industry_sector,
                                            p.job_type,
                                            p.location_preference,
                                            p.mode_of_communication,
                                            p.willing_to_relocate,
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
                                        FROM 
                                            users AS u 
                                        LEFT JOIN 
                                            professional_profile AS p ON u.user_id = p.professional_id 
                                        LEFT JOIN 
                                            professional_experience AS pe ON u.user_id = pe.professional_id 
                                        LEFT JOIN 
                                            professional_education AS ed ON u.user_id = ed.professional_id 
                                        LEFT JOIN 
                                            professional_skill AS ps ON u.user_id = ps.professional_id 
                                        LEFT JOIN 
                                            professional_language AS pl ON u.user_id = pl.professional_id 
                                        LEFT JOIN 
                                            professional_additional_info AS pai ON u.user_id = pai.professional_id 
                                        LEFT JOIN 
                                            professional_social_link AS psl ON u.user_id = psl.professional_id 
                                        LEFT JOIN 
                                            users AS u2 ON u.user_id = u2.user_id 
                                        WHERE 
                                            u.user_id = %s 
                                        ORDER BY 
                                            CASE 
                                                WHEN pe.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(pe.end_year, '0000-00') DESC,
                                            COALESCE(pe.end_month, '00') DESC,
                                            COALESCE(pe.start_year, '0000-00') DESC,
                                            COALESCE(pe.start_month, '00') DESC,
                                            CASE 
                                                WHEN ed.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(ed.end_year, '0000-00') DESC,
                                            COALESCE(ed.end_month, '00') DESC,
                                            COALESCE(ed.start_year, '0000-00') DESC,
                                            COALESCE(ed.start_month, '00') DESC;"""                
                            values = (first_id,)
                            profile_result = execute_query(query, values) 

                            if len(profile_result) > 0:                              
                                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                                intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                                resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                            else:
                                result_json = api_json_response_format(True,"No records found",0,{})     
                                return result_json          
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            if intro_video_name == '':
                                s3_video_key = ""
                            else:
                                s3_video_key = s3_intro_video_folder_name + str(intro_video_name)
                            if resume_name == '':    
                                s3_resume_key = ""
                            else:          
                                s3_resume_key = s3_resume_folder_name + str(resume_name)

                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (first_id, 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(ep.sector, su.sector) AS sector,
                                        COALESCE(ep.company_name, su.company_name) AS company_name,
                                        COALESCE(u.is_active, '') AS is_active
                                        FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            # for prof in prof_details:
                            #     query = 'select job_id from sc_recommendation where professional_id = %s'
                            #     values = (prof['professional_id'],)
                            #     recommended_jobs_id = execute_query(query, values)
                            #     recommended_jobs_list = []
                            #     if len(recommended_jobs_id) > 0:
                            #         for id in recommended_jobs_id:
                            #             if isUserExist("job_post", "id", id['job_id']):
                            #                 query = 'SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, ep.company_name, ep.sector FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id WHERE jp.id = %s'
                            #                 values = (id['job_id'],)
                            #                 detail = execute_query(query, values)
                            #                 txt = detail[0]['sector']
                            #                 txt = txt.replace(", ", "_")
                            #                 txt = txt.replace(" ", "_")
                            #                 sector_name = txt + ".png"
                            #                 detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                            #                 recommended_jobs_list.append(detail[0])
                            #     else:
                            #         recommended_jobs_list = []
                            #     prof.update({'recommended_jobs' : recommended_jobs_list})
                            profile_dict = {
                                'first_name': replace_empty_values1(profile_result[0]['first_name']),
                                'last_name': replace_empty_values1(profile_result[0]['last_name']),
                                'professional_id' : mod_id,                                        
                                'email_id': replace_empty_values1(profile_result[0]['email_id']),
                                'dob': replace_empty_values1(profile_result[0]['dob']),
                                'country_code': replace_empty_values1(profile_result[0]['country_code']),
                                'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                                'city': replace_empty_values1(profile_result[0]['city']),
                                'profile_percentage': replace_empty_values1(profile_result[0]['profile_percentage']),
                                'state': replace_empty_values1(profile_result[0]['state']),
                                'country': replace_empty_values1(profile_result[0]['country']),
                                'pricing_category' : profile_result[0]['pricing_category'],
                                'is_active' : profile_result[0]['is_active'],
                                'payment_status' : profile_result[0]['payment_status'],
                                'profile_image': s3_pic_key,
                                'video_name': s3_video_key,
                                'resume_name': s3_resume_key,
                                'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                                'about': replace_empty_values1(profile_result[0]['about']),
                                'preferences': replace_empty_values1(profile_result[0]['preferences']),
                                'experience': {},
                                'education': {},
                                'skills': {},
                                'languages': {},
                                'additional_info': {},
                                'social_link': {},
                                'job_list' : {},
                                'recommended_jobs' : recommended_jobs_list,
                                'gender' : replace_empty_values1(profile_result[0]['gender']),
                                'years_of_experience' : replace_empty_values1(profile_result[0]['years_of_experience']),
                                'functional_specification' : replace_empty_values1(profile_result[0]['functional_specification']),
                                'sector' : replace_empty_values1(profile_result[0]['sector']),
                                'industry_sector' : replace_empty_values1(profile_result[0]['industry_sector']),
                                'job_type' : replace_empty_values1(profile_result[0]['job_type']),
                                'location_preference' : replace_empty_values1(profile_result[0]['location_preference']),
                                'mode_of_communication' : replace_empty_values1(profile_result[0]['mode_of_communication']),
                                'willing_to_relocate' : replace_empty_values1(profile_result[0]['willing_to_relocate'])
                            }

                            # Grouping experience data
                            experience_set = set()
                            experience_list = []
                            for exp in profile_result:
                                if exp['experience_id'] is not None:
                                    # start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                                    # end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                                    if exp['experience_start_year'] != None:
                                        start_date = exp['experience_start_year']
                                    else:
                                        start_date = ''
                                    if exp['experience_end_year'] != None:
                                        end_date = exp['experience_end_year']
                                    else:
                                        end_date = ''
                                    exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                                    if exp_tuple not in experience_set:
                                        experience_set.add(exp_tuple)
                                        experience_list.append({
                                            'id': exp['experience_id'],
                                            'company_name': replace_empty_values1(exp['company_name']),
                                            'job_title': replace_empty_values1(exp['job_title']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,                                
                                            'job_description': replace_empty_values1(exp['job_description']),
                                            'job_location': replace_empty_values1(exp['job_location'])
                                        })

                            profile_dict['experience'] = experience_list

                            # Grouping education data
                            education_set = set()
                            education_list = []
                            for edu in profile_result:
                                if edu['education_id'] is not None:
                                    # start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                                    # end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                                    if edu['education_start_year'] != None:
                                        start_date = edu['education_start_year']
                                    else:
                                        start_date = ''
                                    if edu['education_end_year'] != None:
                                        end_date = edu['education_end_year']
                                    else:
                                        end_date = ''
                                    edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                                start_date, end_date, edu['institute_location'])
                                    if edu_tuple not in education_set:
                                        education_set.add(edu_tuple)
                                        education_list.append({
                                            'id': edu['education_id'],
                                            'institute_name': replace_empty_values1(edu['institute_name']),
                                            'degree_level': replace_empty_values1(edu['degree_level']),
                                            'specialisation': replace_empty_values1(edu['specialisation']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,
                                            'institute_location': replace_empty_values1(edu['institute_location'])
                                        })

                            profile_dict['education'] = education_list

                            # Grouping skills data
                            skills_set = set()
                            skills_list = []
                            for skill in profile_result:
                                if skill['skill_id'] is not None:
                                    skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                                    if skill_tuple not in skills_set:
                                        skills_set.add(skill_tuple)
                                        skills_list.append({
                                            'id': skill['skill_id'],
                                            'skill_name': replace_empty_values1(skill['skill_name']),
                                            'skill_level': replace_empty_values1(skill['skill_level'])
                                        })

                            profile_dict['skills'] = skills_list

                            # Grouping languages data
                            languages_set = set()
                            languages_list = []
                            for lang in profile_result:
                                if lang['language_id'] is not None:
                                    lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                                    if lang_tuple not in languages_set:
                                        languages_set.add(lang_tuple)
                                        languages_list.append({
                                            'language_known': replace_empty_values1(lang['language_known']),
                                            'id':  lang['language_id'],
                                            'language_level': replace_empty_values1(lang['language_level'])
                                    })

                            profile_dict['languages'] = languages_list
                            # Grouping additional info data
                            additional_info_set = set()
                            additional_info_list = []
                            for info in profile_result:
                                if info['additional_info_id'] is not None:
                                    info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                                    if info_tuple not in additional_info_set:
                                        additional_info_set.add(info_tuple)
                                        additional_info_list.append({
                                        'id': info['additional_info_id'],
                                        'title': replace_empty_values1(info['additional_info_title']),
                                        'description': replace_empty_values1(info['additional_info_description'])
                                    })

                            profile_dict['additional_info'] = additional_info_list
                            # Grouping social link data
                            social_link_set = set()
                            social_link_list = []
                            for link in profile_result:
                                if link['social_link_id'] is not None:
                                    link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                                    if link_tuple not in social_link_set:
                                        social_link_set.add(link_tuple)
                                        social_link_list.append({
                                        'title': replace_empty_values1(link['social_link_title']),
                                        'id':  link['social_link_id'],
                                        'url': replace_empty_values1(link['social_link_url'])
                                    })

                            profile_dict['social_link'] = social_link_list
                            
                            #Get recommended jobs for the professional
                            for prof in prof_details:
                                old_id = prof['professional_id']
                                temp_id = "2C-PR-" + str(prof['professional_id'])
                                prof.update({"professional_id" : temp_id})
                                query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                                values = (3,old_id,)
                                job_id_list = execute_query(query, values)
                                job_detail_list = []
                                if len(job_id_list) > 0:
                                    for job_id in job_id_list:
                                        id = job_id['job_id']
                                        if isUserExist("job_post", "id", id):
                                            query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city,
                                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                            COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                            COALESCE(ep.sector, su.sector) AS sector,
                                            COALESCE(ep.company_name, su.company_name) AS company_name,
                                            COALESCE(u.is_active, '') AS is_active
                                            FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                            values = (id,)
                                            detail = execute_query(query, values)
                                            if len(detail) > 0:
                                                txt = detail[0]['sector']
                                                txt = txt.replace(", ", "_")
                                                txt = txt.replace(" ", "_")
                                                sector_name = txt + ".png"
                                                detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                                img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                                detail[0].update({'profile_image' : img_key})
                                                job_detail_dict = {"job_title" : detail[0]['job_title'],
                                                                    "country" : detail[0]['country'],
                                                                    "city" : detail[0]['city'],
                                                                    "company_name" : detail[0]['company_name'],
                                                                    "sector_image" : detail[0]['sector_image'],
                                                                    "sector" : detail[0]['sector'],
                                                                    "profile_image" : detail[0]['profile_image'],
                                                                    "employer_id" : detail[0]['employer_id'],
                                                                    "job_id" : detail[0]['job_id'],
                                                                    "pricing_category" : detail[0]['pricing_category'],
                                                                    "is_active" : detail[0]['is_active'],
                                                                    "payment_status" : detail[0]['payment_status']}
                                                job_detail_list.append(job_detail_dict)
                                else:
                                    job_detail_list = []
                                s3_prof_pic_key = s3_picture_folder_name + prof['profile_image']
                                prof.update({'profile_image' : s3_prof_pic_key})
                                prof.update({'recommended_jobs' : job_detail_list})
                            profile_dict['professional_details'] = prof_details
                            profile_dict.update({'total_count' : total_count})
                            result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)                    
                    else:
                        result_json = api_json_response_format(False,"User profile not found",500,{})
                else:
                    result_json = api_json_response_format(True,"No records found",0,{})
                # else:
                #     result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving the data. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json    

#def admin_professional_filter_results():
    try:
        result_json = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                page_number = req_data["page_number"]
                offset = (page_number - 1) * 10
                location = req_data["location"]
                skill_value = req_data["skills"]
                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                skill_list = ["Agile Methodologies", "Algorithm Design", "Analytics", "Application Programming Interfaces (APIs)", "Budgeting", "Business Strategy", "Change Management", "Conflict Resolution", "Contract Management Skills", "Data Analysis", "Database Design", "Debugging", "Direct Sales", "Earned Value Management", "Financial Management", "Human Resource Management", "Keyword Research", "Leadership Skills", "Market Research", "Marketing Skills", "Metrics and KPIs", "Mobile Application Development", "Negotiation", "Operations Management", "Organizational Development", "Presentation", "Process Improvement", "Product Knowledge", "Project Management", "Quality Assurance (QA)", "Recruiting", "Revenue Expansion", "Risk Assessment", "SaaS Knowledge", "Sales and Budget Forecasting", "Salesforce", "Strategic Planning", "Supply Chain Management", "Talent Management", "Team Leadership", "Upselling"]
                # industry_sector_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # sector_list = ["Academic", "Corporate", "Non-profit", "Startup"]
                functional_specification_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite and Board"]
                query_job_details = """
                            WITH LatestExperience AS (
                                    SELECT
                                        pe.professional_id,
                                        pe.id AS experience_id,
                                        pe.start_month,
                                        pe.start_year,
                                        pe.job_title,
                                        pe.job_description,
                                        pe.created_at,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY pe.professional_id
                                            ORDER BY
                                                COALESCE(pe.start_year, '0000-00') DESC,
                                                COALESCE(pe.start_month, '00') DESC,
                                                pe.created_at DESC
                                        ) AS rn
                                    FROM
                                        professional_experience AS pe
                                ),
                                SearchableExperience AS (
                                    SELECT
                                        le.professional_id,
                                        le.job_title,
                                        le.job_description
                                    FROM
                                        LatestExperience AS le
                                    WHERE
                                        le.rn <= 3
                                )
                                SELECT
                                    u.user_id,
                                    u.first_name,
                                    u.last_name,
                                    u.email_id,
                                    u.country,
                                    u.state,
                                    u.city,
                                    u.profile_percentage,
                                    u.pricing_category,
                                    u.is_active,
                                    u.payment_status,
                                    u.gender,
                                    p.professional_id,
                                    p.about,
                                    p.sector,
                                    p.industry_sector,
                                    p.functional_specification,
                                    p.job_type,
                                    p.location_preference,
                                    p.willing_to_relocate,
                                    p.mode_of_communication,
                                    p.expert_notes,
                                    p.created_at AS posted_at,
                                    COALESCE(le.job_title, '') AS job_title,
                                    COALESCE(le.created_at, '') AS experience_created_at,
                                    u2.profile_image
                                FROM
                                    users AS u
                                LEFT JOIN
                                    professional_profile AS p ON u.user_id = p.professional_id
                                LEFT JOIN
                                    professional_education AS ped ON u.user_id = ped.professional_id
                                LEFT JOIN
                                    professional_additional_info AS pai ON u.user_id = pai.professional_id
                                LEFT JOIN
                                    professional_language AS pl ON u.user_id = pl.professional_id
                                LEFT JOIN
                                    LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                                LEFT JOIN
                                    users AS u2 ON u.user_id = u2.user_id
                                LEFT JOIN 
                                    professional_skill AS ps ON u.user_id = ps.professional_id 
                                LEFT JOIN
                                    SearchableExperience AS se ON p.professional_id = se.professional_id
                                WHERE
                                    u.user_role_fk = 3 AND u.email_active = 'Y' AND
                """        
                values_job_details = []
                conditions = []

                if req_data['skills']:
                    if "Others" in req_data['skills']:
                        conditions.append("ps.skill_name NOT IN %s")
                        values_job_details.append(tuple(skill_list),)
                    else:
                        conditions.append("ps.skill_name IN %s")
                        values_job_details.append(tuple(skill_value),)
                
                # if req_data['functional_specification']:
                #     if "Others" in req_data['functional_specification']:
                #         conditions.append("p.functional_specification NOT IN %s")
                #         values_job_details.append(tuple(functional_specification_list),)
                #     else:
                #         conditions.append("p.functional_specification IN %s")
                #         values_job_details.append(tuple(req_data['functional_specification']),)
                if req_data["functional_specification"]:
                    specs = req_data["functional_specification"]

                    # If "Others" exists
                    if "Others" in specs:
                        # Exclude only provided specifications
                        exclude_specs = [s for s in specs if s != "Others"]

                        if exclude_specs: 
                            condition_str = " AND ".join([f"NOT FIND_IN_SET(%s, functional_specification)" for _ in exclude_specs])
                            conditions.append(f"({condition_str})")
                            values_job_details.extend(exclude_specs)

                    else:
                        # Match any of the provided specifications
                        condition_str = " OR ".join([f"FIND_IN_SET(%s, functional_specification)" for _ in specs])
                        conditions.append(f"({condition_str})")
                        values_job_details.extend(specs)

                if country:
                    conditions.append("u.country IN %s")
                    values_job_details.append(tuple(country),)
                
                if city:
                    conditions.append("u.city IN %s")
                    values_job_details.append(tuple(city),)
                # if country:
                #     if "Others" in country:
                #         conditions.append("u.country NOT IN %s")
                #         values_job_details.append(tuple(country_list),)
                #     else:
                #         conditions.append("u.country IN %s")
                #         values_job_details.append(tuple(country),)
                if 'profile_percentage' in req_data and req_data['profile_percentage']:
                    percentage_range = req_data['profile_percentage']
                    if percentage_range == "0-30":
                        conditions.append("u.profile_percentage BETWEEN 0 AND 30")
                    elif percentage_range == "30-60":
                        conditions.append("u.profile_percentage BETWEEN 30 AND 60")
                    elif percentage_range == "60-100":
                        conditions.append("u.profile_percentage BETWEEN 60 AND 100")
                    elif percentage_range == "30-100":
                        conditions.append("u.profile_percentage BETWEEN 30 AND 100")
                if req_data['plan']:
                    conditions.append("u.pricing_category IN %s")
                    values_job_details.append(tuple(req_data['plan'],))
                if req_data['gender']:
                    conditions.append("u.gender IN %s")
                    values_job_details.append(tuple(req_data['gender'],))
                if req_data['industry_sector']:
                    conditions.append("p.industry_sector IN %s")
                    values_job_details.append(tuple(req_data['industry_sector'],))
                if req_data['sector']:
                    conditions.append("p.sector IN %s")
                    values_job_details.append(tuple(req_data['sector'],))
                if req_data['job_type']:
                    conditions.append("p.job_type IN %s")
                    values_job_details.append(tuple(req_data['job_type'],))
                if req_data['willing_to_relocate']:
                    conditions.append("p.willing_to_relocate IN %s")
                    values_job_details.append(tuple(req_data['willing_to_relocate'],))
                if req_data['mode_of_communication']:
                    conditions.append("p.mode_of_communication IN %s")
                    values_job_details.append(tuple(req_data['mode_of_communication'],))
                if req_data['location_preference']:
                    conditions.append("p.location_preference IN %s")
                    values_job_details.append(tuple(req_data['location_preference'],))
                
                if 'search_text' in req_data and req_data['search_text']:
                    search_term = f"%{req_data['search_text']}%"
                    search_conditions = """
                        (u.first_name LIKE %s
                        OR u.last_name LIKE %s
                        OR u.email_id LIKE %s
                        OR CONCAT(u.first_name, ' ', u.last_name) LIKE %s
                        OR u.user_id LIKE %s
                        OR u.city LIKE %s
                        OR u.country LIKE %s
                        OR p.about LIKE %s
                        OR p.sector LIKE %s
                        OR p.industry_sector LIKE %s
                        OR p.functional_specification LIKE %s
                        OR p.job_type LIKE %s
                        OR p.location_preference LIKE %s
                        OR p.mode_of_communication LIKE %s
                        OR ps.skill_name LIKE %s
                        OR ped.specialisation LIKE %s
                        OR pl.language_known LIKE %s
                        OR pai.description LIKE %s
                        OR se.job_title LIKE %s
                        OR se.job_description LIKE %s)
                    """
                    conditions.append(search_conditions)
                    # Add the same search term for all placeholders
                    values_job_details.extend([search_term] * 20)

                if conditions:
                    if len(conditions) == 1:
                        query_job_details += conditions[0]
                    else:
                        query_job_details += " (" + " AND ".join(conditions) + ")"
                query_job_details += " GROUP BY u.user_id, u.first_name, u.email_id, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, p.professional_id, u.gender, p.about, p.sector, p.industry_sector, p.functional_specification, p.job_type, p.location_preference, p.willing_to_relocate, p.mode_of_communication, p.expert_notes, p.created_at, le.job_title, le.created_at, u2.profile_image ORDER BY p.professional_id DESC "
                new_query = query_job_details + " LIMIT 10 OFFSET %s"
                val_detail = values_job_details
                query = "SELECT subquery.user_id, COUNT(*) AS total_count FROM (" + query_job_details + ") AS subquery GROUP BY subquery.user_id"
                values = (val_detail)
                total_count = execute_query(query, values)
                if len(total_count) > 0:
                    total_count = len(total_count)
                else:
                    total_count = 0
                # query_job_details += join_query
                values_job_details.append(offset,)
                prof_details = replace_empty_values(execute_query(new_query, values_job_details))
                # total_count = len(prof_details)
                if len(prof_details) > 0:
                    mod_id = '2C-PR-'+ str(prof_details[0]['professional_id'])
                    first_id = prof_details[0]['professional_id']
                    if isUserExist("professional_profile","professional_id",first_id):
                            query = """SELECT 
                                            u.first_name, 
                                            u.last_name, 
                                            u.email_id, 
                                            u.dob, 
                                            u.country_code, 
                                            u.contact_number, 
                                            u.country, 
                                            u.state, 
                                            u.city, 
                                            u.profile_percentage,
                                            u.pricing_category, 
                                            u.is_active,
                                            u.gender,
                                            u.payment_status,
                                            p.professional_id,
                                            p.professional_resume,
                                            p.expert_notes,
                                            p.about,
                                            p.preferences,
                                            p.video_url, 
                                            p.years_of_experience,
                                            p.functional_specification,
                                            p.sector, 
                                            p.industry_sector,
                                            p.job_type,
                                            p.location_preference,
                                            p.mode_of_communication,
                                            p.willing_to_relocate,
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
                                        FROM 
                                            users AS u 
                                        LEFT JOIN 
                                            professional_profile AS p ON u.user_id = p.professional_id 
                                        LEFT JOIN 
                                            professional_experience AS pe ON u.user_id = pe.professional_id 
                                        LEFT JOIN 
                                            professional_education AS ed ON u.user_id = ed.professional_id 
                                        LEFT JOIN 
                                            professional_skill AS ps ON u.user_id = ps.professional_id 
                                        LEFT JOIN 
                                            professional_language AS pl ON u.user_id = pl.professional_id 
                                        LEFT JOIN 
                                            professional_additional_info AS pai ON u.user_id = pai.professional_id 
                                        LEFT JOIN 
                                            professional_social_link AS psl ON u.user_id = psl.professional_id 
                                        LEFT JOIN 
                                            users AS u2 ON u.user_id = u2.user_id 
                                        WHERE 
                                            u.user_id = %s 
                                        ORDER BY 
                                            CASE 
                                                WHEN pe.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(pe.end_year, '0000-00') DESC,
                                            COALESCE(pe.end_month, '00') DESC,
                                            COALESCE(pe.start_year, '0000-00') DESC,
                                            COALESCE(pe.start_month, '00') DESC,
                                            CASE 
                                                WHEN ed.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(ed.end_year, '0000-00') DESC,
                                            COALESCE(ed.end_month, '00') DESC,
                                            COALESCE(ed.start_year, '0000-00') DESC,
                                            COALESCE(ed.start_month, '00') DESC;"""                
                            values = (first_id,)
                            profile_result = execute_query(query, values) 

                            if len(profile_result) > 0:                              
                                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                                intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                                resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                            else:
                                result_json = api_json_response_format(True,"No records found",0,{})     
                                return result_json          
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            if intro_video_name == '':
                                s3_video_key = ""
                            else:
                                s3_video_key = s3_intro_video_folder_name + str(intro_video_name)
                            if resume_name == '':    
                                s3_resume_key = ""
                            else:          
                                s3_resume_key = s3_resume_folder_name + str(resume_name)

                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (first_id, 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(ep.sector, su.sector) AS sector,
                                        COALESCE(ep.company_name, su.company_name) AS company_name,
                                        COALESCE(u.is_active, '') AS is_active
                                        FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            # for prof in prof_details:
                            #     query = 'select job_id from sc_recommendation where professional_id = %s'
                            #     values = (prof['professional_id'],)
                            #     recommended_jobs_id = execute_query(query, values)
                            #     recommended_jobs_list = []
                            #     if len(recommended_jobs_id) > 0:
                            #         for id in recommended_jobs_id:
                            #             if isUserExist("job_post", "id", id['job_id']):
                            #                 query = 'SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, ep.company_name, ep.sector FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id WHERE jp.id = %s'
                            #                 values = (id['job_id'],)
                            #                 detail = execute_query(query, values)
                            #                 txt = detail[0]['sector']
                            #                 txt = txt.replace(", ", "_")
                            #                 txt = txt.replace(" ", "_")
                            #                 sector_name = txt + ".png"
                            #                 detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                            #                 recommended_jobs_list.append(detail[0])
                            #     else:
                            #         recommended_jobs_list = []
                            #     prof.update({'recommended_jobs' : recommended_jobs_list})
                            profile_dict = {
                                'first_name': replace_empty_values1(profile_result[0]['first_name']),
                                'last_name': replace_empty_values1(profile_result[0]['last_name']),
                                'professional_id' : mod_id,                                        
                                'email_id': replace_empty_values1(profile_result[0]['email_id']),
                                'dob': replace_empty_values1(profile_result[0]['dob']),
                                'country_code': replace_empty_values1(profile_result[0]['country_code']),
                                'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                                'city': replace_empty_values1(profile_result[0]['city']),
                                'profile_percentage': replace_empty_values1(profile_result[0]['profile_percentage']),
                                'state': replace_empty_values1(profile_result[0]['state']),
                                'country': replace_empty_values1(profile_result[0]['country']),
                                'pricing_category' : profile_result[0]['pricing_category'],
                                'is_active' : profile_result[0]['is_active'],
                                'payment_status' : profile_result[0]['payment_status'],
                                'profile_image': s3_pic_key,
                                'video_name': s3_video_key,
                                'resume_name': s3_resume_key,
                                'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                                'about': replace_empty_values1(profile_result[0]['about']),
                                'preferences': replace_empty_values1(profile_result[0]['preferences']),
                                'experience': {},
                                'education': {},
                                'skills': {},
                                'languages': {},
                                'additional_info': {},
                                'social_link': {},
                                'job_list' : {},
                                'recommended_jobs' : recommended_jobs_list,
                                'gender' : replace_empty_values1(profile_result[0]['gender']),
                                'years_of_experience' : replace_empty_values1(profile_result[0]['years_of_experience']),
                                'functional_specification' : replace_empty_values1(profile_result[0]['functional_specification']),
                                'sector' : replace_empty_values1(profile_result[0]['sector']),
                                'industry_sector' : replace_empty_values1(profile_result[0]['industry_sector']),
                                'job_type' : replace_empty_values1(profile_result[0]['job_type']),
                                'location_preference' : replace_empty_values1(profile_result[0]['location_preference']),
                                'mode_of_communication' : replace_empty_values1(profile_result[0]['mode_of_communication']),
                                'willing_to_relocate' : replace_empty_values1(profile_result[0]['willing_to_relocate'])
                            }

                            # Grouping experience data
                            experience_set = set()
                            experience_list = []
                            for exp in profile_result:
                                if exp['experience_id'] is not None:
                                    # start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                                    # end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                                    if exp['experience_start_year'] != None:
                                        start_date = exp['experience_start_year']
                                    else:
                                        start_date = ''
                                    if exp['experience_end_year'] != None:
                                        end_date = exp['experience_end_year']
                                    else:
                                        end_date = ''
                                    exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                                    if exp_tuple not in experience_set:
                                        experience_set.add(exp_tuple)
                                        experience_list.append({
                                            'id': exp['experience_id'],
                                            'company_name': replace_empty_values1(exp['company_name']),
                                            'job_title': replace_empty_values1(exp['job_title']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,                                
                                            'job_description': replace_empty_values1(exp['job_description']),
                                            'job_location': replace_empty_values1(exp['job_location'])
                                        })

                            profile_dict['experience'] = experience_list

                            # Grouping education data
                            education_set = set()
                            education_list = []
                            for edu in profile_result:
                                if edu['education_id'] is not None:
                                    # start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                                    # end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                                    if edu['education_start_year'] != None:
                                        start_date = edu['education_start_year']
                                    else:
                                        start_date = ''
                                    if edu['education_end_year'] != None:
                                        end_date = edu['education_end_year']
                                    else:
                                        end_date = ''
                                    edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                                start_date, end_date, edu['institute_location'])
                                    if edu_tuple not in education_set:
                                        education_set.add(edu_tuple)
                                        education_list.append({
                                            'id': edu['education_id'],
                                            'institute_name': replace_empty_values1(edu['institute_name']),
                                            'degree_level': replace_empty_values1(edu['degree_level']),
                                            'specialisation': replace_empty_values1(edu['specialisation']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,
                                            'institute_location': replace_empty_values1(edu['institute_location'])
                                        })

                            profile_dict['education'] = education_list

                            # Grouping skills data
                            skills_set = set()
                            skills_list = []
                            for skill in profile_result:
                                if skill['skill_id'] is not None:
                                    skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                                    if skill_tuple not in skills_set:
                                        skills_set.add(skill_tuple)
                                        skills_list.append({
                                            'id': skill['skill_id'],
                                            'skill_name': replace_empty_values1(skill['skill_name']),
                                            'skill_level': replace_empty_values1(skill['skill_level'])
                                        })

                            profile_dict['skills'] = skills_list

                            # Grouping languages data
                            languages_set = set()
                            languages_list = []
                            for lang in profile_result:
                                if lang['language_id'] is not None:
                                    lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                                    if lang_tuple not in languages_set:
                                        languages_set.add(lang_tuple)
                                        languages_list.append({
                                            'language_known': replace_empty_values1(lang['language_known']),
                                            'id':  lang['language_id'],
                                            'language_level': replace_empty_values1(lang['language_level'])
                                    })

                            profile_dict['languages'] = languages_list
                            # Grouping additional info data
                            additional_info_set = set()
                            additional_info_list = []
                            for info in profile_result:
                                if info['additional_info_id'] is not None:
                                    info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                                    if info_tuple not in additional_info_set:
                                        additional_info_set.add(info_tuple)
                                        additional_info_list.append({
                                        'id': info['additional_info_id'],
                                        'title': replace_empty_values1(info['additional_info_title']),
                                        'description': replace_empty_values1(info['additional_info_description'])
                                    })

                            profile_dict['additional_info'] = additional_info_list
                            # Grouping social link data
                            social_link_set = set()
                            social_link_list = []
                            for link in profile_result:
                                if link['social_link_id'] is not None:
                                    link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                                    if link_tuple not in social_link_set:
                                        social_link_set.add(link_tuple)
                                        social_link_list.append({
                                        'title': replace_empty_values1(link['social_link_title']),
                                        'id':  link['social_link_id'],
                                        'url': replace_empty_values1(link['social_link_url'])
                                    })

                            profile_dict['social_link'] = social_link_list
                            
                            #Get recommended jobs for the professional
                            for prof in prof_details:
                                old_id = prof['professional_id']
                                temp_id = "2C-PR-" + str(prof['professional_id'])
                                prof.update({"professional_id" : temp_id})
                                query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                                values = (3,old_id,)
                                job_id_list = execute_query(query, values)
                                job_detail_list = []
                                if len(job_id_list) > 0:
                                    for job_id in job_id_list:
                                        id = job_id['job_id']
                                        if isUserExist("job_post", "id", id):
                                            query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city,
                                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                            COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                            COALESCE(ep.sector, su.sector) AS sector,
                                            COALESCE(ep.company_name, su.company_name) AS company_name,
                                            COALESCE(u.is_active, '') AS is_active
                                            FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                            values = (id,)
                                            detail = execute_query(query, values)
                                            if len(detail) > 0:
                                                txt = detail[0]['sector']
                                                txt = txt.replace(", ", "_")
                                                txt = txt.replace(" ", "_")
                                                sector_name = txt + ".png"
                                                detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                                img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                                detail[0].update({'profile_image' : img_key})
                                                job_detail_dict = {"job_title" : detail[0]['job_title'],
                                                                    "country" : detail[0]['country'],
                                                                    "city" : detail[0]['city'],
                                                                    "company_name" : detail[0]['company_name'],
                                                                    "sector_image" : detail[0]['sector_image'],
                                                                    "sector" : detail[0]['sector'],
                                                                    "profile_image" : detail[0]['profile_image'],
                                                                    "employer_id" : detail[0]['employer_id'],
                                                                    "job_id" : detail[0]['job_id'],
                                                                    "pricing_category" : detail[0]['pricing_category'],
                                                                    "is_active" : detail[0]['is_active'],
                                                                    "payment_status" : detail[0]['payment_status']}
                                                job_detail_list.append(job_detail_dict)
                                else:
                                    job_detail_list = []
                                s3_prof_pic_key = s3_picture_folder_name + prof['profile_image']
                                prof.update({'profile_image' : s3_prof_pic_key})
                                prof.update({'recommended_jobs' : job_detail_list})
                            profile_dict['professional_details'] = prof_details
                            profile_dict.update({'total_count' : total_count})
                            result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)                    
                    else:
                        result_json = api_json_response_format(False,"User profile not found",500,{})
                else:
                    result_json = api_json_response_format(True,"No records found",0,{})
                # else:
                #     result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving the data. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
# def admin_professional_filter_results_1():
#     try:
#         result_json = {}
#         country = []
#         city = []
#         token_result = get_user_token(request)
#         if token_result["status_code"] == 200:
#             user_data = get_user_data(token_result["email_id"])
#             if user_data["user_role"] == "admin":
#                 req_data = request.get_json()
#                 if 'page_number' not in req_data:
#                     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})
#                     return result_json
#                 page_number = req_data["page_number"]
#                 offset = (page_number - 1) * 10
#                 location = req_data["location"]
#                 skill_value = req_data["skills"]
#                 if len(location) != 0:
#                     for i in range(len(location)):
#                         res = location[i].split("&amp;&amp;&amp;&amp;&amp;")
#                         country.append(res[1])
#                         city.append(res[0])

#                 skill_list = ["Agile Methodologies", ...] # your list
#                 functional_specification_list = ["Sales &amp; Marketing", ...] # your list

#                 # This is the optimised query as per previous suggestions:
#                 optimized_query_job_details = """
#                                                 WITH LatestExperience AS (
#                                                     SELECT
#                                                         pe.professional_id,
#                                                         pe.id AS experience_id,
#                                                         pe.start_month,
#                                                         pe.start_year,
#                                                         pe.job_title,
#                                                         pe.job_description,
#                                                         pe.created_at,
#                                                         ROW_NUMBER() OVER (
#                                                             PARTITION BY pe.professional_id
#                                                             ORDER BY
#                                                                 COALESCE(pe.start_year, 0) DESC,
#                                                                 COALESCE(pe.start_month, 0) DESC,
#                                                                 pe.created_at DESC
#                                                         ) AS rn
#                                                     FROM professional_experience AS pe
#                                                 ),
#                                                 SearchableExperience AS (
#                                                     SELECT
#                                                         le.professional_id,
#                                                         le.job_title,
#                                                         le.job_description
#                                                     FROM LatestExperience AS le
#                                                     WHERE le.rn <= 3
#                                                 )
#                                                 SELECT
#                                                     u.user_id,
#                                                     u.first_name,
#                                                     u.last_name,
#                                                     u.email_id,
#                                                     u.country,
#                                                     u.state,
#                                                     u.city,
#                                                     u.profile_percentage,
#                                                     u.pricing_category,
#                                                     u.is_active,
#                                                     u.payment_status,
#                                                     u.gender,
#                                                     p.professional_id,
#                                                     p.about,
#                                                     p.sector,
#                                                     p.industry_sector,
#                                                     p.functional_specification,
#                                                     p.job_type,
#                                                     p.location_preference,
#                                                     p.willing_to_relocate,
#                                                     p.mode_of_communication,
#                                                     p.expert_notes,
#                                                     p.created_at AS posted_at,
#                                                     COALESCE(le.job_title, '') AS job_title,
#                                                     COALESCE(le.created_at, '') AS experience_created_at,
#                                                     u2.profile_image
#                                                 FROM users AS u
#                                                 LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id
#                                                 LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
#                                                 LEFT JOIN users AS u2 ON u.user_id = u2.user_id
#                                                 WHERE
#                                                     u.user_role_fk = 3 AND u.email_active = 'Y'
#                                                 """

#                 values_job_details = []
#                 conditions = []

#                 # Skills filter
#                 if req_data['skills']:
#                     if "Others" in req_data['skills']:
#                         conditions.append("""NOT EXISTS (SELECT 1 FROM professional_skill ps WHERE ps.professional_id = p.professional_id AND ps.skill_name IN %s)""")
#                         values_job_details.append(tuple(skill_list))
#                     else:
#                         conditions.append("""EXISTS (SELECT 1 FROM professional_skill ps WHERE ps.professional_id = p.professional_id AND ps.skill_name IN %s)""")
#                         values_job_details.append(tuple(skill_value))

#                 # Functional specification filter
#                 if req_data['functional_specification']:
#                     if "Others" in req_data['functional_specification']:
#                         conditions.append("p.functional_specification NOT IN %s")
#                         values_job_details.append(tuple(functional_specification_list))
#                     else:
#                         conditions.append("p.functional_specification IN %s")
#                         values_job_details.append(tuple(req_data['functional_specification']))

#                 # Location Filters
#                 if country:
#                     conditions.append("u.country IN %s")
#                     values_job_details.append(tuple(country))
#                 if city:
#                     conditions.append("u.city IN %s")
#                     values_job_details.append(tuple(city))

#                 # Profile percentage filter
#                 if 'profile_percentage' in req_data and req_data['profile_percentage']:
#                     percentage_range = req_data['profile_percentage']
#                     if percentage_range == "0-30":
#                         conditions.append("u.profile_percentage BETWEEN 0 AND 30")
#                     elif percentage_range == "30-60":
#                         conditions.append("u.profile_percentage BETWEEN 30 AND 60")
#                     elif percentage_range == "60-100":
#                         conditions.append("u.profile_percentage BETWEEN 60 AND 100")
#                     elif percentage_range == "30-100":
#                         conditions.append("u.profile_percentage BETWEEN 30 AND 100")

#                 # Other multi-select filters
#                 if req_data['plan']:
#                     conditions.append("u.pricing_category IN %s")
#                     values_job_details.append(tuple(req_data['plan']))
#                 if req_data['gender']:
#                     conditions.append("u.gender IN %s")
#                     values_job_details.append(tuple(req_data['gender']))
#                 if req_data['industry_sector']:
#                     conditions.append("p.industry_sector IN %s")
#                     values_job_details.append(tuple(req_data['industry_sector']))
#                 if req_data['sector']:
#                     conditions.append("p.sector IN %s")
#                     values_job_details.append(tuple(req_data['sector']))
#                 if req_data['job_type']:
#                     conditions.append("p.job_type IN %s")
#                     values_job_details.append(tuple(req_data['job_type']))
#                 if req_data['willing_to_relocate']:
#                     conditions.append("p.willing_to_relocate IN %s")
#                     values_job_details.append(tuple(req_data['willing_to_relocate']))
#                 if req_data['mode_of_communication']:
#                     conditions.append("p.mode_of_communication IN %s")
#                     values_job_details.append(tuple(req_data['mode_of_communication']))
#                 if req_data['location_preference']:
#                     conditions.append("p.location_preference IN %s")
#                     values_job_details.append(tuple(req_data['location_preference']))

#                 # Search text (optimized as exists for sub-tables)
#                 if 'search_text' in req_data and req_data['search_text']:
#                     search_term = f"%{req_data['search_text']}%"
#                     search_conditions = """
#                     (
#                         u.first_name LIKE %s
#                         OR u.last_name LIKE %s
#                         OR u.email_id LIKE %s
#                         OR CONCAT(u.first_name, ' ', u.last_name) LIKE %s
#                         OR u.user_id LIKE %s
#                         OR u.city LIKE %s
#                         OR u.country LIKE %s
#                         OR p.about LIKE %s
#                         OR p.sector LIKE %s
#                         OR p.industry_sector LIKE %s
#                         OR p.functional_specification LIKE %s
#                         OR p.job_type LIKE %s
#                         OR p.location_preference LIKE %s
#                         OR p.mode_of_communication LIKE %s
#                         OR EXISTS (SELECT 1 FROM professional_skill ps WHERE ps.professional_id = p.professional_id AND ps.skill_name LIKE %s)
#                         OR EXISTS (SELECT 1 FROM professional_education ped WHERE ped.professional_id = p.professional_id AND ped.specialisation LIKE %s)
#                         OR EXISTS (SELECT 1 FROM professional_language pl WHERE pl.professional_id = p.professional_id AND pl.language_known LIKE %s)
#                         OR EXISTS (SELECT 1 FROM professional_additional_info pai WHERE pai.professional_id = p.professional_id AND pai.description LIKE %s)
#                         OR EXISTS (SELECT 1 FROM SearchableExperience se WHERE se.professional_id = p.professional_id AND (se.job_title LIKE %s OR se.job_description LIKE %s))
#                     )
#                     """
#                     conditions.append(search_conditions)
#                     values_job_details.extend([search_term] * 20)
                
#                 # Add conditions to the query
#                 if conditions:
#                     optimized_query_job_details += " AND " + " AND ".join(conditions)

#                 optimized_query_job_details += " GROUP BY u.user_id, u.first_name, u.email_id, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, p.professional_id, u.gender, p.about, p.sector, p.industry_sector, p.functional_specification, p.job_type, p.location_preference, p.willing_to_relocate, p.mode_of_communication, p.expert_notes, p.created_at, le.job_title, le.created_at, u2.profile_image ORDER BY p.professional_id DESC"

#                 new_query = optimized_query_job_details + " LIMIT 10 OFFSET %s"
#                 values_job_details.append(offset)

#                 count_query = "SELECT COUNT(*) as total_count FROM (" + optimized_query_job_details + ") as subquery"
#                 total_count_result = execute_query(count_query, tuple(values_job_details[:-1]))  # Exclude offset for count
#                 total_count = total_count_result[0]['total_count'] if total_count_result and 'total_count' in total_count_result[0] else 0

#                 prof_details = replace_empty_values(execute_query(new_query, tuple(values_job_details)))
#                 # Remaining logic unchanged; you can continue your data packing...
#                 # ... everything below remains just as in your existing code:
#                 # (profile fetching, constructing profile_dict, groupings, job recommendations, etc.)

#                 if len(prof_details) > 0:
#                     # Use your existing logic for profile retrieval and result assembly!
#                     # (Refer to your code in the attachment or earlier message)
#                     # ...
#                     # Example (minimal):
#                     profile_dict = {}  # Fill as per your code logic
#                     profile_dict['professional_details'] = prof_details
#                     profile_dict.update({'total_count': total_count})
#                     result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile_dict)
#                 else:
#                     result_json = api_json_response_format(True, "No records found", 0, {})
#             else:
#                 result_json = api_json_response_format(False, "Unauthorized user", 401, {})
#         else:
#             result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
#     except Exception as error:
#         print(error)
#         result_json = api_json_response_format(False, str(error), 500, {})
#     finally:
#         return result_json


def admin_employer_dashboard():
    try:
        result_json = {}
        key = ''
        param = ''
        req_data = request.get_json()
        if 'employer_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            return result_json
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        if 'key' in req_data:
            key = req_data['key']
        if 'param' in req_data:
            param = req_data['param']
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                employer_id = req_data['employer_id']
                page_number = req_data['page_number']
                offset = (page_number - 1) * 10
                total_query = "SELECT COUNT(*) AS total_count FROM employer_profile ep JOIN users u ON ep.employer_id = u.user_id WHERE u.email_active = 'Y' AND u.user_role_fk = 2"
                values = ()
                total_count = execute_query(total_query,values)
                if len(total_count) > 0:
                    total_count = total_count[0]['total_count']
                else:
                    total_count = 0
                filter_parameters = fetch_employer_filter_params()

                query = "SELECT user_id from users where user_role_fk = %s and user_id > %s and email_active = 'Y'"
                values = (2, employer_id,)
                employer_ids = execute_query(query, values)
                user_ids = [item['user_id'] for item in employer_ids]
                details_list = [{"employee_short_desc" : [],
                                 "first_employee_details" : []
                                 }] 
                if len(user_ids) > 0:
                    if key == 'sort':
                        if param == 'by_date':
                            query = "SELECT u.user_id, u.email_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, u.created_at, e.designation, e.company_name, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id IN %s AND e.employer_id IN %s ORDER BY u.created_at ASC LIMIT 10 OFFSET %s"
                        elif param == 'asc':
                            query = "SELECT u.user_id, u.email_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id IN %s AND e.employer_id IN %s ORDER BY e.company_name ASC LIMIT 10 OFFSET %s"
                        else:
                            query = "SELECT u.user_id, u.email_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id IN %s AND e.employer_id IN %s ORDER BY e.company_name DESC LIMIT 10 OFFSET %s"
                    else:
                        query = "SELECT u.user_id, u.email_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id IN %s AND e.employer_id IN %s ORDER BY u.user_id DESC LIMIT 10 OFFSET %s"
                    values = (tuple(user_ids), tuple(user_ids), offset,)
                    short_desc = execute_query(query, values)
                else:
                    filter_params = [{"filter_parameters" : filter_parameters,"total_count" : 0,"employee_short_desc" : [],"first_employee_details" : {}}]
                    result_json = api_json_response_format(True,"No employer profile found",0,filter_params)
                    return result_json
                if len(short_desc) > 0:
                    for desc in short_desc:
                        s3_pic_key = s3_employer_picture_folder_name + str(desc['profile_image'])
                        desc.update({'profile_image' : s3_pic_key})
                        query = "select transcript_summary from chat_history where email_id = %s order by chat_id desc limit 2;"
                        values = (desc['email_id'],)
                        chat_history_dict = execute_query(query, values)
                        if chat_history_dict:
                            desc.update({"chat_summary" : chat_history_dict})
                        else:
                            desc.update({"chat_summary" : ""})
                        details_list[0]['employee_short_desc'].append(desc)
                
                if len(details_list[0]['employee_short_desc']) > 0:
                    first_id = details_list[0]['employee_short_desc'][0]['user_id']
                    # query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.profile_image, u.company_code, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s"
                    query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.profile_image, u.company_code, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.sector, e.website_url, upd.assisted_jobs_allowed, upd.assisted_jobs_used FROM users u JOIN employer_profile e ON u.user_id = e.employer_id join user_plan_details upd on u.user_id = upd.user_id WHERE u.user_id = %s AND e.employer_id = %s"
                    values = (first_id, first_id,)
                    first_emp_details = execute_query(query, values)
                    if len(first_emp_details) > 0:
                        owner_emp_id = first_emp_details[0]['user_id']
                        get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                        get_sub_users_values = (owner_emp_id,)
                        sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                        sub_users_list = []
                        if sub_users_dict:
                            sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                        sub_users_list.append(owner_emp_id)
                        s3_pic_key = s3_employer_picture_folder_name + str(first_emp_details[0]['profile_image'])
                        first_emp_details[0].update({'profile_image' : s3_pic_key})
                        details_list[0]['first_employee_details'].append(first_emp_details[0])
                        posted_job_details_query = """SELECT
                                                            jp.id AS job_id,
                                                            jp.employer_id,
                                                            jp.job_title,
                                                            jp.job_status,
                                                            jp.created_at AS posted_on,
                                                            COALESCE(vc.view_count, 0) AS view_count,
                                                            COALESCE(ja.applied_count, 0) AS applied_count,
                                                            COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                                                            COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                                                            COALESCE(CAST(ja.contact_count AS SIGNED), 0) AS contacted_count,
                                                            COALESCE(CAST(ja.reject_count AS SIGNED), 0) AS rejected_count,
                                                            GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left,
                                                            COALESCE(hc.hired_count, 0) AS hired_count
                                                            
                                                        FROM
                                                            job_post jp
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS view_count
                                                            FROM
                                                                view_count
                                                            GROUP BY
                                                                job_id
                                                        ) vc ON jp.id = vc.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS applied_count,
                                                                SUM(CASE WHEN application_status = 'Not Reviewed' THEN 1 ELSE 0 END) AS not_reviewed_count,
                                                                SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count,
                                                                SUM(CASE WHEN application_status = 'Rejected' THEN 1 ELSE 0 END) AS reject_count,
                                                                SUM(CASE WHEN application_status = 'Contacted' THEN 1 ELSE 0 END) AS contact_count
                                                            FROM
                                                                job_activity
                                                            GROUP BY
                                                                job_id
                                                        ) ja ON jp.id = ja.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(professional_id) AS hired_count
                                                            FROM
                                                                job_hired_candidates
                                                            GROUP BY
                                                                job_id
                                                        ) hc ON jp.id = hc.job_id
                                                        WHERE
                                                            jp.employer_id IN %s and jp.job_status != 'drafted'
                                                        ORDER BY
                                                            jp.id DESC;"""
                        values = (tuple(sub_users_list),)
                        posted_job_details = execute_query(posted_job_details_query, values)

                        query = "SELECT COUNT(CASE WHEN job_status = 'Opened' THEN 1 END) AS total_opened, COUNT(CASE WHEN job_status = 'Paused' THEN 1 END) AS total_paused, COUNT(CASE WHEN job_status = 'Closed' THEN 1 END) AS total_closed FROM job_post WHERE employer_id IN %s;"
                        values = (tuple(sub_users_list),)
                        job_count_details = execute_query(query, values)
                        if job_count_details:
                            for p in posted_job_details:
                                p['total_opened'] = job_count_details[0]['total_opened']
                                p['total_paused'] = job_count_details[0]['total_paused']
                                p['total_closed'] = job_count_details[0]['total_closed']

                        first_emp_details[0].update({'posted_job_details' : posted_job_details})
                    details_list[0].update({'total_count': total_count})
                    details_list[0].update({'filter_parameters' : filter_parameters})

                    result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                else:
                    filter_params = [{"filter_parameters" : filter_parameters,"total_count" : 0,"employee_short_desc" : [],"first_employee_details" : {}}]
                    result_json = api_json_response_format(True,"No employer profile found",0,filter_params)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def individual_employer_detail():
    try:
        result_json = {}
        req_data = request.get_json()
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                if 'user_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                user_id =req_data['user_id']
                details_list = [{"employee_short_desc" : []}]
                if isUserExist("users", "user_id", req_data['user_id']):
                    query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.profile_image, u.company_code, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s"
                    values = (req_data['user_id'], req_data['user_id'],)
                    emp_details = execute_query(query, values)
                    if len(emp_details) > 0:
                        owner_emp_id = req_data['user_id']
                        get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                        get_sub_users_values = (owner_emp_id,)
                        sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                        sub_users_list = []
                        if sub_users_dict:
                            sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                        sub_users_list.append(owner_emp_id)
                        s3_pic_key = s3_employer_picture_folder_name + str(emp_details[0]['profile_image'])
                        emp_details[0].update({'profile_image' : s3_pic_key})
                        posted_job_details_query = """SELECT
                                                            jp.id AS job_id,
                                                            jp.employer_id,
                                                            jp.job_title,
                                                            jp.job_status,
                                                            jp.created_at AS posted_on,
                                                            COALESCE(vc.view_count, 0) AS view_count,
                                                            COALESCE(ja.applied_count, 0) AS applied_count,
                                                            COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                                                            COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                                                            COALESCE(CAST(ja.contact_count AS SIGNED), 0) AS contacted_count,
                                                            COALESCE(CAST(ja.reject_count AS SIGNED), 0) AS rejected_count,
                                                            GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left,
                                                            COALESCE(hc.hired_count, 0) AS hired_count
                                                            
                                                        FROM
                                                            job_post jp
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS view_count
                                                            FROM
                                                                view_count
                                                            GROUP BY
                                                                job_id
                                                        ) vc ON jp.id = vc.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS applied_count,
                                                                SUM(CASE WHEN application_status = 'Not Reviewed' THEN 1 ELSE 0 END) AS not_reviewed_count,
                                                                SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count,
                                                                SUM(CASE WHEN application_status = 'Rejected' THEN 1 ELSE 0 END) AS reject_count,
                                                                SUM(CASE WHEN application_status = 'Contacted' THEN 1 ELSE 0 END) AS contact_count
                                                            FROM
                                                                job_activity
                                                            GROUP BY
                                                                job_id
                                                        ) ja ON jp.id = ja.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(professional_id) AS hired_count
                                                            FROM
                                                                job_hired_candidates
                                                            GROUP BY
                                                                job_id
                                                        ) hc ON jp.id = hc.job_id
                                                        WHERE
                                                            jp.employer_id IN %s and jp.job_status != 'drafted'
                                                        ORDER BY
                                                            jp.id DESC;"""
                        values = (tuple(sub_users_list),)
                        posted_job_details = execute_query(posted_job_details_query, values)
                        query = "SELECT COUNT(CASE WHEN job_status = 'Opened' THEN 1 END) AS total_opened, COUNT(CASE WHEN job_status = 'Paused' THEN 1 END) AS total_paused, COUNT(CASE WHEN job_status = 'Closed' THEN 1 END) AS total_closed FROM job_post WHERE employer_id IN %s;"
                        values = (tuple(sub_users_list),)
                        job_count_details = execute_query(query, values)
                        if job_count_details:
                            for p in posted_job_details:
                                p['total_opened'] = job_count_details[0]['total_opened']
                                p['total_paused'] = job_count_details[0]['total_paused']
                                p['total_closed'] = job_count_details[0]['total_closed']
                        emp_details[0].update({'posted_job_details' : posted_job_details})
                        query = "select assisted_jobs_allowed, assisted_jobs_used from user_plan_details where user_id = %s"
                        values = (user_id,)
                        res = execute_query(query, values)
                        if not res:
                            result = api_json_response_format(False, "No data found", 0, {})
                            return result
                        assisted_jobs_allowed = res[0]["assisted_jobs_allowed"]
                        assisted_jobs_used = res[0]["assisted_jobs_used"]
                        # current_assist_count = res[0]["assisted_jobs_allowed"] - res[0]["assisted_jobs_used"]
                        emp_details[0]["assisted_jobs_allowed"] = max(assisted_jobs_allowed, 0)
                        emp_details[0]["assisted_jobs_used"] = max(assisted_jobs_used, 0)
                        details_list[0]['employee_short_desc'].append(emp_details[0])
                    result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                else:
                    result_json = api_json_response_format(False,"No employer profile found",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def admin_employer_filter_results():
    try:
        data = []
        profile = {}
        country = []
        city = []

        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                plan = req_data['plan']
                offset = (page_number - 1) * 10
                
                query_emp_details = "SELECT u.user_id, u.pricing_category, u.is_active, u.payment_status, u.country, ep.sector FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id"
                    
                conditions = []
                values_emp_details = []
  
                if req_data['plan']:
                    conditions.append("u.pricing_category IN %s")
                    values_emp_details.append(tuple(plan,))

                # if country:
                #     if "Others" in country:
                #         conditions.append("u.country NOT IN %s")
                #         values_emp_details.append(tuple(country_list),)
                #     else:
                #         conditions.append("u.country IN %s")
                #         values_emp_details.append(tuple(country),)

                if country:
                    conditions.append("u.country IN %s")
                    values_emp_details.append(tuple(country),)
                
                if city:
                    conditions.append("u.city IN %s")
                    values_emp_details.append(tuple(city),)

                if req_data['sector']:
                    if "Others" in req_data['sector']:
                        conditions.append("ep.sector NOT IN %s")
                        values_emp_details.append(tuple(sectors_list),)
                    else:
                        conditions.append("ep.sector IN %s")
                        values_emp_details.append(tuple(req_data['sector']),)

                if conditions:
                    if len(conditions) == 1:
                        query_emp_details += " AND " + conditions[0]
                    else:
                        query_emp_details += " AND (" + " AND ".join(conditions) + ")"
                # query_emp_details += " ORDER BY jp.id DESC "
                new_query = query_emp_details + "ORDER BY user_id DESC LIMIT 10 OFFSET %s"
                val_detail = values_emp_details
                query = "SELECT COUNT(*) AS total_count FROM (" + query_emp_details + ") AS subquery"
                values = (val_detail)
                total_count = execute_query(query, values)
                if len(total_count) > 0:
                    total_count = total_count[0]['total_count']
                else:
                    total_count = 0
                values_emp_details.append(offset,)
                emp_details = replace_empty_values(execute_query(new_query, values_emp_details))
                details_list = [{"employee_short_desc" : [],
                                 "first_employee_details" : []
                                }]
                filter_parameters = fetch_employer_filter_params()
                if len(emp_details) > 0:
                    for emp in emp_details:
                            query = 'SELECT u.user_id, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s'
                            values = (emp['user_id'], emp['user_id'],)
                            single_emp_detail = execute_query(query, values)
                            if len(single_emp_detail) > 0:
                                s3_pic_key = s3_employer_picture_folder_name + str(single_emp_detail[0]['profile_image'])
                                single_emp_detail[0].update({'profile_image' : s3_pic_key})
                                details_list[0]['employee_short_desc'].append(single_emp_detail[0])
                    if len(details_list) > 0:
                        first_id = details_list[0]['employee_short_desc'][0]['user_id']
                        query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.profile_image, u.company_code, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s"
                        values = (first_id, first_id,)
                        first_emp_details = execute_query(query, values)
                        if len(first_emp_details) > 0:
                            owner_emp_id = first_emp_details[0]['user_id']
                            get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                            get_sub_users_values = (owner_emp_id,)
                            sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                            sub_users_list = []
                            if sub_users_dict:
                                sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                            sub_users_list.append(owner_emp_id)
                            s3_pic_key = s3_employer_picture_folder_name + str(first_emp_details[0]['profile_image'])
                            first_emp_details[0].update({'profile_image' : s3_pic_key})
                            details_list[0]['first_employee_details'].append(first_emp_details[0])
                            posted_job_details_query = """SELECT
                                                            jp.id AS job_id,
                                                            jp.employer_id,
                                                            jp.job_title,
                                                            jp.job_status,
                                                            jp.created_at AS posted_on,
                                                            COALESCE(vc.view_count, 0) AS view_count,
                                                            COALESCE(ja.applied_count, 0) AS applied_count,
                                                            COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                                                            COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                                                            COALESCE(CAST(ja.contact_count AS SIGNED), 0) AS contacted_count,
                                                            COALESCE(CAST(ja.reject_count AS SIGNED), 0) AS rejected_count,
                                                            GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left,
                                                            COALESCE(hc.hired_count, 0) AS hired_count

                                                        FROM
                                                            job_post jp
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS view_count
                                                            FROM
                                                                view_count
                                                            GROUP BY
                                                                job_id
                                                        ) vc ON jp.id = vc.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS applied_count,
                                                                SUM(CASE WHEN application_status = 'Not Reviewed' THEN 1 ELSE 0 END) AS not_reviewed_count,
                                                                SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count,
                                                                SUM(CASE WHEN application_status = 'Rejected' THEN 1 ELSE 0 END) AS reject_count,
                                                                SUM(CASE WHEN application_status = 'Contacted' THEN 1 ELSE 0 END) AS contact_count
                                                            FROM
                                                                job_activity
                                                            GROUP BY
                                                                job_id
                                                        ) ja ON jp.id = ja.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(professional_id) AS hired_count
                                                            FROM
                                                                job_hired_candidates
                                                            GROUP BY
                                                                job_id
                                                        ) hc ON jp.id = hc.job_id
                                                        WHERE
                                                            jp.employer_id IN %s and jp.job_status != 'drafted'
                                                        ORDER BY
                                                            jp.id DESC;"""
                            values = (tuple(sub_users_list),)
                            posted_job_details = execute_query(posted_job_details_query, values)
                            query = "SELECT COUNT(CASE WHEN job_status = 'Opened' THEN 1 END) AS total_opened, COUNT(CASE WHEN job_status = 'Paused' THEN 1 END) AS total_paused, COUNT(CASE WHEN job_status = 'Closed' THEN 1 END) AS total_closed FROM job_post WHERE employer_id IN %s;"
                            values = (tuple(sub_users_list),)
                            job_count_details = execute_query(query, values)
                            if job_count_details:
                                for p in posted_job_details:
                                    p['total_opened'] = job_count_details[0]['total_opened']
                                    p['total_paused'] = job_count_details[0]['total_paused']
                                    p['total_closed'] = job_count_details[0]['total_closed']
                            first_emp_details[0].update({'posted_job_details' : posted_job_details})
                        details_list[0].update({'total_count': total_count})
                        details_list[0].update({'filter_parameters' : filter_parameters})
                        result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                    else:
                        result_json = api_json_response_format(False,"No employer profile found",401,{})
                else:
                        result_json = api_json_response_format(False,"No employer profile found",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def admin_partner_dashboard():
    try:
        result_json = {}
        key = ''
        param = ''
        req_data = request.get_json()
        if 'partner_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            return result_json
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        if 'key' in req_data:
            key = req_data['key']
        if 'param' in req_data:
            param = req_data['param']
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                partner_id = req_data['partner_id']
                page_number = req_data['page_number']
                offset = (page_number - 1) * 10

                total_query = "SELECT COUNT(*) AS total_count FROM partner_profile pp JOIN users u ON pp.partner_id = u.user_id WHERE u.email_active = 'Y' AND u.user_role_fk = 6"
                values = ()
                total_count = execute_query(total_query,values)
                if len(total_count) > 0:
                    total_count = total_count[0]['total_count']
                else:
                    total_count = 0
                filter_parameters = fetch_filter_params_partner()

                query = "SELECT user_id from users where user_role_fk = %s and user_id > %s AND email_active = 'Y' ORDER BY user_id DESC"
                values = (6, partner_id,)
                partner_ids = execute_query(query, values)
                user_ids = [item['user_id'] for item in partner_ids]
                details_list = [{"partner_short_desc" : [],
                                 "first_partner_details" : []
                                 }]
                if len(user_ids) > 0:
                    if key == 'sort':
                        if param == 'by_date':
                            query = "SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, u.created_at, p.designation, p.company_name, p.company_description, p.partner_type, p.sector, p.website_url, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM users u JOIN partner_profile p ON u.user_id = p.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE u.user_id IN %s AND p.partner_id IN %s ORDER BY u.created_at ASC LIMIT 10 OFFSET %s"
                        elif param == 'asc':
                            query = "SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, p.designation, p.company_name, p.company_description, p.partner_type, p.sector, p.website_url, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM users u JOIN partner_profile p ON u.user_id = p.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE u.user_id IN %s AND p.partner_id IN %s ORDER BY p.company_name ASC LIMIT 10 OFFSET %s"
                        else:
                            query = "SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, p.designation, p.company_name, p.company_description, p.partner_type, p.sector, p.website_url, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM users u JOIN partner_profile p ON u.user_id = p.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE u.user_id IN %s AND p.partner_id IN %s ORDER BY p.company_name DESC LIMIT 10 OFFSET %s"
                    else:
                        query = "SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, p.designation, p.company_name, p.company_description, p.partner_type, p.sector, p.website_url, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM users u JOIN partner_profile p ON u.user_id = p.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE u.user_id IN %s AND p.partner_id IN %s ORDER BY p.partner_id DESC LIMIT 10 OFFSET %s"
                    values = (tuple(user_ids),tuple(user_ids), offset,)
                    short_desc = execute_query(query, values)
                else:
                    filter_params = [{'filter_parameters' : filter_parameters, "first_partner_details" : {}, "partner_short_desc" : [], "total_count" : 0}]
                    result_json = api_json_response_format(True,"No partner profile found",0,filter_params)
                    return result_json
                if len(short_desc) > 0:
                    for desc in short_desc:
                        s3_pic_key = s3_partner_picture_folder_name + str(desc['profile_image'])
                        desc.update({'profile_image' : s3_pic_key})
                        details_list[0]['partner_short_desc'].append(desc)
                if len(details_list[0]['partner_short_desc']) > 0:
                    first_id = details_list[0]['partner_short_desc'][0]['user_id']
                    query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.company_code, u.pricing_category, u.is_active, u.payment_status, u.profile_image, p.company_name, p.designation, p.company_description, p.partner_type, p.sector, p.website_url FROM partner_profile p INNER JOIN users u ON p.partner_id = u.user_id WHERE u.user_id = %s AND p.partner_id = %s;"
                    values = (first_id, first_id,)
                    first_partner_details = execute_query(query, values)
                    query = "SELECT DISTINCT pp.company_name, u.profile_image, l.id AS learning_id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.post_status, l.created_at, l.is_active, COALESCE(vc.view_count, 0) AS view_count, GREATEST(l.days_left - DATEDIFF(CURDATE(), l.created_at), 0) AS days_left FROM partner_profile pp LEFT JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.partner_id = %s AND l.post_status = %s ORDER BY l.id DESC"
                    values = (first_id, 'opened',)
                    learning_post_details = execute_query(query, values)
                    if len(first_partner_details) > 0:
                        s3_pic_key = s3_partner_picture_folder_name + str(first_partner_details[0]['profile_image'])
                        first_partner_details[0].update({'profile_image' : s3_pic_key})

                    if len(learning_post_details) > 0:
                        for data in learning_post_details:
                            s3_cover_pic_key = s3_partner_cover_pic_folder_name + data['image']
                            s3_attached_file_key = s3_partner_learning_folder_name + data['attached_file']
                            data.update({'image' : s3_cover_pic_key})
                            data.update({'attached_file' : s3_attached_file_key})
                    details_list[0].update({'first_partner_details' : first_partner_details})
                    details_list[0].update({'first_partner_posts' : learning_post_details})
                    # details_list[0]['first_partner_details'].append(learning_post_details)
                    details_list[0].update({'total_count': total_count})
                    details_list[0].update({'filter_parameters' : filter_parameters})

                    result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                else:
                    filter_params = [{'filter_parameters' : filter_parameters, "first_partner_details" : {}, "partner_short_desc" : [], "total_count" : 0}]
                    result_json = api_json_response_format(True,"No partner profile found",0,filter_params)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def individual_partner_detail():
    try:
        result_json = {}
        req_data = request.get_json()
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                if 'user_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                details_list = [{"partner_short_desc" : []}]
                if isUserExist("users", "user_id", req_data['user_id']):
                    # query = "SELECT u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.company_code, u.pricing_category, u.profile_image, p.company_name, p.designation, p.company_description, p.partner_type, p.sector, p.website_url, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM partner_profile p INNER JOIN users u ON p.partner_id = u.user_id INNER JOIN learning l ON p.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE l.post_status = %s AND u.user_id = %s AND p.partner_id = %s ORDER BY l.id DESC;"
                    # values = ('opened', req_data['user_id'], req_data['user_id'],)
                    # partner_details = execute_query(query, values)
                    query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.company_code, u.pricing_category, u.is_active, u.payment_status, u.profile_image, p.company_name, p.designation, p.company_description, p.partner_type, p.sector, p.website_url FROM partner_profile p INNER JOIN users u ON p.partner_id = u.user_id WHERE u.user_id = %s AND p.partner_id = %s;"
                    values = (req_data['user_id'], req_data['user_id'],)
                    partner_details = execute_query(query, values)
                    query = "SELECT DISTINCT pp.company_name, u.profile_image, l.id AS learning_id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.post_status, l.created_at, l.is_active, COALESCE(vc.view_count, 0) AS view_count, GREATEST(l.days_left - DATEDIFF(CURDATE(), l.created_at), 0) AS days_left FROM partner_profile pp LEFT JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.partner_id = %s AND l.post_status = %s ORDER BY l.id DESC"
                    values = (req_data['user_id'], 'opened',)
                    learning_post_details = execute_query(query, values)
                    if len(partner_details) > 0:
                        s3_pic_key = s3_partner_picture_folder_name + str(partner_details[0]['profile_image'])
                        partner_details[0].update({'profile_image' : s3_pic_key})
                    if len(learning_post_details) > 0:
                        for data in learning_post_details:
                            s3_cover_pic_key = s3_partner_cover_pic_folder_name + data['image']
                            s3_attached_file_key = s3_partner_learning_folder_name + data['attached_file']
                            data.update({'image' : s3_cover_pic_key})
                            data.update({'attached_file' : s3_attached_file_key})
                    # partner_details[0].update({'profile_image' : s3_pic_key})
                    details_list[0].update({'partner_short_desc' : partner_details})
                    details_list[0].update({'partner_learning_posts' : learning_post_details})

                    result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                else:
                    result_json = api_json_response_format(False,"No partner profile found",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def admin_partner_filter_results():
    try:
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            filter_parameters = fetch_filter_params_partner() 
            details_list = [{"partner_short_desc" : [],
                                 "first_partner_details" : []
                                }]       
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                partner_type = req_data["partner_type"]
                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # partner_type_list = ["Search firm", "Skill platform", "Assessment company", "Coaching and mentoring firm", "Learning and Development Organization", "Others"]
                plan = req_data['plan']
                offset = (page_number - 1) * 10
                
                query_partner_details = "SELECT u.user_id, u.pricing_category, u.is_active, u.payment_status, u.country, p.sector, p.partner_type FROM users u JOIN partner_profile p ON u.user_id = p.partner_id"
                    
                conditions = []
                values_partner_details = []
  
                if req_data['plan']:
                    conditions.append("u.pricing_category IN %s")
                    values_partner_details.append(tuple(plan,))

                # if country:
                #     if "Others" in country:
                #         conditions.append("u.country NOT IN %s")
                #         values_partner_details.append(tuple(country_list),)
                #     else:
                #         conditions.append("u.country IN %s")
                #         values_partner_details.append(tuple(country),)

                if country:
                    conditions.append("u.country IN %s")
                    values_partner_details.append(tuple(country),)
                
                if city:
                    conditions.append("u.city IN %s")
                    values_partner_details.append(tuple(city),)

                if req_data['sector']:
                    if "Others" in req_data['sector']:
                        conditions.append("p.sector NOT IN %s")
                        values_partner_details.append(tuple(sectors_list),)
                    else:
                        conditions.append("p.sector IN %s")
                        values_partner_details.append(tuple(req_data['sector']),)
                
                if req_data['partner_type']:
                    conditions.append("p.partner_type IN %s")
                    values_partner_details.append(tuple(partner_type,))

                if conditions:
                    if len(conditions) == 1:
                        query_partner_details += " AND " + conditions[0]
                    else:
                        query_partner_details += " AND (" + " AND ".join(conditions) + ")"
                # query_emp_details += " ORDER BY jp.id DESC "
                new_query = query_partner_details + "ORDER BY user_id DESC LIMIT 10 OFFSET %s"
                val_detail = values_partner_details
                query = "SELECT COUNT(*) AS total_count FROM (" + query_partner_details + ") AS subquery"
                values = (val_detail)
                total_count = execute_query(query, values)
                if len(total_count) > 0:
                    total_count = total_count[0]['total_count']
                else:
                    total_count = 0
                values_partner_details.append(offset,)
                partner_details = replace_empty_values(execute_query(new_query, values_partner_details))
                # details_list = [{"partner_short_desc" : [],
                #                  "first_partner_details" : []
                #                 }]
                if len(partner_details) > 0:
                    for partner in partner_details:
                            query = 'SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, p.designation, p.company_name, p.company_description, p.partner_type, p.sector, p.website_url, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM users u JOIN partner_profile p ON u.user_id = p.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE u.user_id = %s AND p.partner_id = %s;'
                            values = (partner['user_id'], partner['user_id'],)
                            single_partner_detail = execute_query(query, values)
                            if len(single_partner_detail) > 0:
                                s3_pic_key = s3_partner_picture_folder_name + str(single_partner_detail[0]['profile_image'])
                                single_partner_detail[0].update({'profile_image' : s3_pic_key})
                                details_list[0]['partner_short_desc'].append(single_partner_detail[0])
                    if len(details_list) > 0:
                        first_id = details_list[0]['partner_short_desc'][0]['user_id']
                        query = "SELECT u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.company_code, u.pricing_category, u.is_active, u.payment_status, u.profile_image, p.company_name, p.designation, p.company_description, p.partner_type, p.sector, p.website_url FROM partner_profile p INNER JOIN users u ON p.partner_id = u.user_id WHERE u.user_id = %s AND p.partner_id = %s"
                        values = (first_id, first_id,)
                        first_partner_details = execute_query(query, values)
                        query = "SELECT DISTINCT pp.company_name, u.profile_image, l.id AS learning_id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.post_status, l.created_at, l.is_active, COALESCE(vc.view_count, 0) AS view_count, GREATEST(l.days_left - DATEDIFF(CURDATE(), l.created_at), 0) AS days_left FROM partner_profile pp LEFT JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.partner_id = %s AND l.post_status = %s ORDER BY l.id DESC"
                        values = (first_id, 'opened',)
                        learning_post_details = execute_query(query, values)
                        if len(first_partner_details) > 0:
                            s3_pic_key = s3_partner_picture_folder_name + str(first_partner_details[0]['profile_image'])
                            first_partner_details[0].update({'profile_image' : s3_pic_key})
                        if len(learning_post_details) > 0:
                            for data in learning_post_details:
                                s3_cover_pic_key = s3_partner_cover_pic_folder_name + data['image']
                                s3_attached_file_key = s3_partner_learning_folder_name + data['attached_file']
                                data.update({'image' : s3_cover_pic_key})
                                data.update({'attached_file' : s3_attached_file_key})
                        details_list[0].update({'first_partner_details' : first_partner_details})
                        details_list[0].update({'first_partner_posts' : learning_post_details})
                        details_list[0].update({'total_count': total_count})
                        details_list[0].update({'filter_parameters' : filter_parameters})
                        result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                    else:
                        details_list[0].update({'filter_parameters' : filter_parameters})
                        result_json = api_json_response_format(False,"No partner profile found",401,details_list)
                else:
                    details_list[0].update({'filter_parameters' : filter_parameters})
                    result_json = api_json_response_format(False,"No partner profile found",401,details_list)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

# def admin_professional_dashboard_search():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()
            # if 'professional_id' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            #     return result_json
            if 'search_text' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
                return result_json
            if 'page_number' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            user_data = get_user_data(token_result["email_id"])
            # professional_id = req_data['professional_id']
            search_text = req_data['search_text']
            page_number = req_data['page_number']
            offset = (page_number - 1) * 10
            if search_text.startswith("2C"):
                split_txt = search_text.split("-")
                if len(split_txt) > 2:
                    search_text = (search_text.split("-")[2])
            if user_data["user_role"] == "admin":
                if search_text != '':
                    query = """WITH LatestExperience AS (
                                SELECT
                                    pe.professional_id,
                                    pe.id AS experience_id,
                                    pe.start_month,
                                    pe.start_year,
                                    pe.job_title,
                                    pe.job_description,
                                    pe.created_at,
                                    pe.company_name,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY pe.professional_id
                                        ORDER BY
                                            CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC,
                                            CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC,
                                            pe.created_at DESC
                                    ) AS rn
                                FROM
                                    professional_experience AS pe
                            )
                            SELECT
                                u.first_name,
                                u.last_name,
                                u.country,
                                u.state,
                                u.city,
                                u.profile_percentage,
                                u.pricing_category,
                                u.is_active,
                                u.payment_status,
                                u.email_id,
                                p.professional_id,
                                p.expert_notes,
                                p.created_at AS posted_at,
                                le.job_title,
                                le.job_description,
                                le.company_name,
                                le.created_at,
                                u2.profile_image,
                                GROUP_CONCAT(DISTINCT ps.skill_name SEPARATOR ', ') AS skills,
                                GROUP_CONCAT(DISTINCT ped.specialisation SEPARATOR ', ') AS specialisation,
                                GROUP_CONCAT(DISTINCT ped.institute_name SEPARATOR ', ') AS institute_name,
                                GROUP_CONCAT(DISTINCT ped.degree_level SEPARATOR ', ') AS degree_level
                            FROM
                                users AS u
                            LEFT JOIN
                                professional_profile AS p ON u.user_id = p.professional_id
                            LEFT JOIN
                                professional_additional_info AS pai ON u.user_id = pai.professional_id
                            LEFT JOIN
                                professional_language AS pl ON u.user_id = pl.professional_id
                            LEFT JOIN
                                professional_education AS ped ON u.user_id = ped.professional_id
                            LEFT JOIN
                                professional_skill AS ps ON u.user_id = ps.professional_id
                            LEFT JOIN
                                professional_experience AS pe ON p.professional_id = pe.professional_id
                            LEFT JOIN
                                LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                            LEFT JOIN
                                users AS u2 ON u.user_id = u2.user_id
                            WHERE
                                p.professional_id > 0
                                AND u.email_active = 'Y'
                                AND (u.first_name LIKE %s
                                OR u.last_name LIKE %s 
                                OR CONCAT(u.first_name, ' ', u.last_name) LIKE %s
                                OR u.user_id LIKE %s
                                OR u.city LIKE %s
                                OR u.country LIKE %s
                                OR ps.skill_name LIKE %s
                                OR ped.specialisation LIKE %s
                                OR pl.language_known LIKE %s
                                OR pai.description LIKE %s
                                OR pe.job_title LIKE %s
                                OR pe.job_description LIKE %s)
                            GROUP BY
                                u.user_id,
                                u.first_name,
                                u.last_name,
                                u.country,
                                u.state,
                                u.city,
                                u.profile_percentage,
                                u.pricing_category,
                                u.is_active,
                                u.payment_status,
                                p.professional_id,
                                p.expert_notes,
                                p.created_at,
                                le.job_title,
                                le.company_name,
                                le.job_description,
                                le.created_at,
                                u2.profile_image
                            ORDER BY
                                p.professional_id DESC"""
                    search_term = f"%{search_text}%"
                    values = (search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term,)
                    total_count = execute_query(query,values)
                    total_count = len(total_count)
                    filter_parameters = fetch_filter_params()
                    data_query = query + " LIMIT 10 OFFSET %s;"
                    values = (search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, offset,)
                    candidates_desc = execute_query(data_query, values)
                    first_id = 0
                     
                    if len(candidates_desc) > 0:
                        temp_id_2 = candidates_desc[0]['professional_id']
                        for obj in candidates_desc:
                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (obj['professional_id'], 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city,
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(ep.sector, su.sector) AS sector,
                                        COALESCE(ep.company_name, su.company_name) AS company_name,
                                        COALESCE(u.is_active, '') AS is_active
                                        FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            obj.update({'recommended_jobs' : recommended_jobs_list})
                            temp_id1 = "2C-PR-" + str(obj['professional_id'])
                            obj.update({"professional_id" : temp_id1})
                            profile_image_name = replace_empty_values1(obj['profile_image'])
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            obj.update({'profile_image' : s3_pic_key})
                        first_id = candidates_desc[0]['professional_id']
                        # temp_id = "2C-PR-" + str(candidates_desc[0]['professional_id'])
                        if isUserExist("professional_profile","professional_id",temp_id_2):
                            query = "SELECT u.first_name, u.last_name, u.email_id, u.dob, u.country_code, u.contact_number, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.country, u.state, u.city, p.professional_id,p.professional_resume,p.expert_notes,p.about,p.preferences, p.video_url, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') THEN 0 ELSE pe.end_year END DESC, CASE WHEN (pe.end_month IS NULL OR pe.end_month = '') THEN 0 ELSE pe.end_month END DESC, CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') OR (pe.end_month IS NULL OR pe.end_month = '') THEN pe.created_at END DESC"                
                            values = (temp_id_2,)
                            profile_result = execute_query(query, values) 

                            if len(profile_result) > 0:                              
                                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                                intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                                resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                            else:
                                result_json = api_json_response_format(True,"No records found",0,{})     
                                return result_json          
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            if intro_video_name == '':
                                s3_video_key = ""
                            else:
                                s3_video_key = s3_intro_video_folder_name + str(intro_video_name)
                            if resume_name == '':    
                                s3_resume_key = ""
                            else:          
                                s3_resume_key = s3_resume_folder_name + str(resume_name)

                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (temp_id_2, 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                                COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                                COALESCE(ep.sector, su.sector) AS sector,
                                                COALESCE(ep.company_name, su.company_name) AS company_name,
                                                COALESCE(u.is_active, '') AS is_active
                                                FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            profile_dict = {
                                'first_name': replace_empty_values1(profile_result[0]['first_name']),
                                'last_name': replace_empty_values1(profile_result[0]['last_name']),
                                'professional_id' : first_id,                                        
                                'email_id': replace_empty_values1(profile_result[0]['email_id']),
                                'dob': replace_empty_values1(profile_result[0]['dob']),
                                'country_code': replace_empty_values1(profile_result[0]['country_code']),
                                'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                                'city': replace_empty_values1(profile_result[0]['city']),
                                'state': replace_empty_values1(profile_result[0]['state']),
                                'country': replace_empty_values1(profile_result[0]['country']),
                                'profile_percentage' : replace_empty_values1(profile_result[0]['profile_percentage']),
                                'pricing_category' : profile_result[0]['pricing_category'],
                                'is_active' : profile_result[0]['is_active'],
                                'payment_status' : profile_result[0]['payment_status'],
                                'profile_image': s3_pic_key,
                                'video_name': s3_video_key,
                                'resume_name': s3_resume_key,
                                'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                                'about': replace_empty_values1(profile_result[0]['about']),
                                'preferences': replace_empty_values1(profile_result[0]['preferences']),
                                'experience': {},
                                'education': {},
                                'skills': {},
                                'languages': {},
                                'additional_info': {},
                                'social_link': {},
                                'job_list' : {},
                                'filter_parameters' : filter_parameters,
                                'recommended_jobs' : recommended_jobs_list,
                                'canidates_description' : {}
                            }

                            # Grouping experience data
                            experience_set = set()
                            experience_list = []
                            for exp in profile_result:
                                if exp['experience_id'] is not None:
                                    start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                                    end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                                    exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                                    if exp_tuple not in experience_set:
                                        experience_set.add(exp_tuple)
                                        experience_list.append({
                                            'id': exp['experience_id'],
                                            'company_name': replace_empty_values1(exp['company_name']),
                                            'job_title': replace_empty_values1(exp['job_title']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,                                
                                            'job_description': replace_empty_values1(exp['job_description']),
                                            'job_location': replace_empty_values1(exp['job_location'])
                                        })

                            profile_dict['experience'] = experience_list

                            # Grouping education data
                            education_set = set()
                            education_list = []
                            for edu in profile_result:
                                if edu['education_id'] is not None:
                                    start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                                    end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                                    edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                                start_date, end_date, edu['institute_location'])
                                    if edu_tuple not in education_set:
                                        education_set.add(edu_tuple)
                                        education_list.append({
                                            'id': edu['education_id'],
                                            'institute_name': replace_empty_values1(edu['institute_name']),
                                            'degree_level': replace_empty_values1(edu['degree_level']),
                                            'specialisation': replace_empty_values1(edu['specialisation']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,
                                            'institute_location': replace_empty_values1(edu['institute_location'])
                                        })

                            profile_dict['education'] = education_list

                            # Grouping skills data
                            skills_set = set()
                            skills_list = []
                            for skill in profile_result:
                                if skill['skill_id'] is not None:
                                    skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                                    if skill_tuple not in skills_set:
                                        skills_set.add(skill_tuple)
                                        skills_list.append({
                                            'id': skill['skill_id'],
                                            'skill_name': replace_empty_values1(skill['skill_name']),
                                            'skill_level': replace_empty_values1(skill['skill_level'])
                                        })

                            profile_dict['skills'] = skills_list

                            # Grouping languages data
                            languages_set = set()
                            languages_list = []
                            for lang in profile_result:
                                if lang['language_id'] is not None:
                                    lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                                    if lang_tuple not in languages_set:
                                        languages_set.add(lang_tuple)
                                        languages_list.append({
                                            'language_known': replace_empty_values1(lang['language_known']),
                                            'id':  lang['language_id'],
                                            'language_level': replace_empty_values1(lang['language_level'])
                                    })

                            profile_dict['languages'] = languages_list
                            # Grouping additional info data
                            additional_info_set = set()
                            additional_info_list = []
                            for info in profile_result:
                                if info['additional_info_id'] is not None:
                                    info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                                    if info_tuple not in additional_info_set:
                                        additional_info_set.add(info_tuple)
                                        additional_info_list.append({
                                        'id': info['additional_info_id'],
                                        'title': replace_empty_values1(info['additional_info_title']),
                                        'description': replace_empty_values1(info['additional_info_description'])
                                    })

                            profile_dict['additional_info'] = additional_info_list
                            # Grouping social link data
                            social_link_set = set()
                            social_link_list = []
                            for link in profile_result:
                                if link['social_link_id'] is not None:
                                    link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                                    if link_tuple not in social_link_set:
                                        social_link_set.add(link_tuple)
                                        social_link_list.append({
                                        'title': replace_empty_values1(link['social_link_title']),
                                        'id':  link['social_link_id'],
                                        'url': replace_empty_values1(link['social_link_url'])
                                    })

                            profile_dict['social_link'] = social_link_list
                            
                            #Get recommended jobs for the professional
                            query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                            values = (3,temp_id_2,)
                            job_id_list = execute_query(query, values)
                            job_detail_list = []
                            if len(job_id_list) > 0:
                                for job_id in job_id_list:
                                    id = job_id['job_id']
                                    if isUserExist("job_post", "id", id):
                                        query = 'select employer_id, job_title, country, city from job_post where id = %s'
                                        values = (id,)
                                        job_detail = execute_query(query, values)
                                        query = 'select company_name from employer_profile where employer_id = %s'
                                        values = (job_detail[0]['employer_id'],)
                                        company_name = execute_query(query, values)
                                    else:
                                        job_detail = [{"job_title" : "","country" : "", "city" : ""}]
                                        company_name = [{'company_name': ""}]

                                    if len(job_detail) > 0 and len(company_name) > 0:
                                        job_detail_dict = {"job_title" : job_detail[0]['job_title'],
                                                            "country" : job_detail[0]['country'],
                                                            "city" : job_detail[0]['city'],
                                                            "company_name" : company_name[0]['company_name']}
                                    else:
                                        job_detail_dict = {}
                                    job_detail_list.append(job_detail_dict)
                                profile_dict['job_list'] = job_detail_list
                            else:
                                profile_dict['job_list'] = []
                            profile_dict['canidates_description'] = candidates_desc
                            profile_dict.update({'total_count' : total_count})
                        result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)                    
                    else:
                        profile_dict = {'filter_parameters' : filter_parameters}
                        profile_dict.update({'job_list' : []})
                        profile_dict.update({'canidates_description' : []})
                        profile_dict.update({'total_count' : 0})
                        profile_dict.update({'recommended_jobs' : []})
                        result_json = api_json_response_format(True,"No records found.",0,profile_dict)
                else:
                    result_json = api_json_response_format(True,"Please enter the valid id",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def admin_employer_dashboard_search():
    try:
        result_json = {}
        req_data = request.get_json()
        # if 'employer_id' not in req_data:
        #     result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
        #     return result_json
        if 'search_text' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            return result_json
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                # employer_id = req_data['employer_id']
                search_text = req_data['search_text']
                page_number = req_data['page_number']
                offset = (page_number - 1) * 10

                # total_query = "SELECT COUNT(*) AS total_count FROM employer_profile ep JOIN users u ON ep.employer_id = u.user_id WHERE u.user_role_fk = 2"
                # values = ()
                # total_count = execute_query(total_query,values)
                # if len(total_count) > 0:
                #     total_count = total_count[0]['total_count']
                # else:
                #     total_count = 0
                filter_parameters = fetch_employer_filter_params()

                query = """SELECT 
                            u.user_id, 
                            u.first_name, 
                            u.last_name, 
                            u.email_id, 
                            u.country, 
                            u.city, 
                            ep.company_name, 
                            ep.designation, 
                            ep.employer_type, 
                            ep.sector 
                        FROM 
                            users AS u
                        LEFT JOIN 
                            employer_profile AS ep ON u.user_id = ep.employer_id
                        WHERE 
                            u.user_role_fk = 2 
                            AND u.email_active = 'Y'
                            AND u.user_id > 0
                            AND (ep.company_name LIKE %s
                            OR u.city LIKE %s
                            OR u.country LIKE %s)
                        ORDER BY 
                            u.user_id DESC 
                        """
                searct_term = f'%{search_text}%'
                values = (searct_term, searct_term, searct_term)
                total_count = execute_query(query, values)
                total_count = len(total_count)
                data_query = query + " LIMIT 10 OFFSET %s"
                values = (searct_term, searct_term, searct_term, offset,)
                employer_ids = execute_query(data_query, values)
                
                details_list = [{"employee_short_desc" : [],
                                 "first_employee_details" : []
                                 }] 
                for i in employer_ids:
                    query = "SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s"
                    values = (i['user_id'],i['user_id'],)
                    short_desc = execute_query(query, values)
                    if len(short_desc) > 0:
                        s3_pic_key = s3_employer_picture_folder_name + str(short_desc[0]['profile_image'])
                        short_desc[0].update({'profile_image' : s3_pic_key})
                        details_list[0]['employee_short_desc'].append(short_desc[0])
                if len(details_list[0]['employee_short_desc']) > 0:
                    first_id = details_list[0]['employee_short_desc'][0]['user_id']
                    query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.profile_image, u.company_code, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s"
                    values = (first_id, first_id,)
                    first_emp_details = execute_query(query, values)
                    if len(first_emp_details) > 0:
                        owner_emp_id = first_emp_details[0]['user_id']
                        get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                        get_sub_users_values = (owner_emp_id,)
                        sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                        sub_users_list = []
                        if sub_users_dict:
                            sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                        sub_users_list.append(owner_emp_id)
                        s3_pic_key = s3_employer_picture_folder_name + str(first_emp_details[0]['profile_image'])
                        first_emp_details[0].update({'profile_image' : s3_pic_key})
                        details_list[0]['first_employee_details'].append(first_emp_details[0])
                        posted_job_details_query = """SELECT
                                                            jp.id AS job_id,
                                                            jp.employer_id,
                                                            jp.job_title,
                                                            jp.job_status,
                                                            jp.created_at AS posted_on,
                                                            COALESCE(vc.view_count, 0) AS view_count,
                                                            COALESCE(ja.applied_count, 0) AS applied_count,
                                                            COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                                                            COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                                                            COALESCE(CAST(ja.contact_count AS SIGNED), 0) AS contacted_count,
                                                            COALESCE(CAST(ja.reject_count AS SIGNED), 0) AS rejected_count,
                                                            GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left,
                                                            COALESCE(hc.hired_count, 0) AS hired_count

                                                        FROM
                                                            job_post jp
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS view_count
                                                            FROM
                                                                view_count
                                                            GROUP BY
                                                                job_id
                                                        ) vc ON jp.id = vc.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS applied_count,
                                                                SUM(CASE WHEN application_status = 'Not Reviewed' THEN 1 ELSE 0 END) AS not_reviewed_count,
                                                                SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count,
                                                                SUM(CASE WHEN application_status = 'Rejected' THEN 1 ELSE 0 END) AS reject_count,
                                                                SUM(CASE WHEN application_status = 'Contacted' THEN 1 ELSE 0 END) AS contact_count
                                                            FROM
                                                                job_activity
                                                            GROUP BY
                                                                job_id
                                                        ) ja ON jp.id = ja.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(professional_id) AS hired_count
                                                            FROM
                                                                job_hired_candidates
                                                            GROUP BY
                                                                job_id
                                                        ) hc ON jp.id = hc.job_id
                                                        WHERE
                                                            jp.employer_id IN %s and jp.job_status != 'drafted'
                                                        ORDER BY
                                                            jp.id DESC;"""
                        values = (tuple(sub_users_list),)
                        posted_job_details = execute_query(posted_job_details_query, values)
                        query = "SELECT COUNT(CASE WHEN job_status = 'Opened' THEN 1 END) AS total_opened, COUNT(CASE WHEN job_status = 'Paused' THEN 1 END) AS total_paused, COUNT(CASE WHEN job_status = 'Closed' THEN 1 END) AS total_closed FROM job_post WHERE employer_id IN %s;"
                        values = (tuple(sub_users_list),)
                        job_count_details = execute_query(query, values)
                        if job_count_details:
                            for p in posted_job_details:
                                p['total_opened'] = job_count_details[0]['total_opened']
                                p['total_paused'] = job_count_details[0]['total_paused']
                                p['total_closed'] = job_count_details[0]['total_closed']
                        first_emp_details[0].update({'posted_job_details' : posted_job_details})
                    details_list[0].update({'total_count': total_count})
                    details_list[0].update({'filter_parameters' : filter_parameters})

                    result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                else:
                    filter_params = [{"filter_parameters" : filter_parameters,"total_count" : 0,"employee_short_desc" : [],"first_employee_details" : {}}]
                    result_json = api_json_response_format(True,"No employer profile found",0,filter_params)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def admin_partner_dashboard_search():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'search_text' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            return result_json
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                search_text = req_data['search_text']
                page_number = req_data['page_number']
                offset = (page_number - 1) * 10

                filter_parameters = fetch_filter_params_partner()

                query = """SELECT 
                            u.user_id, 
                            u.first_name, 
                            u.last_name, 
                            u.email_id, 
                            u.country, 
                            u.city, 
                            pp.company_name, 
                            pp.designation, 
                            pp.partner_type, 
                            pp.sector,
                            GROUP_CONCAT(DISTINCT l.title SEPARATOR ', ') AS title
                        FROM 
                            users AS u
                        LEFT JOIN 
                            partner_profile AS pp ON u.user_id = pp.partner_id
                        LEFT JOIN 
                            learning AS l ON u.user_id = l.partner_id
                        WHERE 
                            u.user_role_fk = 6
                            AND u.email_active = 'Y'
                            AND u.user_id > 0
                            AND (pp.company_name LIKE %s
                            OR u.city LIKE %s
                            OR u.country LIKE %s)
                        GROUP BY 
                            u.user_id, 
                            u.first_name, 
                            u.last_name, 
                            u.email_id, 
                            u.country, 
                            u.city, 
                            pp.company_name, 
                            pp.designation, 
                            pp.partner_type, 
                            pp.sector
                        ORDER BY 
                            u.user_id DESC
                        """
                searct_term = f'%{search_text}%'
                values = (searct_term, searct_term, searct_term,)
                total_count = execute_query(query, values)
                total_count = len(total_count)
                data_query = query + " LIMIT 10 OFFSET %s;"
                values = (searct_term, searct_term, searct_term, offset,)
                partner_ids = execute_query(data_query, values)
                
                details_list = [{"partner_short_desc" : [],
                                 "first_partner_details" : []
                                 }] 
                for i in partner_ids:
                    query = "SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, p.designation, p.company_name, p.company_description, p.partner_type, p.sector, p.website_url, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM users u JOIN partner_profile p ON u.user_id = p.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE u.user_id = %s AND p.partner_id = %s;"
                    values = (i['user_id'],i['user_id'],)
                    short_desc = execute_query(query, values)
                    if len(short_desc) > 0:
                        s3_pic_key = s3_partner_picture_folder_name + str(short_desc[0]['profile_image'])
                        short_desc[0].update({'profile_image' : s3_pic_key})
                        details_list[0]['partner_short_desc'].append(short_desc[0])
                if len(details_list[0]['partner_short_desc']) > 0:
                    first_id = details_list[0]['partner_short_desc'][0]['user_id']
                    query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.company_code, u.pricing_category, u.is_active, u.payment_status, u.profile_image, p.company_name, p.designation, p.company_description, p.partner_type, p.sector, p.website_url FROM partner_profile p INNER JOIN users u ON p.partner_id = u.user_id WHERE u.user_id = %s AND p.partner_id = %s;"
                    values = (first_id, first_id,)
                    first_partner_details = execute_query(query, values)
                    query = "SELECT DISTINCT pp.company_name, u.profile_image, l.id AS learning_id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.post_status, l.created_at, l.is_active, COALESCE(vc.view_count, 0) AS view_count, GREATEST(l.days_left - DATEDIFF(CURDATE(), l.created_at), 0) AS days_left FROM partner_profile pp LEFT JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.partner_id = %s AND l.post_status = %s ORDER BY l.id DESC"
                    values = (first_id, 'opened',)
                    learning_post_details = execute_query(query, values)
                    if len(first_partner_details) > 0:
                        s3_pic_key = s3_partner_picture_folder_name + str(first_partner_details[0]['profile_image'])
                        first_partner_details[0].update({'profile_image' : s3_pic_key})

                    if len(learning_post_details) > 0:
                        for data in learning_post_details:
                            s3_cover_pic_key = s3_partner_cover_pic_folder_name + data['image']
                            s3_attached_file_key = s3_partner_learning_folder_name + data['attached_file']
                            data.update({'image' : s3_cover_pic_key})
                            data.update({'attached_file' : s3_attached_file_key})
                    details_list[0].update({'first_partner_details' : first_partner_details})
                    details_list[0].update({'first_partner_posts' : learning_post_details})
                    # details_list[0]['first_partner_details'].append(learning_post_details)
                    details_list[0].update({'total_count': total_count})
                    details_list[0].update({'filter_parameters' : filter_parameters})

                    result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                else:
                    filter_params = [{'filter_parameters' : filter_parameters, "first_partner_details" : {}, "partner_short_desc" : [], "total_count" : 0}]
                    result_json = api_json_response_format(True,"No partner profile found",0,filter_params)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def s3_exists(s3_bucket, s3_key):
    
    try:
        s3_cient = s3_obj.get_s3_client()
        s3_cient.head_object(Bucket=s3_bucket,Key=s3_key)
        return True
    except Exception as e:
        print("s3_exists error : "+str(e))
        return False
    
def match_profiles(search_text, user_profiles):
    batch_size = 50
    filtered_profiles = []
    if s3_exists(BUCKET_NAME,"admin_search_prompt.json"):     
            s3_resource = s3_obj.get_s3_resource()
            obj = s3_resource.Bucket(BUCKET_NAME).Object("admin_search_prompt.json")
            json_file_content = obj.get()['Body'].read().decode('utf-8')        
            prompt_json = json.loads(json_file_content)
            level_1 = prompt_json["level_1"]
            level_1_prompt = level_1["prompt"]      
            level_1_prompt = level_1_prompt.replace("{{search_text}}", search_text)

    for i in range(0, len(user_profiles), batch_size):
        batch = user_profiles[i:i+batch_size]
        prompt = level_1_prompt.replace("{{batch}}", str(batch))
        # prompt = f"""Match user-provided search text with the most relevant and potentially relevant profiles from the provided list.

        #             Guidelines to filter profiles:
        #             - Search query will be matched against fields like job title, skills, location, and experience in each profile.
        #             - Include profiles where the search text matches fully or partially with the fields.
        #             - For potential matches, consider profiles where the fields suggest relevance to the search query, even if there's no direct match (e.g., related skills, similar job titles, or overlapping locations).
        #             - Ensure the matching is case-insensitive and prioritize relevance when multiple matches are found.

        #             Output Requirements:
        #             - The output must strictly be in valid JSON format.
        #             - Escape all necessary characters to ensure JSON validity.
        #             - Return only the filtered and potentially relevant profiles as a JSON array containing the complete details of matched profiles.
        #             - Store the output in a variable called `user_details`. The variable should contain an array of objects.
        #             - Do not include any additional text, comments, or explanation after the JSON output.

        #             Inputs:
        #             1. Search Text: {search_text}
        #             2. Profiles: {batch}

        #             Example output:
        #             {{
        #             "user_details": [{{"user_id" : <user_id>}}] 
        #             ]
        #             }}"""
    
        req_messages = [{"role": "user", "content": prompt}]
        try:
            client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            response = client.chat.completions.create(
                model=g_summary_model_name,
                messages=req_messages,
                max_tokens=g_openai_completion_token_limit,
                temperature=0,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )
            output = response.choices[0].message.content
            clean_code = output.replace('```json\n', '').replace('\n```', '').replace('\n','').replace('\'','').replace('\\\\n','')
            batch_results = json.loads(clean_code)["user_details"]
            filtered_profiles.extend(batch_results)
        except Exception as e:
            print(f"Error in match_profiles(): {e}")
            return {"error": str(e)}
    return {"user_details": filtered_profiles}

def get_search_query(search_text):
        get_prompt_query = 'select attribute_value from payment_config where attribute_name = %s'
        get_prompt_values = ('search_query_prompt',)
        search_text_prompt_dict = execute_query(get_prompt_query, get_prompt_values)
        search_text_prompt = ''
        if search_text_prompt_dict:
            search_text_prompt = search_text_prompt_dict[0]['attribute_value']
        prompt = search_text_prompt.replace('{search_text}', search_text)
        req_messages = [{"role": "user", "content": prompt}]

        search_model_name = 'gpt-4o'
        try:
            client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            response = client.chat.completions.create(
                model=search_model_name,
                messages=req_messages,
                max_tokens=g_openai_completion_token_limit,
                temperature=0,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )
            output = response.choices[0].message.content
            output = output.replace('\n','').replace('\\\'','').replace('"','')
            print(output)
        except Exception as e:
            print(f"Error in match_profiles(): {e}")
            return {"error": str(e)}
        return {"search_text": output}

def admin_professional_meilisearch_filter_results():
    try:
        result_json = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])  
            user_id = user_data['user_id']          
            if user_data["user_role"] == "admin":
                req_data = request.get_json()

                search_text = req_data["search_text"]
                get_search_query_flag = 'select attribute_value from payment_config where attribute_name = %s'
                get_search_query_flag_values = ('search_query_flag',)
                search_query_flag_dict = execute_query(get_search_query_flag, get_search_query_flag_values)
                search_query_flag = 'N'
                if search_query_flag_dict:
                    search_query_flag = search_query_flag_dict[0]['attribute_value']
                if search_text and search_query_flag == 'Y':
                    search_query_value = get_search_query(search_text)
                    search_text = search_query_value['search_text']
                
                get_flag_query = 'select attribute_value from payment_config where attribute_name = %s'
                get_flag_values = ('open_ai_search_flag',)
                flag_value_dict = execute_query(get_flag_query, get_flag_values)
                open_ai_flag = 'N'
                if flag_value_dict:
                    open_ai_flag = flag_value_dict[0]['attribute_value']
                if open_ai_flag == 'Y':
                    result_json = admin_professional_meilisearch_filter_results_new(request)
                    return result_json
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                check_entries_in_db = "select count(id) from super_admin_search_data where admin_id = %s"
                values = (user_id,)
                is_entries_presents_dict = execute_query(check_entries_in_db, values)
                
                page_number = req_data["page_number"]
                offset = (page_number - 1) * 10
                location = req_data["location"]
                skill_value = req_data["skills"]
                profile_percentage = req_data["profile_percentage"]
                plan = req_data["plan"]
                gender = req_data['gender']
                industry_sector = req_data['industry_sector']
                sector = req_data['sector']
                job_type = req_data['job_type']
                willing_to_relocate = req_data['willing_to_relocate']
                mode_of_communication = req_data['mode_of_communication']
                location_preference = req_data['location_preference']
                functional_specification = req_data['functional_specification']
                # years_of_experience = req_data['years_of_experience']
                flag = req_data["flag"]
                pagination_flag = req_data["pagination_flag"]

                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                
                client = Client(meilisearch_url, master_key)
                index = client.index(meilisearch_professional_index)

                skills_filter = skill_value
                country_filter = country
                city_filter = city
                profile_percentage_filter = ''
                if profile_percentage:
                    profile_percentage_filter = profile_percentage
                plan_filter = plan
                gender_filter = gender
                industry_sector_filter =  industry_sector
                sector_filter =  sector
                job_type_filter =  job_type
                willing_to_relocate_filter = willing_to_relocate
                mode_of_communication_filter = mode_of_communication
                location_preference_filter = location_preference  
                functional_specification_filter =  functional_specification
                # years_of_experience_filter =  years_of_experience

                skill_list = ["Business Strategy", "Change Management", "Conflict Resolution", "Financial Management", "Human Resource Management", "Operations Management", "Organizational Development", "Strategic Planning", "Supply Chain Management", "Talent Management", "Human Resource Management", "Direct Sales", "Leadership Skills", "Market Research", "Negotiation", "Presentation", "Product Knowledge", "Recruiting", "Sales and Budget Forecasting", "Upselling", "Agile Methodologies", "Budgeting", "Contract Management Skills", "Earned Value Management", "Process Improvement", "Risk Assessment", "Analytics", "Data Analysis", "Metrics and KPIs", "Project Management", "Revenue Expansion", "SaaS Knowledge", "Salesforce", "Team Leadership", "Marketing Skills", "Keyword Research", "Algorithm Design", "Application Programming Interfaces (APIs)", "Database Design", "Debugging", "Mobile Application Development", "Quality Assurance (QA)"]
                # industry_sector_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # sector_list = ["Academic", "Corporate", "Non-profit", "Startup"]
                functional_specification_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite and Board"]

                filters = []

                query = "select count(user_id) as total_count from users where user_role_fk = %s"
                values = (3,)
                prof_count = execute_query(query, values)

                if prof_count:
                    total_limit = prof_count[0]['total_count']

                else:
                    total_limit = 2000
                
                # if skills_filter:
                #     skills_query = " OR ".join([f"skill_name = '{skill}'" for skill in skills_filter])
                #     filters.append(f"({skills_query})")
                if flag != 'filter_in_search':
                    if page_number == 1 and pagination_flag != 'first_page':
                        if is_entries_presents_dict:
                            if is_entries_presents_dict[0]['count(id)'] > 0:
                                delete_query = "delete from super_admin_search_data where admin_id = %s"
                                values = (user_id,)
                                update_query(delete_query, values)
                        if skills_filter:
                            if "others" in skills_filter or "Others" in skills_filter:
                                excluded_skills_query = " AND ".join([f"NOT skill_name = '{skill}'" for skill in skill_list])
                                other_skills_query = f"({excluded_skills_query}) AND skill_name != '' AND skill_name IS NOT NULL"
                                skills_filter = [skill for skill in skills_filter if skill != "others" or skill != "Others"]
                                if skills_filter:
                                    specific_skills_query = " OR ".join([f"skill_name = '{skill}'" for skill in skills_filter])
                                    filters.append(f"(({specific_skills_query}) OR ({other_skills_query}))")
                                else:
                                    filters.append(f"({other_skills_query})")
                            else:
                                skills_query = " OR ".join([f"skill_name = '{skill}'" for skill in skills_filter])
                                filters.append(f"({skills_query})")

                        if country_filter:
                            country_query = " OR ".join([f"country = '{country}'" for country in country_filter])
                            filters.append(f"({country_query})")

                        if city_filter:
                            city_query = " OR ".join([f"city = '{city}'" for city in city_filter])
                            filters.append(f"({city_query})")
                        
                        if profile_percentage_filter:
                            # profile_query = " OR ".join([f"profile_percentage = {percentage}" for percentage in profile_percentage_filter])
                            # filters.append(f"({profile_query})")
                            range_start, range_end = map(int, profile_percentage_filter.split("-"))
                            profile_query = f"profile_percentage >= {range_start} AND profile_percentage <= {range_end}"
                            filters.append(f"({profile_query})")

                        if plan_filter:
                            plan_query = " OR ".join([f"pricing_category = '{plan}'" for plan in plan_filter])
                            filters.append(f"({plan_query})")

                        # if gender_filter:
                        #     gender_query = " OR ".join([f"gender = '{gender}'" for gender in gender_filter])
                        #     filters.append(f"({gender_query})")

                        if gender_filter:
                            if isinstance(gender_filter, str):  # Check if gender_filter is a single string
                                gender_filter = [gender_filter]  # Convert to a list for uniform handling
                            gender_query = " OR ".join([f"gender = '{gender}'" for gender in gender_filter])
                            filters.append(f"({gender_query})")
                        
                        if industry_sector_filter:
                            industry_sector_query = " OR ".join([f"industry_sector = '{industry_sector}'" for industry_sector in industry_sector_filter])
                            filters.append(f"({industry_sector_query})")

                        if sector_filter:
                            sector_query = " OR ".join([f"sector = '{sector}'" for sector in sector_filter])
                            filters.append(f"({sector_query})")
                        
                        if job_type_filter:
                            job_type_query = " OR ".join([f"job_type = '{job_type}'" for job_type in job_type_filter])
                            filters.append(f"({job_type_query})")
                        
                        if willing_to_relocate_filter:
                            willing_to_relocate_query = " OR ".join([f"willing_to_relocate = '{willing_to_relocate}'" for willing_to_relocate in willing_to_relocate_filter])
                            filters.append(f"({willing_to_relocate_query})")

                        # if mode_of_communication_filter:
                        #     mode_of_communication_query = " OR ".join([f"mode_of_communication = '{mode_of_communicateion}'" for mode_of_communicateion in mode_of_communication_filter])
                        #     filters.append(f"({mode_of_communication_query})")

                        # if mode_of_communication_filter:
                        #     if "others" in mode_of_communication_filter or "Others" in mode_of_communication_filter:
                        #         excluded_modes = ["Email", "Whatsapp", "Text message", "Phone"]
                        #         # other_modes_query = " AND ".join([f"mode_of_communication != '{mode}'" for mode in excluded_modes])
                        #         other_modes_query = (
                        #             " AND ".join([f"mode_of_communication != '{mode}'" for mode in excluded_modes]) +
                        #             " AND mode_of_communication != '' AND mode_of_communication IS NOT NULL"
                        #         )
                        #         mode_of_communication_filter = [mode for mode in mode_of_communication_filter if mode != "others"]
                        #         if mode_of_communication_filter:
                        #             mode_of_communication_query = " OR ".join([f"mode_of_communication = '{mode}'" for mode in mode_of_communication_filter])
                        #             filters.append(f"(({mode_of_communication_query}) OR ({other_modes_query}))")
                        #         else:
                        #             filters.append(f"({other_modes_query})")
                        #     else:
                        #         mode_of_communication_query = " OR ".join([f"mode_of_communication = '{mode}'" for mode in mode_of_communication_filter])
                        #         filters.append(f"({mode_of_communication_query})")

                        if mode_of_communication_filter:
                            if "others" in mode_of_communication_filter or "Others" in mode_of_communication_filter:
                                excluded_modes = ["Email", "Whatsapp", "Text message", "Phone"]
                                other_modes_query = (
                                    " AND ".join([f"NOT mode_of_communication CONTAINS '{mode}'" for mode in excluded_modes]) +
                                    " AND mode_of_communication != '' AND mode_of_communication IS NOT NULL"
                                )
                                mode_of_communication_filter = [mode for mode in mode_of_communication_filter if mode != "others" or mode != "Others"]
                                if mode_of_communication_filter:
                                    mode_of_communication_query = " OR ".join([f"mode_of_communication CONTAINS '{mode}'" for mode in mode_of_communication_filter])
                                    filters.append(f"(({mode_of_communication_query}) OR ({other_modes_query}))")
                                else:
                                    filters.append(f"({other_modes_query})")
                            else:
                                mode_of_communication_query = " OR ".join([f"mode_of_communication CONTAINS '{mode}'" for mode in mode_of_communication_filter])
                                filters.append(f"({mode_of_communication_query})")

                        if location_preference_filter:
                            location_preference_query = " OR ".join([f"location_preference = '{location_preference}'" for location_preference in location_preference_filter])
                            filters.append(f"({location_preference_query})")

                        # if functional_specification_filter:
                        #     functional_specification_query = " OR ".join([f"functional_specification = '{functional_specification}'" for functional_specification in functional_specification_filter])
                        #     filters.append(f"({functional_specification_query})")

                        if functional_specification_filter:
                            if "others" in functional_specification_filter or "Others" in functional_specification_filter:
                                excluded_specifications_query = " AND ".join([f"NOT functional_specification = '{spec}'" for spec in functional_specification_list])
                                other_specifications_query = f"({excluded_specifications_query}) AND functional_specification != '' AND functional_specification IS NOT NULL"
                                functional_specification_filter = [spec for spec in functional_specification_filter if spec != "others" or spec != "Others"]
                                if functional_specification_filter:
                                    specific_specifications_query = " OR ".join([f"functional_specification = '{spec}'" for spec in functional_specification_filter])
                                    filters.append(f"(({specific_specifications_query}) OR ({other_specifications_query}))")
                                else:
                                    filters.append(f"({other_specifications_query})")
                            else:
                                functional_specification_query = " OR ".join([f"functional_specification = '{spec}'" for spec in functional_specification_filter])
                                filters.append(f"({functional_specification_query})")
                        
                        filters.append("(email_active = 'Y')")
                        filters.append("(user_role_fk = 3)")

                        final_filters = " AND ".join(filters)  # Joining all filters with AND or OR logic.

                        # filters = f"skills = '{skills_filter}' OR country = '{country_filter}' OR city = '{city_filter}' OR profile_percentage = '{profile_percentage_filter}' OR plan = '{plan_filter}'"              
                        # filters = f"genres = '{genre_filter}' AND id = '{id_filter}'"
        
                        # Perform the search
                        # results = index.search(
                        #     search_text,
                        #     {
                        #         'filter': final_filters,
                        #         # 'sort': ['user_id:desc'],
                        #         'limit': 10,
                        #         'offset': offset,
                        #         'showRankingScore': True
                        #         # 'attributesToHighlight': ['title']
                        #     }
                        # )
                        get_semantic_score_query = "select attribute_value from payment_config where attribute_name = %s;"
                        get_semantic_score_values = ("semantic_score",)
                        semantic_value_dict = execute_query(get_semantic_score_query, get_semantic_score_values)
                        if semantic_value_dict:
                            semantic_score = float(semantic_value_dict[0]["attribute_value"])
                            if semantic_score == 1:
                                semantic_score = int(semantic_score)
                        else:
                            semantic_score = 0
                        results = index.search(
                            search_text,
                            {
                                "hybrid": {
                                    "semanticRatio": semantic_score,
                                    "embedder": "2c"
                                },
                                "filter": final_filters,
                                "limit": total_limit,
                                # "offset": offset,
                                'showRankingScore': True
                            }
                        )
                        ranking_score_limit = 0
                    

                        if results['hits']:
                            if results['hits'][0]['_rankingScore'] > 0.15:
                                ranking_score_limit = results['hits'][0]['_rankingScore'] - 0.15
                        filtered_results = [hit for hit in results['hits'] if ranking_score_limit <= hit.get('_rankingScore', 0) <= 1]
                        # filtered_results = [hit for hit in results['hits'] if 0.65 <= hit.get('_rankingScore', 0) <= 1]
                        # fetched_data = results['hits']
                        # total_count = results['estimatedTotalHits']
                        result_user_ids = [id['user_id'] for id in filtered_results]
                        if result_user_ids:
                            get_users_details_query = """WITH LatestExperience AS (SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, 
                                                        ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY COALESCE(pe.start_year, '0000-00') DESC, COALESCE(pe.start_month, '00') DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe )
                                                        SELECT u.user_id, u.first_name, u.last_name, u.country, u.city, u.profile_percentage, u.pricing_category, u.payment_status, u.gender, u.is_active,
                                                        p.expert_notes, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.years_of_experience,
                                                        p.professional_id,
                                                        COALESCE(GROUP_CONCAT(DISTINCT ps.skill_name SEPARATOR ', '), '') AS skills,
                                                        COALESCE(le.job_title, '') AS job_title 
                                                        FROM users AS u
                                                        LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id
                                                        LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                                                        LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id 
                                                        WHERE u.user_id IN %s 
                                                        GROUP BY u.user_id, u.first_name, u.last_name, u.country, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.gender,
                                                        p.professional_id, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate,
                                                        p.expert_notes, p.years_of_experience, le.job_title, le.created_at"""
                            get_users_details_values = (tuple(result_user_ids),)
                            users_details_dict = execute_query(get_users_details_query, get_users_details_values)
                        else:
                            result_json = api_json_response_format(True, "No records found", 0, {})
                            return result_json
                        
                        def get_ranking_score(id, results):
                            for result in results:
                                if result['user_id'] == id:
                                    return result['_rankingScore']
                            return 0  
                        
                        if users_details_dict:
                            for record in users_details_dict:
                                ranking_score = get_ranking_score(record['user_id'], filtered_results)
                                record_insert_query = 'INSERT INTO super_admin_search_data (admin_id, user_id, professional_id, first_name, last_name, profile_percentage, gender, city, country, pricing_category, payment_status, job_title, skills, expert_notes, functional_specification, industry_sector, job_type, sector, location_preference, mode_of_communication, willing_to_relocate, years_of_experience, is_active, flag, ranking_score) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                                professional_id = '2C-PR-' + str(record['user_id'])
                                values = (user_id,
                                record['user_id'], professional_id, record['first_name'], record['last_name'], record['profile_percentage'], record['gender'], 
                                record['city'], record['country'], record['pricing_category'], 
                                record['payment_status'], record['job_title'], record['skills'], record['expert_notes'], 
                                record['functional_specification'], record['industry_sector'], 
                                record['job_type'], record['sector'], record['location_preference'], 
                                record['mode_of_communication'], record['willing_to_relocate'], 
                                record['years_of_experience'], record['is_active'], 'sample', ranking_score,)
                                update_query(record_insert_query, values)
                    get_count_query = 'select count(user_id) as count from super_admin_search_data where admin_id = %s'
                    count_values = (user_id,)
                    total_count_dict = execute_query(get_count_query, count_values)
                    if total_count_dict:
                        total_count = total_count_dict[0]['count']
                    else:
                        total_count = 0
                    get_user_ids = 'select user_id from super_admin_search_data where admin_id = %s ORDER BY ranking_score DESC, user_id DESC LIMIT 10 OFFSET %s'
                    values = (user_id, offset,)
                    result_user_ids_dict = execute_query(get_user_ids, values)
                    if result_user_ids_dict:
                        result_user_ids = [id['user_id'] for id in result_user_ids_dict]
                    else:
                        result_user_ids = []
                else:
                    if is_entries_presents_dict:
                        if is_entries_presents_dict[0]['count(id)'] == 0:
                            result_json = api_json_response_format(True,"No records found",0,{})
                            return result_json
                        else:
                            conditions = []
                            values_job_details = []
                            # if req_data['skills']:
                            #     if "Others" in req_data['skills']:
                            #         conditions.append("skills NOT IN %s")
                            #         values_job_details.append(tuple(skill_list),)
                            #     else:
                            #         conditions.append("skills IN %s")
                            #         values_job_details.append(tuple(skill_value),)
                            values_job_details.append((user_id,))
                            if req_data['skills']:
                                if "Others" in req_data['skills']:
                                    skill_conditions = ["skills NOT LIKE %s" for _ in skill_list]
                                    conditions.append(f"({' AND '.join(skill_conditions)})")
                                    for skill in skill_list:
                                        values_job_details.append(f"%{skill}%")
                                else:
                                    skill_conditions = ["skills LIKE %s" for _ in skill_value]
                                    conditions.append(f"({' OR '.join(skill_conditions)})")
                                    for skill in skill_value:
                                        values_job_details.append(f"%{skill}%")
                            if req_data['functional_specification']:
                                if "Others" in req_data['functional_specification']:
                                    conditions.append("functional_specification NOT IN %s")
                                    values_job_details.append(tuple(functional_specification_list),)
                                else:
                                    conditions.append("functional_specification IN %s")
                                    values_job_details.append(tuple(req_data['functional_specification']),)
                            if country:
                                conditions.append("country IN %s")
                                values_job_details.append(tuple(country),)    
                            if city:
                                conditions.append("city IN %s")
                                values_job_details.append(tuple(city),)
                            if 'profile_percentage' in req_data and req_data['profile_percentage']:
                                percentage_range = req_data['profile_percentage']
                                if percentage_range == "0-30":
                                    conditions.append("profile_percentage BETWEEN 0 AND 30")
                                elif percentage_range == "30-60":
                                    conditions.append("profile_percentage BETWEEN 30 AND 60")
                                elif percentage_range == "60-100":
                                    conditions.append("profile_percentage BETWEEN 60 AND 100")
                                elif percentage_range == "30-100":
                                    conditions.append("profile_percentage BETWEEN 30 AND 100")
                            if req_data['plan']:
                                conditions.append("pricing_category IN %s")
                                values_job_details.append(tuple(req_data['plan'],))
                            if req_data['gender']:
                                conditions.append("gender IN %s")
                                values_job_details.append(tuple(req_data['gender'],))
                            if req_data['industry_sector']:
                                conditions.append("industry_sector IN %s")
                                values_job_details.append(tuple(req_data['industry_sector'],))
                            if req_data['sector']:
                                conditions.append("sector IN %s")
                                values_job_details.append(tuple(req_data['sector'],))
                            if req_data['job_type']:
                                conditions.append("job_type IN %s")
                                values_job_details.append(tuple(req_data['job_type'],))
                            if req_data['willing_to_relocate']:
                                conditions.append("willing_to_relocate IN %s")
                                values_job_details.append(tuple(req_data['willing_to_relocate'],))
                            # if req_data['mode_of_communication']:
                            #     conditions.append("mode_of_communication IN %s")
                            #     values_job_details.append(tuple(req_data['mode_of_communication'],))
                            if req_data['mode_of_communication']:
                                moc_conditions = ["mode_of_communication LIKE %s" for _ in mode_of_communication]
                                conditions.append(f"({' OR '.join(moc_conditions)})")
                                for moc in mode_of_communication:
                                    values_job_details.append(f"%{moc}%")
                            if req_data['location_preference']:
                                conditions.append("location_preference IN %s")
                                values_job_details.append(tuple(req_data['location_preference'],))
                            search_query = '''SELECT 
                                            user_id, first_name, last_name, gender, city, country, pricing_category, profile_percentage,
                                            payment_status, job_title, skills, expert_notes, functional_specification, 
                                            industry_sector, job_type, sector, location_preference, mode_of_communication, 
                                            willing_to_relocate, years_of_experience, is_active, flag, created_at
                                        FROM 
                                            super_admin_search_data
                                        WHERE admin_id IN %s
                                        '''
                            if conditions:
                                # if len(conditions) == 1:
                                #     search_query += conditions[0]
                                # else:
                                    # search_query += " (" + " AND ".join(conditions) + ")"
                                    search_query += " AND " + " AND ".join(conditions)
                            new_query = search_query + " ORDER BY ranking_score DESC LIMIT 10 OFFSET %s"
                            val_detail = values_job_details
                            total_count_query = "SELECT subquery.user_id, COUNT(*) AS total_count FROM (" + search_query + ") AS subquery GROUP BY subquery.user_id"
                            total_count_values = (val_detail)
                            total_count = execute_query(total_count_query, total_count_values)
                            if len(total_count) > 0:
                                total_count = len(total_count)
                            else:
                                total_count = 0
                            # values_job_details.append(offset,)
                            values_job_details = values_job_details + [offset]
                            professional_details = replace_empty_values(execute_query(new_query, values_job_details))
                            if professional_details:
                                result_user_ids = [id['user_id'] for id in professional_details]
                            else:
                                result_user_ids = []
                    else:
                        result_json = api_json_response_format(True,"No records found",0,{})
                        return result_json
                # print(result_user_ids)
                prof_details = []
                if result_user_ids:
                    for temp_id in result_user_ids:
                        query_job_details = """
                                    WITH LatestExperience AS (
                                            SELECT
                                                pe.professional_id,
                                                pe.id AS experience_id,
                                                pe.start_month,
                                                pe.start_year,
                                                pe.job_title,
                                                pe.created_at,
                                                ROW_NUMBER() OVER (
                                                    PARTITION BY pe.professional_id
                                                    ORDER BY
                                                        COALESCE(pe.start_year, '0000-00') DESC,
                                                        COALESCE(pe.start_month, '00') DESC,
                                                        pe.created_at DESC
                                                ) AS rn
                                            FROM
                                                professional_experience AS pe
                                        )
                                        SELECT
                                            u.user_id,
                                            u.first_name,
                                            u.last_name,
                                            u.country,
                                            u.state,
                                            u.city,
                                            u.profile_percentage,
                                            u.pricing_category,
                                            u.is_active,
                                            u.payment_status,
                                            u.gender,
                                            (
                                                SELECT CAST(ROUND(sa2.ranking_score, 4) AS DECIMAL(10,4))
                                                FROM super_admin_search_data sa2
                                                WHERE sa2.user_id = u.user_id
                                                ORDER BY sa2.updated_at DESC
                                                LIMIT 1
                                            ) AS ranking_score,
                                            p.years_of_experience,
                                            p.functional_specification,
                                            p.sector,
                                            p.industry_sector,
                                            p.job_type,
                                            p.location_preference,
                                            p.mode_of_communication,
                                            p.willing_to_relocate,
                                            p.professional_id,
                                            p.expert_notes,
                                            p.created_at AS posted_at,
                                            COALESCE(le.job_title, '') AS job_title,
                                            COALESCE(le.created_at, '') AS experience_created_at,
                                            u2.profile_image
                                        FROM
                                            users AS u
                                        LEFT JOIN
                                            professional_profile AS p ON u.user_id = p.professional_id
                                        LEFT JOIN
                                            LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                                        LEFT JOIN
                                            users AS u2 ON u.user_id = u2.user_id
                                        LEFT JOIN 
                                            professional_skill AS ps ON u.user_id = ps.professional_id 
                                        LEFT JOIN 
                                            super_admin_search_data AS sa ON u.user_id = sa.user_id
                                        WHERE
                                            u.user_role_fk = 3 AND u.email_active = 'Y' AND u.user_id = %s 
                                            GROUP BY u.user_id, u.first_name, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.gender,
                                            p.professional_id, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.expert_notes, p.years_of_experience, p.created_at, 
                                            le.job_title, le.created_at, u2.profile_image 
                        """        #ORDER BY p.professional_id DESC
                        values_job_details = (temp_id,)
                        temp_result = replace_empty_values(execute_query(query_job_details, values_job_details))
                        if temp_result:
                            prof_details.append(temp_result[0])
                else:
                    prof_details = []

                if len(prof_details) > 0:
                    mod_id = '2C-PR-'+ str(prof_details[0]['professional_id'])
                    first_id = prof_details[0]['professional_id']
                    if isUserExist("professional_profile","professional_id",first_id):
                            query = """SELECT 
                                            u.first_name, 
                                            u.last_name, 
                                            u.email_id, 
                                            u.dob, 
                                            u.country_code, 
                                            u.contact_number, 
                                            u.country, 
                                            u.state, 
                                            u.city, 
                                            u.profile_percentage,
                                            u.pricing_category, 
                                            u.is_active,
                                            u.payment_status,
                                            u.gender,
                                            p.years_of_experience,
                                            p.functional_specification,
                                            p.sector, p.industry_sector,
                                            p.job_type,
                                            p.location_preference,
                                            p.mode_of_communication,
                                            p.willing_to_relocate,
                                            p.professional_id,
                                            p.professional_resume,
                                            p.expert_notes,
                                            p.about,
                                            p.preferences,
                                            p.video_url, 
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
                                        FROM 
                                            users AS u 
                                        LEFT JOIN 
                                            professional_profile AS p ON u.user_id = p.professional_id 
                                        LEFT JOIN 
                                            professional_experience AS pe ON u.user_id = pe.professional_id 
                                        LEFT JOIN 
                                            professional_education AS ed ON u.user_id = ed.professional_id 
                                        LEFT JOIN 
                                            professional_skill AS ps ON u.user_id = ps.professional_id 
                                        LEFT JOIN 
                                            professional_language AS pl ON u.user_id = pl.professional_id 
                                        LEFT JOIN 
                                            professional_additional_info AS pai ON u.user_id = pai.professional_id 
                                        LEFT JOIN 
                                            professional_social_link AS psl ON u.user_id = psl.professional_id 
                                        LEFT JOIN 
                                            users AS u2 ON u.user_id = u2.user_id 
                                        WHERE 
                                            u.user_id = %s 
                                        ORDER BY 
                                            CASE 
                                                WHEN pe.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(pe.end_year, '0000-00') DESC,
                                            COALESCE(pe.end_month, '00') DESC,
                                            COALESCE(pe.start_year, '0000-00') DESC,
                                            COALESCE(pe.start_month, '00') DESC,
                                            CASE 
                                                WHEN ed.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(ed.end_year, '0000-00') DESC,
                                            COALESCE(ed.end_month, '00') DESC,
                                            COALESCE(ed.start_year, '0000-00') DESC,
                                            COALESCE(ed.start_month, '00') DESC;"""                
                            values = (first_id,)
                            profile_result = execute_query(query, values) 

                            if len(profile_result) > 0:                              
                                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                                intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                                resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                            else:
                                result_json = api_json_response_format(True,"No records found",0,{})     
                                return result_json          
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            if intro_video_name == '':
                                s3_video_key = ""
                            else:
                                s3_video_key = s3_intro_video_folder_name + str(intro_video_name)
                            if resume_name == '':    
                                s3_resume_key = ""
                            else:          
                                s3_resume_key = s3_resume_folder_name + str(resume_name)

                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (first_id, 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(ep.sector, su.sector) AS sector,
                                        COALESCE(ep.company_name, su.company_name) AS company_name,
                                        COALESCE(u.is_active, '') AS is_active
                                        FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            # for prof in prof_details:
                            #     query = 'select job_id from sc_recommendation where professional_id = %s'
                            #     values = (prof['professional_id'],)
                            #     recommended_jobs_id = execute_query(query, values)
                            #     recommended_jobs_list = []
                            #     if len(recommended_jobs_id) > 0:
                            #         for id in recommended_jobs_id:
                            #             if isUserExist("job_post", "id", id['job_id']):
                            #                 query = 'SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, ep.company_name, ep.sector FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id WHERE jp.id = %s'
                            #                 values = (id['job_id'],)
                            #                 detail = execute_query(query, values)
                            #                 txt = detail[0]['sector']
                            #                 txt = txt.replace(", ", "_")
                            #                 txt = txt.replace(" ", "_")
                            #                 sector_name = txt + ".png"
                            #                 detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                            #                 recommended_jobs_list.append(detail[0])
                            #     else:
                            #         recommended_jobs_list = []
                            #     prof.update({'recommended_jobs' : recommended_jobs_list})
                            profile_dict = {
                                'first_name': replace_empty_values1(profile_result[0]['first_name']),
                                'last_name': replace_empty_values1(profile_result[0]['last_name']),
                                'professional_id' : mod_id,                                        
                                'email_id': replace_empty_values1(profile_result[0]['email_id']),
                                'dob': replace_empty_values1(profile_result[0]['dob']),
                                'country_code': replace_empty_values1(profile_result[0]['country_code']),
                                'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                                'city': replace_empty_values1(profile_result[0]['city']),
                                'profile_percentage': replace_empty_values1(profile_result[0]['profile_percentage']),
                                'state': replace_empty_values1(profile_result[0]['state']),
                                'country': replace_empty_values1(profile_result[0]['country']),
                                'pricing_category' : profile_result[0]['pricing_category'],
                                'is_active' : profile_result[0]['is_active'],
                                'payment_status' : profile_result[0]['payment_status'],
                                'profile_image': s3_pic_key,
                                'video_name': s3_video_key,
                                'resume_name': s3_resume_key,
                                'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                                'about': replace_empty_values1(profile_result[0]['about']),
                                'preferences': replace_empty_values1(profile_result[0]['preferences']),
                                'experience': {},
                                'education': {},
                                'skills': {},
                                'languages': {},
                                'additional_info': {},
                                'social_link': {},
                                'job_list' : {},
                                'recommended_jobs' : recommended_jobs_list,
                                'gender' : replace_empty_values1(profile_result[0]['gender']),
                                'years_of_experience' : replace_empty_values1(profile_result[0]['years_of_experience']),
                                'functional_specification' : replace_empty_values1(profile_result[0]['functional_specification']),
                                'sector' : replace_empty_values1(profile_result[0]['sector']),
                                'industry_sector' : replace_empty_values1(profile_result[0]['industry_sector']),
                                'job_type' : replace_empty_values1(profile_result[0]['job_type']),
                                'location_preference' : replace_empty_values1(profile_result[0]['location_preference']),
                                'mode_of_communication' : replace_empty_values1(profile_result[0]['mode_of_communication']),
                                'willing_to_relocate' : replace_empty_values1(profile_result[0]['willing_to_relocate'])
                            }

                            # Grouping experience data
                            experience_set = set()
                            experience_list = []
                            for exp in profile_result:
                                if exp['experience_id'] is not None:
                                    # start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                                    # end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                                    if exp['experience_start_year'] != None:
                                        start_date = exp['experience_start_year']
                                    else:
                                        start_date = ''
                                    if exp['experience_end_year'] != None:
                                        end_date = exp['experience_end_year']
                                    else:
                                        end_date = ''
                                    exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                                    if exp_tuple not in experience_set:
                                        experience_set.add(exp_tuple)
                                        experience_list.append({
                                            'id': exp['experience_id'],
                                            'company_name': replace_empty_values1(exp['company_name']),
                                            'job_title': replace_empty_values1(exp['job_title']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,                                
                                            'job_description': replace_empty_values1(exp['job_description']),
                                            'job_location': replace_empty_values1(exp['job_location'])
                                        })

                            profile_dict['experience'] = experience_list

                            # Grouping education data
                            education_set = set()
                            education_list = []
                            for edu in profile_result:
                                if edu['education_id'] is not None:
                                    # start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                                    # end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                                    if edu['education_start_year'] != None:
                                        start_date = edu['education_start_year']
                                    else:
                                        start_date = ''
                                    if edu['education_end_year'] != None:
                                        end_date = edu['education_end_year']
                                    else:
                                        end_date = ''
                                    edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                                start_date, end_date, edu['institute_location'])
                                    if edu_tuple not in education_set:
                                        education_set.add(edu_tuple)
                                        education_list.append({
                                            'id': edu['education_id'],
                                            'institute_name': replace_empty_values1(edu['institute_name']),
                                            'degree_level': replace_empty_values1(edu['degree_level']),
                                            'specialisation': replace_empty_values1(edu['specialisation']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,
                                            'institute_location': replace_empty_values1(edu['institute_location'])
                                        })

                            profile_dict['education'] = education_list

                            # Grouping skills data
                            skills_set = set()
                            skills_list = []
                            for skill in profile_result:
                                if skill['skill_id'] is not None:
                                    skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                                    if skill_tuple not in skills_set:
                                        skills_set.add(skill_tuple)
                                        skills_list.append({
                                            'id': skill['skill_id'],
                                            'skill_name': replace_empty_values1(skill['skill_name']),
                                            'skill_level': replace_empty_values1(skill['skill_level'])
                                        })

                            profile_dict['skills'] = skills_list

                            # Grouping languages data
                            languages_set = set()
                            languages_list = []
                            for lang in profile_result:
                                if lang['language_id'] is not None:
                                    lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                                    if lang_tuple not in languages_set:
                                        languages_set.add(lang_tuple)
                                        languages_list.append({
                                            'language_known': replace_empty_values1(lang['language_known']),
                                            'id':  lang['language_id'],
                                            'language_level': replace_empty_values1(lang['language_level'])
                                    })

                            profile_dict['languages'] = languages_list
                            # Grouping additional info data
                            additional_info_set = set()
                            additional_info_list = []
                            for info in profile_result:
                                if info['additional_info_id'] is not None:
                                    info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                                    if info_tuple not in additional_info_set:
                                        additional_info_set.add(info_tuple)
                                        additional_info_list.append({
                                        'id': info['additional_info_id'],
                                        'title': replace_empty_values1(info['additional_info_title']),
                                        'description': replace_empty_values1(info['additional_info_description'])
                                    })

                            profile_dict['additional_info'] = additional_info_list
                            # Grouping social link data
                            social_link_set = set()
                            social_link_list = []
                            for link in profile_result:
                                if link['social_link_id'] is not None:
                                    link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                                    if link_tuple not in social_link_set:
                                        social_link_set.add(link_tuple)
                                        social_link_list.append({
                                        'title': replace_empty_values1(link['social_link_title']),
                                        'id':  link['social_link_id'],
                                        'url': replace_empty_values1(link['social_link_url'])
                                    })

                            profile_dict['social_link'] = social_link_list
                            
                            #Get recommended jobs for the professional
                            for prof in prof_details:
                                old_id = prof['professional_id']
                                temp_id = "2C-PR-" + str(prof['professional_id'])
                                prof.update({"professional_id" : temp_id})
                                query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                                values = (3,old_id,)
                                job_id_list = execute_query(query, values)
                                job_detail_list = []
                                if len(job_id_list) > 0:
                                    for job_id in job_id_list:
                                        id = job_id['job_id']
                                        if isUserExist("job_post", "id", id):
                                            query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city,
                                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                            COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                            COALESCE(ep.sector, su.sector) AS sector,
                                            COALESCE(ep.company_name, su.company_name) AS company_name,
                                            COALESCE(u.is_active, '') AS is_active
                                            FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                            values = (id,)
                                            detail = execute_query(query, values)
                                            if len(detail) > 0:
                                                txt = detail[0]['sector']
                                                txt = txt.replace(", ", "_")
                                                txt = txt.replace(" ", "_")
                                                sector_name = txt + ".png"
                                                detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                                img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                                detail[0].update({'profile_image' : img_key})
                                                job_detail_dict = {"job_title" : detail[0]['job_title'],
                                                                    "country" : detail[0]['country'],
                                                                    "city" : detail[0]['city'],
                                                                    "company_name" : detail[0]['company_name'],
                                                                    "sector_image" : detail[0]['sector_image'],
                                                                    "sector" : detail[0]['sector'],
                                                                    "profile_image" : detail[0]['profile_image'],
                                                                    "employer_id" : detail[0]['employer_id'],
                                                                    "job_id" : detail[0]['job_id'],
                                                                    "pricing_category" : detail[0]['pricing_category'],
                                                                    "is_active" : detail[0]['is_active'],
                                                                    "payment_status" : detail[0]['payment_status']}
                                                job_detail_list.append(job_detail_dict)
                                else:
                                    job_detail_list = []
                                s3_prof_pic_key = s3_picture_folder_name + prof['profile_image']
                                prof.update({'profile_image' : s3_prof_pic_key})
                                prof.update({'recommended_jobs' : job_detail_list})
                            profile_dict['professional_details'] = prof_details
                            profile_dict.update({'total_count' : total_count})
                            result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)                    
                    else:
                        result_json = api_json_response_format(False,"User profile not found",500,{})
                else:
                    result_json = api_json_response_format(True,"No records found",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def admin_professional_meilisearch_filter_results_new(request):
    try:
        result_json = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                user_id = user_data['user_id']
                check_entries_in_db = "select count(id) from super_admin_search_data where admin_id = %s"
                values = (user_id,)
                is_entries_presents_dict = execute_query(check_entries_in_db, values)
                
                page_number = req_data["page_number"]
                offset = (page_number - 1) * 10
                location = req_data["location"]
                skill_value = req_data["skills"]
                profile_percentage = req_data["profile_percentage"]
                plan = req_data["plan"]
                gender = req_data['gender']
                industry_sector = req_data['industry_sector']
                sector = req_data['sector']
                job_type = req_data['job_type']
                willing_to_relocate = req_data['willing_to_relocate']
                mode_of_communication = req_data['mode_of_communication']
                location_preference = req_data['location_preference']
                functional_specification = req_data['functional_specification']
                # years_of_experience = req_data['years_of_experience']
                flag = req_data['flag']
                pagination_flag = req_data['pagination_flag']

                search_text = req_data["search_text"]
                get_search_query_flag = 'select attribute_value from payment_config where attribute_name = %s'
                get_search_query_flag_values = ('search_query_flag',)
                search_query_flag_dict = execute_query(get_search_query_flag, get_search_query_flag_values)
                search_query_flag = 'N'
                if search_query_flag_dict:
                    search_query_flag = search_query_flag_dict[0]['attribute_value']
                if search_query_flag == 'Y':
                    search_query_value = get_search_query(search_text)
                    search_text = search_query_value['search_text']

                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                skill_list = ["Business Strategy", "Change Management", "Conflict Resolution", "Financial Management", "Human Resource Management", "Operations Management", "Organizational Development", "Strategic Planning", "Supply Chain Management", "Talent Management", "Human Resource Management", "Direct Sales", "Leadership Skills", "Market Research", "Negotiation", "Presentation", "Product Knowledge", "Recruiting", "Sales and Budget Forecasting", "Upselling", "Agile Methodologies", "Budgeting", "Contract Management Skills", "Earned Value Management", "Process Improvement", "Risk Assessment", "Analytics", "Data Analysis", "Metrics and KPIs", "Project Management", "Revenue Expansion", "SaaS Knowledge", "Salesforce", "Team Leadership", "Marketing Skills", "Keyword Research", "Algorithm Design", "Application Programming Interfaces (APIs)", "Database Design", "Debugging", "Mobile Application Development", "Quality Assurance (QA)"]
                # industry_sector_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # sector_list = ["Academic", "Corporate", "Non-profit", "Startup"]
                functional_specification_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite and Board"]
                if flag != 'filter_in_search':
                    if page_number == 1 and pagination_flag != 'first_page':
                        if is_entries_presents_dict:
                            if is_entries_presents_dict[0]['count(id)'] > 0:
                                delete_query = "delete from super_admin_search_data where admin_id = %s"
                                values = (user_id,)
                                update_query(delete_query, values)
                        client = Client(meilisearch_url, master_key)
                        index = client.index(meilisearch_professional_index)

                        skills_filter = skill_value
                        country_filter = country
                        city_filter = city
                        profile_percentage_filter = ''
                        if profile_percentage:
                            profile_percentage_filter = profile_percentage
                        plan_filter = plan
                        gender_filter = gender
                        industry_sector_filter =  industry_sector
                        sector_filter =  sector
                        job_type_filter =  job_type
                        willing_to_relocate_filter = willing_to_relocate
                        mode_of_communication_filter = mode_of_communication
                        location_preference_filter = location_preference  
                        functional_specification_filter =  functional_specification
                        # years_of_experience_filter =  years_of_experience

                        # skill_list = ["Business Strategy", "Change Management", "Conflict Resolution", "Financial Management", "Human Resource Management", "Operations Management", "Organizational Development", "Strategic Planning", "Supply Chain Management", "Talent Management", "Human Resource Management", "Direct Sales", "Leadership Skills", "Market Research", "Negotiation", "Presentation", "Product Knowledge", "Recruiting", "Sales and Budget Forecasting", "Upselling", "Agile Methodologies", "Budgeting", "Contract Management Skills", "Earned Value Management", "Process Improvement", "Risk Assessment", "Analytics", "Data Analysis", "Metrics and KPIs", "Project Management", "Revenue Expansion", "SaaS Knowledge", "Salesforce", "Team Leadership", "Marketing Skills", "Keyword Research", "Algorithm Design", "Application Programming Interfaces (APIs)", "Database Design", "Debugging", "Mobile Application Development", "Quality Assurance (QA)"]
                        # # industry_sector_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                        # # sector_list = ["Academic", "Corporate", "Non-profit", "Startup"]
                        # functional_specification_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite and Board"]

                        filters = []

                        if skills_filter:
                            if "others" in skills_filter or "Others" in skills_filter:
                                excluded_skills_query = " AND ".join([f"NOT skill_name = '{skill}'" for skill in skill_list])
                                other_skills_query = f"({excluded_skills_query}) AND skill_name != '' AND skill_name IS NOT NULL"
                                skills_filter = [skill for skill in skills_filter if skill != "others" or skill != "Others"]
                                if skills_filter:
                                    specific_skills_query = " OR ".join([f"skill_name = '{skill}'" for skill in skills_filter])
                                    filters.append(f"(({specific_skills_query}) OR ({other_skills_query}))")
                                else:
                                    filters.append(f"({other_skills_query})")
                            else:
                                skills_query = " OR ".join([f"skill_name = '{skill}'" for skill in skills_filter])
                                filters.append(f"({skills_query})")

                        if country_filter:
                            country_query = " OR ".join([f"country = '{country}'" for country in country_filter])
                            filters.append(f"({country_query})")

                        if city_filter:
                            city_query = " OR ".join([f"city = '{city}'" for city in city_filter])
                            filters.append(f"({city_query})")
                        
                        if profile_percentage_filter:
                            # profile_query = " OR ".join([f"profile_percentage = {percentage}" for percentage in profile_percentage_filter])
                            # filters.append(f"({profile_query})")
                            range_start, range_end = map(int, profile_percentage_filter.split("-"))
                            profile_query = f"profile_percentage >= {range_start} AND profile_percentage <= {range_end}"
                            filters.append(f"({profile_query})")

                        if plan_filter:
                            plan_query = " OR ".join([f"pricing_category = '{plan}'" for plan in plan_filter])
                            filters.append(f"({plan_query})")

                        # if gender_filter:
                        #     gender_query = " OR ".join([f"gender = '{gender}'" for gender in gender_filter])
                        #     filters.append(f"({gender_query})")

                        if gender_filter:
                            if isinstance(gender_filter, str):  # Check if gender_filter is a single string
                                gender_filter = [gender_filter]  # Convert to a list for uniform handling
                            gender_query = " OR ".join([f"gender = '{gender}'" for gender in gender_filter])
                            filters.append(f"({gender_query})")
                        
                        if industry_sector_filter:
                            industry_sector_query = " OR ".join([f"industry_sector = '{industry_sector}'" for industry_sector in industry_sector_filter])
                            filters.append(f"({industry_sector_query})")

                        if sector_filter:
                            sector_query = " OR ".join([f"sector = '{sector}'" for sector in sector_filter])
                            filters.append(f"({sector_query})")
                        
                        if job_type_filter:
                            job_type_query = " OR ".join([f"job_type = '{job_type}'" for job_type in job_type_filter])
                            filters.append(f"({job_type_query})")
                        
                        if willing_to_relocate_filter:
                            willing_to_relocate_query = " OR ".join([f"willing_to_relocate = '{willing_to_relocate}'" for willing_to_relocate in willing_to_relocate_filter])
                            filters.append(f"({willing_to_relocate_query})")

                        if mode_of_communication_filter:
                            if "others" in mode_of_communication_filter or "Others" in mode_of_communication_filter:
                                excluded_modes = ["Email", "Whatsapp", "Text message", "Phone"]
                                other_modes_query = (
                                    " AND ".join([f"NOT mode_of_communication CONTAINS '{mode}'" for mode in excluded_modes]) +
                                    " AND mode_of_communication != '' AND mode_of_communication IS NOT NULL"
                                )
                                mode_of_communication_filter = [mode for mode in mode_of_communication_filter if mode != "others" or mode != "Others"]
                                if mode_of_communication_filter:
                                    mode_of_communication_query = " OR ".join([f"mode_of_communication CONTAINS '{mode}'" for mode in mode_of_communication_filter])
                                    filters.append(f"(({mode_of_communication_query}) OR ({other_modes_query}))")
                                else:
                                    filters.append(f"({other_modes_query})")
                            else:
                                mode_of_communication_query = " OR ".join([f"mode_of_communication CONTAINS '{mode}'" for mode in mode_of_communication_filter])
                                filters.append(f"({mode_of_communication_query})")

                        if location_preference_filter:
                            location_preference_query = " OR ".join([f"location_preference = '{location_preference}'" for location_preference in location_preference_filter])
                            filters.append(f"({location_preference_query})")

                        if functional_specification_filter:
                            if "others" in functional_specification_filter or "Others" in functional_specification_filter:
                                excluded_specifications_query = " AND ".join([f"NOT functional_specification = '{spec}'" for spec in functional_specification_list])
                                other_specifications_query = f"({excluded_specifications_query}) AND functional_specification != '' AND functional_specification IS NOT NULL"
                                functional_specification_filter = [spec for spec in functional_specification_filter if spec != "others" or spec != "Others"]
                                if functional_specification_filter:
                                    specific_specifications_query = " OR ".join([f"functional_specification = '{spec}'" for spec in functional_specification_filter])
                                    filters.append(f"(({specific_specifications_query}) OR ({other_specifications_query}))")
                                else:
                                    filters.append(f"({other_specifications_query})")
                            else:
                                functional_specification_query = " OR ".join([f"functional_specification = '{spec}'" for spec in functional_specification_filter])
                                filters.append(f"({functional_specification_query})")
                        
                        filters.append("(email_active = 'Y')")
                        filters.append("(user_role_fk = 3)")

                        final_filters = " AND ".join(filters)  # Joining all filters with AND or OR logic.
        
                        # Perform the search
                        # results = index.search(
                        #     search_text,
                        #     {
                        #         'filter': final_filters,
                        #         # 'sort': ['user_id:desc'],
                        #         'limit': 10,
                        #         'offset': offset,
                        #         'showRankingScore': True
                        #         # 'attributesToHighlight': ['title']
                        #     }
                        # )
                        get_semantic_score_query = "select attribute_value from payment_config where attribute_name = %s;"
                        get_semantic_score_values = ("semantic_score",)
                        semantic_value_dict = execute_query(get_semantic_score_query, get_semantic_score_values)
                        if semantic_value_dict:
                            semantic_score = float(semantic_value_dict[0]["attribute_value"])
                            if semantic_score == 1:
                                semantic_score = int(semantic_score)
                        else:
                            semantic_score = 0
                        results = index.search(
                            search_text,
                            {
                                "hybrid": {
                                    "semanticRatio": semantic_score,
                                    "embedder": "2c"
                                },
                                "filter": final_filters,
                                "limit": 100,
                                # "offset": offset,
                                'showRankingScore': True
                            }
                        )
                        ranking_score_limit = 0
                        if results['hits']:
                            if results['hits'][0]['_rankingScore'] > 0.15:
                                ranking_score_limit = results['hits'][0]['_rankingScore'] - 0.15

                        filtered_results = [hit for hit in results['hits'] if ranking_score_limit <= hit.get('_rankingScore', 0) <= 1]

                        open_ai_result = match_profiles(search_text, filtered_results)
                        if "error" in open_ai_result:
                            result_json = api_json_response_format(False, "Error in match_profiles()", 500, {})
                            return result_json
                        # clean_code = ai_gen_prof_details.replace('```json\n', '').replace('\n```', '').replace('\n','').replace('\'','').replace('\\\\n','')
                        # clean_code = json.loads(clean_code)
                        # final_data = [{'user_id': 100179}, {'user_id': 100245}, {'user_id': 100276}, {'user_id': 100297}]
                        final_data = open_ai_result['user_details']
                        resultant_search_ids = [id['user_id'] for id in final_data]
                        if resultant_search_ids:
                            get_users_details_query = """WITH LatestExperience AS (SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, 
                                                        ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY COALESCE(pe.start_year, '0000-00') DESC, COALESCE(pe.start_month, '00') DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe )
                                                        SELECT u.user_id, u.first_name, u.last_name, u.country, u.city, u.profile_percentage, u.pricing_category, u.payment_status, u.gender, u.is_active,
                                                        p.expert_notes, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.years_of_experience,
                                                        p.professional_id,
                                                        COALESCE(GROUP_CONCAT(DISTINCT ps.skill_name SEPARATOR ', '), '') AS skills,
                                                        COALESCE(le.job_title, '') AS job_title 
                                                        FROM users AS u
                                                        LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id
                                                        LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                                                        LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id 
                                                        WHERE u.user_id IN %s 
                                                        GROUP BY u.user_id, u.first_name, u.last_name, u.country, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.gender,
                                                        p.professional_id, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate,
                                                        p.expert_notes, p.years_of_experience, le.job_title, le.created_at"""
                            get_users_details_values = (tuple(resultant_search_ids),)
                            users_details_dict = execute_query(get_users_details_query, get_users_details_values)
                        else:
                            result_json = api_json_response_format(True, "No records found", 0, {})
                            return result_json
                        
                        def get_ranking_score(user_id, results):
                            for result in results:
                                if result['user_id'] == user_id:
                                    return result['ranking_score']
                            return 0  
                        
                        if users_details_dict:
                            for record in users_details_dict:
                                ranking_score = get_ranking_score(record['user_id'], final_data)
                                record_insert_query = 'INSERT INTO super_admin_search_data (admin_id, user_id, professional_id, first_name, last_name, profile_percentage, gender, city, country, pricing_category, payment_status, job_title, skills, expert_notes, functional_specification, industry_sector, job_type, sector, location_preference, mode_of_communication, willing_to_relocate, years_of_experience, is_active, flag, ranking_score) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                                professional_id = '2C-PR-' + str(record['user_id'])
                                values = (user_id,
                                record['user_id'], professional_id, record['first_name'], record['last_name'], record['profile_percentage'], record['gender'], 
                                record['city'], record['country'], record['pricing_category'], 
                                record['payment_status'], record['job_title'], record['skills'], record['expert_notes'], 
                                record['functional_specification'], record['industry_sector'], 
                                record['job_type'], record['sector'], record['location_preference'], 
                                record['mode_of_communication'], record['willing_to_relocate'], 
                                record['years_of_experience'], record['is_active'], 'sample', ranking_score,)
                                update_query(record_insert_query, values)

                    # keys_to_extract = [
                    #     "user_id", "first_name", "last_name", "email_id", "contact_number", "profile_percentage",
                    #     "gender", "pricing_category", "payment_status", "expert_notes", "functional_specification", 
                    #     "industry_sector", "contact_number", "job_type", "sector", "location_preference", "location_preference", 
                    #     "willing_to_relocate", "is_active", "mode_of_communication",
                    #     "country", "city", "job_title", "job_description", "years_of_experience",
                    #     "skill_name", "skill_level", "education_id", "institute_name",
                    #     "degree_level", "specialisation", "experience_start_year",
                    #     "experience_end_year"
                    # ]

                    # def clean_profile(profile):
                    #     cleaned_profile = {key: profile.get(key, None) for key in keys_to_extract}
                    #     cleaned_profile["job_title"] = profile.get("job_title", [])
                    #     cleaned_profile["job_description"] = profile.get("job_description", [])
                    #     return cleaned_profile

                    # cleaned_data = [clean_profile(profile) for profile in final_data]
                    # for record in cleaned_data:
                    #     job_title = ", ".join(record['job_title']) if record['job_title'] else None
                    #     skills = ", ".join(record['skill_name']) if record['skill_name'] else None
                    #     record_insert_query = 'INSERT INTO super_admin_search_data (admin_id, user_id, professional_id, first_name, last_name, profile_percentage, gender, city, country, pricing_category, payment_status, job_title, skills, expert_notes, functional_specification, industry_sector, job_type, sector, location_preference, mode_of_communication, willing_to_relocate, years_of_experience, is_active, flag) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    #     professional_id = '2C-PR-' + str(record['user_id'])
                    #     values = (user_id,
                    #         record['user_id'], professional_id, record['first_name'], record['last_name'], record['profile_percentage'], record['gender'], 
                    #         record['city'], record['country'], record['pricing_category'], 
                    #         record['payment_status'], job_title, skills, record['expert_notes'], 
                    #         record['functional_specification'], record['industry_sector'], 
                    #         record['job_type'], record['sector'], record['location_preference'], 
                    #         record['mode_of_communication'], record['willing_to_relocate'], 
                    #         record['years_of_experience'], record['is_active'], 'sample',)
                    #     update_query(record_insert_query, values)

                    # fetched_data = results['hits']
                    # total_count = results['estimatedTotalHits']
                    get_count_query = 'select count(user_id) as count from super_admin_search_data where admin_id = %s'
                    count_values = (user_id,)
                    total_count_dict = execute_query(get_count_query, count_values)
                    if total_count_dict:
                        total_count = total_count_dict[0]['count']
                    else:
                        total_count = 0
                    get_user_ids = 'select user_id from super_admin_search_data where admin_id = %s ORDER BY ranking_score DESC LIMIT 10 OFFSET %s'
                    values = (user_id, offset,)
                    result_user_ids_dict = execute_query(get_user_ids, values)
                    if result_user_ids_dict:
                        result_user_ids = [id['user_id'] for id in result_user_ids_dict]
                    else:
                        result_user_ids = []
                else:
                    if is_entries_presents_dict:
                        if is_entries_presents_dict[0]['count(id)'] == 0:
                            result_json = api_json_response_format(True,"No records found",0,{})
                            return result_json
                        else:
                            conditions = []
                            values_job_details = []
                            # if req_data['skills']:
                            #     if "Others" in req_data['skills']:
                            #         conditions.append("skills NOT IN %s")
                            #         values_job_details.append(tuple(skill_list),)
                            #     else:
                            #         conditions.append("skills IN %s")
                            #         values_job_details.append(tuple(skill_value),)
                            values_job_details.append((user_id,))
                            if req_data['skills']:
                                if "Others" in req_data['skills']:
                                    skill_conditions = ["skills NOT LIKE %s" for _ in skill_list]
                                    conditions.append(f"({' AND '.join(skill_conditions)})")
                                    for skill in skill_list:
                                        values_job_details.append(f"%{skill}%")
                                else:
                                    skill_conditions = ["skills LIKE %s" for _ in skill_value]
                                    conditions.append(f"({' OR '.join(skill_conditions)})")
                                    for skill in skill_value:
                                        values_job_details.append(f"%{skill}%")
                            if req_data['functional_specification']:
                                if "Others" in req_data['functional_specification']:
                                    conditions.append("functional_specification NOT IN %s")
                                    values_job_details.append(tuple(functional_specification_list),)
                                else:
                                    conditions.append("functional_specification IN %s")
                                    values_job_details.append(tuple(req_data['functional_specification']),)
                            if country:
                                conditions.append("country IN %s")
                                values_job_details.append(tuple(country),)    
                            if city:
                                conditions.append("city IN %s")
                                values_job_details.append(tuple(city),)
                            if 'profile_percentage' in req_data and req_data['profile_percentage']:
                                percentage_range = req_data['profile_percentage']
                                if percentage_range == "0-30":
                                    conditions.append("profile_percentage BETWEEN 0 AND 30")
                                elif percentage_range == "30-60":
                                    conditions.append("profile_percentage BETWEEN 30 AND 60")
                                elif percentage_range == "60-100":
                                    conditions.append("profile_percentage BETWEEN 60 AND 100")
                                elif percentage_range == "30-100":
                                    conditions.append("profile_percentage BETWEEN 30 AND 100")
                            if req_data['plan']:
                                conditions.append("pricing_category IN %s")
                                values_job_details.append(tuple(req_data['plan'],))
                            if req_data['gender']:
                                conditions.append("gender IN %s")
                                values_job_details.append(tuple(req_data['gender'],))
                            if req_data['industry_sector']:
                                conditions.append("industry_sector IN %s")
                                values_job_details.append(tuple(req_data['industry_sector'],))
                            if req_data['sector']:
                                conditions.append("sector IN %s")
                                values_job_details.append(tuple(req_data['sector'],))
                            if req_data['job_type']:
                                conditions.append("job_type IN %s")
                                values_job_details.append(tuple(req_data['job_type'],))
                            if req_data['willing_to_relocate']:
                                conditions.append("willing_to_relocate IN %s")
                                values_job_details.append(tuple(req_data['willing_to_relocate'],))
                            # if req_data['mode_of_communication']:
                            #     conditions.append("mode_of_communication IN %s")
                            #     values_job_details.append(tuple(req_data['mode_of_communication'],))
                            if req_data['mode_of_communication']:
                                moc_conditions = ["mode_of_communication LIKE %s" for _ in mode_of_communication]
                                conditions.append(f"({' OR '.join(moc_conditions)})")
                                for moc in mode_of_communication:
                                    values_job_details.append(f"%{moc}%")
                            if req_data['location_preference']:
                                conditions.append("location_preference IN %s")
                                values_job_details.append(tuple(req_data['location_preference'],))
                            search_query = '''SELECT 
                                            user_id, first_name, last_name, gender, city, country, pricing_category, profile_percentage,
                                            payment_status, job_title, skills, expert_notes, functional_specification, 
                                            industry_sector, job_type, sector, location_preference, mode_of_communication, 
                                            willing_to_relocate, years_of_experience, is_active, flag, created_at
                                        FROM 
                                            super_admin_search_data
                                        WHERE admin_id IN %s
                                        '''
                            if conditions:
                                # if len(conditions) == 1:
                                #     search_query += conditions[0]
                                # else:
                                    # search_query += " (" + " AND ".join(conditions) + ")"
                                    search_query += " AND " + " AND ".join(conditions)
                            new_query = search_query + " ORDER BY ranking_score DESC LIMIT 10 OFFSET %s"
                            val_detail = values_job_details
                            total_count_query = "SELECT subquery.user_id, COUNT(*) AS total_count FROM (" + search_query + ") AS subquery GROUP BY subquery.user_id"
                            total_count_values = (val_detail)
                            total_count = execute_query(total_count_query, total_count_values)
                            if len(total_count) > 0:
                                total_count = len(total_count)
                            else:
                                total_count = 0
                            # values_job_details.append(offset,)
                            values_job_details = values_job_details + [offset]
                            professional_details = replace_empty_values(execute_query(new_query, values_job_details))
                            if professional_details:
                                result_user_ids = [id['user_id'] for id in professional_details]
                            else:
                                result_user_ids = []
                    else:
                        result_json = api_json_response_format(True,"No records found",0,{})
                        return result_json

                # result_user_ids = [id['user_id'] for id in fetched_data]
                prof_details = []
                if result_user_ids:
                    for temp_id in result_user_ids:
                        query_job_details = """
                                    WITH LatestExperience AS (
                                            SELECT
                                                pe.professional_id,
                                                pe.id AS experience_id,
                                                pe.start_month,
                                                pe.start_year,
                                                pe.job_title,
                                                pe.created_at,
                                                ROW_NUMBER() OVER (
                                                    PARTITION BY pe.professional_id
                                                    ORDER BY
                                                        COALESCE(pe.start_year, '0000-00') DESC,
                                                        COALESCE(pe.start_month, '00') DESC,
                                                        pe.created_at DESC
                                                ) AS rn
                                            FROM
                                                professional_experience AS pe
                                        )
                                        SELECT
                                            u.user_id,
                                            u.first_name,
                                            u.last_name,
                                            u.country,
                                            u.state,
                                            u.city,
                                            u.profile_percentage,
                                            u.pricing_category,
                                            u.is_active,
                                            u.payment_status,
                                            u.gender,
                                            (
                                                SELECT CAST(ROUND(sa2.ranking_score, 4) AS DECIMAL(10,4))
                                                FROM super_admin_search_data sa2
                                                WHERE sa2.user_id = u.user_id
                                                ORDER BY sa2.updated_at DESC
                                                LIMIT 1
                                            ) AS ranking_score,
                                            p.years_of_experience,
                                            p.functional_specification,
                                            p.sector,
                                            p.industry_sector,
                                            p.job_type,
                                            p.location_preference,
                                            p.mode_of_communication,
                                            p.willing_to_relocate,
                                            p.professional_id,
                                            p.expert_notes,
                                            p.created_at AS posted_at,
                                            COALESCE(le.job_title, '') AS job_title,
                                            COALESCE(le.created_at, '') AS experience_created_at,
                                            u2.profile_image
                                        FROM
                                            users AS u
                                        LEFT JOIN
                                            professional_profile AS p ON u.user_id = p.professional_id
                                        LEFT JOIN
                                            LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                                        LEFT JOIN
                                            users AS u2 ON u.user_id = u2.user_id
                                        LEFT JOIN 
                                            professional_skill AS ps ON u.user_id = ps.professional_id 
                                        WHERE
                                            u.user_role_fk = 3 AND u.email_active = 'Y' AND u.user_id = %s 
                                            GROUP BY u.user_id, u.first_name, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.gender,
                                            p.professional_id, p.functional_specification, p.sector, p.industry_sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.expert_notes, p.years_of_experience, p.created_at, 
                                            le.job_title, le.created_at, u2.profile_image 
                        """        #ORDER BY p.professional_id DESC
                        values_job_details = (temp_id,)
                        temp_result = replace_empty_values(execute_query(query_job_details, values_job_details))
                        if temp_result:
                            prof_details.append(temp_result[0])

                else:
                    prof_details = []

                if len(prof_details) > 0:
                    mod_id = '2C-PR-'+ str(prof_details[0]['professional_id'])
                    first_id = prof_details[0]['professional_id']
                    if isUserExist("professional_profile","professional_id",first_id):
                            query = """SELECT 
                                            u.first_name, 
                                            u.last_name, 
                                            u.email_id, 
                                            u.dob, 
                                            u.country_code, 
                                            u.contact_number, 
                                            u.country, 
                                            u.state, 
                                            u.city, 
                                            u.profile_percentage,
                                            u.pricing_category, 
                                            u.is_active,
                                            u.payment_status,
                                            u.gender,
                                            p.years_of_experience,
                                            p.functional_specification,
                                            p.sector, p.industry_sector,
                                            p.job_type,
                                            p.location_preference,
                                            p.mode_of_communication,
                                            p.willing_to_relocate,
                                            p.professional_id,
                                            p.professional_resume,
                                            p.expert_notes,
                                            p.about,
                                            p.preferences,
                                            p.video_url, 
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
                                        FROM 
                                            users AS u 
                                        LEFT JOIN 
                                            professional_profile AS p ON u.user_id = p.professional_id 
                                        LEFT JOIN 
                                            professional_experience AS pe ON u.user_id = pe.professional_id 
                                        LEFT JOIN 
                                            professional_education AS ed ON u.user_id = ed.professional_id 
                                        LEFT JOIN 
                                            professional_skill AS ps ON u.user_id = ps.professional_id 
                                        LEFT JOIN 
                                            professional_language AS pl ON u.user_id = pl.professional_id 
                                        LEFT JOIN 
                                            professional_additional_info AS pai ON u.user_id = pai.professional_id 
                                        LEFT JOIN 
                                            professional_social_link AS psl ON u.user_id = psl.professional_id 
                                        LEFT JOIN 
                                            users AS u2 ON u.user_id = u2.user_id 
                                        WHERE 
                                            u.user_id = %s 
                                        ORDER BY 
                                            CASE 
                                                WHEN pe.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(pe.end_year, '0000-00') DESC,
                                            COALESCE(pe.end_month, '00') DESC,
                                            COALESCE(pe.start_year, '0000-00') DESC,
                                            COALESCE(pe.start_month, '00') DESC,
                                            CASE 
                                                WHEN ed.end_year = 'Present' THEN 1 
                                                ELSE 0 
                                            END DESC,
                                            COALESCE(ed.end_year, '0000-00') DESC,
                                            COALESCE(ed.end_month, '00') DESC,
                                            COALESCE(ed.start_year, '0000-00') DESC,
                                            COALESCE(ed.start_month, '00') DESC;"""                
                            values = (first_id,)
                            profile_result = execute_query(query, values) 

                            if len(profile_result) > 0:                              
                                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                                intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                                resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                            else:
                                result_json = api_json_response_format(True,"No records found",0,{})     
                                return result_json          
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            if intro_video_name == '':
                                s3_video_key = ""
                            else:
                                s3_video_key = s3_intro_video_folder_name + str(intro_video_name)
                            if resume_name == '':    
                                s3_resume_key = ""
                            else:          
                                s3_resume_key = s3_resume_folder_name + str(resume_name)

                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (first_id, 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(ep.sector, su.sector) AS sector,
                                        COALESCE(ep.company_name, su.company_name) AS company_name,
                                        COALESCE(u.is_active, '') AS is_active
                                        FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            # for prof in prof_details:
                            #     query = 'select job_id from sc_recommendation where professional_id = %s'
                            #     values = (prof['professional_id'],)
                            #     recommended_jobs_id = execute_query(query, values)
                            #     recommended_jobs_list = []
                            #     if len(recommended_jobs_id) > 0:
                            #         for id in recommended_jobs_id:
                            #             if isUserExist("job_post", "id", id['job_id']):
                            #                 query = 'SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, ep.company_name, ep.sector FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id WHERE jp.id = %s'
                            #                 values = (id['job_id'],)
                            #                 detail = execute_query(query, values)
                            #                 txt = detail[0]['sector']
                            #                 txt = txt.replace(", ", "_")
                            #                 txt = txt.replace(" ", "_")
                            #                 sector_name = txt + ".png"
                            #                 detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                            #                 recommended_jobs_list.append(detail[0])
                            #     else:
                            #         recommended_jobs_list = []
                            #     prof.update({'recommended_jobs' : recommended_jobs_list})
                            profile_dict = {
                                'first_name': replace_empty_values1(profile_result[0]['first_name']),
                                'last_name': replace_empty_values1(profile_result[0]['last_name']),
                                'professional_id' : mod_id,                                        
                                'email_id': replace_empty_values1(profile_result[0]['email_id']),
                                'dob': replace_empty_values1(profile_result[0]['dob']),
                                'country_code': replace_empty_values1(profile_result[0]['country_code']),
                                'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                                'city': replace_empty_values1(profile_result[0]['city']),
                                'profile_percentage': replace_empty_values1(profile_result[0]['profile_percentage']),
                                'state': replace_empty_values1(profile_result[0]['state']),
                                'country': replace_empty_values1(profile_result[0]['country']),
                                'pricing_category' : profile_result[0]['pricing_category'],
                                'is_active' : profile_result[0]['is_active'],
                                'payment_status' : profile_result[0]['payment_status'],
                                'profile_image': s3_pic_key,
                                'video_name': s3_video_key,
                                'resume_name': s3_resume_key,
                                'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                                'about': replace_empty_values1(profile_result[0]['about']),
                                'preferences': replace_empty_values1(profile_result[0]['preferences']),
                                'experience': {},
                                'education': {},
                                'skills': {},
                                'languages': {},
                                'additional_info': {},
                                'social_link': {},
                                'job_list' : {},
                                'recommended_jobs' : recommended_jobs_list,
                                'gender' : replace_empty_values1(profile_result[0]['gender']),
                                'years_of_experience' : replace_empty_values1(profile_result[0]['years_of_experience']),
                                'functional_specification' : replace_empty_values1(profile_result[0]['functional_specification']),
                                'sector' : replace_empty_values1(profile_result[0]['sector']),
                                'industry_sector' : replace_empty_values1(profile_result[0]['industry_sector']),
                                'job_type' : replace_empty_values1(profile_result[0]['job_type']),
                                'location_preference' : replace_empty_values1(profile_result[0]['location_preference']),
                                'mode_of_communication' : replace_empty_values1(profile_result[0]['mode_of_communication']),
                                'willing_to_relocate' : replace_empty_values1(profile_result[0]['willing_to_relocate'])
                            }

                            # Grouping experience data
                            experience_set = set()
                            experience_list = []
                            for exp in profile_result:
                                if exp['experience_id'] is not None:
                                    # start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                                    # end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                                    if exp['experience_start_year'] != None:
                                        start_date = exp['experience_start_year']
                                    else:
                                        start_date = ''
                                    if exp['experience_end_year'] != None:
                                        end_date = exp['experience_end_year']
                                    else:
                                        end_date = ''
                                    exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                                    if exp_tuple not in experience_set:
                                        experience_set.add(exp_tuple)
                                        experience_list.append({
                                            'id': exp['experience_id'],
                                            'company_name': replace_empty_values1(exp['company_name']),
                                            'job_title': replace_empty_values1(exp['job_title']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,                                
                                            'job_description': replace_empty_values1(exp['job_description']),
                                            'job_location': replace_empty_values1(exp['job_location'])
                                        })

                            profile_dict['experience'] = experience_list

                            # Grouping education data
                            education_set = set()
                            education_list = []
                            for edu in profile_result:
                                if edu['education_id'] is not None:
                                    # start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                                    # end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                                    if edu['education_start_year'] != None:
                                        start_date = edu['education_start_year']
                                    else:
                                        start_date = ''
                                    if edu['education_end_year'] != None:
                                        end_date = edu['education_end_year']
                                    else:
                                        end_date = ''
                                    edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                                start_date, end_date, edu['institute_location'])
                                    if edu_tuple not in education_set:
                                        education_set.add(edu_tuple)
                                        education_list.append({
                                            'id': edu['education_id'],
                                            'institute_name': replace_empty_values1(edu['institute_name']),
                                            'degree_level': replace_empty_values1(edu['degree_level']),
                                            'specialisation': replace_empty_values1(edu['specialisation']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,
                                            'institute_location': replace_empty_values1(edu['institute_location'])
                                        })

                            profile_dict['education'] = education_list

                            # Grouping skills data
                            skills_set = set()
                            skills_list = []
                            for skill in profile_result:
                                if skill['skill_id'] is not None:
                                    skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                                    if skill_tuple not in skills_set:
                                        skills_set.add(skill_tuple)
                                        skills_list.append({
                                            'id': skill['skill_id'],
                                            'skill_name': replace_empty_values1(skill['skill_name']),
                                            'skill_level': replace_empty_values1(skill['skill_level'])
                                        })

                            profile_dict['skills'] = skills_list

                            # Grouping languages data
                            languages_set = set()
                            languages_list = []
                            for lang in profile_result:
                                if lang['language_id'] is not None:
                                    lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                                    if lang_tuple not in languages_set:
                                        languages_set.add(lang_tuple)
                                        languages_list.append({
                                            'language_known': replace_empty_values1(lang['language_known']),
                                            'id':  lang['language_id'],
                                            'language_level': replace_empty_values1(lang['language_level'])
                                    })

                            profile_dict['languages'] = languages_list
                            # Grouping additional info data
                            additional_info_set = set()
                            additional_info_list = []
                            for info in profile_result:
                                if info['additional_info_id'] is not None:
                                    info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                                    if info_tuple not in additional_info_set:
                                        additional_info_set.add(info_tuple)
                                        additional_info_list.append({
                                        'id': info['additional_info_id'],
                                        'title': replace_empty_values1(info['additional_info_title']),
                                        'description': replace_empty_values1(info['additional_info_description'])
                                    })

                            profile_dict['additional_info'] = additional_info_list
                            # Grouping social link data
                            social_link_set = set()
                            social_link_list = []
                            for link in profile_result:
                                if link['social_link_id'] is not None:
                                    link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                                    if link_tuple not in social_link_set:
                                        social_link_set.add(link_tuple)
                                        social_link_list.append({
                                        'title': replace_empty_values1(link['social_link_title']),
                                        'id':  link['social_link_id'],
                                        'url': replace_empty_values1(link['social_link_url'])
                                    })

                            profile_dict['social_link'] = social_link_list
                            
                            #Get recommended jobs for the professional
                            for prof in prof_details:
                                old_id = prof['professional_id']
                                temp_id = "2C-PR-" + str(prof['professional_id'])
                                prof.update({"professional_id" : temp_id})
                                query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                                values = (3,old_id,)
                                job_id_list = execute_query(query, values)
                                job_detail_list = []
                                if len(job_id_list) > 0:
                                    for job_id in job_id_list:
                                        id = job_id['job_id']
                                        if isUserExist("job_post", "id", id):
                                            query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city,
                                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                            COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                            COALESCE(ep.sector, su.sector) AS sector,
                                            COALESCE(ep.company_name, su.company_name) AS company_name,
                                            COALESCE(u.is_active, '') AS is_active
                                            FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                            values = (id,)
                                            detail = execute_query(query, values)
                                            if len(detail) > 0:
                                                txt = detail[0]['sector']
                                                txt = txt.replace(", ", "_")
                                                txt = txt.replace(" ", "_")
                                                sector_name = txt + ".png"
                                                detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                                img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                                detail[0].update({'profile_image' : img_key})
                                                job_detail_dict = {"job_title" : detail[0]['job_title'],
                                                                    "country" : detail[0]['country'],
                                                                    "city" : detail[0]['city'],
                                                                    "company_name" : detail[0]['company_name'],
                                                                    "sector_image" : detail[0]['sector_image'],
                                                                    "sector" : detail[0]['sector'],
                                                                    "profile_image" : detail[0]['profile_image'],
                                                                    "employer_id" : detail[0]['employer_id'],
                                                                    "job_id" : detail[0]['job_id'],
                                                                    "pricing_category" : detail[0]['pricing_category'],
                                                                    "is_active" : detail[0]['is_active'],
                                                                    "payment_status" : detail[0]['payment_status']}
                                                job_detail_list.append(job_detail_dict)
                                else:
                                    job_detail_list = []
                                s3_prof_pic_key = s3_picture_folder_name + prof['profile_image']
                                prof.update({'profile_image' : s3_prof_pic_key})
                                prof.update({'recommended_jobs' : job_detail_list})
                            profile_dict['professional_details'] = prof_details
                            profile_dict.update({'total_count' : total_count})
                            result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)                    
                    else:
                        result_json = api_json_response_format(False,"User profile not found",500,{})
                else:
                    result_json = api_json_response_format(True,"No records found",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def admin_employer_meilisearch_filter_results():
    try:
        data = []
        profile = {}
        country = []
        city = []

        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                plan = req_data['plan']
                plan_status = req_data['status']
                offset = (page_number - 1) * 10
                search_text = req_data["search_text"]
                sector = req_data["sector"]

                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                # sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                
                client = Client(meilisearch_url, master_key)
                index = client.index(meilisearch_employer_index)

                country_filter = country
                city_filter = city
                plan_filter = plan
                plan_status_filter = plan_status
                sector_filter = sector

                filters = []

                if country_filter:
                    country_query = " OR ".join([f"country = '{country}'" for country in country_filter])
                    filters.append(f"({country_query})")

                if city_filter:
                    city_query = " OR ".join([f"city = '{city}'" for city in city_filter])
                    filters.append(f"({city_query})")
                
                if plan_filter:
                    plan_query = " OR ".join([f"pricing_category = '{plan}'" for plan in plan_filter])
                    filters.append(f"({plan_query})")
                
                if plan_status_filter:
                    plan_status_query = " OR ".join([f"payment_status = '{status}'" for status in plan_status_filter])
                    filters.append(f"({plan_status_query})")
                
                if sector_filter:
                    sector_query = " OR ".join([f"sector = '{sector}'" for sector in sector_filter])
                    filters.append(f"({sector_query})")
                
                filters.append("(email_active = 'Y')")
                filters.append("(user_role_fk = 2)")

                final_filters = " AND ".join(filters)

                results = index.search(
                    search_text,
                    {
                        'filter': final_filters,
                        'sort': ['user_id:desc'],
                        'limit': 10,
                        'offset': offset
                        # 'attributesToHighlight': ['title']
                    }
                )
                # get_semantic_score_query = "select attribute_value from payment_config where attribute_name = %s;"
                # get_semantic_score_values = ("semantic_score",)
                # semantic_value_dict = execute_query(get_semantic_score_query, get_semantic_score_values)
                # if semantic_value_dict:
                #     semantic_score = float(semantic_value_dict[0]["attribute_value"])
                #     if semantic_score == 1:
                #         semantic_score = int(semantic_score)
                # else:
                #     semantic_score = 0
                # results = index.search(
                #     search_text,
                #     {
                #         "hybrid": {
                #             "semanticRatio": semantic_score,
                #             "embedder": "2c"
                #         },
                #         "filter": final_filters,
                #         "limit": 10,
                #         "offset": offset,
                #         'showRankingScore': True
                #     }
                # )

                fetched_data = results['hits']
                total_count = results['estimatedTotalHits']

                result_user_ids = [id['user_id'] for id in fetched_data]
                if result_user_ids:
                    query_emp_details = "SELECT u.user_id, u.pricing_category, u.is_active, u.payment_status, u.country, ep.sector FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id where u.user_role_fk = 2 AND u.user_id IN %s" #ORDER BY user_id DESC
                    val_detail = (tuple(result_user_ids),)
                    emp_details = replace_empty_values(execute_query(query_emp_details, val_detail))
                else:
                    emp_details = []
                details_list = [{"employee_short_desc" : [],
                                 "first_employee_details" : []
                                }]
                filter_parameters = fetch_employer_filter_params()
                if len(emp_details) > 0:
                    for emp in emp_details:
                           # query = 'SELECT u.user_id, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s'
                            query = 'SELECT u.user_id, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.website_url, upd.assisted_jobs_allowed, upd.assisted_jobs_used FROM users u JOIN employer_profile e ON u.user_id = e.employer_id join user_plan_details upd on upd.user_id = u.user_id WHERE u.user_id = %s AND e.employer_id = %s'
                            values = (emp['user_id'], emp['user_id'],)
                            single_emp_detail = execute_query(query, values)
                            if len(single_emp_detail) > 0:
                                s3_pic_key = s3_employer_picture_folder_name + str(single_emp_detail[0]['profile_image'])
                                single_emp_detail[0].update({'profile_image' : s3_pic_key})
                                details_list[0]['employee_short_desc'].append(single_emp_detail[0])
                    if len(details_list) > 0:
                        first_id = details_list[0]['employee_short_desc'][0]['user_id']
                        query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.profile_image, u.company_code, u.pricing_category, u.is_active, u.payment_status, e.designation, e.company_name, e.company_description, e.employer_type, e.sector, e.website_url FROM users u JOIN employer_profile e ON u.user_id = e.employer_id WHERE u.user_id = %s AND e.employer_id = %s"
                        values = (first_id, first_id,)
                        first_emp_details = execute_query(query, values)
                        if len(first_emp_details) > 0:
                            owner_emp_id = first_emp_details[0]['user_id']
                            get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                            get_sub_users_values = (owner_emp_id,)
                            sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                            sub_users_list = []
                            if sub_users_dict:
                                sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                            sub_users_list.append(owner_emp_id)
                            s3_pic_key = s3_employer_picture_folder_name + str(first_emp_details[0]['profile_image'])
                            first_emp_details[0].update({'profile_image' : s3_pic_key})
                            details_list[0]['first_employee_details'].append(first_emp_details[0])
                            posted_job_details_query = """SELECT
                                                            jp.id AS job_id,
                                                            jp.employer_id,
                                                            jp.job_title,
                                                            jp.job_status,
                                                            jp.created_at AS posted_on,
                                                            COALESCE(vc.view_count, 0) AS view_count,
                                                            COALESCE(ja.applied_count, 0) AS applied_count,
                                                            COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                                                            COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                                                            COALESCE(CAST(ja.contact_count AS SIGNED), 0) AS contacted_count,
                                                            COALESCE(CAST(ja.reject_count AS SIGNED), 0) AS rejected_count,
                                                            GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left,
                                                            COALESCE(hc.hired_count, 0) AS hired_count

                                                        FROM
                                                            job_post jp
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS view_count
                                                            FROM
                                                                view_count
                                                            GROUP BY
                                                                job_id
                                                        ) vc ON jp.id = vc.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(*) AS applied_count,
                                                                SUM(CASE WHEN application_status = 'Not Reviewed' THEN 1 ELSE 0 END) AS not_reviewed_count,
                                                                SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count,
                                                                SUM(CASE WHEN application_status = 'Rejected' THEN 1 ELSE 0 END) AS reject_count,
                                                                SUM(CASE WHEN application_status = 'Contacted' THEN 1 ELSE 0 END) AS contact_count
                                                            FROM
                                                                job_activity
                                                            GROUP BY
                                                                job_id
                                                        ) ja ON jp.id = ja.job_id
                                                        LEFT JOIN (
                                                            SELECT
                                                                job_id,
                                                                COUNT(professional_id) AS hired_count
                                                            FROM
                                                                job_hired_candidates
                                                            GROUP BY
                                                                job_id
                                                        ) hc ON jp.id = hc.job_id
                                                        WHERE
                                                            jp.employer_id IN %s and jp.job_status != 'drafted'
                                                        ORDER BY
                                                            jp.id DESC;"""
                            values = (tuple(sub_users_list),)
                            posted_job_details = execute_query(posted_job_details_query, values)
                            query = "SELECT COUNT(CASE WHEN job_status = 'Opened' THEN 1 END) AS total_opened, COUNT(CASE WHEN job_status = 'Paused' THEN 1 END) AS total_paused, COUNT(CASE WHEN job_status = 'Closed' THEN 1 END) AS total_closed FROM job_post WHERE employer_id IN %s;"
                            values = (tuple(sub_users_list),)
                            job_count_details = execute_query(query, values)
                            if job_count_details:
                                for p in posted_job_details:
                                    p['total_opened'] = job_count_details[0]['total_opened']
                                    p['total_paused'] = job_count_details[0]['total_paused']
                                    p['total_closed'] = job_count_details[0]['total_closed']
                            first_emp_details[0].update({'posted_job_details' : posted_job_details})
                        details_list[0].update({'total_count': total_count})
                        details_list[0].update({'filter_parameters' : filter_parameters})
                        result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                    else:
                        result_json = api_json_response_format(False,"No employer profile found",401,{})
                else:
                        result_json = api_json_response_format(False,"No employer profile found",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def admin_partner_meilisearch_filter_results():
    try:
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            filter_parameters = fetch_filter_params_partner() 
            details_list = [{"partner_short_desc" : [],
                                 "first_partner_details" : []
                                }]       
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                partner_type = req_data["partner_type"]
                search_text = req_data["search_text"]
                sector = req_data["sector"]
                plan = req_data['plan']
                plan_status = req_data['status']
                offset = (page_number - 1) * 10

                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                # sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # partner_type_list = ["Search firm", "Skill platform", "Assessment company", "Coaching and mentoring firm", "Learning and Development Organization", "Others"]
                client = Client(meilisearch_url, master_key)
                index = client.index(meilisearch_partner_index)

                country_filter = country
                city_filter = city
                plan_filter = plan
                plan_status_filter = plan_status
                sector_filter = sector
                partner_type_filter = partner_type

                filters = []

                if country_filter:
                    country_query = " OR ".join([f"country = '{country}'" for country in country_filter])
                    filters.append(f"({country_query})")

                if city_filter:
                    city_query = " OR ".join([f"city = '{city}'" for city in city_filter])
                    filters.append(f"({city_query})")
                
                if plan_filter:
                    plan_query = " OR ".join([f"pricing_category = '{plan}'" for plan in plan_filter])
                    filters.append(f"({plan_query})")

                if plan_status_filter:
                    plan_status_query = " OR ".join([f"payment_status = '{status}'" for status in plan_status_filter])
                    filters.append(f"({plan_status_query})")
                
                if sector_filter:
                    sector_query = " OR ".join([f"sector = '{sector}'" for sector in sector_filter])
                    filters.append(f"({sector_query})")
                
                if partner_type_filter:
                    partner_type_query = " OR ".join([f"partner_type = '{partner_type}'" for partner_type in partner_type_filter])
                    filters.append(f"({partner_type_query})")
                
                filters.append("(email_active = 'Y')")
                filters.append("(user_role_fk = 6)")

                final_filters = " AND ".join(filters)

                results = index.search(
                    search_text,
                    {
                        'filter': final_filters,
                        'sort': ['user_id:desc'],
                        'limit': 10,
                        'offset': offset
                        # 'attributesToHighlight': ['title']
                    }
                )
                # get_semantic_score_query = "select attribute_value from payment_config where attribute_name = %s;"
                # get_semantic_score_values = ("semantic_score",)
                # semantic_value_dict = execute_query(get_semantic_score_query, get_semantic_score_values)
                # if semantic_value_dict:
                #     semantic_score = float(semantic_value_dict[0]["attribute_value"])
                #     if semantic_score == 1:
                #         semantic_score = int(semantic_score)
                # else:
                #     semantic_score = 0
                # results = index.search(
                #     search_text,
                #     {
                #         "hybrid": {
                #             "semanticRatio": semantic_score,
                #             "embedder": "2c"
                #         },
                #         "filter": final_filters,
                #         "limit": 10,
                #         "offset": offset,
                #         'showRankingScore': True
                #     }
                # )

                fetched_data = results['hits']
                total_count = results['estimatedTotalHits']

                result_user_ids = [id['user_id'] for id in fetched_data]

                if result_user_ids:
                    query_partner_details = "SELECT u.user_id, u.pricing_category, u.is_active, u.payment_status, u.country, p.sector, p.partner_type FROM users u JOIN partner_profile p ON u.user_id = p.partner_id where u.user_role_fk = 6 AND u.user_id IN %s ORDER BY user_id DESC"
                    val_detail = (tuple(result_user_ids),)
                    partner_details = replace_empty_values(execute_query(query_partner_details, val_detail))
                else:
                    partner_details = []
                if len(partner_details) > 0:
                    for partner in partner_details:
                            query = 'SELECT u.user_id, u.country, u.state, u.city, u.profile_image, u.pricing_category, u.is_active, u.payment_status, p.designation, p.company_name, p.company_description, p.partner_type, p.sector, p.website_url, upd.no_of_jobs, upd.user_plan, upd.total_jobs FROM users u JOIN partner_profile p ON u.user_id = p.partner_id LEFT JOIN user_plan_details upd ON u.user_id = upd.user_id WHERE u.user_id = %s AND p.partner_id = %s;'
                            values = (partner['user_id'], partner['user_id'],)
                            single_partner_detail = execute_query(query, values)
                            if len(single_partner_detail) > 0:
                                s3_pic_key = s3_partner_picture_folder_name + str(single_partner_detail[0]['profile_image'])
                                single_partner_detail[0].update({'profile_image' : s3_pic_key})
                                details_list[0]['partner_short_desc'].append(single_partner_detail[0])
                    if len(details_list) > 0:
                        first_id = details_list[0]['partner_short_desc'][0]['user_id']
                        query = "SELECT u.first_name, u.last_name, u.email_id, u.dob, u.gender, u.country_code, u.contact_number, u.country, u.state, u.city, u.company_code, u.pricing_category, u.is_active, u.payment_status, u.profile_image, p.company_name, p.designation, p.company_description, p.partner_type, p.sector, p.website_url FROM partner_profile p INNER JOIN users u ON p.partner_id = u.user_id WHERE u.user_id = %s AND p.partner_id = %s"
                        values = (first_id, first_id,)
                        first_partner_details = execute_query(query, values)
                        query = "SELECT DISTINCT pp.company_name, u.profile_image, l.id AS learning_id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.post_status, l.created_at, l.is_active, COALESCE(vc.view_count, 0) AS view_count, GREATEST(l.days_left - DATEDIFF(CURDATE(), l.created_at), 0) AS days_left FROM partner_profile pp LEFT JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.partner_id = %s AND l.post_status = %s ORDER BY l.id DESC"
                        values = (first_id, 'opened',)
                        learning_post_details = execute_query(query, values)
                        if len(first_partner_details) > 0:
                            s3_pic_key = s3_partner_picture_folder_name + str(first_partner_details[0]['profile_image'])
                            first_partner_details[0].update({'profile_image' : s3_pic_key})
                        if len(learning_post_details) > 0:
                            for data in learning_post_details:
                                s3_cover_pic_key = s3_partner_cover_pic_folder_name + data['image']
                                s3_attached_file_key = s3_partner_learning_folder_name + data['attached_file']
                                data.update({'image' : s3_cover_pic_key})
                                data.update({'attached_file' : s3_attached_file_key})
                        details_list[0].update({'first_partner_details' : first_partner_details})
                        details_list[0].update({'first_partner_posts' : learning_post_details})
                        details_list[0].update({'total_count': total_count})
                        details_list[0].update({'filter_parameters' : filter_parameters})
                        result_json = api_json_response_format(True, "Details fetched successfully",0, details_list)
                    else:
                        details_list[0].update({'filter_parameters' : filter_parameters})
                        result_json = api_json_response_format(False,"No partner profile found",401,details_list)
                else:
                    details_list[0].update({'filter_parameters' : filter_parameters})
                    result_json = api_json_response_format(False,"No partner profile found",401,details_list)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
#def admin_jobs_meilisearch_filter_results():
    try:
        data = []
        profile = {}
        # country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin" or user_data["user_role"] == "professional":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                job_status = req_data['job_status']
                job_type = req_data['job_type']
                plan = req_data['plan']
                sector = req_data['sector']
                skills = req_data['skills']
                specialisation = req_data["specialisation"]
                work_schedule = req_data['work_schedule']
                workplace_type = req_data['workplace_type']
                search_text = req_data["search_text"]
                country = req_data["country"]
                offset = (page_number - 1) * 10

                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        # country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                # skills_list = ["Agile Methodologies", "Algorithm Design", "Analytics", "Application Programming Interfaces (APIs)", "Budgeting", "Business Strategy", "Change Management", "Conflict Resolution", "Contract Management Skills", "Data Analysis", "Database Design", "Debugging", "Direct Sales", "Earned Value Management", "Financial Management", "Human Resource Management", "Keyword Research", "Leadership Skills", "Market Research", "Marketing Skills", "Metrics and KPIs", "Mobile Application Development", "Negotiation", "Operations Management", "Organizational Development", "Presentation", "Process Improvement", "Product Knowledge", "Project Management", "Quality Assurance (QA)", "Recruiting", "Revenue Expansion", "Risk Assessment", "SaaS Knowledge", "Sales and Budget Forecasting", "Salesforce", "Strategic Planning", "Supply Chain Management", "Talent Management", "Team Leadership", "Upselling"]
                specialisation_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite And Board"]
                skill_list = ["Business Strategy", "Change Management", "Conflict Resolution", "Financial Management", "Human Resource Management", "Operations Management", "Organizational Development", "Strategic Planning", "Supply Chain Management", "Talent Management", "Human Resource Management", "Direct Sales", "Leadership Skills", "Market Research", "Negotiation", "Presentation", "Product Knowledge", "Recruiting", "Sales and Budget Forecasting", "Upselling", "Agile Methodologies", "Budgeting", "Contract Management Skills", "Earned Value Management", "Process Improvement", "Risk Assessment", "Analytics", "Data Analysis", "Metrics and KPIs", "Project Management", "Revenue Expansion", "SaaS Knowledge", "Salesforce", "Team Leadership", "Marketing Skills", "Keyword Research", "Algorithm Design", "Application Programming Interfaces (APIs)", "Database Design", "Debugging", "Mobile Application Development", "Quality Assurance (QA)"]
                # sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # schedule_list = ["Fixed", "Flexible", "Monday to Friday", "Weekend only"]
                
                client = Client(meilisearch_url, master_key)
                index = client.index(meilisearch_job_index)
                index_admin_job = client.index(meilisearch_admin_job_index)

                city_filter = city
                job_status_filter = job_status
                job_type_filter = job_type
                plan_filter = plan
                sector_filter = sector
                skills_filter = skills
                specialisation_filter = specialisation
                work_schedule_filter = work_schedule
                workplace_type_filter = workplace_type

                filters = []
                filters_admin = []

                query_ext_job_details = """SELECT
                                                ap.job_reference_id AS id, ap.job_title, ap.job_type, ap.job_overview, ap.company_name, ap.employer_id,
                                                ap.job_description AS job_desc, NULL AS responsibilities, NULL AS additional_info,
                                                ap.skills, ap.country, ap.state AS job_state, ap.city, ap.schedule AS work_schedule,
                                                ap.workplace_type, NULL AS is_paid, NULL AS time_commitment, NULL AS timezone,
                                                NULL AS duration, ap.apply_link, NULL AS currency,
                                                NULL AS benefits, NULL AS required_resume, NULL AS required_cover_letter,
                                                NULL AS required_background_check, NULL AS required_subcontract,
                                                NULL AS is_application_deadline, NULL AS application_deadline_date,
                                                ap.is_active, NULL AS share_url, ap.functional_specification AS specialisation, ap.created_at, ap.admin_job_status AS job_status,   

                                                COALESCE(u.profile_image, su.profile_image) AS profile_image,
                                                NULL AS pricing_category,
                                                NULL AS payment_status,
                                                NULL AS company_country,
                                                NULL AS company_city,
                                                ap.company_sector AS sector,
                                                NULL AS company_description,
                                                NULL AS employer_type, NULL AS website_url, NULL AS company_email,
                                                NULL AS custom_notes, NULL AS calendly_link

                                            FROM admin_job_post ap
                                            LEFT JOIN users u ON ap.employer_id = u.user_id
                                            LEFT JOIN sub_users su ON ap.employer_id = su.sub_user_id

                                            WHERE ap.admin_job_status = 'opened' AND ap.job_reference_id IN %s ORDER BY ap.id DESC LIMIT %s;"""

                if country.lower() != "others":
                    # filter only if not 'Others'
                    country_query = "job_country = " + f"'{country}'"
                    filters.append(f"({country_query})")
                    filters_admin.append(f"({country_query})")

                if city_filter:
                    city_query = " OR ".join([f"job_city = '{city}'" for city in city_filter])
                    filters.append(f"({city_query})")
                    filters_admin.append(f"({city_query})")

                if user_data['user_role'] == 'admin':
                    if job_status_filter:
                        job_status_query = " OR ".join([f"job_status = '{job_status}'" for job_status in job_status_filter])
                        filters.append(f"({job_status_query})")
                        filters_admin.append(f"({job_status_query})")
                
                if job_type_filter:
                    job_type_query = " OR ".join([f"job_type = '{job_type}'" for job_type in job_type_filter])
                    filters.append(f"({job_type_query})")
                    filters_admin.append(f"({job_type_query})")
                
                if plan_filter:
                    plan_query = " OR ".join([f"pricing_category = '{plan}'" for plan in plan_filter])
                    filters.append(f"({plan_query})")
                
                if sector_filter:
                    sector_query = " OR ".join([f"sector = '{sector}'" for sector in sector_filter])
                    filters.append(f"({sector_query})")
                    filters_admin.append(f"({sector_query})")
                
                # if skills_filter:
                #     skills_query = " OR ".join([f"skills = '{skills}'" for skills in skills_filter])
                #     filters.append(f"({skills_query})")

                if skills_filter:
                    if "others" in skills_filter or "Others" in skills_filter:
                        excluded_skills_query = " AND ".join([f"NOT skills = '{skill}'" for skill in skill_list])
                        other_skills_query = f"({excluded_skills_query}) AND skills != '' AND skills IS NOT NULL"
                        skills_filter = [skill for skill in skills_filter if skill != "others" or skill != "Others"]
                        if skills_filter:
                            specific_skills_query = " OR ".join([f"skills = '{skill}'" for skill in skills_filter])
                            filters.append(f"(({specific_skills_query}) OR ({other_skills_query}))")
                            filters_admin.append(f"(({specific_skills_query}) OR ({other_skills_query}))")
                        else:
                            filters.append(f"({other_skills_query})")
                            filters_admin.append(f"({other_skills_query})")
                    else:
                        skills_query = " OR ".join([f"skills = '{skill}'" for skill in skills_filter])
                        filters.append(f"({skills_query})")
                        filters_admin.append(f"({skills_query})")

                # if specialisation_filter:
                #     specialisation_query = " OR ".join([f"specialisation = '{specialisation}'" for specialisation in specialisation_filter])
                #     filters.append(f"({specialisation_query})")

                if specialisation_filter:
                    if "others" in specialisation_filter or "Others" in specialisation_filter:
                        excluded_specialisation_query = " AND ".join([f"NOT specialisation = '{spec}'" for spec in specialisation_list])
                        other_specialisation_query = f"({excluded_specialisation_query}) AND specialisation != '' AND specialisation IS NOT NULL"
                        functional_specialisation_filter = [spec for spec in specialisation_filter if spec != "others" or spec != "Others"]
                        if functional_specialisation_filter:
                            specific_specialisation_query = " OR ".join([f"specialisation = '{spec}'" for spec in functional_specialisation_filter])
                            filters.append(f"(({specific_specialisation_query}) OR ({other_specialisation_query}))")
                            filters_admin.append(f"(({specific_specialisation_query}) OR ({other_specialisation_query}))")
                        else:
                            filters.append(f"({other_specialisation_query})")
                            filters_admin.append(f"({other_specialisation_query})")
                    else:
                        functional_specialisation_query = " OR ".join([f"specialisation = '{spec}'" for spec in specialisation_filter])
                        filters.append(f"({functional_specialisation_query})")
                        filters_admin.append(f"({functional_specialisation_query})")
                
                if work_schedule_filter:
                    work_schedule_query = " OR ".join([f"work_schedule = '{work_schedule}'" for work_schedule in work_schedule_filter])
                    filters.append(f"({work_schedule_query})")
                    filters_admin.append(f"({work_schedule_query})")

                if workplace_type_filter:
                    workplace_type_query = " OR ".join([f"workplace_type = '{workplace_type}'" for workplace_type in workplace_type_filter])
                    filters.append(f"({workplace_type_query})")
                    filters_admin.append(f"({workplace_type_query})")

                if user_data["user_role"] == "professional":
                    job_stat = 'opened'
                    # filters.append("(job_status IN ['opened'])")
                    filters.append(f"job_status={job_stat}")
                    filters_admin.append(f"job_status={job_stat}")

                    get_applied_job_ids = 'select job_id from job_activity where professional_id = %s'
                    get_ids_values = (user_data['user_id'],)
                    applied_job_ids_dict = execute_query(get_applied_job_ids, get_ids_values)
                    applied_job_ids = []
                    if applied_job_ids_dict:
                        applied_job_ids = [job_id['job_id'] for job_id in applied_job_ids_dict]
                    filters.append(f"(job_id NOT IN {applied_job_ids})")
                # else:
                #     filters.append("(job_status != 'drafted')")
                final_filters = " AND ".join(filters)
                final_filters_admin_job = " AND ".join(filters_admin)

                results = index.search(
                    search_text,
                    {
                        'filter': final_filters,
                        'sort': ['job_id:desc'],
                        'limit': 10,
                        'offset': offset
                        # 'attributesToHighlight': ['title']
                    }
                )

                fetched_data = results['hits']
                job_total_count = results['estimatedTotalHits']

                result_job_ids = [id['job_id'] for id in fetched_data]
                if result_job_ids:
                    query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.job_status, jp.custom_notes, jp.country, jp.state as job_state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.timezone, jp.specialisation,
                                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                            COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                            COALESCE(u.country, su.country) AS company_country, 
                                            COALESCE(u.city, su.city) AS company_city, 
                                            COALESCE(u.email_id, su.email_id) AS company_email, 
                                            COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                                            COALESCE(ep.sector, su.sector) AS sector, 
                                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                                            COALESCE(ep.website_url, su.website_url) AS website_url
                                            FROM job_post jp 
                                            LEFT JOIN users u ON jp.employer_id = u.user_id 
                                            LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id 
                                            LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status != 'drafted' AND jp.id IN %s 
                                            ORDER BY CASE
                                                    WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                    WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                    WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                    ELSE 4
                                                END ASC, jp.id DESC;"""

                    val_detail = (tuple(result_job_ids),)
                    job_details = replace_empty_values(execute_query(query_job_details, val_detail))
                else:
                    job_details = []

                records_from_table2_needed = 10 - len(job_details)
                records_from_table2_already_shown = max(0, (page_number - 1) * 10 - job_total_count)

                results_admin_job = index_admin_job.search(
                    search_text,
                    {
                        'filter': final_filters_admin_job,
                        'sort': ['job_id:desc'],
                        'limit': records_from_table2_needed,
                        'offset': records_from_table2_already_shown
                        # 'attributesToHighlight': ['title']
                    }
                )
                
                fetched_admin_job_data = results_admin_job['hits']
                ext_job_total_count = results_admin_job['estimatedTotalHits']

                result_admin_job_ids = [id['job_id'] for id in fetched_admin_job_data]
                if user_data['user_role'] == 'professional':
                    if len(job_details) > 0:
                        if len(job_details) < 10 and records_from_table2_needed > 0:
                            if len(result_admin_job_ids) > 0:
                                ext_job_values = (result_admin_job_ids, records_from_table2_needed) #records_from_table2_already_shown)
                                ext_job_details = execute_query(query_ext_job_details, ext_job_values)
                                if len(ext_job_details) > 0:
                                    job_details.extend(ext_job_details)
                            # job_total_count = job_total_count + len(ext_job_details)
                    else:
                        if len(result_admin_job_ids) > 0:
                            ext_job_values = (result_admin_job_ids, 10) #,records_from_table2_already_shown)
                            job_details = execute_query(query_ext_job_details, ext_job_values)
                        # job_total_count = job_total_count + len(job_details)
                if len(job_details) > 0:
                    for job in job_details:
                            query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                            values = (job['id'], 2,)
                            prof_id_list = execute_query(query, values)
                            temp_list = []
                            for j in prof_id_list:
                                query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, u.is_active, u.payment_status, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                                values = (j['professional_id'],)
                                info = execute_query(query, values)
                                if len(info) > 0:
                                    mod_id = "2C-PR-" + str(info[0]['professional_id'])
                                    info[0].update({'professional_id' : mod_id})
                                    if info[0]['profile_image'] != '':
                                        s3_pic_key = s3_picture_folder_name + str(info[0]['profile_image'])
                                    else:
                                        s3_pic_key = ''
                                    info[0].update({'professional_profile_image' : s3_pic_key})
                                    temp_list.append(info[0])
                                # j.update({"recommended_professional" : temp_list})
                            if user_data["user_role"] == "professional":
                                professional_id = user_data['user_id']
                                profile_percentage = show_percentage(professional_id)
                                quest_dict = {"questions" : []}
                                job_id = job['id']
                                query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                                values = (job_id,)
                                result = execute_query(query, values)
                                if len(result) != 0:
                                    for r in result:
                                        quest_dict["questions"].append(r)
                                job.update(quest_dict)
                                job.update({'created_at': str(job['created_at'])})

                                query = 'select count(job_id) from saved_job where job_id = %s and professional_id = %s'
                                values = (job_id,professional_id,)
                                rslt = execute_query(query, values)
                                if rslt[0]['count(job_id)'] > 0:
                                    job_saved_status = "saved"
                                else:
                                    job_saved_status = "unsaved"
                                job.update({'saved_status': job_saved_status})

                                query = 'select count(job_id) from job_activity where job_id = %s and professional_id = %s'
                                values = (job_id, professional_id,)
                                rslt = execute_query(query, values)
                                if rslt[0]['count(job_id)'] == 0:
                                    job_applied_status = 'not_applied'
                                else:
                                    job_applied_status = 'applied'
                                txt = job['sector']
                                if txt != None:
                                    txt = txt.replace(", ", "_")
                                    txt = txt.replace(" ", "_")
                                else:
                                    txt = "default_sector_image"
                                sector_name = txt + ".png"

                                query = 'select professional_resume from professional_profile where professional_id = %s'
                                values = (professional_id,)
                                rslt = execute_query(query, values)
                                resume_name = rslt[0]['professional_resume']

                                query = 'SELECT ij.employer_feedback, (SELECT COUNT(job_id) FROM invited_jobs WHERE professional_id = ij.professional_id AND job_id = ij.job_id) AS job_count FROM invited_jobs ij WHERE ij.professional_id = %s AND ij.job_id = %s AND ij.is_invite_sent = "Y";'
                                values = (professional_id, job_id,)
                                employee_feedback = execute_query(query, values)
                                if len(employee_feedback) > 0:
                                    if employee_feedback[0]['employer_feedback'] is not None:
                                        job.update({'invited_message' : employee_feedback[0]['employer_feedback']})
                                    else:
                                        job.update({'invited_message' : ''})
                                    job.update({'invited_by_employer' : '1Y'})
                                else:
                                    job.update({'invited_message' : ''})
                                    job.update({'invited_by_employer' : 'N'})
                                if job['profile_image'] != None:
                                    profile_image = s3_employeer_logo_folder_name + job['profile_image']
                                else:
                                    profile_image = s3_employeer_logo_folder_name + "default_profile_picture.png"
                                job.update({'user_resume' : resume_name})
                                job.update({'applied_status': job_applied_status})
                                job.update({'profile_image' : profile_image})
                                job.update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                job.update({'sector' : job['sector']})
                                job.update({'employer_type' : job['employer_type']})
                                job.update({'company_description' : job['company_description']})
                                job.update({'profile_percentage' : profile_percentage})
                            job.update({'sector' : job['sector']})
                            job.update({'employer_type' : job['employer_type']})
                            job.update({"recommended_professional" : temp_list})


                    # job_id = job_details[0]['id']
                    # query = 'select professional_id from sc_recommendation where job_id = %s'
                    # values = (job_id,)
                    # recommended_professional_id = execute_query(query, values)
                    # recommended_professional_list = []
                    # if len(recommended_professional_id) > 0:
                    #     for id in recommended_professional_id:
                    #         query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                    #         values = (id['professional_id'],)
                    #         detail = execute_query(query, values) 
                    #         recommended_professional_list.append(detail[0])
                    # else:
                    #     recommended_professional_list = []
                else:
                    result_json = api_json_response_format(True, "No records found", 0, profile)
                    return result_json
                # recommended_professional_dict = {'recommended_professionals' : recommended_professional_list}
                # profile.update(recommended_professional_dict)               
                job_details_dict = {'job_details': job_details}
                profile.update(job_details_dict)
                profile.update({'total_count' : job_total_count + ext_job_total_count})
                data = fetch_job_filter_params()
                profile.update(data)
                  
                if job_details == "" or job_details == []:
                    result_json = api_json_response_format(True, "No records found", 0, profile)
                else:
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def admin_jobs_meilisearch_filter_results():
    try:
        data = []
        profile = {}
        # country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin" or user_data["user_role"] == "professional":
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                job_status = req_data['job_status']
                job_type = req_data['job_type']
                plan = req_data['plan']
                sector = req_data['sector']
                skills = req_data['skills']
                specialisation = req_data["specialisation"]
                work_schedule = req_data['work_schedule']
                workplace_type = req_data['workplace_type']
                search_text = req_data["search_text"]
                country = req_data["country"]
                offset = (page_number - 1) * 10

                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        # country.append(res[1])
                        city.append(res[0])
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                # skills_list = ["Agile Methodologies", "Algorithm Design", "Analytics", "Application Programming Interfaces (APIs)", "Budgeting", "Business Strategy", "Change Management", "Conflict Resolution", "Contract Management Skills", "Data Analysis", "Database Design", "Debugging", "Direct Sales", "Earned Value Management", "Financial Management", "Human Resource Management", "Keyword Research", "Leadership Skills", "Market Research", "Marketing Skills", "Metrics and KPIs", "Mobile Application Development", "Negotiation", "Operations Management", "Organizational Development", "Presentation", "Process Improvement", "Product Knowledge", "Project Management", "Quality Assurance (QA)", "Recruiting", "Revenue Expansion", "Risk Assessment", "SaaS Knowledge", "Sales and Budget Forecasting", "Salesforce", "Strategic Planning", "Supply Chain Management", "Talent Management", "Team Leadership", "Upselling"]
                specialisation_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite And Board"]
                skill_list = ["Business Strategy", "Change Management", "Conflict Resolution", "Financial Management", "Human Resource Management", "Operations Management", "Organizational Development", "Strategic Planning", "Supply Chain Management", "Talent Management", "Human Resource Management", "Direct Sales", "Leadership Skills", "Market Research", "Negotiation", "Presentation", "Product Knowledge", "Recruiting", "Sales and Budget Forecasting", "Upselling", "Agile Methodologies", "Budgeting", "Contract Management Skills", "Earned Value Management", "Process Improvement", "Risk Assessment", "Analytics", "Data Analysis", "Metrics and KPIs", "Project Management", "Revenue Expansion", "SaaS Knowledge", "Salesforce", "Team Leadership", "Marketing Skills", "Keyword Research", "Algorithm Design", "Application Programming Interfaces (APIs)", "Database Design", "Debugging", "Mobile Application Development", "Quality Assurance (QA)"]
                # sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                # schedule_list = ["Fixed", "Flexible", "Monday to Friday", "Weekend only"]
                
                client = Client(meilisearch_url, master_key)
                index = client.index(meilisearch_job_index)
                index_admin_job = client.index(meilisearch_admin_job_index)

                city_filter = city
                job_status_filter = job_status
                job_type_filter = job_type
                plan_filter = plan
                sector_filter = sector
                skills_filter = skills
                specialisation_filter = specialisation
                work_schedule_filter = work_schedule
                workplace_type_filter = workplace_type

                filters = []
                filters_admin = []

                query_ext_job_details = """SELECT
                                                ap.job_reference_id AS id, ap.job_title, ap.job_type, ap.job_overview, ap.company_name, ap.employer_id,
                                                ap.job_description AS job_desc, NULL AS responsibilities, NULL AS additional_info,
                                                ap.skills, ap.country, ap.state AS job_state, ap.city, ap.schedule AS work_schedule,
                                                ap.workplace_type, NULL AS is_paid, NULL AS time_commitment, NULL AS timezone,
                                                NULL AS duration, ap.apply_link, NULL AS currency,
                                                NULL AS benefits, NULL AS required_resume, NULL AS required_cover_letter,
                                                NULL AS required_background_check, NULL AS required_subcontract,
                                                NULL AS is_application_deadline, NULL AS application_deadline_date,
                                                ap.is_active, NULL AS share_url, ap.functional_specification AS specialisation, ap.created_at, ap.admin_job_status AS job_status,   

                                                COALESCE(u.profile_image, su.profile_image) AS profile_image,
                                                NULL AS pricing_category,
                                                NULL AS payment_status,
                                                NULL AS company_country,
                                                NULL AS company_city,
                                                ap.company_sector AS sector,
                                                NULL AS company_description,
                                                NULL AS employer_type, NULL AS website_url, NULL AS company_email,
                                                NULL AS custom_notes, NULL AS calendly_link

                                            FROM admin_job_post ap
                                            LEFT JOIN users u ON ap.employer_id = u.user_id
                                            LEFT JOIN sub_users su ON ap.employer_id = su.sub_user_id

                                            WHERE ap.admin_job_status = 'opened' AND ap.job_reference_id IN %s ORDER BY ap.id DESC LIMIT %s;"""

                if country.lower() != "others":
                    # filter only if not 'Others'
                    country_query = "job_country = " + f"'{country}'"
                    filters.append(f"({country_query})")
                    filters_admin.append(f"({country_query})")

                if city_filter:
                    city_query = " OR ".join([f"job_city = '{city}'" for city in city_filter])
                    filters.append(f"({city_query})")
                    filters_admin.append(f"({city_query})")

                if user_data['user_role'] == 'admin':
                    if job_status_filter:
                        job_status_query = " OR ".join([f"job_status = '{job_status}'" for job_status in job_status_filter])
                        filters.append(f"({job_status_query})")
                        filters_admin.append(f"({job_status_query})")
                
                if job_type_filter:
                    job_type_query = " OR ".join([f"job_type = '{job_type}'" for job_type in job_type_filter])
                    filters.append(f"({job_type_query})")
                    filters_admin.append(f"({job_type_query})")
                
                if plan_filter:
                    plan_query = " OR ".join([f"pricing_category = '{plan}'" for plan in plan_filter])
                    filters.append(f"({plan_query})")
                
                if sector_filter:
                    sector_query = " OR ".join([f"sector = '{sector}'" for sector in sector_filter])
                    filters.append(f"({sector_query})")
                    filters_admin.append(f"({sector_query})")
                
                # if skills_filter:
                #     skills_query = " OR ".join([f"skills = '{skills}'" for skills in skills_filter])
                #     filters.append(f"({skills_query})")

                if skills_filter:
                    if "others" in skills_filter or "Others" in skills_filter:
                        excluded_skills_query = " AND ".join([f"NOT skills = '{skill}'" for skill in skill_list])
                        other_skills_query = f"({excluded_skills_query}) AND skills != '' AND skills IS NOT NULL"
                        skills_filter = [skill for skill in skills_filter if skill != "others" or skill != "Others"]
                        if skills_filter:
                            specific_skills_query = " OR ".join([f"skills = '{skill}'" for skill in skills_filter])
                            filters.append(f"(({specific_skills_query}) OR ({other_skills_query}))")
                            filters_admin.append(f"(({specific_skills_query}) OR ({other_skills_query}))")
                        else:
                            filters.append(f"({other_skills_query})")
                            filters_admin.append(f"({other_skills_query})")
                    else:
                        skills_query = " OR ".join([f"skills = '{skill}'" for skill in skills_filter])
                        filters.append(f"({skills_query})")
                        filters_admin.append(f"({skills_query})")

                # if specialisation_filter:
                #     specialisation_query = " OR ".join([f"specialisation = '{specialisation}'" for specialisation in specialisation_filter])
                #     filters.append(f"({specialisation_query})")

                if specialisation_filter:
                    if "others" in specialisation_filter or "Others" in specialisation_filter:
                        excluded_specialisation_query = " AND ".join([f"NOT specialisation = '{spec}'" for spec in specialisation_list])
                        other_specialisation_query = f"({excluded_specialisation_query}) AND specialisation != '' AND specialisation IS NOT NULL"
                        functional_specialisation_filter = [spec for spec in specialisation_filter if spec != "others" or spec != "Others"]
                        if functional_specialisation_filter:
                            specific_specialisation_query = " OR ".join([f"specialisation = '{spec}'" for spec in functional_specialisation_filter])
                            filters.append(f"(({specific_specialisation_query}) OR ({other_specialisation_query}))")
                            filters_admin.append(f"(({specific_specialisation_query}) OR ({other_specialisation_query}))")
                        else:
                            filters.append(f"({other_specialisation_query})")
                            filters_admin.append(f"({other_specialisation_query})")
                    else:
                        functional_specialisation_query = " OR ".join([f"specialisation = '{spec}'" for spec in specialisation_filter])
                        filters.append(f"({functional_specialisation_query})")
                        filters_admin.append(f"({functional_specialisation_query})")
                
                if work_schedule_filter:
                    work_schedule_query = " OR ".join([f"work_schedule = '{work_schedule}'" for work_schedule in work_schedule_filter])
                    filters.append(f"({work_schedule_query})")
                    filters_admin.append(f"({work_schedule_query})")

                if workplace_type_filter:
                    workplace_type_query = " OR ".join([f"workplace_type = '{workplace_type}'" for workplace_type in workplace_type_filter])
                    filters.append(f"({workplace_type_query})")
                    filters_admin.append(f"({workplace_type_query})")

                if user_data["user_role"] == "professional":
                    job_stat = 'opened'
                    # filters.append("(job_status IN ['opened'])")
                    filters.append(f"job_status={job_stat}")
                    filters_admin.append(f"job_status={job_stat}")

                    get_applied_job_ids = 'select job_id from job_activity where professional_id = %s'
                    get_ids_values = (user_data['user_id'],)
                    applied_job_ids_dict = execute_query(get_applied_job_ids, get_ids_values)
                    applied_job_ids = []
                    if applied_job_ids_dict:
                        applied_job_ids = [job_id['job_id'] for job_id in applied_job_ids_dict]
                    filters.append(f"(job_id NOT IN {applied_job_ids})")
                # else:
                #     filters.append("(job_status != 'drafted')")
                final_filters = " AND ".join(filters)
                final_filters_admin_job = " AND ".join(filters_admin)

                results = index.search(
                    search_text,
                    {
                        'filter': final_filters,
                        'sort': ['job_id:desc'],
                        'limit': 10,
                        'offset': offset
                        # 'attributesToHighlight': ['title']
                    }
                )

                fetched_data = results['hits']
                job_total_count = results['estimatedTotalHits']

                result_job_ids = [id['job_id'] for id in fetched_data]
                if result_job_ids:
                    query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.job_status, jp.custom_notes, jp.country, jp.state as job_state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.timezone, jp.specialisation,
                                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                            COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                            COALESCE(u.country, su.country) AS company_country, 
                                            COALESCE(u.city, su.city) AS company_city, 
                                            COALESCE(u.email_id, su.email_id) AS company_email, 
                                            COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                                            COALESCE(ep.sector, su.sector) AS sector, 
                                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                                            COALESCE(ep.website_url, su.website_url) AS website_url
                                            FROM job_post jp 
                                            LEFT JOIN users u ON jp.employer_id = u.user_id 
                                            LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id 
                                            LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status != 'drafted' AND jp.id IN %s 
                                            ORDER BY CASE
                                                    WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                    WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                    WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                    ELSE 4
                                                END ASC, jp.id DESC;"""

                    val_detail = (tuple(result_job_ids),)
                    job_details = replace_empty_values(execute_query(query_job_details, val_detail))
                else:
                    job_details = []

                # if user_data['user_role'] != 'admin':
                #     records_from_table2_needed = 10 - len(job_details)
                #     records_from_table2_already_shown = max(0, (page_number - 1) * 10 - job_total_count)
                #     results_admin_job = index_admin_job.search(
                #         search_text,
                #         {
                #             'filter': final_filters_admin_job,
                #             'sort': ['job_id:desc'],
                #             'limit': records_from_table2_needed,
                #             'offset': records_from_table2_already_shown
                #             # 'attributesToHighlight': ['title']
                #         }
                #     )
                    
                #     fetched_admin_job_data = results_admin_job['hits']
                #     ext_job_total_count = results_admin_job['estimatedTotalHits']

                #     result_admin_job_ids = [id['job_id'] for id in fetched_admin_job_data]

                
                #     if len(job_details) > 0:
                #         if len(job_details) < 10 and records_from_table2_needed > 0:
                #             if len(result_admin_job_ids) > 0:
                #                 ext_job_values = (result_admin_job_ids, records_from_table2_needed) #records_from_table2_already_shown)
                #                 ext_job_details = execute_query(query_ext_job_details, ext_job_values)
                #                 if len(ext_job_details) > 0:
                #                     job_details.extend(ext_job_details)
                #             # job_total_count = job_total_count + len(ext_job_details)
                #     else:
                #         if len(result_admin_job_ids) > 0:
                #             ext_job_values = (result_admin_job_ids, 10) #,records_from_table2_already_shown)
                #             job_details = execute_query(query_ext_job_details, ext_job_values)
                #         # job_total_count = job_total_count + len(job_details)
                if user_data['user_role'] != 'admin':

                    page_start = (page_number - 1) * 10
                    page_end = page_start + 10

                    # --------------------------------------------------
                    # CASE 1: Page fully inside NORMAL jobs
                    # --------------------------------------------------
                    if page_end <= job_total_count:
                        ext_job_total_count = 0
                        # job_details already contains 10 normal jobs

                    # --------------------------------------------------
                    # CASE 2: MIXED PAGE (normal ends on this page)
                    # --------------------------------------------------
                    elif page_start < job_total_count:
                        normal_needed = job_total_count - page_start
                        admin_needed = 10 - normal_needed

                        # fetch admin jobs (start from beginning)
                        results_admin_job = index_admin_job.search(
                            search_text,
                            {
                                'filter': final_filters_admin_job,
                                'sort': ['job_id:desc'],
                                'limit': admin_needed,
                                'offset': 0
                            }
                        )

                        fetched_admin_job_data = results_admin_job['hits']
                        ext_job_total_count = results_admin_job['estimatedTotalHits']

                        result_admin_job_ids = [job['job_id'] for job in fetched_admin_job_data]

                        if len(result_admin_job_ids) > 0:
                            ext_job_values = (result_admin_job_ids, admin_needed)
                            ext_job_details = execute_query(query_ext_job_details, ext_job_values)
                            job_details.extend(ext_job_details)

                    # --------------------------------------------------
                    # CASE 3: Page fully inside ADMIN jobs
                    # --------------------------------------------------
                    else:
                        admin_offset = page_start - job_total_count

                        results_admin_job = index_admin_job.search(
                            search_text,
                            {
                                'filter': final_filters_admin_job,
                                'sort': ['job_id:desc'],
                                'limit': 10,
                                'offset': admin_offset
                            }
                        )

                        fetched_admin_job_data = results_admin_job['hits']
                        ext_job_total_count = results_admin_job['estimatedTotalHits']

                        result_admin_job_ids = [job['job_id'] for job in fetched_admin_job_data]

                        if len(result_admin_job_ids) > 0:
                            ext_job_values = (result_admin_job_ids, 10)
                            job_details = execute_query(query_ext_job_details, ext_job_values)


                else:
                    ext_job_total_count = 0
                if len(job_details) > 0:
                    for job in job_details:
                            query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                            values = (job['id'], 2,)
                            prof_id_list = execute_query(query, values)
                            temp_list = []
                            for j in prof_id_list:
                                query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, u.is_active, u.payment_status, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                                values = (j['professional_id'],)
                                info = execute_query(query, values)
                                if len(info) > 0:
                                    mod_id = "2C-PR-" + str(info[0]['professional_id'])
                                    info[0].update({'professional_id' : mod_id})
                                    if info[0]['profile_image'] != '':
                                        s3_pic_key = s3_picture_folder_name + str(info[0]['profile_image'])
                                    else:
                                        s3_pic_key = ''
                                    info[0].update({'professional_profile_image' : s3_pic_key})
                                    temp_list.append(info[0])
                                # j.update({"recommended_professional" : temp_list})
                            if user_data["user_role"] == "professional":
                                professional_id = user_data['user_id']
                                profile_percentage = show_percentage(professional_id)
                                quest_dict = {"questions" : []}
                                job_id = job['id']
                                query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                                values = (job_id,)
                                result = execute_query(query, values)
                                if len(result) != 0:
                                    for r in result:
                                        quest_dict["questions"].append(r)
                                job.update(quest_dict)
                                job.update({'created_at': str(job['created_at'])})

                                query = 'select count(job_id) from saved_job where job_id = %s and professional_id = %s'
                                values = (job_id,professional_id,)
                                rslt = execute_query(query, values)
                                if rslt[0]['count(job_id)'] > 0:
                                    job_saved_status = "saved"
                                else:
                                    job_saved_status = "unsaved"
                                job.update({'saved_status': job_saved_status})

                                query = 'select count(job_id) from job_activity where job_id = %s and professional_id = %s'
                                values = (job_id, professional_id,)
                                rslt = execute_query(query, values)
                                if rslt[0]['count(job_id)'] == 0:
                                    job_applied_status = 'not_applied'
                                else:
                                    job_applied_status = 'applied'
                                txt = job['sector']
                                if txt != None:
                                    txt = txt.replace(", ", "_")
                                    txt = txt.replace(" ", "_")
                                else:
                                    txt = "default_sector_image"
                                sector_name = txt + ".png"

                                query = 'select professional_resume from professional_profile where professional_id = %s'
                                values = (professional_id,)
                                rslt = execute_query(query, values)
                                resume_name = rslt[0]['professional_resume']

                                query = 'SELECT ij.employer_feedback, (SELECT COUNT(job_id) FROM invited_jobs WHERE professional_id = ij.professional_id AND job_id = ij.job_id) AS job_count FROM invited_jobs ij WHERE ij.professional_id = %s AND ij.job_id = %s AND ij.is_invite_sent = "Y";'
                                values = (professional_id, job_id,)
                                employee_feedback = execute_query(query, values)
                                if len(employee_feedback) > 0:
                                    if employee_feedback[0]['employer_feedback'] is not None:
                                        job.update({'invited_message' : employee_feedback[0]['employer_feedback']})
                                    else:
                                        job.update({'invited_message' : ''})
                                    job.update({'invited_by_employer' : '1Y'})
                                else:
                                    job.update({'invited_message' : ''})
                                    job.update({'invited_by_employer' : 'N'})
                                if job['profile_image'] != None:
                                    profile_image = s3_employeer_logo_folder_name + job['profile_image']
                                else:
                                    profile_image = s3_employeer_logo_folder_name + "default_profile_picture.png"
                                job.update({'user_resume' : resume_name})
                                job.update({'applied_status': job_applied_status})
                                job.update({'profile_image' : profile_image})
                                job.update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                job.update({'sector' : job['sector']})
                                job.update({'employer_type' : job['employer_type']})
                                job.update({'company_description' : job['company_description']})
                                job.update({'profile_percentage' : profile_percentage})
                            job.update({'sector' : job['sector']})
                            job.update({'employer_type' : job['employer_type']})
                            job.update({"recommended_professional" : temp_list})


                    # job_id = job_details[0]['id']
                    # query = 'select professional_id from sc_recommendation where job_id = %s'
                    # values = (job_id,)
                    # recommended_professional_id = execute_query(query, values)
                    # recommended_professional_list = []
                    # if len(recommended_professional_id) > 0:
                    #     for id in recommended_professional_id:
                    #         query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                    #         values = (id['professional_id'],)
                    #         detail = execute_query(query, values) 
                    #         recommended_professional_list.append(detail[0])
                    # else:
                    #     recommended_professional_list = []
                else:
                    result_json = api_json_response_format(True, "No records found", 0, profile)
                    return result_json
                # recommended_professional_dict = {'recommended_professionals' : recommended_professional_list}
                # profile.update(recommended_professional_dict)               
                job_details_dict = {'job_details': job_details}
                profile.update(job_details_dict)
                profile.update({'total_count' : job_total_count + ext_job_total_count})
                data = fetch_job_filter_params()
                profile.update(data)
                  
                if job_details == "" or job_details == []:
                    result_json = api_json_response_format(True, "No records found", 0, profile)
                else:
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def super_admin_best_fit_applicants():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "admin":
                final_result = []
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False, "Please fill in all the required fields(job_id).", 409, {})
                    return result_json
                
                job_id = req_data['job_id']

                get_job_title_query = "select job_title from job_post where id = %s"
                job_title_dict = execute_query(get_job_title_query, (job_id,))
                if job_title_dict:
                    job_title = job_title_dict[0]['job_title']
                else:
                    job_title = ''
                            
                query = 'SELECT professional_id FROM invited_jobs ij WHERE job_id = %s AND invite_mode = %s AND NOT EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.professional_id = ij.professional_id AND ja.job_id = ij.job_id )'
                values = (job_id, 'from BFA',)
                professional_id_dict = execute_query(query, values)
                if professional_id_dict:
                    new_id_list = [j['professional_id'] for j in professional_id_dict]
                else:
                    new_id_list = []

                for id in new_id_list:
                    query = """
                    SELECT DISTINCT
                            p.about, 
                            p.id, 
                            p.professional_id, 
                            ps.id AS skill_id, 
                            ps.skill_name, 
                            ps.skill_level, 
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
                            ed.institute_location 
                        FROM 
                            users AS u 
                        LEFT JOIN 
                            professional_profile AS p ON u.user_id = p.professional_id 
                        LEFT JOIN (
                            SELECT DISTINCT * 
                            FROM professional_experience 
                            WHERE professional_id = %s 
                            ORDER BY 
                                CASE 
                                    WHEN end_year = 'Present' THEN 9999  -- Large value for 'Present'
                                    ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                END DESC, 
                                CAST(SUBSTRING_INDEX(end_year, '-', -1) AS UNSIGNED) DESC, 
                                CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC, 
                                CAST(SUBSTRING_INDEX(start_year, '-', -1) AS UNSIGNED) DESC
                        ) AS pe ON u.user_id = pe.professional_id 
                        LEFT JOIN (
                            SELECT DISTINCT * 
                            FROM professional_education 
                            WHERE professional_id = %s
                            ORDER BY 
                                CASE 
                                    WHEN end_year = 'Present' THEN 9999  -- Large value for 'Present'
                                    ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                END DESC, 
                                CAST(SUBSTRING_INDEX(end_year, '-', -1) AS UNSIGNED) DESC, 
                                CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC, 
                                CAST(SUBSTRING_INDEX(start_year, '-', -1) AS UNSIGNED) DESC
                        ) AS ed ON u.user_id = ed.professional_id 
                        LEFT JOIN 
                            professional_skill AS ps ON u.user_id = ps.professional_id 
                        WHERE 
                            u.user_id = %s
                        ORDER BY 
                            CASE 
                                WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 
                                ELSE CAST(SUBSTRING_INDEX(pe.start_year, '-', 1) AS UNSIGNED) 
                            END DESC, 
                            CASE 
                                WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 
                                ELSE CAST(SUBSTRING_INDEX(pe.start_year, '-', -1) AS UNSIGNED) 
                            END DESC,
                            CASE 
                                WHEN (ed.end_year IS NULL OR ed.end_year = '') THEN 0 
                                ELSE CAST(SUBSTRING_INDEX(ed.end_year, '-', 1) AS UNSIGNED) 
                            END DESC, 
                            CASE 
                                WHEN (ed.end_month IS NULL OR ed.end_month = '') THEN 0 
                                ELSE CAST(SUBSTRING_INDEX(ed.end_year, '-', -1) AS UNSIGNED) 
                            END DESC;
                    """
                    values = (id,id,id,)
                    profile_result = execute_query(query, values)

                    if len(profile_result) == 0:
                        continue

                    profile_dict = {
                        'id': profile_result[0]['id'],
                        'professional_id': "2C-PR-" + str(profile_result[0]['professional_id']),
                        'about': replace_empty_values1(profile_result[0]['about']),
                        'experience': [],
                        'education': [],
                        'skills': []
                    }

                    experience_set = set()
                    education_set = set()
                    skills_set = set()

                    for row in profile_result:
                        # Experience data
                        if row['experience_id'] and row['experience_id'] not in experience_set:
                            # start_date = format_date(row['experience_start_year'], row['experience_start_month'])
                            # end_date = format_date(row['experience_end_year'], row['experience_end_month'])
                            if row['experience_start_year'] != None:
                                start_date = row['experience_start_year']
                            else:
                                start_date = ''
                            if row['experience_end_year'] != None:
                                end_date = row['experience_end_year']
                            else:
                                end_date = ''
                            profile_dict['experience'].append({
                                'id': row['experience_id'],
                                'company_name': replace_empty_values1(row['company_name']),
                                'job_title': replace_empty_values1(row['job_title']),
                                'start_date': start_date,
                                'end_date': end_date,
                                'job_description': replace_empty_values1(row['job_description']),
                                'job_location': replace_empty_values1(row['job_location'])
                            })
                            experience_set.add(row['experience_id'])

                        # Education data
                        if row['education_id'] and row['education_id'] not in education_set:
                            # start_date = format_date(row['education_start_year'], row['education_start_month'])
                            # end_date = format_date(row['education_end_year'], row['education_end_month'])
                            if row['education_start_year'] != None:
                                start_date = row['education_start_year']
                            else:
                                start_date = ''
                            if row['education_end_year'] != None:
                                end_date = row['education_end_year']
                            else:
                                end_date = ''
                            profile_dict['education'].append({
                                'id': row['education_id'],
                                'institute_name': replace_empty_values1(row['institute_name']),
                                'degree_level': replace_empty_values1(row['degree_level']),
                                'specialisation': replace_empty_values1(row['specialisation']),
                                'start_date': start_date,
                                'end_date': end_date,
                                'institute_location': replace_empty_values1(row['institute_location'])
                            })
                            education_set.add(row['education_id'])

                        # Skills data
                        if row['skill_id'] and row['skill_id'] not in skills_set:
                            if row['skill_name'] == "" and row['skill_level'] == "":
                                continue
                            else:
                                profile_dict['skills'].append({
                                    'id': row['skill_id'],
                                    'skill_name': replace_empty_values1(row['skill_name']),
                                    'skill_level': replace_empty_values1(row['skill_level'])
                                })
                                skills_set.add(row['skill_id'])

                    if profile_dict['skills'] or profile_dict['education'] or profile_dict['experience'] or profile_dict['about']:
                        final_result.append(profile_dict)
                        # if len(final_result) >= profiles_to_fetch:
                        #     break

                    # current_id = res['id']

                if not final_result:
                    final_dict = {
                        "final_result": [],
                        "job_title" : job_title,
                        "total_professionals": 0
                    }
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Total Professionals' : 0,
                                    'Message': "There is no professionals to display."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Bestfit Applicants", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Bestfit Applicants",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: super admin best fit applicants, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, final_dict)
                    return result_json
                final_dict = {
                    "final_result": final_result,
                    "job_title" : job_title
                }
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': "Professionals details displayed successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Bestfit Applicants", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Bestfit Applicants",event_properties, temp_dict.get('Message'), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: super admin best fit applicants, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, final_dict)
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching professional details on the super admin best fit applicants."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Bestfit Applicants Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Bestfit Applicants Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: super admin best fit applicants Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:
        return result_json

def get_job_share_link():
    try:
        req_data = request.get_json()                        
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"job_id required",204,{})  
            return result_json
        job_id = req_data['job_id'] 
        
        if isUserExist("job_post","id",job_id):
            # query = '''SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.specialisation, jp.timezone, jp.additional_info, jp.skills, jp.job_status, jp.custom_notes, jp.country as job_country, jp.state as job_state, jp.city as job_city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, 
            # COALESCE(u.profile_image, su.profile_image) AS profile_image, 
            # COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
            # COALESCE(u.payment_status, su.payment_status) AS payment_status, 
            # COALESCE(u.country, su.country) AS company_country, 
            # COALESCE(u.city, su.city) AS company_city, 
            # COALESCE(u.email_id, su.email_id) AS company_email, 
            # COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
            # COALESCE(ep.sector, su.sector) AS sector, 
            # COALESCE(ep.company_description, su.company_description) AS company_description, 
            # COALESCE(ep.company_name, su.company_name) AS company_name, 
            # COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
            # COALESCE(ep.website_url, su.website_url) AS website_url
            # FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s'''

            query = '''SELECT jp.id, jp.job_title, jp.job_desc, jp.job_status, jp.employer_id,
                       COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                       COALESCE(ep.sector, su.sector) AS sector
                       FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id 
                       LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id 
                       LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s'''
            values = (job_id,)
            job_details_data_set = execute_query(query,values)

            if len(job_details_data_set) > 0:
                if job_details_data_set[0]['profile_image'] != None:
                    profile_image = s3_employer_picture_folder_name + job_details_data_set[0]['profile_image']
                else:
                    profile_image = s3_employer_picture_folder_name + "default_profile_picture.png"
                
                txt = job_details_data_set[0]['sector']
                if txt != None:
                    txt = txt.replace(", ", "_")
                    txt = txt.replace(" ", "_")
                else:
                    txt = "default_sector_image"
                sector_name = txt + ".png"
                job_details_data_set[0].update({'profile_image' : profile_image})
                job_details_data_set[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(job_details_data_set))
            else:
                job_details_data_set = []
                result_json = api_json_response_format(False,"Job not found",204,{})
        else:
            result_json = api_json_response_format(False,"Job not found",500,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json


def get_users_feedback():
    try:
        result_json = {}
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            req_data = request.get_json()                        
            if 'user_role' not in req_data:
                result_json = api_json_response_format(False,"user_role required",204,{})  
                return result_json
            if 'page_number' not in req_data:
                result_json = api_json_response_format(False,"page_number required",204,{})  
                return result_json
            user_role = req_data['user_role']
            page_number = req_data['page_number']
            offset = (page_number - 1) * 10
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                if user_role == 'employer':
                    user_role = ['employer', 'employer_sub_admin', 'recruiter']
                    query = "SELECT ch.user_id, ch.user_name AS full_name, ch.email_id, ch.user_role, ch.rating, MAX(ch.created_at) AS latest_created_at FROM chat_history ch WHERE ch.user_role IN %s and ch.transcript_summary != '' GROUP BY ch.user_id, ch.user_name, ch.email_id, ch.user_role ORDER BY `ch`.`email_id` ASC LIMIT 10 OFFSET %s"
                    values = (tuple(user_role), offset,)
                    count_query = "SELECT DISTINCT(email_id) FROM `chat_history` where user_role IN %s and transcript_summary != '';"
                    count_values = (tuple(user_role),)
                else:
                    query = "SELECT ch.user_id, ch.user_name AS full_name, ch.email_id, ch.user_role, ch.rating, MAX(ch.created_at) AS latest_created_at FROM chat_history ch WHERE ch.user_role = %s and ch.transcript_summary != '' GROUP BY ch.user_id, ch.user_name, ch.email_id, ch.user_role ORDER BY `ch`.`email_id` ASC LIMIT 10 OFFSET %s"
                    values = (user_role, offset,)
                    count_query = "SELECT DISTINCT(email_id) FROM `chat_history` where user_role = %s and transcript_summary != '';"
                    count_values = (user_role,)
                users_feedback_data_set = execute_query(query,values)
                total_users_dict = execute_query(count_query, count_values)
                if total_users_dict:
                    total_count = len(total_users_dict)
                else:
                    total_count = 0
                final_rslt = {"feedback" : [], "total_count" : 0}
                if len(users_feedback_data_set) > 0:
                    final_rslt.update({"feedback" : users_feedback_data_set})
                    final_rslt.update({"total_count" : total_count})
                    result_json = api_json_response_format(True, "Feedbacks fetched successfully", 0, final_rslt)
                else:
                    final_rslt.update({"feedback" : users_feedback_data_set})
                    final_rslt.update({"total_count" : total_count})
                    result_json = api_json_response_format(False, "No feedbacks found", 204, final_rslt)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def get_user_summary():
    try:
        result_json = {}
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            req_data = request.get_json()                        
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"email_id required",204,{})  
                return result_json
            email_id = req_data['email_id']
            user_data = get_user_data(token_result["email_id"])

            if user_data["user_role"] == "admin":
                query = "select user_id, user_name as full_name, rating, email_id, transcript_summary, feedback, created_at from chat_history where email_id = %s;"   #and transcript_summary !='';
                values = (email_id,)
                user_summary_data_set = execute_query(query,values)
                if len(user_summary_data_set) > 0:
                    result_json = api_json_response_format(True, "Summary fetched successfully", 0, user_summary_data_set)
                else:
                    result_json = api_json_response_format(False, "No summary found", 204, {})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def feedback_search():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'user_role' not in req_data:
            result_json = api_json_response_format(False,"user_role required",204,{})  
            return result_json
        if 'search_text' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})
            return result_json
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                user_role = req_data['user_role']
                search_text = req_data['search_text']
                page_number = req_data['page_number']
                offset = (page_number - 1) * 10
                if user_role == 'employer':
                    user_role = ['employer', 'employer_sub_admin', 'recruiter']
                else:
                    user_role = ['professional']
                # data_query = """SELECT ch.user_id, ch.user_name AS full_name, ch.email_id, ch.user_role, ch.rating, ch.created_at FROM chat_history ch INNER JOIN ( SELECT user_id, MAX(created_at) AS latest_created_at FROM chat_history WHERE user_role IN %s AND ( user_id LIKE %s OR email_id LIKE %s OR user_name LIKE %s ) GROUP BY user_id ) subquery ON ch.user_id = subquery.user_id AND ch.created_at = subquery.latest_created_at WHERE ch.transcript_summary != '' ORDER BY ch.created_at DESC""" #transcript_summary != '' AND
                data_query = "SELECT ch.user_id, ch.user_name AS full_name, ch.email_id, ch.user_role, ch.rating, ch.created_at FROM chat_history ch INNER JOIN ( SELECT user_id, MAX(created_at) AS latest_created_at FROM chat_history WHERE user_role IN %s AND ( user_id LIKE %s OR email_id LIKE %s OR user_name LIKE %s ) GROUP BY user_id ) subquery ON ch.user_id = subquery.user_id AND ch.created_at = subquery.latest_created_at ORDER BY ch.created_at DESC"
                search_term = f'%{search_text}%'
                count_values = (tuple(user_role), search_term, search_term, search_term,)
                total_count = execute_query(data_query, count_values)
                total_count = len(total_count)
                data_query = data_query + " LIMIT 10 OFFSET %s"
                data_values = (tuple(user_role), search_term, search_term, search_term, offset,)
                feedback_details = execute_query(data_query, data_values)
                
                final_rslt = {"feedback" : [], "total_count" : 0}
                if len(feedback_details) > 0:
                    final_rslt.update({"feedback" : feedback_details})
                    final_rslt.update({"total_count" : total_count})
                    result_json = api_json_response_format(True, "Feedbacks fetched successfully", 0, final_rslt)
                else:
                    final_rslt.update({"feedback" : feedback_details})
                    final_rslt.update({"total_count" : total_count})
                    result_json = api_json_response_format(False, "No feedbacks found", 204, final_rslt)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def admin_make_recommendation_search():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()
            if 'search_text' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
                return result_json
            if 'job_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
                return result_json
            user_data = get_user_data(token_result["email_id"])
            search_text = req_data['search_text']
            job_id = req_data['job_id']
            if search_text.startswith("2C"):
                split_txt = search_text.split("-")
                if len(split_txt) > 2:
                    search_text = (search_text.split("-")[2])
            if user_data["user_role"] == "admin":
                if search_text != '':
                    query = """WITH LatestExperience AS (SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, 
                                                            pe.created_at,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY pe.professional_id
                                                                ORDER BY
                                                                    COALESCE(pe.start_year, '0000-00') DESC,
                                                                    COALESCE(pe.start_month, '00') DESC,
                                                                    pe.created_at DESC
                                                                ) AS rn
                                                        FROM
                                                            professional_experience AS pe
                                                        )
                                SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, u.payment_status, u.is_active, u.email_id, 
                                u.profile_percentage, p.professional_id, u2.profile_image, le.job_title FROM users AS u 
                                LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id
                                LEFT JOIN professional_experience AS pe ON p.professional_id = pe.professional_id
                                LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                                LEFT JOIN users AS u2 ON u.user_id = u2.user_id
                                WHERE p.professional_id > 0 AND u.email_active = 'Y' AND (u.first_name LIKE %s OR u.last_name LIKE %s OR 
                                CONCAT(u.first_name, ' ', u.last_name) LIKE %s OR u.user_id LIKE %s)
                                GROUP BY u.user_id, u.first_name, u.last_name, u.country, u.state, u.city, u.profile_percentage, u.pricing_category, 
                                u.is_active, u.payment_status, p.professional_id, u2.profile_image, le.job_title ORDER BY p.professional_id DESC;"""
                    search_term = f"%{search_text}%"
                    values = (search_term, search_term, search_term, search_term,)
                    # total_count = execute_query(query,values)
                    # total_count = len(total_count)
                    # data_query = query + " LIMIT 10 OFFSET %s;"
                    # values = (search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, offset,)
                    # candidates_desc = execute_query(data_query, values)
                    professional_details_dict = execute_query(query, values)
                    if professional_details_dict:
                        for i in professional_details_dict:
                            check_applied_status_query = "select count(id) as count from job_activity where professional_id = %s and job_id = %s"
                            check_applied_status_values = (i['professional_id'], job_id,)
                            check_applied_status = execute_query(check_applied_status_query, check_applied_status_values)
                            if check_applied_status:
                                if check_applied_status[0]['count'] > 0:
                                    applied_status_flag = "applied"
                                else:
                                    applied_status_flag = "not_applied"
                            else:
                                applied_status_flag = "not_applied"
                            temp_id = "2C-PR-"+str(i['professional_id'])
                            i.update({"professional_id" : temp_id})
                            i.update({"profile_image" : s3_picture_folder_name + i["profile_image"]})
                            i.update({"applied_status" : applied_status_flag})
                        result_json = api_json_response_format(True,"Details fetched successfully!",0,professional_details_dict)                    
                    else:
                        result_json = api_json_response_format(True,"No records found.",0,{})
                else:
                    result_json = api_json_response_format(True,"Please enter the valid input",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
# def filter_candidates(candidates, filters):
#     filtered_candidates = []

#     for candidate in candidates:
#         match_found = False

#         for key, filter_value in filters.items():
#             if candidate.get(key) is None:
#                 continue
#             candidate_value = candidate[key]

#             if key == "skills" or key == "mode_of_communication" or key == "functional_specification" or key == "job_type" or key == "industry_sector" or key == "sector":
#                 candidate_set = set(map(str.strip, candidate_value.split(',')))
#                 filter_set = set(filter_value)
#                 if candidate_set & filter_set:
#                     match_found = True

#             elif key == "profile_percentage":
#                 min_val, max_val = map(int, filter_value.split('-'))
#                 if min_val <= candidate_value <= max_val:
#                     match_found = True

#             elif isinstance(filter_value, list): # For country, city
#                 if candidate_value in filter_value:
#                     match_found = True

#             else:
#                 if candidate_value == filter_value:
#                     match_found = True

#             if match_found:
#                 filtered_candidates.append(candidate)
#                 break 
#     return filtered_candidates

def filter_candidates(candidates, filters):
    filtered_candidates = []

    for candidate in candidates:
        match_found = True  # Assume the candidate matches unless proven otherwise

        for key, filter_value in filters.items():
            if not filter_value:  # Skip filters that are not provided or are empty
                continue
            
            if key == 'plan':
                key = 'pricing_category'
            candidate_value = candidate.get(key)

            # If the candidate's value is None, it does not satisfy this filter
            if candidate_value is None:
                match_found = False
                break

            if key in {"skills", "mode_of_communication", "functional_specification", "job_type", "industry_sector", "sector"}:
                candidate_set = set(map(str.strip, candidate_value.split(','))) if candidate_value else set()
                filter_set = set(filter_value)
                if not (candidate_set & filter_set):  # No intersection means no match
                    match_found = False
                    break

            elif key == "profile_percentage":
                min_val, max_val = map(int, filter_value.split('-'))
                if not (min_val <= candidate_value <= max_val):  # Not in the range
                    match_found = False
                    break

            elif isinstance(filter_value, list):  # For multi-option fields like country or city
                if candidate_value not in filter_value:
                    match_found = False
                    break

            else:  # For exact match filters
                if candidate_value != filter_value:
                    match_found = False
                    break

        if match_found:  # Include the candidate only if all provided filters match
            filtered_candidates.append(candidate)

    return filtered_candidates


def admin_professional_dashboard_search_filter():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()
            # if 'professional_id' not in req_data:
            #     result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            #     return result_json
            if 'search_text' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
                return result_json
            if 'page_number' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            user_data = get_user_data(token_result["email_id"])
            # professional_id = req_data['professional_id']
            search_text = req_data['search_text']
            page_number = req_data['page_number']
            country = []
            city = []
            filter_data = {}
            filter_data["skills"] = req_data["skills"]
            filter_data["plan"] = req_data["plan"]
            filter_data['profile_percentage'] = req_data["profile_percentage"]
            location = req_data["location"]
            filter_data["gender"] = req_data['gender']
            filter_data['industry_sector'] = req_data['industry_sector']
            filter_data['sector'] = req_data['sector']
            filter_data['job_type'] = req_data['job_type']
            filter_data['willing_to_relocate'] = req_data['willing_to_relocate']
            filter_data['mode_of_communication'] = req_data['mode_of_communication']
            filter_data['location_preference'] = req_data['location_preference']
            filter_data['functional_specification'] = req_data['functional_specification']

            if len(location) != 0:
                for i in range(len(location)):
                    res = location[i].split("&&&&&")
                    country.append(res[1])
                    city.append(res[0])
                filter_data['country'] = country
                filter_data['city'] = city
            # print(filter_data)
            offset = (page_number - 1) * 10
            if search_text.startswith("2C"):
                split_txt = search_text.split("-")
                if len(split_txt) > 2:
                    search_text = (search_text.split("-")[2])
            if user_data["user_role"] == "admin":
                if search_text != '':
                    query = """WITH LatestExperience AS (
                                SELECT
                                    pe.professional_id,
                                    pe.id AS experience_id,
                                    pe.start_month,
                                    pe.start_year,
                                    pe.job_title,
                                    pe.job_description,
                                    pe.created_at,
                                    pe.company_name,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY pe.professional_id
                                        ORDER BY
                                            CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC,
                                            CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC,
                                            pe.created_at DESC
                                    ) AS rn
                                FROM
                                    professional_experience AS pe
                            )
                            SELECT
                                u.first_name,
                                u.last_name,
                                u.country,
                                u.state,
                                u.city,
                                u.profile_percentage,
                                u.pricing_category,
                                u.is_active,
                                u.payment_status,
                                u.email_id,
                                u.gender,
                                p.professional_id,
                                p.expert_notes,
                                p.industry_sector,
                                p.sector,
                                p.job_type,
                                p.willing_to_relocate,
                                p.mode_of_communication,
                                p.location_preference,
                                p.functional_specification,
                                p.created_at AS posted_at,
                                le.job_title,
                                le.job_description,
                                le.company_name,
                                le.created_at,
                                u2.profile_image,
                                GROUP_CONCAT(DISTINCT ps.skill_name SEPARATOR ', ') AS skills,
                                GROUP_CONCAT(DISTINCT ped.specialisation SEPARATOR ', ') AS specialisation,
                                GROUP_CONCAT(DISTINCT ped.institute_name SEPARATOR ', ') AS institute_name,
                                GROUP_CONCAT(DISTINCT ped.degree_level SEPARATOR ', ') AS degree_level
                            FROM
                                users AS u
                            LEFT JOIN
                                professional_profile AS p ON u.user_id = p.professional_id
                            LEFT JOIN
                                professional_additional_info AS pai ON u.user_id = pai.professional_id
                            LEFT JOIN
                                professional_language AS pl ON u.user_id = pl.professional_id
                            LEFT JOIN
                                professional_education AS ped ON u.user_id = ped.professional_id
                            LEFT JOIN
                                professional_skill AS ps ON u.user_id = ps.professional_id
                            LEFT JOIN
                                professional_experience AS pe ON p.professional_id = pe.professional_id
                            LEFT JOIN
                                LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1
                            LEFT JOIN
                                users AS u2 ON u.user_id = u2.user_id
                            WHERE
                                p.professional_id > 0
                                AND u.email_active = 'Y'
                                AND (u.first_name LIKE %s
                                OR u.last_name LIKE %s 
                                OR u.email_id LIKE %s 
                                OR CONCAT(u.first_name, ' ', u.last_name) LIKE %s
                                OR u.user_id LIKE %s
                                OR u.city LIKE %s
                                OR u.country LIKE %s
                                OR ps.skill_name LIKE %s
                                OR ped.specialisation LIKE %s
                                OR pl.language_known LIKE %s
                                OR pai.description LIKE %s
                                OR pe.job_title LIKE %s
                                OR pe.job_description LIKE %s)
                            GROUP BY
                                u.user_id,
                                u.first_name,
                                u.last_name,
                                u.country,
                                u.state,
                                u.city,
                                u.profile_percentage,
                                u.pricing_category,
                                u.is_active,
                                u.payment_status,
                                u.gender,
                                p.professional_id,
                                p.expert_notes,
                                p.created_at,
                                p.industry_sector,
                                p.sector,
                                p.job_type,
                                p.willing_to_relocate,
                                p.mode_of_communication,
                                p.location_preference,
                                p.functional_specification,
                                p.years_of_experience,
                                le.job_title,
                                le.company_name,
                                le.job_description,
                                le.created_at,
                                u2.profile_image
                            ORDER BY
                                p.professional_id DESC"""
                    search_term = f"%{search_text}%"
                    values = (search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term,)
                    total_count = execute_query(query,values)
                    total_count = len(total_count)
                    filter_parameters = fetch_filter_params()
                    data_query = query + " LIMIT 10 OFFSET %s;"
                    values = (search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, search_term, offset,)
                    candidates_desc = execute_query(data_query, values)
                    first_id = 0
                    
                    candidates_desc = filter_candidates(candidates_desc,filter_data)
                    # print(candidates_desc)
                    if len(candidates_desc) > 0:
                        temp_id_2 = candidates_desc[0]['professional_id']
                        for obj in candidates_desc:
                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (obj['professional_id'], 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city,
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(ep.sector, su.sector) AS sector,
                                        COALESCE(ep.company_name, su.company_name) AS company_name,
                                        COALESCE(u.is_active, '') AS is_active
                                        FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            obj.update({'recommended_jobs' : recommended_jobs_list})
                            temp_id1 = "2C-PR-" + str(obj['professional_id'])
                            obj.update({"professional_id" : temp_id1})
                            profile_image_name = replace_empty_values1(obj['profile_image'])
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            obj.update({'profile_image' : s3_pic_key})
                        first_id = candidates_desc[0]['professional_id']
                        # temp_id = "2C-PR-" + str(candidates_desc[0]['professional_id'])
                        if isUserExist("professional_profile","professional_id",temp_id_2):
                            query = "SELECT u.first_name, u.last_name, u.email_id, u.dob, u.country_code, u.contact_number, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.country, u.state, u.city, u.gender, p.professional_id,p.professional_resume,p.expert_notes,p.about,p.preferences, p.video_url,p.industry_sector,p.sector,p.job_type,p.willing_to_relocate,p.mode_of_communication,p.location_preference,p.functional_specification,p.years_of_experience, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') THEN 0 ELSE pe.end_year END DESC, CASE WHEN (pe.end_month IS NULL OR pe.end_month = '') THEN 0 ELSE pe.end_month END DESC, CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') OR (pe.end_month IS NULL OR pe.end_month = '') THEN pe.created_at END DESC"                
                            values = (temp_id_2,)
                            profile_result = execute_query(query, values) 

                            if len(profile_result) > 0:                              
                                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                                intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                                resume_name = replace_empty_values1(profile_result[0]['professional_resume'])
                            else:
                                result_json = api_json_response_format(True,"No records found",0,{})     
                                return result_json          
                            s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                            if intro_video_name == '':
                                s3_video_key = ""
                            else:
                                s3_video_key = s3_intro_video_folder_name + str(intro_video_name)
                            if resume_name == '':    
                                s3_resume_key = ""
                            else:          
                                s3_resume_key = s3_resume_folder_name + str(resume_name)

                            query = 'select job_id from sc_recommendation where professional_id = %s and user_role_id = %s'
                            values = (temp_id_2, 3,)
                            recommended_jobs_id = execute_query(query, values)
                            recommended_jobs_list = []
                            if len(recommended_jobs_id) > 0:
                                for id in recommended_jobs_id:
                                    if isUserExist("job_post", "id", id['job_id']):
                                        query = '''SELECT jp.id as job_id, jp.employer_id, jp.job_title, jp.country, jp.city, 
                                                COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                                COALESCE(ep.sector, su.sector) AS sector,
                                                COALESCE(ep.company_name, su.company_name) AS company_name,
                                                COALESCE(u.is_active, '') AS is_active
                                                FROM job_post AS jp LEFT JOIN employer_profile AS ep ON jp.employer_id = ep.employer_id LEFT JOIN users u on u.user_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id = %s and jp.job_status = "opened"'''
                                        values = (id['job_id'],)
                                        detail = execute_query(query, values)
                                        if len(detail) > 0:
                                            txt = detail[0]['sector']
                                            txt = txt.replace(", ", "_")
                                            txt = txt.replace(" ", "_")
                                            sector_name = txt + ".png"
                                            detail[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                            img_key = s3_employer_picture_folder_name + detail[0]['profile_image']
                                            detail[0].update({'profile_image' : img_key})
                                            recommended_jobs_list.append(detail[0])
                            else:
                                recommended_jobs_list = []
                            profile_dict = {
                                'first_name': replace_empty_values1(profile_result[0]['first_name']),
                                'last_name': replace_empty_values1(profile_result[0]['last_name']),
                                'professional_id' : first_id,
                                'user_id' : first_id,
                                'email_id': replace_empty_values1(profile_result[0]['email_id']),
                                'dob': replace_empty_values1(profile_result[0]['dob']),
                                'country_code': replace_empty_values1(profile_result[0]['country_code']),
                                'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                                'city': replace_empty_values1(profile_result[0]['city']),
                                'state': replace_empty_values1(profile_result[0]['state']),
                                'country': replace_empty_values1(profile_result[0]['country']),
                                'profile_percentage' : replace_empty_values1(profile_result[0]['profile_percentage']),
                                'pricing_category' : profile_result[0]['pricing_category'],
                                'is_active' : profile_result[0]['is_active'],
                                'payment_status' : profile_result[0]['payment_status'],
                                'profile_image': s3_pic_key,
                                'video_name': s3_video_key,
                                'resume_name': s3_resume_key,
                                'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                                'about': replace_empty_values1(profile_result[0]['about']),
                                'preferences': replace_empty_values1(profile_result[0]['preferences']),
                                'experience': {},
                                'education': {},
                                'skills': {},
                                'languages': {},
                                'additional_info': {},
                                'social_link': {},
                                'job_list' : {},
                                'filter_parameters' : filter_parameters,
                                'recommended_jobs' : recommended_jobs_list,
                                'professional_details' : {},
                                'gender':replace_empty_values1(profile_result[0]['gender']),
                                'years_of_experience':replace_empty_values1(profile_result[0]['years_of_experience']),
                                'functional_specification':replace_empty_values1(profile_result[0]['functional_specification']),
                                'sector':replace_empty_values1(profile_result[0]['sector']),
                                'industry_sector':replace_empty_values1(profile_result[0]['industry_sector']),
                                'job_type':replace_empty_values1(profile_result[0]['job_type']),
                                'location_preference':replace_empty_values1(profile_result[0]['location_preference']),
                                'mode_of_communication':replace_empty_values1(profile_result[0]['mode_of_communication']),
                                'willing_to_relocate':replace_empty_values1(profile_result[0]['willing_to_relocate'])
                            }

                            # Grouping experience data
                            experience_set = set()
                            experience_list = []
                            for exp in profile_result:
                                if exp['experience_id'] is not None:
                                    start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                                    end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                                    exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], start_date, end_date, exp['job_description'], exp['job_location'])
                                    if exp_tuple not in experience_set:
                                        experience_set.add(exp_tuple)
                                        experience_list.append({
                                            'id': exp['experience_id'],
                                            'company_name': replace_empty_values1(exp['company_name']),
                                            'job_title': replace_empty_values1(exp['job_title']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,                                
                                            'job_description': replace_empty_values1(exp['job_description']),
                                            'job_location': replace_empty_values1(exp['job_location'])
                                        })

                            profile_dict['experience'] = experience_list

                            # Grouping education data
                            education_set = set()
                            education_list = []
                            for edu in profile_result:
                                if edu['education_id'] is not None:
                                    start_date = format_date(edu['education_start_year'], edu['education_start_month'])
                                    end_date = format_date(edu['education_end_year'], edu['education_end_month'])
                                    edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                                start_date, end_date, edu['institute_location'])
                                    if edu_tuple not in education_set:
                                        education_set.add(edu_tuple)
                                        education_list.append({
                                            'id': edu['education_id'],
                                            'institute_name': replace_empty_values1(edu['institute_name']),
                                            'degree_level': replace_empty_values1(edu['degree_level']),
                                            'specialisation': replace_empty_values1(edu['specialisation']),
                                            'start_date': start_date,                                
                                            'end_date': end_date,
                                            'institute_location': replace_empty_values1(edu['institute_location'])
                                        })

                            profile_dict['education'] = education_list

                            # Grouping skills data
                            skills_set = set()
                            skills_list = []
                            for skill in profile_result:
                                if skill['skill_id'] is not None:
                                    skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
                                    if skill_tuple not in skills_set:
                                        skills_set.add(skill_tuple)
                                        skills_list.append({
                                            'id': skill['skill_id'],
                                            'skill_name': replace_empty_values1(skill['skill_name']),
                                            'skill_level': replace_empty_values1(skill['skill_level'])
                                        })

                            profile_dict['skills'] = skills_list

                            # Grouping languages data
                            languages_set = set()
                            languages_list = []
                            for lang in profile_result:
                                if lang['language_id'] is not None:
                                    lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
                                    if lang_tuple not in languages_set:
                                        languages_set.add(lang_tuple)
                                        languages_list.append({
                                            'language_known': replace_empty_values1(lang['language_known']),
                                            'id':  lang['language_id'],
                                            'language_level': replace_empty_values1(lang['language_level'])
                                    })

                            profile_dict['languages'] = languages_list
                            # Grouping additional info data
                            additional_info_set = set()
                            additional_info_list = []
                            for info in profile_result:
                                if info['additional_info_id'] is not None:
                                    info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
                                    if info_tuple not in additional_info_set:
                                        additional_info_set.add(info_tuple)
                                        additional_info_list.append({
                                        'id': info['additional_info_id'],
                                        'title': replace_empty_values1(info['additional_info_title']),
                                        'description': replace_empty_values1(info['additional_info_description'])
                                    })

                            profile_dict['additional_info'] = additional_info_list
                            # Grouping social link data
                            social_link_set = set()
                            social_link_list = []
                            for link in profile_result:
                                if link['social_link_id'] is not None:
                                    link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
                                    if link_tuple not in social_link_set:
                                        social_link_set.add(link_tuple)
                                        social_link_list.append({
                                        'title': replace_empty_values1(link['social_link_title']),
                                        'id':  link['social_link_id'],
                                        'url': replace_empty_values1(link['social_link_url'])
                                    })

                            profile_dict['social_link'] = social_link_list
                            
                            #Get recommended jobs for the professional
                            query = 'select job_id from sc_recommendation where user_role_id = %s and professional_id = %s'
                            values = (3,temp_id_2,)
                            job_id_list = execute_query(query, values)
                            job_detail_list = []
                            if len(job_id_list) > 0:
                                for job_id in job_id_list:
                                    id = job_id['job_id']
                                    if isUserExist("job_post", "id", id):
                                        query = 'select employer_id, job_title, country, city from job_post where id = %s'
                                        values = (id,)
                                        job_detail = execute_query(query, values)
                                        query = 'select company_name from employer_profile where employer_id = %s'
                                        values = (job_detail[0]['employer_id'],)
                                        company_name = execute_query(query, values)
                                    else:
                                        job_detail = [{"job_title" : "","country" : "", "city" : ""}]
                                        company_name = [{'company_name': ""}]

                                    if len(job_detail) > 0 and len(company_name) > 0:
                                        job_detail_dict = {"job_title" : job_detail[0]['job_title'],
                                                            "country" : job_detail[0]['country'],
                                                            "city" : job_detail[0]['city'],
                                                            "company_name" : company_name[0]['company_name']}
                                    else:
                                        job_detail_dict = {}
                                    job_detail_list.append(job_detail_dict)
                                profile_dict['job_list'] = job_detail_list
                            else:
                                profile_dict['job_list'] = []
                            profile_dict['professional_details'] = candidates_desc
                            profile_dict.update({'total_count' : total_count})
                        result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)                    
                    else:
                        profile_dict = {'filter_parameters' : filter_parameters}
                        profile_dict.update({'job_list' : []})
                        profile_dict.update({'professional_details' : []})
                        profile_dict.update({'total_count' : 0})
                        profile_dict.update({'recommended_jobs' : []})
                        result_json = api_json_response_format(True,"No records found.",0,profile_dict)
                else:
                    result_json = admin_professional_filter_results(request)
                    # result_json = api_json_response_format(True,"Please enter the valid id",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def website_data():
    try:
        result_json = {}
        auth = request.authorization
        if not auth:
            print("Could not get token...Authorization is required")
            result_json = api_json_response_format(False,str("Sorry, we are unable to issue token. Authorization is required"),401,{})
            return result_json

        if not auth.password:
            print("Could not get token...Client secret is required")
            result_json = api_json_response_format(False,str("Sorry, we are unable to issue token. Client secret is required"),401,{})         
            return result_json
        
        if not auth.username:
            print("Could not get token...Client ID is required")
            result_json = api_json_response_format(False,str("Sorry, we are unable to issue token. Client ID is required."),401,{})
            return result_json
            
        client_secret_req = auth.password       
        client_id_req = auth.username
        client_secret_db = None
        query = "SELECT attribute_value FROM payment_config WHERE role = %s ORDER BY id "
        values = ('Admin',)
        rs_obj = execute_query(query,values)            
        if rs_obj:
            client_id_db = rs_obj[0]['attribute_value']
            client_secret_db = rs_obj[1]['attribute_value']
        else:
            print(f"Client ID : {client_id_req} not found.")
            result_json = api_json_response_format(False,str("Sorry, we are unable to issue token. Client ID not found."),401,{}) 
            return result_json
        if auth and client_secret_req == client_secret_db and client_id_db == client_id_req:
            get_professional_details_query = """WITH LatestExperience AS 
            ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() 
            OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) 
            SELECT u.country, u.city, u.gender, p.functional_specification, p.industry_sector, le.job_title FROM users u 
            LEFT JOIN professional_profile p ON u.user_id = p.professional_id 
            LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 
            LEFT JOIN users AS u2 ON u.user_id = u2.user_id 
            WHERE u.user_role_fk = %s AND u.email_active = %s AND u.profile_percentage > %s ORDER BY u.user_id DESC LIMIT 5 OFFSET 0;"""
            professional_details_values = (3, 'Y', 60,)
            professional_details_dict = execute_query(get_professional_details_query, professional_details_values)

            get_job_details_query = """SELECT 
                                            jp.job_title,
                                            jp.specialisation,
                                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                                            COALESCE(ep.sector, su.sector) AS sector, 
                                            COALESCE(u.country, su.country) AS country, 
                                            COALESCE(u.city, su.city) AS city, 
                                            CASE 
                                                WHEN LENGTH(jp.skills) - LENGTH(REPLACE(jp.skills, ',', '')) >= 1 THEN 
                                                    CONCAT(SUBSTRING_INDEX(jp.skills, ',', 1), ', ', SUBSTRING_INDEX(SUBSTRING_INDEX(jp.skills, ',', 2), ',', -1))
                                                ELSE 
                                                    jp.skills 
                                            END AS skills 
                                        FROM job_post jp 
                                        LEFT JOIN users u ON u.user_id = jp.employer_id
                                        LEFT JOIN sub_users su ON su.sub_user_id = jp.employer_id
                                        LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id
                                       WHERE jp.job_status = %s ORDER BY jp.id DESC LIMIT 5"""
            job_details_values = ('opened',)
            job_details_dict = execute_query(get_job_details_query, job_details_values)
            result = {"professional_details" : professional_details_dict,
                      "job_details" : job_details_dict}
            result_json = api_json_response_format(True, "Details fetched successfully!", 0, result)
            return result_json
        else:
            result_json = api_json_response_format(False,str("Sorry, we are unable to issue token. Client ID not found."),401,{})
            return result_json
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def generate_sso_payload(params, secret):
    """
    Generate the SSO payload and signature.
    """
    # Encode the parameters into a query string
    query_string = urllib.parse.urlencode(params)
    
    # Base64 encode the query string
    base64_encoded = base64.b64encode(query_string.encode()).decode()
    
    # Generate HMAC SHA256 signature
    signature = hmac.new(secret.encode(), base64_encoded.encode(), hashlib.sha256).hexdigest()
    
    return base64_encoded, signature

def sso():
    result_json = {}
    try:
        sso_param = request.args.get("sso")
        sig_param = request.args.get("sig")
        email_id = request.args.get("email_id")
        
        user_details = get_user_data(email_id)
        
        # Verify the incoming signature
        expected_sig = hmac.new(SECRET.encode(), sso_param.encode(), hashlib.sha256).hexdigest()
        if sig_param != expected_sig:
            return "Invalid signature", 400
        
        # Decode the SSO payload
        decoded_sso = base64.b64decode(sso_param).decode()
        params = dict(urllib.parse.parse_qsl(decoded_sso))
        
        # Populate the user details
        if user_details['is_exist']:
            user_params = {
                "nonce": params["nonce"],  # This must be sent back as it was received
                "email": user_details['email_id'],
                "name": user_details['first_name'] + ' ' + user_details['last_name'],
                "username": user_details['last_name'],
                "external_id": user_details['user_id'],  # Unique ID for the user in your application
                "return_path" : '/categories'
            }
        else:
            result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        # Generate the payload and signature
        sso_payload, sso_signature = generate_sso_payload(user_params, SECRET)
        
        # Redirect back to Discourse
        redirect_url = f"{DISCOURSE_URL}?sso={sso_payload}&sig={sso_signature}"
        result_json = api_json_response_format(True, '2C Exchange login successful', 0, {"redirect_url": redirect_url})
        return result_json
    except Exception as error:
        print("Error in discourse_sso(): ", error)
        result_json = api_json_response_format(False, str(error), 500, {})
        return result_json
    finally:
        return result_json
    
from flask import Flask, request
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')

def discourse_webhook():
    try:
        result_json = {}
        data = request.json
        if 'user' in data:
            user_email = data['user']['email']
            print(f"User email: {user_email}")
            username = data['user']['username']
            print(f"Username: {username}")
            # Compose your welcome email
            subject = f"Welcome to the Community, {username}!"
            content = f"Hi {username},\n\nWelcome to our Discourse forum! Here are some helpful links..."
            try:
                from_addr = 'info@2ndcareers.com'
                sg = SendGridAPIClient(sendgrid_api_key)
                message = Mail(
                    from_email="2nd Careers <" + from_addr + ">",
                    to_emails=user_email,
                    subject=subject,
                    plain_text_content=content
                )
                sg.send(message)
                result_json = api_json_response_format(True, 'Email sent successfully', 0, {})
                return result_json
            except Exception as e:
                print(f"Error sending email: {e}")
                result_json = api_json_response_format(False, str(e), 500, {})
                return result_json
        else:
            result_json = api_json_response_format(False, 'Post related data received', 500, {})
            return result_json
    except Exception as error:
        print("Error in discourse_webhook(): ", error)
        result_json = api_json_response_format(False, str(error), 500, {})
        return result_json
    finally:
        return result_json

def admin_upload_event():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                type = "Event"
                title = request.form.get("title") #title
                short_description = request.form.get("title_description") #title description
                detailed_description = request.form.get("what_to_expect") #what_to_expect
                speaker_name = request.form.get("speaker_name") #speaker_name
                image = request.files.get("speaker_photo") #speaker_photo
                join_url = request.form.get("join_url") #join url
                event_date = request.form.get("date_time") #Date&time
                additional_notes = request.form.get("type_of_offering") #type_of_offering
                share_url = request.form.get("share_url")
                type_of_community = "Register here"

                created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if not all([type,title,short_description,detailed_description,image,join_url,event_date,type_of_community,additional_notes,speaker_name]):
                    result_json =  api_json_response_format(False,"fill all the required fields",400,{})
                    return result_json

                if image:
                    file_name = image.filename
                    if file_name != '':
                        try:
                            s3_pro = s3_obj.get_s3_client()
                            s3_pro.upload_fileobj(image, S3_BUCKET_NAME, s3_community_cover_pic_folder_name+file_name)
                        except Exception as e:
                            print("S3 upload error:", e)
                            return api_json_response_format(False, "Image not uploaded to S3",500, {})
                    else:
                        result_json = api_json_response_format(False, "Invalid image filename", 400, {})
                        return result_json
                else:
                    result_json = api_json_response_format(False, "Missing image file", 400, {})
                    return result_json
                
                insert_query = "insert into community(type,title,short_description,detailed_description,image,join_url,event_date,additional_notes,type_of_community,share_url,created_at,speaker_name) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                insert_values = (type,title,short_description,detailed_description,file_name,join_url,event_date,additional_notes,type_of_community,share_url,created_at,speaker_name)
                event_result = update_query(insert_query,insert_values)

                if event_result > 0:
                    result_json = api_json_response_format(True,"Event Updated Successfully",0,{})
                    return result_json
                else:
                    result_json = api_json_response_format(False,"Event Not Created",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",403,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception while creating event:", error)
        return api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json
    
def admin_update_event():
    result_json = {}
    try:
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "admin":
                event_id = request.form.get("event_id") 
                if not event_id:
                    result_json = api_json_response_format(False, "Event ID is required",1, {})
                    return result_json

                is_image_changed = request.form.get("is_image_changed", "N")
                title = request.form.get("title")
                short_description = request.form.get("title_description")
                detailed_description = request.form.get("what_to_expect")
                speaker_name = request.form.get("speaker_name")
                image = request.files.get("speaker_photo")
                join_url = request.form.get("join_url")
                event_date = request.form.get("date_time")
                additional_notes = request.form.get("type_of_offering")
                share_url = request.form.get("share_url")

                if not all([title,short_description,detailed_description,join_url,event_date,additional_notes,speaker_name]):
                    result_json =  api_json_response_format(False,"fill all the required fields",400,{})
                    return result_json

                update_fields = []
                update_values = []

                field_value_pairs = [("title", title),("short_description", short_description),("speaker_name", speaker_name),("detailed_description", detailed_description),("join_url", join_url),("event_date", event_date),("additional_notes", additional_notes),("share_url", share_url)]

                for field, value in field_value_pairs:
                    if value:
                        update_fields.append(f"{field} = %s")
                        update_values.append(value)

                if is_image_changed == 'Y':
                    try:
                        if image:
                            file_name = image.filename
                            if file_name != '':
                                try:
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(image, S3_BUCKET_NAME, s3_community_cover_pic_folder_name+file_name)
                                    update_fields.append("image = %s")
                                    update_values.append(file_name)
                                
                                except Exception as e:
                                    print("S3 upload error:", e)
                                    return api_json_response_format(False, "Image not uploaded to S3", 500, {})
                            else:
                                result_json = api_json_response_format(False,"Invalid image filename",400,{})
                                return result_json
                        else:
                            result_json = api_json_response_format(False, "Image not provided",400, {})
                            return result_json
                    except Exception as e:
                        print("Error in uploading image:", e)  
                        return api_json_response_format(False, "Could not upload speaker image",500, {})

                update_fields.append("updated_at = %s")
                update_values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                sql_query = f"UPDATE community SET {', '.join(update_fields)} WHERE id = %s"
                update_values.append(event_id)

                update_result = update_query(sql_query, update_values)

                if update_result > 0:
                    result_json = api_json_response_format(True, "Event Updated Successfully", 0, {})
                    return result_json
                else:
                    result_json = api_json_response_format(False, "Event Not Updated", 500, {})
                    return result_json
            else:
                result_json = api_json_response_format(False, "Unauthorized User", 403, {})
                return result_json
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
            return result_json
    except Exception as error:
        print("Exception while updating event:", error)
        return api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def admin_delete_event():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                event_id = req_data["event_id"]
                image_name = req_data["image"]

                if not event_id:
                    result_json = api_json_response_format(False, "Event ID is required", 404, {})
                    return result_json

                delete_event_query = "DELETE FROM community WHERE id = %s"
                delete_values = (event_id,)
                delete_result = update_query(delete_event_query, delete_values)

                if delete_result > 0:
                    s3_client = s3_obj.get_s3_client()
                    try:
                        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=image_name) 
                        print("Super Admin event image file deleted from s3 Bucket successfully.")
                        result_json = api_json_response_format(True, "Event Deleted Successfully", 0, {})
                        return result_json
                    except Exception as e:
                        if e.response['Error']['Code'] == 'NoSuchKey':
                            print("File not found.")
                            return False
                        else:
                            print("Error deleting file:", e)
                            raise e
                else:
                    result_json = api_json_response_format(False, "Something went wrong", 500, {})
                    return result_json
            else:
                result_json = api_json_response_format(False,"Unauthorized User",403,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception while creating event:", error)
        return api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json
    
def get_admin_jobs_post():
    result_json = None
    try:
        # param = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":     
                user_id = user_data["user_id"]  
                req_data = request.json
                page_number = req_data.get('page_number')
                # if 'param' in req_data:
                #     param = req_data['param']
                param = req_data.get("param", {})

                fetch_expired_query = f"""
                            SELECT id FROM admin_job_post
                            WHERE DATEDIFF(CURDATE(), created_at) >= days_left;
                        """
                values = ()
                expired_jobs_id = execute_query(fetch_expired_query, values)
                for i in expired_jobs_id:
                        query = 'UPDATE admin_job_post SET is_active = %s, admin_job_status = %s WHERE id = %s'
                        values = ('N', 'closed',i['id'],)
                        closed_row_count = update_query(query, values)
                        delete_query = "DELETE FROM admin_job_post WHERE id = %s AND admin_job_status = %s"
                        delete_values = (i['id'], 'closed')
                        deleted_count = update_query(delete_query, delete_values)

                query = "Select count(*) as total_count from admin_job_post where employer_id = %s and admin_job_status = %s and is_active = %s"
                values = (user_id,"Opened", 'Y')
                total_count = execute_query(query, values)
                total_count = total_count[0]['total_count']
                page_number = req_data["page_number"]
                limit = 10
                offset = (page_number - 1) * limit
                query = """
                    SELECT
                        job_reference_id,
                        job_title,
                        job_overview,
                        job_description,
                        workplace_type,
                        job_type,
                        schedule,
                        skills,
                        company_sector,
                        functional_specification,
                        city,
                        state,
                        country,
                        apply_link,
                        source,
                        seniority,
                        company_name,
                        functional_specification_others,
                        created_at,
                        updated_at
                    FROM admin_job_post where employer_id = %s 
                                ORDER BY {}
                                limit 10 offset %s
                """

                order_by_map = {
                    'by_date': 'created_at ASC',
                    'by_date_desc': 'created_at DESC',
                    'asc': 'job_title ASC',
                    'desc': 'job_title DESC'
                }

                param_value = param.get("order") if isinstance(param, dict) else param

                order_by_clause = order_by_map.get(param_value, 'created_at DESC')
                query = query.format(order_by_clause)
                values = (user_id,offset)
                res = execute_query(query,values)

                data = {
                    "total_count": total_count,
                    "data": res
                }


                result_json = api_json_response_format(True, "Read Excel data fetched successfully", 200, data)

    except Exception as error:
        # return jsonify({
        #     "success": False,
        #     "message": f"Error: {str(e)}"
        # }), 500

        print("Exception while Fetching the excel data:", error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def convert_xls_to_xlsx(input_file_path, output_file_path):
    sheet = pe.get_book(file_name=input_file_path)
    sheet.save_as(output_file_path)

def upload_job_excel():
    result = None  #initialize to avoid None in finally

    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":     
                user_id = user_data["user_id"]  

                #Validate file
                if "file" not in request.files:
                    result = api_json_response_format(False, "No file provided in form-data with key 'file'", 400,{})

                else:
                    file = request.files["file"]

                    if file.filename == "":
                        result = api_json_response_format(False, "No file selected", 400, {})

                    else:
                        filename = file.filename.lower()
                        content = file.read()  
                        if len(content) == 0:
                            return api_json_response_format(False, "Uploaded file is empty.", 400, {})

                        file.seek(0)

                        if filename.endswith(".xlsx"):
                            df = pd.read_excel(file, engine="openpyxl")
                        elif filename.endswith(".xls"):
                            book = pe.get_book(file_stream=file.stream, file_type="xls")
                            with NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_xlsx:
                                book.save_as(temp_xlsx.name)
                                df = pd.read_excel(temp_xlsx.name, engine="openpyxl")
                        elif filename.endswith(".csv"):
                            df = pd.read_csv(file)
                        else:
                            result = api_json_response_format(False, "Unsupported file format. Only .xls, .xlsx, .csv are allowed.", 400, {})
                            return result
                        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
                        df.replace("", pd.NA, inplace=True)
                        df.dropna(how="all", inplace=True)


                        required_fields = [
                            "Ref_Job_ID", "Job_Title", "Job_Overview", "Job_Description",
                            "Workplace_Type", "Job_Type", "Schedule", "Skills",
                            "Company_Sector", "Functional_Specification",
                            "City", "State", "Country", "Company_Name"
                        ]

                        optional_fields = ["Functional_Specification_others", "Apply_Link", "Source", "Seniority"]

                        expected_columns = required_fields + optional_fields
                        missing_columns = [col for col in expected_columns if col not in df.columns]

                        if missing_columns:
                            result = api_json_response_format(False, "Column headers differ from the template", 400, {})

                        else:

                            empty_required_columns = [col for col in required_fields if df[col].isna().all()]
                            if empty_required_columns:
                                result = api_json_response_format(False,"Column values are empty",400,{})
                            
                            else:

                                #Check required fields NULL
                                null_required_rows = df[df[required_fields].isnull().any(axis=1)]
                                if not null_required_rows.empty:
                                    result =  api_json_response_format(
                                        False,
                                        "Any mandatory field missing data.",
                                        400,
                                        {}
                                    )
                                else:
                                    #Validation rules
                                    validation_rules = {
                                        "Functional_Specification": [
                                            "Sales & Marketing", "Human Resources", "Technology & Technology Management",
                                            "Finance & Accounting", "C Suite and Board", "Others"
                                        ],
                                        "Workplace_Type": ["Hybrid", "Remote", "On-site"],
                                        "Job_Type": ["Contract", "Permanent Full-time", "Permanent Part-time", "Other", "Volunteer"],
                                        "Schedule": ["Fixed", "Flexible", "Weekend only", "Monday to Friday", "Others"],
                                        "Company_Sector": [
                                            "Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design",
                                            "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking",
                                            "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit",
                                            "Professional Services", "Public Administration", "Public Safety", "Real Estate",
                                            "Recreation and Travel", "Retail", "Software and IT Services",
                                            "Transportation and Logistics", "Wellness and Fitness", "Professional Services"
                                        ],
                                        "Country":["India", "United States", "All"]
                                    }


                                    invalid_rows = []

                                    for idx, row in df.iterrows():
                                        for field, allowed_list in validation_rules.items():
                                            value = str(row[field]).strip()

                                            if value.lower() not in [x.lower() for x in allowed_list]:
                                                invalid_rows.append(True)


                                    if invalid_rows:
                                        result =  api_json_response_format(
                                            False,
                                            "Invalid values in dropdown fields.",
                                            400,
                                            {}
                                        )
                                        
                                    
                                    else:

                                        df = df.astype(str)
                                        #Replace all null values with NIL
                                        df.fillna("NIL", inplace=True)
                                        invalid_rows = []   # collect errors to show user

                                        for idx, row in df.iterrows():
                                            excel_row = idx + 2  # +2 because pandas index starts at 0 and excel starts at 1 (header row included)

                                            company_name = row.get("Company_Name", "").strip()
                                            job_title = row.get("Job_Title", "").strip()
                                            job_overview = row.get("Job_Overview", "").strip()
                                            job_description = row.get("Job_Description", "").strip()

                                            # Validation 1: Company_Name <= 80 chars
                                            if len(company_name) > 80:
                                                invalid_rows.append({
                                                    "message": "Company_Name must not exceed 80 characters."
                                                })

                                            # Validation 2: Job_Title <= 80 chars
                                            if len(job_title) > 80:
                                                invalid_rows.append({
                                                    "message": "Job_Title must not exceed 80 character"
                                                })

                                            # Validation 3: Job_Overview min 30 / max 300 chars
                                            if not (30 <= len(job_overview) <= 300):
                                                invalid_rows.append({
                                                    "message": f"Job_Overview Must be 30-300 chars (Given: {len(job_overview)})"
                                                })

                                            # Validation 4: Job_Description min 250 / max 10,000 chars
                                            if not (250 <= len(job_description) <= 10000):
                                                invalid_rows.append({
                                                    "message": "Job_Description Must be 250-10,000 chars"
                                                })

                                        # If any validation fails → stop and return error
                                        if invalid_rows:
                                            result = api_json_response_format(False,invalid_rows[0]["message"],400,{})

                                        
                                        else:
                                            # Convert dataframe to list of dict rows
                                            data = df.to_dict(orient="records")

                                            try:

                                                client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
                                                index = client.index(MEILISEARCH_ADMIN_JOB_INDEX)

                                                query = "select job_reference_id from admin_job_post"
                                                delete_ids = execute_query(query)   # <-- returns list of rows

                                                # Extract only ID values as a list:  [ 101, 102, 103 ]
                                                delete_ids = [row["job_reference_id"] for row in delete_ids]
                                                if delete_ids:
                                                    # background_runner.delete_admin_job_details_in_meilisearch(delete_ids)
                                                    index.delete_all_documents()
                                                    print("✅ Meilisearch cleared")

                                                    delete_query = "DELETE FROM admin_job_post"
                                                    update_query(delete_query, None)
                                                else:
                                                    print("No old rows are available to delete")

                                            except Exception as e:
                                                print("Delete Logic Error:", e)
                                                result = api_json_response_format(False, "Error in uploading file", 500, {})
                                                return result

                                            # Normalize comma separated values
                                            def normalize(val):
                                                if val and val != "NIL":
                                                    return ", ".join([v.strip() for v in str(val).split(",")])
                                                return None
                                            store_job_details = []
                                            for item in data:

                                                #Validate fields from excel based on allowed list
                                                for field, allowed_list in validation_rules.items():
                                                    excel_value = str(item.get(field, "")).strip()
                                                    if excel_value.lower() not in [x.lower() for x in allowed_list]:
                                                        print(f"Invalid value in `{field}`: {excel_value} → Setting to Others")

                                                workplace = normalize(item.get("Workplace_Type"))
                                                job_type = normalize(item.get("Job_Type"))
                                                schedule = normalize(item.get("Schedule"))
                                                company_sector = normalize(item.get("Company_Sector"))
                                                # functional_specification = normalize(item.get("Functional_Specification"))
                                                functional_specification = normalize(item.get("Functional_Specification"))
                                                functional_specification_others = item.get("Functional_Specification_others")

                                                # If Functional_Specification is "Others", take the value from Functional_Specification_others
                                                if functional_specification and functional_specification.lower() == "others":
                                                    if functional_specification_others and functional_specification_others != "nan":
                                                        functional_specification = functional_specification_others.strip()
                                                        functional_specification_others = ""
                                                    else:
                                                        result = api_json_response_format(False, "Functional specification value is mandatory", 500, {})
                                                        return result

                                                sql = """
                                                    INSERT INTO admin_job_post (
                                                        job_reference_id, employer_id, job_title, job_overview, job_description,
                                                        workplace_type, job_type, schedule, skills,
                                                        company_sector, functional_specification, city,
                                                        state, country, apply_link, source,
                                                        seniority, company_name, functional_specification_others,
                                                        created_at, updated_at
                                                    )
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                                    ON DUPLICATE KEY UPDATE
                                                        job_title = VALUES(job_title),
                                                        job_overview = VALUES(job_overview),
                                                        job_description = VALUES(job_description),
                                                        workplace_type = VALUES(workplace_type),
                                                        job_type = VALUES(job_type),
                                                        schedule = VALUES(schedule),
                                                        skills = VALUES(skills),
                                                        company_sector = VALUES(company_sector),
                                                        functional_specification = VALUES(functional_specification),
                                                        city = VALUES(city),
                                                        state = VALUES(state),
                                                        country = VALUES(country),
                                                        apply_link = VALUES(apply_link),
                                                        source = VALUES(source),
                                                        seniority = VALUES(seniority),
                                                        company_name = VALUES(company_name),
                                                        functional_specification_others = VALUES(functional_specification_others),
                                                        updated_at = CURRENT_TIMESTAMP;
                                                """

                                                values = (
                                                    item.get("Ref_Job_ID"),
                                                    user_id,
                                                    item.get("Job_Title"),
                                                    item.get("Job_Overview"),
                                                    item.get("Job_Description"),
                                                    workplace,
                                                    job_type,
                                                    schedule,
                                                    item.get("Skills"),
                                                    company_sector,
                                                    functional_specification,
                                                    item.get("City"),
                                                    item.get("State"),
                                                    item.get("Country"),
                                                    item.get("Apply_Link"),
                                                    item.get("Source"),
                                                    item.get("Seniority"),
                                                    item.get("Company_Name"),
                                                    item.get("Functional_Specification_others")
                                                )

                                                update_query(sql, values)
                                                query = "select id, admin_job_status, is_active from admin_job_post where employer_id = %s"
                                                values = (user_id,)
                                                res = execute_query(query, values)
                                                job_status = res[0]['admin_job_status']
                                                key_id = res[0]['id']
                                                is_active = res[0]['is_active']
                                                created_at = datetime.now().isoformat()           
                                                job_details = {
                                                    "id": item.get("Ref_Job_ID"),
                                                    "employer_id": user_id,
                                                    "job_title": item.get("Job_Title"),
                                                    "job_type": job_type,
                                                    "work_schedule": schedule,
                                                    "job_overview": item.get("Job_Overview"),
                                                    "workplace_type": workplace,
                                                    "country": item.get("Country"),
                                                    "city": item.get("City"),
                                                    "specialisation": functional_specification,
                                                    "required_subcontract": "",
                                                    "skills": item.get("Skills"),
                                                    "job_desc": item.get("Job_Description"),
                                                    "required_resume": "",
                                                    "required_cover_letter": "",
                                                    "required_background_check": "",
                                                    "time_commitment": "",
                                                    "time_zone": "",
                                                    "duration": "",
                                                    "job_status": job_status,
                                                    "is_paid": "",
                                                    "is_active": is_active,
                                                    "created_at": created_at
                                                }
                                                store_job_details.append(job_details)
                                                background_runner.get_admin_job_details(item.get("Ref_Job_ID"))
                                            index.add_documents(store_job_details)
                                            store_in_meilisearch(store_job_details)                                
                                            result = api_json_response_format(True, "External jobs added successfully", 200, {})
            else:
                result = api_json_response_format(False, "Unauthorized user", 400, {})


    except Exception as error:
        print("Error executing query:", error)
        result = api_json_response_format(False, str(error), 500, {})

    finally:
        return result
    
def get_individual_external_job_details():
    # def individual_job_details():
    try:
        req_data = request.get_json()                        
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"job_id required",204,{})  
            return result_json
        job_id = req_data['job_id'] 
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:            
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                if isUserExist("admin_job_post","job_reference_id",job_id):
                    query = '''SELECT ap.job_reference_id, ap.job_title, ap.job_type, ap.job_overview, ap.job_description, ap.functional_specification, ap.skills, ap.admin_job_status, ap.company_sector, ap.company_name, ap.country as job_country, ap.state as job_state, ap.city as job_city, ap.schedule, ap.workplace_type,ap.source, ap.apply_link, ap.is_active, ap.seniority, ap.created_at, 
                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                    COALESCE(ap.pricing_category) AS pricing_category, 
                    COALESCE(u.country, su.country) AS company_country, 
                    COALESCE(u.city, su.city) AS company_city, 
                    COALESCE(u.email_id, su.email_id) AS company_email, 
                    COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                    COALESCE(ep.sector, su.sector) AS sector, 
                    COALESCE(ap.job_description) AS company_description, 
                    COALESCE(ap.company_name) AS company_name, 
                    COALESCE(ep.employer_type, su.employer_type) AS employer_type
                    FROM admin_job_post ap LEFT JOIN users u ON ap.employer_id = u.user_id LEFT JOIN employer_profile ep ON ap.employer_id = ep.employer_id LEFT JOIN sub_users su ON ap.employer_id = su.sub_user_id WHERE ap.job_reference_id = %s'''
                    values = (job_id,)
                    job_details_data_set = execute_query(query,values)
                    

                    query = 'select employer_id from admin_job_post where job_reference_id = %s'
                    values = (job_id,)
                    employer_id = execute_query(query, values)
                    if len(employer_id) > 0:
                        if employer_id[0]['employer_id'] < 500000:
                            query = 'SELECT e.employer_id, e.employer_type, e.sector, e.company_name, e.website_url, e.company_description, u.country, u.city FROM employer_profile AS e LEFT JOIN users AS u ON e.employer_id = u.user_id WHERE e.employer_id = %s'
                            values = (employer_id[0]['employer_id'],)
                            employer_details = execute_query(query, values)
                        else:
                            query = 'SELECT su.sub_user_id as employer_id, su.employer_type, su.sector, su.company_name, su.website_url, su.company_description, su.country, su.city FROM sub_users AS su WHERE su.sub_user_id = %s'
                            values = (employer_id[0]['employer_id'],)
                            employer_details = execute_query(query, values)
                    else:
                        employer_details = []

                    if len(employer_details) > 0:
                        if employer_id[0]['employer_id'] < 500000:
                            query = 'select profile_image from users where user_id = %s'
                        else:
                            query = 'select profile_image from sub_users where sub_user_id = %s'
                        values = (employer_id[0]['employer_id'],)
                        profile_pic = execute_query(query, values)
                        job_details_data_set[0].update({'profile_image' : s3_employer_picture_folder_name + str(profile_pic[0]['profile_image'])})
                        job_details_data_set[0].update({'employer_id' : employer_details[0]['employer_id']})
                        job_details_data_set[0].update({'employer_type' : employer_details[0]['employer_type']})
                        job_details_data_set[0].update({'sector' : employer_details[0]['sector']})
                        job_details_data_set[0].update({'company_name' : employer_details[0]['company_name']})
                        job_details_data_set[0].update({'country' : employer_details[0]['country']})
                        job_details_data_set[0].update({'city' : employer_details[0]['city']})
                        job_details_data_set[0].update({'company_description' : employer_details[0]['company_description']})
                        job_details_data_set[0].update({'website_url' : employer_details[0]['website_url']})
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(job_details_data_set))
                else:
                    result_json = api_json_response_format(False,"Job not found",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 


def create_training_event():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin" or user_data["user_role"] == "professional":
                employer_id = user_data['user_id']
                req_data = request.form
                title = req_data.get('title')
                title_description = req_data.get('title_description')
                type_of_offering = req_data.get('type_of_offering')
                program_level = req_data.get('program_level')
                about_speaker = req_data.get('about_speaker')
                about_program = req_data.get('about_program')
                what_to_expect = req_data.get('what_to_expect')
                speaker_name = req_data.get('speaker_name')
                image = request.files.get("image") #speaker_photo
                registration_link = req_data.get('registration_link')
                certificate_program = req_data.get('certificate_program')
                event_date_time = req_data.get('date_and_time')
                certification_name = req_data.get('certification_name')
                if not title:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json

                if not type_of_offering:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json

                if not about_speaker:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if not about_program:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if not what_to_expect:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if not speaker_name:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if not registration_link:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if not title_description:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if not certificate_program:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if certificate_program == "Yes":
                    if not certification_name:
                        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                        return result_json
                else:
                    certification_name = ''
                if image:
                            file_name = image.filename
                            if file_name != '':
                                try:
                                    s3_pro = s3_obj.get_s3_client()
                                    s3_pro.upload_fileobj(image, S3_BUCKET_NAME, s3_trailing_cover_pic_folder_name+file_name)
                                except Exception as e:
                                    print("S3 upload error:", e)
                                    return api_json_response_format(False, "Image not uploaded to S3",500, {})
                            else:
                                result_json = api_json_response_format(False, "Invalid image filename", 400, {})
                                return result_json
                else:
                    result_json = api_json_response_format(False, "Missing image file", 400, {})
                    return result_json  
                        

                query = """insert into training_table(user_id, title,title_description,type_of_offering,program_level, about_speaker, about_program,what_to_expect,speaker_name,registration_link,certificate_program,image,event_date_time, certification_name ) values (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s,%s, %s,%s,%s)"""
                values = (employer_id, title, title_description, type_of_offering, program_level, about_speaker, about_program, what_to_expect,speaker_name, registration_link,certificate_program, file_name, event_date_time, certification_name )
                res = update_query(query, values)
                result_json = api_json_response_format(True,'Successfully inserted',200, {})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",403,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})


    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def admin_training_update_event():
    result_json = {}
    try:
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "admin":
                event_id = request.form.get("event_id") 
                if not event_id:
                    result_json = api_json_response_format(False, "Event ID is required",1, {})
                    return result_json

                is_image_changed = request.form.get("is_image_changed", "N")
                title = request.form.get("title")
                title_description = request.form.get('title_description')
                type_of_offering = request.form.get("type_of_offering")
                program_level = request.form.get("program_level")
                about_speaker = request.form.get("about_speaker")
                about_program = request.form.get("about_program")
                what_to_expect = request.form.get("what_to_expect")
                registration_link = request.form.get("registration_link")
                event_date_time = request.form.get("date_and_time")
                certification_name = request.form.get("certification_name")
                speaker_name = request.form.get("speaker_name")
                certificate_program = request.form.get("certificate_program")
                image = request.files.get("image")
                required_common = [
                    title,type_of_offering,program_level,about_speaker,about_program,what_to_expect,registration_link,event_date_time,speaker_name,certificate_program, title_description
                ]
                if not all(required_common):
                    result_json = api_json_response_format(False, "Fill all required fields", 400, {})
                    return result_json
                if certificate_program == "Yes":
                    if not certification_name:
                        result_json = api_json_response_format(False, "Fill all required fields", 400, {})
                        return result_json
            
                #     if not all([title,type_of_offering,program_level,about_speaker,about_program,what_to_expect,registration_link,event_date_time,speaker_name,certificate_program, title_description, certification_name]):
                #         result_json =  api_json_response_format(False,"fill all the required fields",400,{})
                #         return result_json

                update_fields = []
                update_values = []
                field_value_pairs = [("title", title),("type_of_offering", type_of_offering),("program_level", program_level),("about_speaker", about_speaker),("about_program", about_program),("what_to_expect", what_to_expect),("registration_link", registration_link),("event_date_time", event_date_time),("certification_name", certification_name), ("speaker_name", speaker_name), ("certificate_program", certificate_program),("title_description",title_description)]
                for field, value in field_value_pairs:
                    if value:
                        update_fields.append(f"{field} = %s")
                        update_values.append(value)
                if is_image_changed == 'Y':
                    try:
                        if image:
                            file_name = image.filename
                            if file_name != '':
                                try:
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(image, S3_BUCKET_NAME, s3_trailing_cover_pic_folder_name+file_name)
                                    update_fields.append("image = %s")
                                    update_values.append(file_name)
                                
                                except Exception as e:
                                    print("S3 upload error:", e)
                                    return api_json_response_format(False, "Image not uploaded to S3", 500, {})
                            else:
                                result_json = api_json_response_format(False,"Invalid image filename",400,{})
                                return result_json
                        else:
                            result_json = api_json_response_format(False, "Image not provided",400, {})
                            return result_json
                    except Exception as e:
                        print("Error in uploading image:", e)  
                        return api_json_response_format(False, "Could not upload speaker image",500, {})

                update_fields.append("updated_at = %s")
                update_values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                sql_query = f"UPDATE training_table SET {', '.join(update_fields)} WHERE id = %s"
                update_values.append(event_id)

                update_result = update_query(sql_query, update_values)

                if update_result > 0:
                    result_json = api_json_response_format(True, "Training Event Updated Successfully", 0, {})
                    return result_json
                else:
                    result_json = api_json_response_format(False, "Training Event Not Updated", 500, {})
                    return result_json
            else:
                result_json = api_json_response_format(False, "Unauthorized User", 403, {})
                return result_json
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
            return result_json
    except Exception as error:
        print("Exception while updating event:", error)
        return api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def admin_training_delete_event():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin":
                req_data = request.get_json()
                event_id = req_data["event_id"]
                image_name = req_data["image"]

                if not event_id:
                    result_json = api_json_response_format(False, "Event ID is required", 404, {})
                    return result_json

                delete_event_query = "DELETE FROM training_table WHERE id = %s"
                delete_values = (event_id,)
                delete_result = update_query(delete_event_query, delete_values)

                if delete_result > 0:
                    s3_client = s3_obj.get_s3_client()
                    try:
                        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=image_name) 
                        print("Super Admin event image file deleted from s3 Bucket successfully.")
                        result_json = api_json_response_format(True, "Event Deleted Successfully", 0, {})
                        return result_json
                    except Exception as e:
                        if e.response['Error']['Code'] == 'NoSuchKey':
                            print("File not found.")
                            return False
                        else:
                            print("Error deleting file:", e)
                            raise e
                else:
                    result_json = api_json_response_format(False, "Something went wrong", 500, {})
                    return result_json
            else:
                result_json = api_json_response_format(False,"Unauthorized User",403,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print("Exception while creating event:", error)
        return api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json


def job_preview_details():
    try:
        result = {}
        job_id = request.args.get('job_id')
        if not job_id:
            return api_json_response_format(False, "job_id is required", 400, {})
        query = "Select jp.job_title, jp.job_overview, u.profile_image from job_post jp join users u on u.user_id = jp.employer_id where jp.id = %s UNION select ajp.job_title, ajp.job_overview, u.profile_image FROM admin_job_post ajp join users u on u.user_id = ajp.employer_id WHERE ajp.job_reference_id = %s;"
        values = (job_id,job_id)
        res = execute_query(query, values)
        if res:
            result = api_json_response_format(True, "Data fetahed Successfully", 0, res)
        else:
            result = api_json_response_format(False, "Job Not Found", 0, {})
    except Exception as error:
        print("Exception while get the job_details:", error)
        return api_json_response_format(False, str(error), 500, {})

    finally:
        return result

def employer_assist_job_count():
    try:
        result = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin" or user_data["user_role"] == "employer":
                req_data = request.get_json()
                user_id = req_data.get("user_id")
                query = "select assisted_jobs_allowed, assisted_jobs_used from user_plan_details where user_id = %s"
                values = (user_id,)
                res = execute_query(query, values)
                if not res:
                    result = api_json_response_format(False, "No data found", 0, {})
                    return result
                current_assist_count = res[0]["assisted_jobs_allowed"] - res[0]["assisted_jobs_used"]
                
                if current_assist_count:
                    result = api_json_response_format(True, "Current_assist_count", 0, current_assist_count)
                else:
                    result = api_json_response_format(False, "No data found", 0, {})
            else:
                result = api_json_response_format(False,"Unauthorized User",403,{})
        else:
            result = api_json_response_format(False,"Invalid Token. Please try again",401,{})
            

    except Exception as error:
        print("Exception while get the job_details:", error)
        return api_json_response_format(False, str(error), 500, {})

    finally:
        return result

def assist_job_decrease_count():
    try:
        result = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "admin" or user_data["user_role"] == "employer":
                req_data = request.get_json()
                user_id = req_data.get("user_id")
                decrease_count = req_data.get("decrease_count")
                if decrease_count <= 0:
                    result = api_json_response_format(False, "Invalid decrease count", 400, {})
                    return result
                query = "select assisted_jobs_allowed, assisted_jobs_used from user_plan_details where user_id = %s"
                values = (user_id,)
                res = execute_query(query, values)
                if not res:
                    result = api_json_response_format(False, "Item not found", 404, {})
                    return result
                row = res[0]
                allowed = row["assisted_jobs_allowed"]
                used = row["assisted_jobs_used"]

                remaining = allowed - used
                # current_count = res[0]["assisted_jobs_allowed"]
                # new_count = current_count + decrease_count
                if decrease_count > remaining:
                    result = api_json_response_format(False, "Assisted job count go below 0",400, remaining)
                    return result
                new_used = used + decrease_count 
                query = "Update user_plan_details SET  assisted_jobs_used = %s where user_id = %s "
                values = (new_used,user_id)
                res = update_query(query, values)

                result = api_json_response_format(False, "Count decreased", 200, new_used)
                # return result
            else:
                result = api_json_response_format(False,"Unauthorized User",403,{})
        else:
            result = api_json_response_format(False,"Invalid Token. Please try again",401,{})
            
    except Exception as error:
        print("Exception while get the job_details:", error)
        result = api_json_response_format(False, str(error), 500, {})

    finally:
        return result