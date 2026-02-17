# from flask import Flask,request

# import app.models.mysql_connector as db_con
from src import app
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from flask import jsonify, request, redirect, url_for, session
from src.controllers.jwt_tokens.jwt_token_required import get_user_token, get_jwt_access_token
from src.models.user_authentication import get_user_data,isUserExist,api_json_response_format, get_sub_user_data
from datetime import datetime,date
from  openai import OpenAI
import meilisearch
from src.models.aws_resources import S3_Client
from dotenv import load_dotenv
import os
import json
from langchain_community.vectorstores import Meilisearch
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import JSONLoader
from src.models.email.Send_email import sendgrid_mail_interview, sendgrid_mail
from datetime import datetime as dt
import time
import platform
import json
from datetime import datetime
import uuid
from flask_executor import Executor
from src.models.background_task import BackgroundTask
from meilisearch import Client
from base64 import b64encode,b64decode

BUCKET_NAME = os.environ.get('PROMPT_BUCKET')
S3_BUCKET_NAME = os.environ.get('CDN_BUCKET')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
JOB_POST_INDEX = os.environ.get("JOB_POST_INDEX")
PROFILE_INDEX = os.environ.get("PROFILE_INDEX")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
API_URI = os.environ.get('API_URI')
MEILISEARCH_JOB_INDEX = os.environ.get("MEILISEARCH_JOB_INDEX")
MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
MEILISEARCH_PROFESSIONAL_INDEX = os.environ.get("MEILISEARCH_PROFESSIONAL_INDEX")
MEILISEARCH_CLOUD_URL = os.environ.get("MEILISEARCH_URL")

os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

executor = Executor(app)
background_runner = BackgroundTask(executor)

home_dir = "/home"
load_dotenv(home_dir+"/.env")
meilisearch_url = os.environ.get("MEILI_HTTP_ADDR")
g_summary_model_name = os.environ.get('SUMMARY_MODEL_NAME')
g_token_encoding_txt = os.environ.get('TOKEN_ENCODING_TEXT')
g_openai_token_limit =int(os.environ.get('OPENAI_TOKEN_LIMIT'))
g_openai_completion_token_limit =int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))
s3_picture_folder_name = "professional/profile-pic/"
s3_employer_picture_folder_name = "employer/logo/"
s3_intro_video_folder_name = "professional/profile-video/"
s3_resume_folder_name = "professional/resume/"
s3_cover_letter_folder_name = "professional/cover-letter/"
s3_partner_learning_folder_name = "partner/learning-doc/"
s3_partner_cover_pic_folder_name = "partner/cover-pic/"
s3_sc_community_cover_pic_folder_name = "2ndcareers/cover-pic/"
s3_employeer_logo_folder_name = "employer/logo/"
s3_obj = S3_Client()

def recruter_signup():         
    try:
        print("recruter sign up")
        return "success"
    except Exception as error:
        print(error)
        return {"error ":error}


# class DocumentWrapper:
#     def __init__(self, document):
#         self.document = document
#         self.page_content = json.dumps(document)  # Serialize the document to a JSON string
#         self.metadata = document  # Use the document itself as metadata

#     def to_dict(self):
#         return self.document

class DocumentWrapper:
    def __init__(self, document):
        self.document = document
        self.page_content = json.dumps(document)  # Serialize the document to a JSON string
        self.metadata = document  # Use the document itself as metadata
        self.id = str(uuid.uuid4())  # Generate a unique ID for each document

    def to_dict(self):
        return self.document

def employer_job_post():
    try:
        key_id = 0
        result_json = {}       
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            flag = 0
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])  
                employer_id = user_data["sub_user_id"]
                owner_emp_id = user_data['user_id']
                flag = 1
            else:
                employer_id = user_data["user_id"]
                owner_emp_id = employer_id
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                req_data = request.get_json()        
                if 'job_title' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_overview' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_type' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'work_schedule' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'workplace_type' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'time_commitment' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                # if 'duration' not in req_data:
                #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                #     return result_json
                if 'is_paid' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'time_zone' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'is_active' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'specialisation' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'skills' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_desc' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_resume' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_cover_letter' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_subcontract' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json                
                if 'job_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'receive_notification' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_background_check' in req_data:
                    required_background_check = req_data['required_background_check']
                else:
                    required_background_check = 'N'
                if 'pre_screen_ques' in req_data:
                    pre_screen_ques = req_data["pre_screen_ques"]
                else:
                    pre_screen_ques = "" 
                if 'key_id' in req_data:
                    key_id = req_data['key_id']

                created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                query = 'SELECT skill_name FROM `filter_skills` where is_active = %s'
                values = ('Y',)
                skill_list = execute_query(query, values)
                temp_skill_list = []
                for i in skill_list:
                    temp_skill_list.append(i['skill_name'])
                txt = req_data["skills"]
                arr = txt.split(",")
                for s in arr :
                    if s not in temp_skill_list:
                        s = s.lower()
                        query = "select count(skill_name) as count from filter_skills where skill_name = %s"
                        values = (s,)
                        count_skill = execute_query(query, values)
                        if count_skill[0]['count'] == 0:
                            query = "insert into filter_skills (skill_name, is_active, created_at) values (%s,%s,%s)"
                            values = (s, 'N', created_at,)
                            rslt = update_query(query, values)
                
                query = 'SELECT specialisation_name FROM `filter_specialisation` where is_active = %s'
                values = ('Y',)
                specialisation_list = execute_query(query, values)
                m = req_data['specialisation']
                temp_specialisation_list = []
                for i in specialisation_list:
                    temp_specialisation_list.append(i['specialisation_name'])
                if m not in temp_specialisation_list:
                    m = m.lower()
                    query = "select count(specialisation_name) as count from filter_specialisation where specialisation_name = %s"
                    values = (m,)
                    count_specialisation = execute_query(query, values)
                    if count_specialisation[0]['count'] == 0:
                        query = "insert into filter_specialisation (specialisation_name, is_active, created_at) values (%s,%s,%s)"
                        values = (m, 'N', created_at,)
                        rslt = update_query(query, values)

                # Extract job details
                job_title = req_data['job_title'] 
                job_type = req_data['job_type']
                work_schedule = req_data['work_schedule']
                workplace_type = req_data['workplace_type']    
                country = req_data['country']
                city = req_data['city']
                time_zone = req_data['time_zone']
                skills = req_data['skills']
                specialisation = req_data['specialisation']
                job_desc = req_data['job_desc']
                required_resume = req_data['required_resume']
                required_cover_letter = req_data['required_cover_letter']
                required_subcontract = req_data['required_subcontract']
                time_commitment = req_data['time_commitment']
                receive_notification = req_data['receive_notification']
                if 'duration' in req_data:
                    duration = req_data['duration']
                else:
                    duration = ''
                job_status = req_data['job_status']
                is_paid = req_data['is_paid']
                is_active = req_data['is_active']
                # employer_id = user_data["user_id"]
                job_overview = req_data["job_overview"]
                # get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                # get_sub_users_values = (owner_emp_id,)
                # sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                # sub_users_list = []
                # if sub_users_dict:
                #     sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                # sub_users_list.append(owner_emp_id)
                # jobs_left_query = 'SELECT COUNT(jp.id) AS opened_jobs_count FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE COALESCE(u.user_id, su.sub_user_id) IN %s AND (jp.job_status = %s OR jp.job_status = %s);'
                # values = (tuple(sub_users_list), 'opened', 'paused',)
                # opened_jobs_dict = execute_query(jobs_left_query, values)
                # get_total_jobs = 'select * from user_plan_details where user_id = %s'
                # values = (owner_emp_id,)
                # total_jobs_dict = execute_query(get_total_jobs, values)
                # total_jobs = 0 
                # if total_jobs_dict:
                #     additional_jobs_count = int(total_jobs_dict[0]['additional_jobs_count'])
                #     total_jobs = total_jobs_dict[0]['total_jobs'] + additional_jobs_count
                # opened_jobs = 0
                # if len(opened_jobs_dict) > 0:
                #     opened_jobs = opened_jobs_dict[0]['opened_jobs_count']
                # job_left = total_jobs - opened_jobs

                query = "SELECT * FROM user_plan_details WHERE user_id = %s"
                user_plan_res = execute_query(query, (owner_emp_id,))

                job_left = 0

                if(user_plan_res and user_plan_res[0]['no_of_jobs'] > 0):
                    job_left = user_plan_res[0]['no_of_jobs']

                if not key_id == 0:
                    # Check if the job exists and update if necessary
                    if isUserExist("job_post", "id", key_id):
                        # query = 'select no_of_jobs from user_plan_details where user_id = %s'
                        # if flag == 1:
                        #     employer_id = (user_data['user_id'],)   #getting employer_id in the case of admin or recruiter
                        # else:
                        #     employer_id = (employer_id,)
                        # total_jobs = execute_query(query, values)
                        # rem_jobs = 0
                        # if len(total_jobs) > 0:
                        #     rem_jobs = total_jobs[0]['no_of_jobs']

                        if job_left == 0:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Job Title' : job_title,
                                        'Job Type' : job_type,
                                        'Workplace Type' : workplace_type,
                                        'Work Schedule' : work_schedule,
                                        'Specialisation' : specialisation,
                                        'Job City' : city,
                                        'Job Country' : country,
                                        'Job Timezone' : time_zone,
                                        'Post Created Status' : "Failure",
                                        'Error' : "Plan expired Error"
                                    }
                                message = f"Unable to create Job post because the plan has expired for {user_data['email_id']}."
                                event_name = "Create Job Post Error"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                            except Exception as e:  
                                print("Error in employer_job_post mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new jobs.",201,{})
                            return result_json     
                        created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")         
                        query = 'update job_post set job_title=%s, job_type=%s, work_schedule=%s,job_overview=%s,workplace_type=%s,country=%s, city=%s, specialisation=%s, required_subcontract=%s, skills=%s, job_desc=%s, required_resume=%s, required_cover_letter=%s, required_background_check=%s,time_commitment=%s, receive_notification=%s, timezone=%s, duration=%s, job_status=%s, is_paid=%s, is_active=%s, created_at=%s, updated_at=%s where id=%s'
                        values = (job_title, job_type, work_schedule, job_overview,workplace_type,country, city, specialisation, required_subcontract, skills, job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, time_zone, duration, job_status, is_paid, is_active, created_at, created_at, key_id,)
                        result = update_query_last_index(query, values)
                        if pre_screen_ques != "": 
                            if isUserExist("pre_screen_ques", "job_id", key_id):
                                query = "delete from pre_screen_ques where job_id = %s"
                                values = (key_id,)
                                del_job = update_query(query, values)
                                for ques in pre_screen_ques:
                                        query = 'insert into pre_screen_ques  (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                                        values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                                        result = update_query_last_index(query, values) 
                                # for ques in pre_screen_ques:
                                #     if 'id' in ques:
                                #         query = 'update pre_screen_ques set custom_pre_screen_ques=%s where job_id=%s and id=%s'
                                #         values = (ques['custom_pre_screen_ques'],key_id,ques['id'],)
                                #         result = update_query_last_index(query, values)
                            else:
                                for ques in pre_screen_ques:
                                        query = 'insert into pre_screen_ques  (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                                        values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                                        result = update_query_last_index(query, values)                     
                    else:             
                        created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                        # query = 'select no_of_jobs + additional_jobs_count as no_of_jobs from user_plan_details where user_id = %s'
                        # if flag == 1:
                        #     values = (user_data['user_id'],)   #getting employer_id in the case of admin or recruiter
                        # else:
                        #     values = (employer_id,)
                        # total_jobs = execute_query(query, values)
                        # rem_jobs = 0
                        # if len(total_jobs) > 0:
                        #     rem_jobs = total_jobs[0]['no_of_jobs']
                        if job_left == 0:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Job Title' : job_title,
                                        'Job Type' : job_type,
                                        'Workplace Type' : workplace_type,
                                        'Work Schedule' : work_schedule,
                                        'Specialisation' : specialisation,
                                        'Job City' : city,
                                        'Job Country' : country,
                                        'Job Timezone' : time_zone,
                                        'Post Created Status' : "Failure",
                                        'Error' : "Plan expired Error"
                                    }
                                message = f"Unable to create Job post because the plan has expired for {user_data['email_id']}."
                                event_name = "Create Job Post Error"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                            except Exception as e:  
                                print("Error in employer_job_post mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new jobs.",201,{})
                            return result_json
                        query = 'insert into job_post (employer_id, job_title, job_type, work_schedule,job_overview,workplace_type,country, city, specialisation, required_subcontract, skills,job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, timezone, duration, job_status, is_paid, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                        values = (employer_id, job_title, job_type, work_schedule,job_overview, workplace_type,country, city, specialisation, required_subcontract, skills, job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, time_zone, duration, job_status, is_paid, is_active, created_at,)
                        result = update_query_last_index(query, values)
                        key_id = result["last_index"]
                        if result["row_count"] > 0:
                            if pre_screen_ques != "":
                                for ques in pre_screen_ques:
                                    query = 'insert into pre_screen_ques  (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                                    values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                                    result = update_query_last_index(query, values)
                else:
                    created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                    # query = 'select no_of_jobs + additional_jobs_count as no_of_jobs from user_plan_details where user_id = %s'
                    # if flag == 1:
                    #     values = (user_data['user_id'],)   #getting employer_id in the case of admin or recruiter
                    # else:
                    #     values = (employer_id,)
                    # total_jobs = execute_query(query, values)
                    # rem_jobs = 0
                    # if len(total_jobs) > 0:
                    #     rem_jobs = total_jobs[0]['no_of_jobs']
                    if job_left == 0:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Job Title' : job_title,
                                    'Job Type' : job_type,
                                    'Workplace Type' : workplace_type,
                                    'Work Schedule' : work_schedule,
                                    'Specialisation' : specialisation,
                                    'Job City' : city,
                                    'Job Country' : country,
                                    'Job Timezone' : time_zone,
                                    'Post Created Status' : "Failure",
                                    'Error' : "Plan expired Error"
                                }
                            message = f"Unable to create Job post because the plan has expired for {user_data['email_id']}."
                            event_name = "Create Job Post Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in employer_job_post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False, "Please upgrade your plan to continue posting new jobs.",201,{})
                        return result_json
                    query = 'insert into job_post (employer_id, job_title, job_type, work_schedule,job_overview,workplace_type,country, city, specialisation, required_subcontract, skills,job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, timezone, duration, job_status, is_paid, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    values = (employer_id, job_title, job_type, work_schedule,job_overview, workplace_type,country, city, specialisation, required_subcontract, skills, job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, time_zone, duration, job_status, is_paid, is_active, created_at,)
                    result = update_query_last_index(query, values)
                    key_id = result["last_index"]
                    if result["row_count"] > 0:
                        if pre_screen_ques != "":
                            for ques in pre_screen_ques:
                                query = 'insert into pre_screen_ques  (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                                values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                                result = update_query_last_index(query, values)
                
                job_details = {
                    "id": key_id,
                    "employer_id": employer_id,
                    "job_title": req_data['job_title'],
                    "job_type": req_data['job_type'],
                    "work_schedule": req_data['work_schedule'],
                    "job_overview": req_data["job_overview"],
                    "workplace_type": req_data['workplace_type'],
                    "country": req_data['country'],
                    "city": req_data['city'],
                    "specialisation": req_data['specialisation'],
                    "required_subcontract": req_data['required_subcontract'],
                    "skills": req_data['skills'],
                    "job_desc": req_data['job_desc'],
                    "required_resume": req_data['required_resume'],
                    "required_cover_letter": req_data['required_cover_letter'],
                    "required_background_check": required_background_check,
                    "time_commitment": req_data['time_commitment'],
                    "time_zone": req_data['time_zone'],
                    "duration": req_data.get('duration', ''),
                    "job_status": req_data['job_status'],
                    "is_paid": req_data['is_paid'],
                    "is_active": req_data['is_active'],
                    "created_at": created_at
                }
                store_in_meilisearch([job_details])
                background_runner.get_job_details(key_id)

                if result["row_count"] >= 0:
                    no_of_jobs_query = 'select no_of_jobs from user_plan_details where user_id = %s'
                    no_of_jobs_values = (owner_emp_id,)
                    no_of_jobs_dict = execute_query(no_of_jobs_query, no_of_jobs_values)
                    no_of_jobs = 0
                    if no_of_jobs_dict:
                        no_of_jobs = no_of_jobs_dict[0]["no_of_jobs"]
                    if no_of_jobs > 0:
                        query = 'UPDATE user_plan_details SET no_of_jobs = no_of_jobs - 1 WHERE user_id = %s'
                        values = (owner_emp_id,)
                        rs = update_query_last_index(query, values)
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Job Title' : job_title,
                                'Job Type' : job_type,
                                'Workplace Type' : workplace_type,
                                'Work Schedule' : work_schedule,
                                'Specialisation' : specialisation,
                                'Job City' : city,
                                'Job Country' : country,
                                'Job Timezone' : time_zone,
                                'Post Created Status' : "Success"
                            }
                        message = f"Job Post created successfully for {user_data['email_id']}."
                        event_name = "Create Job Post"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in employer_job_post mixpanel_event_log : %s",str(e))

                    get_job_id_query = 'select id from job_post where employer_id = %s order by id desc limit 1'
                    get_job_id_values = (employer_id,)
                    get_job_id = execute_query(get_job_id_query, get_job_id_values)
                    if get_job_id:
                        job_id = get_job_id[0]['id']
                    insert_assigned_jobs = 'insert into assigned_jobs (employer_id, job_id, job_status, user_id, created_at) values (%s, %s, %s, %s, %s)'
                    insert_assigned_values = (owner_emp_id, job_id, job_status, employer_id, created_at,)
                    row_count = update_query(insert_assigned_jobs, insert_assigned_values)
                    result_json = api_json_response_format(True, "Job posted successfully", 0, {})
                else:
                    try:
                        event_properties = {    
                                '$distinct_id' : user_data["email_id"], 
                                '$time': int(time.mktime(dt.now().timetuple())),
                                '$os' : platform.system(),          
                                'Email' : user_data["email_id"],
                                'Job Title' : job_title,
                                'Job Type' : job_type,
                                'Workplace Type' : workplace_type,
                                'Work Schedule' : work_schedule,
                                'Specialisation' : specialisation,
                                'Job City' : city,
                                'Job Country' : country,
                                'Job Timezone' : time_zone,
                                'Post Created Status' : "Failure",
                                'Error' : "Data Base Error"
                            }
                        message = f"Unable to create Job post due to Data base Error for {user_data['email_id']}."
                        event_name = "Create Job Post Error"
                        background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                    except Exception as e:  
                        print("Error in employer_job_post mixpanel_event_log : %s",str(e))
                    result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.", 500, {})
            else:
                try:
                    event_properties = {    
                            '$distinct_id' : user_data["email_id"], 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Email' : user_data["email_id"],
                            'Job Title' : job_title,
                            'Job Type' : job_type,
                            'Workplace Type' : workplace_type,
                            'Work Schedule' : work_schedule,
                            'Specialisation' : specialisation,
                            'Job City' : city,
                            'Job Country' : country,
                            'Job Timezone' : time_zone,
                            'Post Created Status' : "Failure",
                            'Error' : "Unauthorized User Error"
                        }
                    message = f"Unable to create job post due to unauthorized user error for {user_data['email_id']}."
                    event_name = "Create Job Post Error"
                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                except Exception as e:  
                    print("Error in employer_job_post mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False, "Unauthorized User", 401, {})
        else:
            try:
                event_properties = {    
                        '$distinct_id' : user_data["email_id"], 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Email' : user_data["email_id"],
                        'Job Title' : job_title,
                        'Job Type' : job_type,
                        'Workplace Type' : workplace_type,
                        'Work Schedule' : work_schedule,
                        'Specialisation' : specialisation,
                        'Job City' : city,
                        'Job Country' : country,
                        'Job Timezone' : time_zone,
                        'Post Created Status' : "Failure",
                        'Error' : "Token Error"
                    }
                message = f"Unable to create Job post due to Token error for {user_data['email_id']}."
                event_name = "Create Job Post Error"
                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
            except Exception as e:  
                print("Error in employer_job_post mixpanel_event_log : %s",str(e))
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    

# def store_in_meilisearch(documents):
#     embeddings = OpenAIEmbeddings()
#     embedders = {
#         "adra": {
#             "source": "userProvided",
#             "dimensions": 1536,
#         }
#     }
#     embedder_name = "adra"
#     index_name = JOB_POST_INDEX
#     wrapped_documents = [DocumentWrapper(doc) for doc in documents]

#     vector_store = Meilisearch.from_documents(
#         documents=wrapped_documents,
#         embedding=embeddings,
#         embedders=embedders,
#         embedder_name=embedder_name,
#         index_name=index_name
#     )
#     print("Stored {} documents in Meilisearch".format(len(documents)))

def store_in_meilisearch(documents):
    try:
        embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")
        embedders = {
            "adra": {
                "source": "userProvided",
                "dimensions": 1536,
            }
        }
        embedder_name = "adra"
        index_name = JOB_POST_INDEX
        wrapped_documents = [DocumentWrapper(doc) for doc in documents]

        # Ensure each document has an 'id' field
        for doc in wrapped_documents:
            if not hasattr(doc, 'id') or doc.id is None:
                raise ValueError(f"Document {doc} is missing an 'id' field.")
        
        # Attempt to store in Meilisearch
        vector_store = Meilisearch.from_documents(
            documents=wrapped_documents,
            embedding=embeddings,
            embedders=embedders,
            embedder_name=embedder_name,
            index_name=index_name
        )
        print("Documents successfully stored in Meilisearch.")

    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as error:
        print(f"An error occurred while storing documents in Meilisearch: {error}")

# def store_in_meilisearch_cloud(documet):
#     try:
#         # Store documents in Meilisearch
#         index_name = MEILISEARCH_JOB_INDEX
#         client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
#         index = client.index(index_name)
#         index.add_documents(documet)
#         print("Documents successfully stored in Meilisearch Cloud.")
#     except ValueError as ve:
#         print(f"ValueError: {ve}")
#     except Exception as error:
#         print(f"An error occurred while storing documents in Meilisearch: {error}")

def get_job_posts():
    try:
        token_result = get_user_token(request)  
        req_data = request.get_json()                                           
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id =  user_data['sub_user_id']
                owner_emp_id = user_data['user_id']
            else:
                employer_id =  user_data['user_id']
                owner_emp_id = employer_id
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                sub_users_list = []
                if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin":
                    get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                    get_sub_users_values = (owner_emp_id,)
                    sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                    if sub_users_dict:
                        sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                    sub_users_list.append(owner_emp_id)

                if user_data["user_role"] == "recruiter":
                    sub_users_list.append(user_data['sub_user_id'])
                if 'job_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json  
                job_status = req_data['job_status']  
                # employer_id = user_data['user_id']
                new_job_list = []
                if job_status == "drafted":
                    query = """
                        SELECT 
                            jp.id, jp.job_title, jp.job_type, jp.employer_id, jp.job_overview, jp.job_desc, jp.responsibilities, jp.specialisation,
                            jp.additional_info, jp.skills, jp.job_status, jp.country, jp.state, jp.city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.timezone,
                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                            jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.receive_notification,
                            jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at
                            FROM 
                                job_post jp
                            WHERE 
                                (jp.job_status = 'drafted') and jp.employer_id = %s ORDER BY jp.created_at DESC """
                    values = (employer_id,)
                else:
                    query = """
                        SELECT 
                            jp.id, jp.job_title, jp.employer_id, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.specialisation,
                            jp.additional_info, jp.skills, jp.job_status, jp.country, jp.state, jp.city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.timezone,
                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                            jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.receive_notification,
                            jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at
                        FROM 
                            job_post jp 
                        WHERE 
                            (jp.job_status IN ('opened', 'paused', 'closed')) and jp.employer_id IN %s ORDER BY jp.created_at DESC """
                    values = (tuple(sub_users_list),)
                job_list = execute_query(query, values)
                
                if job_list:
                    query = 'select company_description, sector, employer_type from employer_profile where employer_id = %s'
                    values = (owner_emp_id,)
                    company_details = execute_query(query, values)
                    if company_details:
                        for job in job_list:
                            job.update({"company_description" : company_details[0]['company_description']})
                            job.update({"sector" : company_details[0]['sector']})
                            job.update({"employer_type" : company_details[0]['employer_type']})

                query = 'select distinct skill_name from filter_skills where is_active = %s'
                values = ('Y',)
                skill_dict = execute_query(query, values)
                skill_list = []
                for i in skill_dict:
                    if i['skill_name'] != "Others":
                        skill_list.append(i['skill_name'])

                query = 'select distinct specialisation_name from filter_specialisation where is_active = %s'
                values = ('Y',)
                specialisation_dict = execute_query(query, values)
                specialisation_list = []
                for j in specialisation_dict:
                    if j['specialisation_name'] != "Others":
                        specialisation_list.append(j['specialisation_name'])

                for obj in job_list:
                    query = "select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s"
                    values = (obj['id'],)
                    question_list = execute_query(query, values)
                    dt_object = obj['created_at']
                    formatted_date1 = dt_object.strftime("%d %B %Y")
                    obj.update({"created_at" : formatted_date1})
                    obj.update({"question_list" : question_list})
                    new_job_list.append(obj)
                    temp_skill_list = []
                    final_skill_list = []
                    final_specialisation_list = []
                    # temp = 
                    temp_arr = obj['skills'].split(",")
                    for a in temp_arr:
                        if a not in skill_list:
                            temp_skill_list.append(a)
                    final_skill_list = skill_list + temp_skill_list

                    skills_array = [{"value": index + 1, "label": skill.strip()} for index, skill in enumerate(final_skill_list) if skill is not None]
                    skills_array_sorted = sorted(skills_array, key=lambda x: x["label"])

                    if obj['specialisation'] not in specialisation_list:
                        specialisation_list.append(obj['specialisation'])
                        final_specialisation_list = specialisation_list
                    else:
                        final_specialisation_list = specialisation_list
                    specialisation_array = [{"value": index + 1, "label": specialisation.strip()} for index, specialisation in enumerate(final_specialisation_list) if specialisation is not None]
                    specialisation_array_sorted = sorted(specialisation_array, key=lambda x: x["label"])

                    obj.update({"specialisation_list" : specialisation_array_sorted})
                    obj.update({"skill_list" : skills_array_sorted})

                if len(job_list) > 0:
                    try:
                        temp_dict = {'Message': "Employer job posts fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Job Posts", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Job Posts",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Employer Job Posts, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, new_job_list)
                else:
                    result_json = api_json_response_format(True, "No records found", 0, {})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching employer job posts."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Job Posts Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Job Posts Error",event_properties,  temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Get Employer Job Posts Error, {str(e)}")
        print(error)        
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

def get_org_description():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            id = user_data["user_id"]
            table_name, column_name, query = "", "", ""
            if user_data["user_role"] == "employer":                                        
                table_name = "employer_profile"
                column_name = "employer_id"
                query = 'select company_description from employer_profile where employer_id = %s'
            if user_data["user_role"] == "partner":                                        
                table_name = "partner_profile"
                column_name = "partner_id"
                query = 'select company_description from partner_profile where partner_id = %s'
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
            if isUserExist(table_name,column_name,id):                                
                values = (id,)
                about_data = execute_query(query, values)
                result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(about_data))
            else:
                result_json = api_json_response_format(False,"Profile not found",204,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def update_org_description():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                id =  user_data['user_id']
            else:
                id =  user_data['user_id']
            req_data = request.get_json()
            if 'org_desc' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            # id = user_data["user_id"]
            org_description = req_data["org_desc"]
            table_name, column_name, query, user_role = "", "", "", ""
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                user_role = "Employer"
                table_name = "employer_profile"
                column_name = "employer_id"
                query = 'update employer_profile set company_description = %s where employer_id = %s'
            if user_data["user_role"] == "partner": 
                user_role = "Partner"                                       
                table_name = "partner_profile"
                column_name = "partner_id"
                query = 'update partner_profile set company_description = %s where partner_id = %s'
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})                                      
            if isUserExist(table_name,column_name,id):
                values = (org_description, id,)
                row_count = update_query(query,values)
                if row_count > 0:
                    try:
                        temp_dict = {'$distinct_id' : user_data['email_id'], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'Message': f"{user_role} {user_data['email_id']}'s company description updated successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], f"{user_role} Company Description Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'], f"{user_role} Company Description Updation", event_properties,  temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: {user_role} Company Description Updation, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                    background_runner.get_employer_details(user_data['user_id'])
                else:
                    try:
                        temp_dict = {'$distinct_id' : user_data['email_id'], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'Message': f"An error occurred while {user_role} {user_data['email_id']} was updating company description."}
                        event_properties = background_runner.process_dict(user_data["email_id"], f"{user_role} Company Description Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'], f"{user_role} Company Description Updation Error", event_properties,  temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: {user_role} Company Description Updation Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:                        
                result_json = api_json_response_format(False,"User profile Not Found",204,{})                   
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Unable to update {user_role} company description."}
            event_properties = background_runner.process_dict(user_data["email_id"], f"{user_role} Company Description Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],f"{user_role} Company Description Updation Error",event_properties,  temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: {user_role} Company Description Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def update_interview_status():
    try:
        result_json = {}                
        req_data = request.get_json()
        if 'subject' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields",204,{})  
            return result_json
        if 'message' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields",204,{})  
            return result_json
        if 'job_id' not in req_data :
            result_json = api_json_response_format(False,"Please fill in all the fields",204,{})  
            return result_json
        if 'email_id' not in req_data :
            result_json = api_json_response_format(False,"Please fill in all the fields",204,{})  
            return result_json
        if 'professional_id' not in req_data :
            result_json = api_json_response_format(False,"Please fill in all the fields",204,{})  
            return result_json
        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                subject = req_data['subject']
                job_id = req_data['job_id']
                email_id = req_data['email_id']
                professional_id = req_data['professional_id']
                message = req_data['message']
                recipients = [email_id]
                cc_recipients = [token_result["email_id"]]
                body = message
                sendgrid_mail_interview(SENDER_EMAIL,recipients,cc_recipients,subject,body,"Sending Interview Invite")
                query = "select job_title from job_post where id = %s"
                values = (job_id,)
                job_title = execute_query(query, values)

                query = "update job_activity set invite_to_interview = %s,application_status = %s WHERE professional_id =  %s and job_id = %s"
                values = ('Y','Contacted', professional_id, job_id,)
                update_status = update_query(query, values)

                notification_msg = f"We are pleased to inform you that you have been invited for an interview for the {job_title[0]['job_title']}. Kindly check your e-mail."
                created_at = datetime.now()                    
                query = "insert into user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                values = (professional_id, notification_msg, created_at,)
                update_notification = update_query(query, values)

                # result_json = api_json_response_format(True," A verification link has been sent to your registered email. Please verify to proceed using the platform.",0,{}) 

                result_json = api_json_response_format(True,"success!",0,{})   
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def get_hiring_team_details():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            employer_id = user_data["user_id"]             
            if user_data["user_role"] == "employer":
                if isUserExist("employer_profile","employer_id",employer_id):
                    if isUserExist("hiring_team", "employer_id", employer_id):                                                       
                        query = 'select * from hiring_team where employer_id = %s'
                        values = (employer_id,)
                        about_data = execute_query(query, values)                
                        result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(about_data))
                    else:
                        result_json = api_json_response_format(False,"No records found for this id",204,{})
                else:                        
                    result_json = api_json_response_format(False,"Employer profile Not Found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def update_hiring_team_details():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            emp_id = 0
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "employer":                                        
                req_data = request.get_json()
                # if 'emp_id' in req_data:
                #     emp_id = req_data['emp_id']
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
                employer_id = user_data["user_id"]                
                first_name = req_data['first_name']
                last_name = req_data['last_name']
                emp_designation = req_data["designation"]
                contact_number = req_data['contact_number']
                country_code = None
                if 'country_code' in req_data:
                    country_code = req_data['country_code']
                if isUserExist("employer_profile","employer_id",employer_id):
                    if isUserExist("hiring_team", "employer_id", employer_id): 
                        query = 'update employer_profile set designation = %s where employer_id = %s'
                        values = (emp_designation, employer_id,)
                        row_count = update_query(query,values)
                        query = 'update hiring_team set first_name = %s, last_name = %s, designation = %s, country_code = %s, contact_number = %s where employer_id = %s'
                        values = (first_name, last_name, emp_designation, country_code, contact_number, employer_id,)
                        row_count = update_query(query,values)
                        query = 'update users set first_name = %s, last_name = %s, country_code = %s, contact_number = %s where user_id = %s'
                        values = (first_name, last_name, country_code, contact_number, employer_id,)
                        update_users_tbl = update_query(query, values)
                        if row_count > 0:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'First Name' : first_name,
                                            'Last Name' : last_name,
                                            'Designation' : emp_designation,
                                            'Message': 'Employer team details updated successfully'}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Updation", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Updation",event_properties,  temp_dict.get('Message'),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Employer Team Updation, {str(e)}")
                            background_runner.get_employer_details(user_data['user_id'])
                            result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                        else:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'First Name' : first_name,
                                            'Last Name' : last_name,
                                            'Designation' : emp_designation,
                                            'Message': 'Unable to update employer team details.'}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Updation Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Updation Error",event_properties,  temp_dict.get('Message'),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Employer Team Updation Error, {str(e)}")
                            result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
                    else:
                        result_json = api_json_response_format(False,"No records found for this employer",204,{})
                else:
                     result_json = api_json_response_format(False,"Employer profile Not Found",204,{})                  
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update employer team details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Updation Error",event_properties,  temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Team Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def assign_jobs():
    result_json = {}
    try:
        req_data = request.get_json()
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                owner_emp_id = user_data['user_id']
            else:
                owner_emp_id = user_data['user_id']
            if user_data["user_role"] == "employer" or user_data['user_role'] == 'employer_sub_admin':
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'user_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'employer_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                job_id = req_data['job_id']
                job_status = req_data['job_status']
                user_id = req_data['user_id']
                # employer_id = req_data['employer_id']
                assigned_jobs_query = 'select count(id) as count from assigned_jobs where job_id = %s and employer_id = %s'
                assigned_jobs_values = (job_id, owner_emp_id,)
                assigned_jobs_count_dict = execute_query(assigned_jobs_query, assigned_jobs_values)
                if assigned_jobs_count_dict:
                    created_at = datetime.now()
                    assigned_jobs_count = assigned_jobs_count_dict[0]['count']
                    if assigned_jobs_count == 0:
                        insert_assigned_jobs = 'insert into assigned_jobs (employer_id, job_id, job_status, user_id, created_at) values (%s, %s, %s, %s, %s)'
                        insert_assigned_values = (owner_emp_id, job_id, job_status, user_id, created_at,)
                        row_count = update_query(insert_assigned_jobs, insert_assigned_values)
                    else:
                        update_assigned_jobs = 'update assigned_jobs set user_id = %s, job_status = %s where job_id = %s and employer_id = %s'
                        update_values = (user_id, job_status, job_id, owner_emp_id,)
                        row_count = update_query(update_assigned_jobs, update_values)
                    if row_count > 0:
                        update_job_post_query = 'update job_post set employer_id = %s where id = %s'
                        update_job_post_values = (user_id, job_id,)
                        update_job_post_count = update_query(update_job_post_query, update_job_post_values)

                        query = "insert into user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                        values = (user_id, 'A job has been assigned to you.', created_at,)
                        update_notification = update_query(query, values)
                        result_json = api_json_response_format(True, "Job assigned successfully.",200,{})
                    else:
                        result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def add_team_members():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])    
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin":                                        
                req_data = request.get_json()
                if 'first_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'last_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'email_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'designation' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'country_code' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'contact_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'access_type' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                employer_id = user_data["user_id"]                
                first_name = req_data["first_name"]
                last_name = req_data["last_name"]
                email_id = req_data["email_id"]
                emp_designation = req_data["designation"]
                country_code = req_data['country_code']
                contact_number = req_data['contact_number']
                access_type = req_data["access_type"]
                if isUserExist("sub_users", "email_id", email_id) or isUserExist("users", "email_id", email_id):
                    result_json = api_json_response_format(False, "User profile already exists with the same email_id.",204,{})
                    return result_json
                else:
                    get_employer_details_query = "SELECT CONCAT(u.first_name,' ', u.last_name) as emp_full_name, u.email_id AS emp_email_id, u.contact_number as emp_contact, u.profile_image, u.city, u.country, u.pricing_category, u.payment_status, ep.designation, ep.company_name, ep.company_description, ep.employer_type, ep.sector, ep.website_url FROM users u LEFT JOIN employer_profile ep ON u.user_id = ep.employer_id WHERE u.user_id = %s"
                    get_employer_details_values = (employer_id,)
                    employer_details_dict = execute_query(get_employer_details_query, get_employer_details_values)
                    pricing_category = ''
                    payment_status = ''
                    emp_email_id = ''
                    emp_full_name = ''
                    emp_contact = ''
                    company_name = ''
                    if employer_details_dict:
                        pricing_category = employer_details_dict[0]['pricing_category']
                        payment_status = employer_details_dict[0]['payment_status']
                        emp_email_id = employer_details_dict[0]['emp_email_id']
                        emp_full_name = employer_details_dict[0]['emp_full_name']
                        emp_contact = employer_details_dict[0]['emp_contact']
                        profile_image = employer_details_dict[0]['profile_image']
                        city = employer_details_dict[0]['city']
                        country = employer_details_dict[0]['country']
                        owner_designation = employer_details_dict[0]['designation']
                        company_name = employer_details_dict[0]['company_name']
                        company_description = employer_details_dict[0]['company_description']
                        employer_type = employer_details_dict[0]['employer_type']
                        sector = employer_details_dict[0]['sector']
                        website_url = employer_details_dict[0]['website_url']
                    get_role_id_query = 'select role_id from user_role where user_role = %s'
                    get_role_id_values = (access_type,)
                    get_role_id_dict = execute_query(get_role_id_query, get_role_id_values)
                    role_id = 10  #defaulted to recruiter's role_id because of minimal access
                    if get_role_id_dict:
                        role_id = get_role_id_dict[0]["role_id"]
                    
                    insert_sub_user_query = 'insert into sub_users (user_id, role_id, first_name, last_name, title, email_id, country_code, phone_number, profile_image, pricing_category, payment_status, city, country, company_name, company_description, employer_type, sector, website_url, login_mode, email_active, is_active) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    insert_sub_user_values = (employer_id, role_id, first_name, last_name, emp_designation, email_id, country_code, contact_number, profile_image, pricing_category, payment_status, city, country, company_name, company_description, employer_type, sector, website_url, 'Manual', 'Y', 'Y',)
                    row_count = update_query(insert_sub_user_query,insert_sub_user_values)
                    if row_count > 0:
                        get_sub_user_id = 'select sub_user_id from sub_users where email_id = %s'
                        values = (email_id,)
                        sub_user_id_dict = execute_query(get_sub_user_id, values)
                        sub_user_id = ''
                        if sub_user_id_dict:
                            sub_user_id = sub_user_id_dict[0]['sub_user_id']
                        token_result = get_jwt_access_token(sub_user_id, email_id)
                        if token_result["status"] == "success":
                            access_token =  token_result["access_token"]                                     
                            subject = "Set Profile Password"
                            recipients = [email_id] 
                            # print(os.getcwd()+"/templates/sub_user_invite.html")                   
                            index = open(os.getcwd()+"/templates/sub_user_invite.html",'r').read()
                            # index = open("/home/applied-sw02/Documents/1_SC_SRC/Nov_29_Dev/2ndcareers-back-end/second_careers_project/templates/sub_user_invite.html", 'r').read()
                            encoded_email_id = b64encode(email_id.encode("utf-8")).decode("utf-8")
                            password_link = f'https://devapp.2ndcareers.com/create_sub_user_password?email_id={encoded_email_id}'
                            index = index.replace("{Your_Name}", emp_full_name)
                            index = index.replace("{Your_Position}", owner_designation)
                            index = index.replace("{Organization_Name}", company_name)
                            index = index.replace("{Contact_Information}", str(emp_contact))
                            index = index.replace("{password_set_link}", password_link)
                            
                            body = index
                            sendgrid_mail(SENDER_EMAIL, recipients, subject, body, "Employer Password Setup")                          
                            res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
                            result_json = api_json_response_format(True,"An account has been created. A verification link has been sent to the registered email. Please verify to proceed using the platform. If you don't see the email, please check your spam or junk folder.",0,res_data)
                            try:
                                temp_dict = {'$distinct_id' : email_id, 
                                            '$time': int(time.mktime(dt.now().timetuple())),
                                            '$os' : platform.system(),
                                            'Name' : first_name,
                                            'Designation' : emp_designation,
                                            'Email Id' : email_id,
                                            'Role Id' : role_id,
                                            'Access Type' : access_type,
                                            'Employer Email' : emp_email_id,
                                            'Message': 'Employer team member added successfully'}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Members Addition", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Members Addition",event_properties, temp_dict["Message"],user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Employer Team Members Addition, {str(e)}")
                        else:
                            try:
                                event_properties = {    
                                    '$distinct_id' : email_id, 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),
                                    'Name' : first_name,
                                    'Designation' : emp_designation,
                                    'Email Id' : email_id,
                                    'Role Id' : role_id,
                                    'Access Type' : access_type,
                                    'Employer Email' : emp_email_id,
                                    'Error' : 'Token error ' + str(token_result["stauts"])            
                                }
                                message = f"Token error in Profile {user_data['email_id']}"
                                event_name = "Employer Profile Error"
                                background_runner.mixpanel_event_async(email_id,event_name,event_properties, message, user_data)
                            except Exception as e:  
                                print("Error in Employer Profile Creation mixpanel_user_and_event_log : %s",str(e))
                            result_json = api_json_response_format(False,token_result["stauts"],401,{})
                    else:
                        try:
                            temp_dict = {'$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'Name' : first_name,
                                        'Designation' : emp_designation,
                                        'Email Id' : email_id,
                                        'Role Id' : role_id,
                                        'Access Type' : access_type,
                                        'Employer Email' : emp_email_id,
                                        'Message': 'Unable to add employer team details.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Members Addition Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Members Addition Error",event_properties, temp_dict['Message'],user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Employer Team Members Addition Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})                 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to add employer team details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Updation Error",event_properties, temp_dict["Message"],user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Team Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def update_team_members():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])    
            if user_data["user_role"] == "recruiter" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "employer":                                         
                req_data = request.get_json()
                if 'first_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'last_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'email_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'designation' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'country_code' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'contact_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'access_type' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'sub_user_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                employer_id = user_data["user_id"]
                sub_user_id = req_data["sub_user_id"]
                first_name = req_data["first_name"]
                last_name = req_data["last_name"]
                email_id = req_data["email_id"]
                emp_designation = req_data["designation"]
                country_code = req_data['country_code']
                contact_number = req_data['contact_number']
                access_type = req_data["access_type"]
                if isUserExist("sub_users", "sub_user_id", sub_user_id):
                    get_role_id_query = 'select role_id from user_role where user_role = %s'
                    get_role_id_values = (access_type,)
                    get_role_id_dict = execute_query(get_role_id_query, get_role_id_values)
                    role_id = 10  #defaulted to recruiter's role_id because of minimal access
                    if get_role_id_dict:
                        role_id = get_role_id_dict[0]["role_id"]
                    query = 'update sub_users set role_id = %s, first_name = %s, last_name = %s, email_id = %s, title = %s, country_code = %s, phone_number = %s where sub_user_id = %s and user_id = %s'
                    values = (role_id, first_name, last_name, email_id, emp_designation, country_code, contact_number, sub_user_id, employer_id,)
                    row_count = update_query(query,values)
                    if row_count > 0:
                        try:
                            temp_dict = {'$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'Name' : first_name,
                                        'Designation' : emp_designation,
                                        'Email Id' : email_id,
                                        'Role Id' : role_id,
                                        'Access Type' : access_type,
                                        'Message': 'Employer team member updated successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Members Updation", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Members Updation",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Employer Team Members Updation, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                    else:
                        try:
                            temp_dict = {'$distinct_id' : email_id, 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),
                                        'Name' : first_name,
                                        'Designation' : emp_designation,
                                        'Email Id' : email_id,
                                        'Role Id' : role_id,
                                        'Access Type' : access_type,
                                        'Message': 'Unable to update employer team details.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Members Updation Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Members Updation Error",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Employer Team Members Updation Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})                 
                else:
                    result_json = api_json_response_format(False,"User profile not found",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update employer team details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Team Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Team Updation Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Team Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def delete_team_members():
    result_json = {}
    req_data = request.get_json()
    if 'sub_user_id' not in req_data:
        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})
        return result_json
    sub_user_id = req_data['sub_user_id']
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])   
            employer_id = user_data['user_id'] 
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin":
                if isUserExist("employer_profile","employer_id",employer_id):
                    if isUserExist("sub_users", "sub_user_id", sub_user_id):
                        switch_job_posts = 'update job_post set employer_id = %s where employer_id = %s'
                        switch_job_posts_data = (employer_id, sub_user_id,)
                        update_query(switch_job_posts, switch_job_posts_data)
                        switch_assign_access = 'update assigned_jobs set user_id = %s where user_id = %s'
                        switch_job_access_data = (employer_id, sub_user_id,)
                        update_query(switch_assign_access, switch_job_access_data)
                        query = 'delete from sub_users where sub_user_id = %s and user_id = %s'
                        values = (sub_user_id, employer_id,)
                        delete_data = update_query(query, values)                
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                    else:
                        result_json = api_json_response_format(False,"No records found for this id",204,{})
                else:                        
                    result_json = api_json_response_format(False,"Employer profile Not Found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def delete_hiring_team_details():
    result_json = {}
    emp_id = 0
    req_data = request.get_json()
    if 'emp_id' not in req_data:
        result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})
        return result_json
    emp_id = req_data['emp_id']
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            employer_id = user_data["user_id"]             
            if user_data["user_role"] == "employer":
                if isUserExist("employer_profile","employer_id",employer_id):
                    if isUserExist("hiring_team", "id", emp_id):                                                       
                        query = 'delete from hiring_team where id = %s and employer_id = %s'
                        values = (emp_id, employer_id,)
                        about_data = execute_query(query, values)                
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,replace_empty_values(about_data))
                    else:
                        result_json = api_json_response_format(False,"No records found for this id",204,{})
                else:                        
                    result_json = api_json_response_format(False,"Employer profile Not Found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def get_company_details():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            id = user_data["user_id"]
            table_name, column_name, query = "", "", ""
            if user_data["user_role"] == "employer":                                        
                table_name = "employer_profile"
                column_name = "employer_id"
                query = 'select website_url, sector, employer_type from employer_profile where employer_id = %s'
            if user_data["user_role"] == "partner":                                        
                table_name = "partner_profile"
                column_name = "partner_id"
                query = 'select website_url,sector, partner_type from partner_profile where partner_id = %s'
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})                                        
            if isUserExist(table_name,column_name,id):                                
                values = (id,)
                company_data = execute_query(query, values) 
                query = "select city, country from users where user_id = %s"
                values = (id,)
                location_data = execute_query(query, values)
                location_dict = {"location" : location_data}
                company_data[0].update(location_dict)               
                result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(company_data))
            else:
                result_json = api_json_response_format(False,"Profile not found",204,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def update_company_details():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
            req_data = request.get_json()
            id = user_data["user_id"]
            table_name, column_name, query, values = "", "", "", ""
            if 'website_url' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'org_type' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'country' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'city' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json    
            if 'sector' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            website_url = req_data["website_url"]
            org_type = req_data["org_type"]
            country = req_data["country"]
            city = req_data["city"]
            sector = req_data["sector"]
            if user_data["user_role"] == "employer" or user_data['user_role'] == "employer_sub_admin":                                        
                table_name = "employer_profile"
                column_name = "employer_id"
                query = 'update employer_profile set website_url = %s, sector = %s, employer_type = %s where employer_id = %s'
                query_1 = 'update sub_users set website_url = %s, sector = %s, employer_type = %s where user_id = %s'
                values = (website_url, sector, org_type, id,)
            if user_data["user_role"] == "partner":                                        
                table_name = "partner_profile"
                column_name = "partner_id"
                query = 'update partner_profile set website_url = %s, sector = %s, partner_type = %s where partner_id = %s'
                values = (website_url, sector, org_type, id,)
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
            
            if isUserExist(table_name,column_name,id): 
                data_row_count = update_query(query,values)
                if user_data["user_role"] == "employer" or user_data['user_role'] == "employer_sub_admin": 
                    data_row_count_1 = update_query(query_1,values)
                query = 'update users set country = %s, city = %s where user_id = %s'
                values = (country, city, id,)
                location_row_count = update_query(query,values)
                if data_row_count > 0 and location_row_count > 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Sector' : sector,
                                    'Website URL' : website_url,
                                    'Company Type' : org_type,
                                    'Message': 'Employer company details updated successfully.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Employer Company Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Employer Company Updation",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Employer Company Updation, {str(e)}")
                    if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
                        background_runner.get_employer_details(user_data['user_id'])
                    elif user_data['user_role'] == 'partner':
                        background_runner.get_partner_details(user_data['user_id'])
                    result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Sector' : sector,
                                    'Website URL' : website_url,
                                    'Company Type' : org_type,
                                    'Message': 'An error occurred while updating employer company details.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Employer Company Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Employer Company Updation Error",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Employer Company Updation Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:                        
                result_json = api_json_response_format(False,"Profile not found",204,{})                   
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update employer company details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Company Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Company Updation Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Company Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_employer_profile_dashboard_data():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])  
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])    
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":  
                employer_id =  user_data['user_id']
                query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.country_code, u.contact_number, u.pricing_category, u.country, u.state, u.city, u.profile_image, ep.designation, ep.company_name, ep.company_description, ep.employer_type, ep.sector, ep.website_url, COUNT(CASE WHEN jp.job_status NOT IN ('drafted', 'closed') THEN jp.id END) AS assigned_jobs_count FROM employer_profile ep LEFT JOIN users u ON ep.employer_id = u.user_id LEFT JOIN job_post jp ON ep.employer_id = jp.employer_id WHERE ep.employer_id = %s GROUP BY u.user_id, u.first_name, u.last_name, u.email_id, u.contact_number, u.pricing_category, u.country, u.state, u.city, u.profile_image, ep.designation, ep.company_name, ep.company_description, ep.employer_type, ep.sector, ep.website_url;"
                values = (employer_id,)
                profile_result = execute_query(query, values) 
                profile_image_name = ''
                s3_pic_key = ''
                if profile_result:
                    profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                    s3_pic_key = s3_employer_picture_folder_name+str(profile_image_name)

                profile_dict = {
                    'owner_designation' : replace_empty_values1(profile_result[0]['designation']),
                    'company_name' : replace_empty_values1(profile_result[0]['company_name']),
                    'company_description' : replace_empty_values1(profile_result[0]['company_description']),
                    'org_type' : replace_empty_values1(profile_result[0]['employer_type']),
                    'sector' : replace_empty_values1(profile_result[0]['sector']),
                    'website_url' : replace_empty_values1(profile_result[0]['website_url']),
                    'city': replace_empty_values1(profile_result[0]['city']),
                    'state': replace_empty_values1(profile_result[0]['state']),
                    'country': replace_empty_values1(profile_result[0]['country']),
                    'profile_image': s3_pic_key,
                    'pricing_category':  replace_empty_values1(profile_result[0]['pricing_category'])
                }

                sub_users_query = 'select sub_user_id from sub_users where user_id = %s'
                sub_users_values = (employer_id,)
                sub_users_result = execute_query(sub_users_query, sub_users_values)
                sub_users = ()
                # sub_users_result = {}
                if sub_users_result:
                    sub_users = tuple(i['sub_user_id'] for i in sub_users_result)

                    hiring_team_query = """
                            SELECT su.sub_user_id AS user_id, su.first_name, su.last_name, su.email_id, su.country_code, su.phone_number, su.pricing_category, CASE WHEN su.role_id = 9 THEN 'employer_sub_admin' ELSE 'recruiter' END AS role_name, su.title, COALESCE(COUNT(CASE WHEN jp.job_status NOT IN ('drafted', 'closed') THEN jp.id END), 0) AS assigned_jobs_count FROM sub_users su LEFT JOIN job_post jp ON su.sub_user_id = jp.employer_id WHERE su.sub_user_id IN %s GROUP BY su.sub_user_id, su.first_name, su.last_name, su.email_id, su.phone_number, su.pricing_category, su.role_id, su.title;"""
                    hiring_team_values = (sub_users,)
                    hiring_team_result = execute_query(hiring_team_query, hiring_team_values)
                else:
                    hiring_team_result = []
                                
                if profile_result:
                    hiring_team_result.append({'user_id' : profile_result[0]['user_id'], 'first_name' : profile_result[0]['first_name'], 'last_name' : profile_result[0]['last_name'], 'country_code' : profile_result[0]['country_code'],
                                               'email_id' : profile_result[0]['email_id'], 'phone_number' : profile_result[0]['contact_number'],
                                               'pricing_category' : profile_result[0]['pricing_category'], 'title' : profile_result[0]['designation'],
                                               'assigned_jobs_count' : profile_result[0]['assigned_jobs_count'], 'role_name' : 'OWNER'})
                    profile_dict.update({'hiring_team' : hiring_team_result})
                    role_order = {"OWNER": 0, "ADMIN": 1, "RECRUITER": 2}

                    # Sort the hiring_team list
                    profile_dict["hiring_team"] = sorted( profile_dict["hiring_team"], key=lambda x: role_order.get(x["role_name"], float('inf')))
                try:
                    temp_dict = {'Message': f"User {user_data['email_id']}'s profile details fetched successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Employer Profile Details", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Employer Profile Details",event_properties, temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Employer Profile Details, {str(e)}")
                result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching {user_data['email_id']}'s profile details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Profile Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Profile Details Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Profile Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json


# def home_dashboard_view():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "employer":  
                cost_to_extend = int(os.environ.get('COST_TO_EXTEND'))
                cost_per_job = int(os.environ.get('COST_PER_JOB'))

                employer_id =  user_data['user_id']
                req_data = request.get_json()
                if 'job_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                job_status = req_data["job_status"]
                # query = "SELECT id, job_title from job_post WHERE employer_id = %s and job_status = %s"
                # values = (employer_id,job_status,)
                # posted_job_list = execute_query(query, values)
                job_stats_list = []  # Initialize an array to store job statistics
                query = """
                    SELECT 
                        jp.id AS job_id,
                        jp.job_title,
                        jp.receive_notification,
                        jp.created_at AS posted_on,
                        COALESCE(vc.view_count, 0) AS view_count,
                        COALESCE(ja.applied_count, 0) AS applied_count,
                        COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                        COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                        GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left
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
                            SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count
                        FROM 
                            job_activity
                        GROUP BY 
                            job_id
                    ) ja ON jp.id = ja.job_id
                    WHERE 
                        jp.employer_id = %s
                        AND jp.job_status = %s
                    ORDER BY 
                        jp.id DESC;
                """
                values = (employer_id,job_status,)
                job_stats_list = execute_query(query, values)
                for j in job_stats_list:
                    query = 'update job_post set calc_day = %s where id = %s'
                    values = (j['days_left'], j['job_id'])
                    updation = update_query(query, values)
                    if j['days_left'] <= 5:
                        notification_msg = f"Your job post {j['job_title']} will expire soon. If you’d like to keep it active, you can extend the validity of the post."
                        query = "select count(id) as count from user_notifications where notification_msg = %s and user_id = %s"
                        values = (notification_msg,employer_id,)
                        id_count = execute_query(query,values)
                        if len(id_count) >0:
                            if id_count[0]["count"] == 0:
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.now()                    
                                values = (employer_id,f"Your job post {j['job_title']} will expire soon. If you’d like to keep it active, you can extend the validity of the post.",created_at,)
                                update_query(query,values)
                    if j['days_left'] == 0:
                        query = 'SELECT id, job_title, created_at, DATEDIFF(NOW(), created_at) AS days_since_creation FROM job_post WHERE id = %s'
                        values = (j['job_id'],)
                        date_created = execute_query(query, values)
                        if len(date_created) > 0:
                            if date_created[0]['days_since_creation'] >= 100:
                                notification_msg = f"Your job post {date_created[0]['job_title']} has expired. If you'd like to reopen it and continue attracting candidates, you can extend the validity at any time."
                                query = "select count(id) as count from user_notifications where notification_msg = %s and user_id = %s"
                                values = (notification_msg,employer_id,)
                                id_count = execute_query(query,values)
                                if len(id_count) >0:
                                    if id_count[0]["count"] == 0:
                                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                        created_at = datetime.now()                    
                                        values = (employer_id,f"Your job post {date_created[0]['job_title']} has expired. If you'd like to reopen it and continue attracting candidates, you can extend the validity at any time.",created_at,)
                                        update_query(query,values)
                jobs_left_query = 'select no_of_jobs, user_plan, total_jobs from user_plan_details where user_id = %s'
                values = (employer_id,)
                rs = execute_query(jobs_left_query, values)
                job_left = 0
                total_jobs = 0
                if len(rs) > 0:
                    job_left = rs[0]['no_of_jobs']
                    total_jobs = rs[0]['total_jobs']
                    if rs[0]['user_plan'] == 'Basic' and job_left == 0:
                        notification_msg = f"You have reached the job posting limit for the Basic plan. You can either add an additional job post or upgrade your plan to continue."
                        query = "select count(id) as count from user_notifications where notification_msg = %s and user_id = %s"
                        values = (notification_msg,employer_id,)
                        id_count = execute_query(query,values)
                        if len(id_count) >0:
                            if id_count[0]["count"] == 0:
                                query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                                created_at = datetime.now()                    
                                values = (employer_id,f"You have reached the job posting limit for the Basic plan. You can either add an additional job post or upgrade your plan to continue.",created_at,)
                                update_query(query,values)
                    # if rs[0]['user_plan'] == 'Basic':
                    #     total_jobs = 5
                    # elif rs[0]['user_plan'] == 'Premium':
                    #     total_jobs = 10
                    # elif rs[0]['user_plan'] == 'Platinum':
                    #     total_jobs = 20
                
                if job_stats_list == []:
                    query = 'select emp_welcome_count from employer_profile where employer_id = %s'
                    values = (employer_id,)
                    count = execute_query(query, values)
                    if count[0]['emp_welcome_count'] == 0:
                        result_list = [{"job_list" : job_stats_list},
                                       {"jobs_left" : job_left},
                                       {"cost_per_job" : cost_per_job},
                                       {"cost_to_extend" : cost_to_extend},
                                       {"total_jobs" : total_jobs}]
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Jobs Left' : job_left,
                                        'Total Jobs' : total_jobs,
                                        'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                        result_json = api_json_response_format(True, "No records found", 200, result_list)
                        query = 'update employer_profile set emp_welcome_count = %s where employer_id = %s'
                        values = (1, employer_id,)
                        temp = update_query(query, values)
                    else:
                        result_list = [{"job_list" : job_stats_list},
                                       {"jobs_left" : job_left},
                                       {"cost_per_job" : cost_per_job},
                                       {"cost_to_extend" : cost_to_extend},
                                       {"total_jobs" : total_jobs}]
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Jobs Left' : job_left,
                                        'Total Jobs' : total_jobs,
                                        'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                        result_json = api_json_response_format(True, "No records found", 0, result_list)
                else:
                    result_list = [{"job_list" : job_stats_list},
                                       {"jobs_left" : job_left},
                                       {"cost_per_job" : cost_per_job},
                                       {"cost_to_extend" : cost_to_extend},
                                       {"total_jobs" : total_jobs}]
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Jobs Left' : job_left,
                                    'Total Jobs' : total_jobs,
                                    'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties,temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, result_list)
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching user {user_data['email_id']}'s home dashboard details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page Error",event_properties,temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Get Employer Home Page Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def home_dashboard_view():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            sub_user_id = ''
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id =  user_data['user_id']
                sub_user_id = user_data['sub_user_id']
                owner_emp_id = employer_id
            else:
                employer_id =  user_data['user_id']
                owner_emp_id = employer_id
            cost_to_extend = int(os.environ.get('COST_TO_EXTEND'))
            cost_per_job = int(os.environ.get('COST_PER_JOB'))

            req_data = request.get_json()
            if 'job_status' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            job_status = req_data["job_status"]
            get_sub_users_query = 'select sub_user_id from sub_users where user_id = %s'
            get_sub_users_values = (employer_id,)
            sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
            sub_users = []
            if sub_users_dict:
                for sub_user in sub_users_dict:
                    sub_users.append(sub_user['sub_user_id'])
            sub_users.append(employer_id)
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin":  
                # print(sub_users)
                job_stats_list = []
                query = """
                    SELECT 
                        jp.employer_id,
                        jp.id AS job_id,
                        jp.job_title,
                        jp.receive_notification,
                        jp.job_closed_status_flag,
                        jp.created_at AS posted_on,
                        COALESCE(vc.view_count, 0) AS view_count,
                        COALESCE(ja.applied_count, 0) AS applied_count,
                        COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                        COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                        GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left
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
                            SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count
                        FROM 
                            job_activity
                        GROUP BY 
                            job_id
                    ) ja ON jp.id = ja.job_id
                    WHERE 
                        jp.employer_id IN %s
                        AND jp.job_status = %s
                    ORDER BY 
                        jp.id DESC;
                """
                values = (tuple(sub_users),job_status,)
                job_stats_list = execute_query(query, values)
                if job_stats_list:
                    for job in job_stats_list:
                        job_id = job['job_id']
                        check_query = 'select user_id from assigned_jobs where job_id = %s'
                        check_values = (job_id,)
                        is_assigned = execute_query(check_query, check_values)
                        if is_assigned:
                            check_sub_users_query = 'SELECT first_name, last_name FROM sub_users WHERE sub_user_id = %s'
                            values = (is_assigned[0]['user_id'],)
                            assigned_user_name = execute_query(check_sub_users_query, values)
                            if not assigned_user_name:
                                check_users_query = 'SELECT first_name, last_name FROM users WHERE user_id = %s'
                                values = (is_assigned[0]['user_id'],)
                                assigned_user_name = execute_query(check_users_query, values)
                                if assigned_user_name:
                                    job['assigned_user_name'] = assigned_user_name[0]['first_name'] + " " +assigned_user_name[0]['last_name']
                                else:
                                    job['assigned_user_name'] = ''
                            else:
                                job['assigned_user_name'] = assigned_user_name[0]['first_name'] + " " +assigned_user_name[0]['last_name']
                        else:
                            job['assigned_user_name'] = ''
                        get_recruiters_query = 'select sub_user_id, first_name, last_name AS full_name from sub_users where user_id = %s'
                        values = (employer_id,)
                        recruiters = execute_query(get_recruiters_query, values)
                        if recruiters:
                            job.update({"recruiters" : recruiters})
                            # job.update({"employer_id" : employer_id})
                        else:
                            job.update({"recruiters" : []})
                            # job.update({"employer_id" : employer_id})
            elif user_data['user_role'] == 'recruiter':
                get_job_ids_query = 'SELECT aj.job_id, jp.job_status FROM assigned_jobs aj JOIN job_post jp ON aj.job_id = jp.id WHERE aj.user_id = %s AND jp.job_status = %s;'
                get_job_ids_values = (sub_user_id, job_status,)
                job_ids_dict = execute_query(get_job_ids_query, get_job_ids_values)
                get_posted_job_ids_query = 'select id from job_post where employer_id = %s and job_status = %s'
                get_posted_job_ids_values = (sub_user_id, job_status,)
                posted_job_ids_dict = execute_query(get_posted_job_ids_query, get_posted_job_ids_values)
                job_id_list = []
                for i in job_ids_dict:
                    job_id_list.append(i['job_id'])
                for j in posted_job_ids_dict:
                    job_id_list.append(j['id'])
                query = """
                    SELECT 
                        jp.id AS job_id,
                        jp.job_title,
                        jp.receive_notification,
                        jp.job_closed_status_flag,
                        jp.created_at AS posted_on,
                        COALESCE(vc.view_count, 0) AS view_count,
                        COALESCE(ja.applied_count, 0) AS applied_count,
                        COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                        COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                        GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left
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
                            SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count
                        FROM 
                            job_activity
                        GROUP BY 
                            job_id
                    ) ja ON jp.id = ja.job_id
                    WHERE 
                        jp.id IN %s
                        AND jp.job_status = %s
                    ORDER BY 
                        jp.id DESC;
                """
                values = (tuple(job_id_list), job_status,)
                job_stats_list = execute_query(query, values)
                for j in job_stats_list:
                    j.update({"employer_id" : employer_id})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
                return result_json
            
            jobs_left_query ="SELECT COUNT(jp.id) AS opened_jobs_count, MAX(COALESCE(u.pricing_category, su.pricing_category)) AS pricing_category FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE COALESCE(u.user_id, su.sub_user_id) IN %s AND (jp.job_status = %s OR jp.job_status = %s);"
            values = (tuple(sub_users), 'opened', 'paused',)
            opened_jobs_dict = execute_query(jobs_left_query, values)

            total_jobs = 0 
            get_total_jobs = 'select * from user_plan_details where user_id = %s'
            values = (employer_id,)
            total_jobs_dict = execute_query(get_total_jobs, values)
            if total_jobs_dict:
                additional_jobs_count = int(total_jobs_dict[0]['additional_jobs_count'])
                total_jobs = total_jobs_dict[0]['total_jobs'] + additional_jobs_count

            opened_jobs = 0
            user_plan = 'trialing'
            
            if len(opened_jobs_dict) > 0:
                opened_jobs = opened_jobs_dict[0]['opened_jobs_count']
                user_plan = opened_jobs_dict[0]['pricing_category']
            # job_left = total_jobs - opened_jobs
            jobs_left_query = "SELECT no_of_jobs FROM user_plan_details WHERE user_id = %s"
            jobs_left_res = execute_query(jobs_left_query, (employer_id,))

            job_left = 0

            # Check the number of jobs left in the user's plan
            if(jobs_left_res and jobs_left_res[0]['no_of_jobs'] > 0):
                job_left = jobs_left_res[0]['no_of_jobs']

            if user_data['user_role'] == 'employer_sub_admin' or user_data['user_role'] == 'recruiter':
                check_welcome_count = 'select welcome_count as emp_welcome_count from sub_users where sub_user_id = %s'
                values = (sub_user_id,)
            elif user_data['user_role'] == 'employer':
                check_welcome_count = 'select emp_welcome_count from employer_profile where employer_id = %s'
                values = (employer_id,)
            
            welcome_count_dict = execute_query(check_welcome_count, values)
            if welcome_count_dict:
                welcome_count = welcome_count_dict[0]['emp_welcome_count']
                if welcome_count == 0:
                    result_list = [{"job_list" : job_stats_list},
                                    {"jobs_left" : job_left},
                                    {"cost_per_job" : cost_per_job},
                                    {"cost_to_extend" : cost_to_extend},
                                    {"total_jobs" : total_jobs},
                                    {"user_plan" : user_plan},
                                    {"email_id" : user_data['email_id']},
                                    {"pricing_category" : user_data['pricing_category']},
                                    {"payment_status" : user_data['payment_status']},
                                    {"profile_image" : s3_employer_picture_folder_name + str(user_data['profile_image'])}]
                    try:
                        temp_dict = {'Jobs Left' : job_left,
                                    'Total Jobs' : total_jobs,
                                    'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties,temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 200, result_list)
                    if user_data['user_role'] == 'recruiter' or user_data['user_role'] == 'employer_sub_admin':
                        query = 'update sub_users set welcome_count = %s where sub_user_id = %s'
                        values = (1, sub_user_id,)
                    else:
                        query = 'update employer_profile set emp_welcome_count = %s where employer_id = %s'
                        values = (1, employer_id,)
                    temp = update_query(query, values)
                else:
                    result_list = [{"job_list" : job_stats_list},
                                    {"jobs_left" : job_left},
                                    {"cost_per_job" : cost_per_job},
                                    {"cost_to_extend" : cost_to_extend},
                                    {"total_jobs" : total_jobs},
                                    {"user_plan" : user_plan},
                                    {"email_id" : user_data['email_id']},
                                    {"pricing_category" : user_data['pricing_category']},
                                    {"payment_status" : user_data['payment_status']},
                                    {"profile_image" : s3_employer_picture_folder_name + str(user_data['profile_image'])}]
                    try:
                        temp_dict = {'Jobs Left' : job_left,
                                    'Total Jobs' : total_jobs,
                                    'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, result_list)
            else:
                result_list = [{"job_list" : job_stats_list},
                                    {"jobs_left" : job_left},
                                    {"cost_per_job" : cost_per_job},
                                    {"cost_to_extend" : cost_to_extend},
                                    {"total_jobs" : total_jobs},
                                    {"user_plan" : user_plan},
                                    {"email_id" : user_data['email_id']},
                                    {"pricing_category" : user_data['pricing_category']},
                                    {"payment_status" : user_data['payment_status']},
                                    {"profile_image" : s3_employer_picture_folder_name + str(user_data['profile_image'])}]
                try:
                    temp_dict = {'Jobs Left' : job_left,
                                'Total Jobs' : total_jobs,
                                'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties, temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, result_list)
            # else:
            #     result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching user {user_data['email_id']}'s home dashboard details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Get Employer Home Page Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def status_update_job_posted():
    result_json = {}
    try:   
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id =  user_data['user_id']
            else:        
                employer_id = user_data['user_id']
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                req_data = request.get_json()   
                job_id = req_data["job_id"] 
                job_status = req_data["job_status"]
                query = 'select job_title from job_post where id = %s'
                values = (job_id,)
                job_title = execute_query(query,values)
                if isUserExist("job_post","id",job_id):
                    if job_status == 'opened':
                        jobs_left_query = 'SELECT COUNT(jp.id) AS opened_jobs_count, u.pricing_category FROM users u LEFT JOIN job_post jp ON u.user_id = jp.employer_id WHERE u.user_id = %s AND (jp.job_status = %s) GROUP BY u.user_id;'
                        values = (employer_id, 'opened',)
                        opened_jobs_dict = execute_query(jobs_left_query, values)
                        if len(opened_jobs_dict) > 0:
                            pricing_category = opened_jobs_dict[0]['pricing_category']
                            opened_jobs = opened_jobs_dict[0]['opened_jobs_count']
                            if pricing_category == 'Basic' and opened_jobs == 2:
                                result_json = api_json_response_format(False,"You have already reached the job posting limit. Please upgrade your plan to continue",500,{})
                                return result_json
                            elif pricing_category == 'Premium' and opened_jobs == 5:
                                result_json = api_json_response_format(False,"You have already reached the job posting limit. Please upgrade your plan to continue",500,{})
                                return result_json
                            elif pricing_category == 'Platinum' and opened_jobs == 10:
                                result_json = api_json_response_format(False,"You have already reached the job posting limit. Please upgrade your plan to continue",500,{})
                                return result_json
                    updated_at = datetime.now()
                    query = 'update job_post set job_status = %s, updated_at = %s where id = %s'
                    values = (job_status,updated_at,job_id,)
                    row_count = update_query(query,values)
                    if job_status == 'paused' or job_status == 'Paused':
                        query = 'delete from home_page_jobs where job_id = %s'
                        values = (job_id,)
                        update_query(query,values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Job Title' : job_title[0]['job_title'],
                                        'Job Status' : job_status,
                                        'Message': f"Status of '{job_title[0]['job_title']}' updated to {job_status}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Job Status Updation", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Job Status Updation",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Job Status Updation, {str(e)}")
                        background_runner.get_job_details(job_id)
                        result_json = api_json_response_format(True,"The job status has been updated successfully.",0,{})
                    else:
                        try:
                            temp_dict = {'Job Title' : job_title[0]['job_title'],
                                        'Job Status' : job_status,
                                        'Message': f"Error in updating the status of '{job_title[0]['job_title']}'."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Job Status Updation Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Job Status Updation Error",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Job Status Updation Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with pause your posted job. We request you to retry.",500,{})
                else:                        
                    result_json = api_json_response_format(False,"Job Not Found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in updating employer job status."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Job Status Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Job Status Updation Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Job Status Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def close_job_posted():
    result_json = {}
    try:  
        candidate_id = 0
        feedback = ""     
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data['user_id']
            else:
                employer_id = user_data['user_id']
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":                                        
                req_data = request.get_json()
                if 'is_role_filled' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                is_role_filled = req_data["is_role_filled"]
                candidate_id = req_data["candidate_id"]
                candidate_id_list = tuple(candidate_id)
                job_status = "Closed"
                feedback = req_data["feedback"]       
                job_id = req_data["job_id"]     
                query = 'select job_title from job_post where id = %s'
                values = (job_id,)
                job_title = execute_query(query,values)         
                if isUserExist("job_post","id",job_id): 
                    closed_on = datetime.now()
                    query = 'update job_post set job_closed_status_flag = %s, is_role_filled = %s, feedback = %s, job_status = %s, closed_on = %s where id = %s'
                    values = (1, is_role_filled, feedback, job_status, closed_on, job_id,)
                    row_count = update_query(query,values)
                    query = 'update user_plan_details set no_of_jobs = no_of_jobs + 0 where user_id = %s'
                    values = (employer_id,)
                    row_count = update_query(query,values)
                    if is_role_filled == 'Y':
                        if row_count > 0:
                            if not candidate_id == None:
                                for id in candidate_id:
                                    query = 'insert into job_hired_candidates (job_id, professional_id) values (%s, %s)'
                                    values = (job_id, id,)
                                    hired_updation = update_query(query, values)
                                query = 'update job_activity set application_status = %s where job_id = %s and professional_id IN %s'
                                values = ("Hired",job_id,candidate_id_list,)
                                update_query(query,values)
                            query = 'delete from home_page_jobs where job_id = %s'
                            values = (job_id,)
                            update_query(query,values)
                            try:
                                temp_dict = {'Job Title' : job_title[0]['job_title'],
                                            'Job Status' : job_status,
                                            'Is Role Filled' : is_role_filled,
                                            'Candidate ID' : candidate_id,
                                            'Feedback' : feedback,
                                            'Message': f"Status of '{job_title[0]['job_title']}' updated to {job_status}."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Employer Close Job", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Employer Close Job",event_properties, temp_dict.get('Message'),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Employer Close Job, {str(e)}")
                            background_runner.get_job_details(job_id)
                            result_json = api_json_response_format(True,"The job has been successfully closed. We would like to thank you for posting the job.",0,{})
                        else:
                            try:
                                temp_dict = {'Job Title' : job_title[0]['job_title'],
                                            'Job Status' : job_status,
                                            'Is Role Filled' : is_role_filled,
                                            'Candidate ID' : candidate_id,
                                            'Feedback' : feedback,
                                            'Message': f"Error in closing the job post '{job_title[0]['job_title']}'."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Employer Close Job Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Employer Close Job Error",event_properties, temp_dict.get('Message'),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Employer Close Job Error, {str(e)}")
                            result_json = api_json_response_format(False,"Sorry! We had an issue with closing your posted job. We request you to retry.",500,{})
                    else:
                        try:
                            temp_dict = {'Job Title' : job_title[0]['job_title'],
                                        'Job Status' : job_status,
                                        'Is Role Filled' : is_role_filled,
                                        'Feedback' : feedback,
                                        'Message': f"Status of '{job_title[0]['job_title']}' updated to {job_status}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Close Job", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Close Job",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Employer Close Job, {str(e)}")
                        background_runner.get_job_details(job_id)
                        result_json = api_json_response_format(True,"The job has been successfully closed. We would like to thank you for posting the job.",0,{})                   
                else:                        
                    result_json = api_json_response_format(False,"Job Not Found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in updating employer job status."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Close Post Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Close Post Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Close Post Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def get_applied_close_job_data():
    result_json = {}
    try:    
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])   
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])        
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":                                        
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json     
                job_id = req_data["job_id"] 
                query = 'select job_title from job_post where id = %s'
                values = (job_id,)
                job_title = execute_query(query,values)             
                if isUserExist("job_post","id",job_id): 
                    query = "select professional_id from job_activity where job_id = %s"
                    values = (job_id,)
                    professional_id_list = execute_query(query, values)
                    candidate_list = []
                    for id in professional_id_list:
                        query = "select first_name,last_name from users where user_id = %s"
                        values = (id["professional_id"],)
                        name_list = execute_query(query, values)
                        first_name = name_list[0]["first_name"]
                        last_name = name_list[0]["last_name"]
                        candidate_list.append({
                            "id" : id["professional_id"],
                            "first_name" : first_name,
                            "last_name" : last_name,
                        })
                    try:
                        temp_dict = {'Job Title' : job_title[0]['job_title'],
                                    'Message': f"Details of candidates applied for the job '{job_title[0]['job_title']}' fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Applied Candidates List", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Applied Candidates List",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Applied Candidates List, {str(e)}")
                    result_json = api_json_response_format(True,"Details fetched successfully!", 0, candidate_list)
                else:
                    result_json = api_json_response_format(False,"Job Not Found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching list of candidates applied for a job."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Applied Candidates List Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Applied Candidates List Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Applied Candidates List Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json


def on_click_load_more():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            user_data = get_user_data(token_result["email_id"])     
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id']) 
            if user_data["user_role"] == "employer" or user_data["user_role"] == "partner" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "admin":
                # employer_id = user_data["user_id"]
                # prof_id = 100182
                req_data = request.get_json()
                if 'professional_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields(ID).",409,{})
                    return result_json
                prof_id = req_data['professional_id']
                temp_id = prof_id.split("-")
                prof_id = temp_id[2]
                query = "SELECT p.about,p.preferences, ps.id AS skill_id, ps.skill_name, ps.skill_level, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, u2.user_id FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s"                
                values = (prof_id,)
                profile_result = execute_query(query, values)                                                                         

                profile_dict = {
                    'about': replace_empty_values1(profile_result[0]['about']),
                    'preferences': replace_empty_values1(profile_result[0]['preferences']),
                    'experience': {},
                    'education': {},
                    'skills': {},
                    'languages': {},
                    'additional_info': {}
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
                result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def format_date(year,month):
    if year == None or month == None:
        return ""
    else:
        date = year + '-' + month
        return date
    
    
def pool_dashboard_view():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id']) 
            if user_data["user_role"] == "employer" or user_data["user_role"] == "partner" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                user_role = ""     
                if user_data["user_role"] == "employer":
                    user_role = "Employer"
                elif user_data['user_role'] == "partner":
                    user_role = "Partner"
                elif user_data['user_role'] == "employer_sub_admin":
                    user_role = "Emloyer Admin"
                elif user_data['user_role'] == "recruiter":
                    user_role = "Recruiter"
                final_result = []
                req_data = request.get_json()
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False, "Please fill in all the required fields(page_number).", 409, {})
                    return result_json
                page_number = req_data["page_number"]
                offset = (page_number - 1) * 10
                # query = "SELECT COUNT(DISTINCT p.professional_id) AS total_candidates FROM professional_profile AS p LEFT JOIN professional_experience AS pe ON p.professional_id = pe.professional_id LEFT JOIN professional_education AS ed ON p.professional_id = ed.professional_id LEFT JOIN professional_skill AS ps ON p.professional_id = ps.professional_id WHERE (SELECT COUNT(*) FROM professional_experience WHERE professional_id = p.professional_id) > 0 OR (SELECT COUNT(*) FROM professional_education WHERE professional_id = p.professional_id) > 0 OR (SELECT COUNT(*) FROM professional_skill WHERE professional_id = p.professional_id) > 0"
                query = "SELECT COUNT(DISTINCT p.professional_id) AS total_candidates FROM professional_profile AS p LEFT JOIN professional_experience AS pe ON p.professional_id = pe.professional_id LEFT JOIN professional_education AS ed ON p.professional_id = ed.professional_id LEFT JOIN professional_skill AS ps ON p.professional_id = ps.professional_id JOIN users u ON p.professional_id = u.user_id WHERE u.email_active = 'Y' AND ( pe.professional_id IS NOT NULL OR ed.professional_id IS NOT NULL OR ps.professional_id IS NOT NULL OR p.about IS NOT NULL);"
                values = ()
                total_candidates = execute_query(query, values)

                # current_id = req_data['id']
                profiles_to_fetch = 10

                # while len(final_result) < profiles_to_fetch:
                query = "SELECT DISTINCT(p.professional_id) FROM professional_profile AS p LEFT JOIN professional_experience AS pe ON p.professional_id = pe.professional_id LEFT JOIN professional_education AS ed ON p.professional_id = ed.professional_id LEFT JOIN professional_skill AS ps ON p.professional_id = ps.professional_id JOIN users u ON p.professional_id = u.user_id WHERE u.email_active = 'Y' AND ( pe.professional_id IS NOT NULL OR ed.professional_id IS NOT NULL OR ps.professional_id IS NOT NULL OR p.about IS NOT NULL) LIMIT %s OFFSET %s;"
                values = (profiles_to_fetch, offset,)
                result = execute_query(query, values)
                    # if len(result) == 0:
                    #     break
                if result:
                    for res in result:                    
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
                        values = (res['professional_id'],res['professional_id'],res['professional_id'],)
                        profile_result = execute_query(query, values)

                        if profile_result:
                            
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
                else:
                    final_result = []

                if not final_result:
                    final_dict = {
                        "final_result": [],
                        "total_professionals": 0
                    }
                    try:
                        temp_dict = {'Total Professionals' : 0,
                                    'Message': "There is no professionals to display."}
                        event_properties = background_runner.process_dict(user_data["email_id"], f"{user_role} Pool Dashboard Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],f"{user_role} Pool Dashboard Page",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: {user_role} Pool Dashboard Page, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, final_dict)
                    return result_json

                final_dict = {
                    "final_result": final_result,
                    "total_professionals": total_candidates[0]['total_candidates']
                }
                try:
                    temp_dict = {'Total Professionals' : total_candidates[0]['total_candidates'],
                                'Message': "Professionals details displayed successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], f"{user_role} Pool Dashboard Page", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],f"{user_role} Pool Dashboard Page",event_properties, temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: {user_role} Pool Dashboard Page, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, final_dict)
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching professional details on the {user_role} Pool dashboard page."}
            event_properties = background_runner.process_dict(user_data["email_id"], f"{user_role} Pool Dashboard Page Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],f"{user_role} Pool Dashboard Page Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: {user_role} Pool Dashboard Page Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:
        return result_json

    
def get_selected_professional_detail():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            req_data = request.get_json()
            if 'job_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            if 'professional_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})
            professional_id = req_data['professional_id']
            job_id = req_data['job_id']
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "admin":
                if isUserExist("professional_profile","professional_id",professional_id):
                    query = "SELECT invite_to_interview, application_status, custom_notes,professional_resume,professional_cover_letter,feedback FROM job_activity WHERE job_id = %s and professional_id = %s"
                    values = (job_id, professional_id,)
                    professional_status = execute_query(query, values)

                    query = """SELECT 
                                    u.first_name, 
                                    u.last_name, 
                                    u.email_id, 
                                    u.country_code, 
                                    u.contact_number, 
                                    u.country, 
                                    u.state, 
                                    u.city, 
                                    u.pricing_category, 
                                    p.professional_id,
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
                                    -- Experience ordering
                                    CASE 
                                        WHEN pe.end_year = 'Present' THEN 9999 
                                        ELSE CAST(SUBSTRING_INDEX(pe.end_year, '-', 1) AS UNSIGNED) 
                                    END DESC, 
                                    CAST(SUBSTRING_INDEX(pe.end_year, '-', -1) AS UNSIGNED) DESC, 
                                    CASE 
                                        WHEN pe.end_month = 'Present' THEN 12 
                                        ELSE CAST(SUBSTRING_INDEX(pe.end_month, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    -- Education ordering
                                    CASE 
                                        WHEN ed.end_year = 'Present' THEN 9999 
                                        ELSE CAST(SUBSTRING_INDEX(ed.end_year, '-', 1) AS UNSIGNED) 
                                    END DESC, 
                                    CAST(SUBSTRING_INDEX(ed.end_year, '-', -1) AS UNSIGNED) DESC, 
                                    CASE 
                                        WHEN ed.end_month = 'Present' THEN 12 
                                        ELSE CAST(SUBSTRING_INDEX(ed.end_month, '-', 1) AS UNSIGNED) 
                                    END DESC;"""                
                    values = (professional_id,)
                    profile_result = execute_query(query, values) 

                    if len(profile_result) > 0:                              
                        profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                        intro_video_name = replace_empty_values1(profile_result[0]['video_url']) 
                        feedback = replace_empty_values1(professional_status[0]['feedback'])
                        resume_name = replace_empty_values1(professional_status[0]['professional_resume'])
                        cover_letter_name = replace_empty_values1(professional_status[0]['professional_cover_letter'])
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': f"User details fetched successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Selected Professional Details", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Selected Professional Details",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Selected Professional Details, {str(e)}")
                        result_json = api_json_response_format(True,"No records found",0,{})     
                        return result_json          
                    s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                    if not profile_result[0]['video_url'] == "":
                        if not profile_result[0]['video_url'] == None:
                            s3_video_key = s3_intro_video_folder_name + str(replace_empty_values1(profile_result[0]['video_url'])) 
                        else:
                            s3_video_key = ""              
                    else:
                        s3_video_key = ""              
                    s3_resume_key = s3_resume_folder_name + str(resume_name)
                    s3_cover_letter_key = s3_cover_letter_folder_name + str(cover_letter_name)
                    profile_dict = {
                        'first_name': replace_empty_values1(profile_result[0]['first_name']),
                        'last_name': replace_empty_values1(profile_result[0]['last_name']),
                        'professional_id' : profile_result[0]['professional_id'],                                        
                        'email_id': replace_empty_values1(profile_result[0]['email_id']),
                        'country_code': replace_empty_values1(profile_result[0]['country_code']),
                        'pricing_category': replace_empty_values1(profile_result[0]['pricing_category']),
                        'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                        'city': replace_empty_values1(profile_result[0]['city']),
                        'state': replace_empty_values1(profile_result[0]['state']),
                        'country': replace_empty_values1(profile_result[0]['country']),
                        'profile_image': s3_pic_key,
                        'video_name': s3_video_key,
                        'resume_name': s3_resume_key,
                        'cover_letter_name' : s3_cover_letter_key,
                        'feedback': feedback,
                        'about': replace_empty_values1(profile_result[0]['about']),
                        'preferences': replace_empty_values1(profile_result[0]['preferences']),
                        'experience': {},
                        'education': {},
                        'skills': {},
                        'languages': {},
                        'additional_info': {},
                        'social_link': {},
                        'question_answers' : {},
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
                    # profile_dict['experience'] = experience_list
                    experience_list_1 = []

                    if experience_list:
                        sorted_data = sorted(
                            experience_list, 
                            key=lambda x: x['end_date'] or '9999-12',
                            reverse=True
                        )
                        for item in sorted_data:
                            experience_list_1.append(item)
                        profile_dict['experience'] = experience_list_1
                    if experience_list_1:
                        profile_dict['job_title'] = replace_empty_values1(experience_list_1[0]['job_title'])
                    else:
                        profile_dict['job_title'] = ''
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
                    quest_dict = {"questions" : []}
                    query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                    values = (job_id,)
                    result = execute_query(query, values)
                    if len(result) > 0:
                        for r in result:
                            query = 'select custom_pre_screen_ans from pre_screen_ans where job_id = %s and pre_screen_ques_id = %s and professional_id = %s'
                            values = (job_id, r['id'],professional_id,)
                            ans_result = execute_query(query, values)
                            if len(ans_result) > 0:
                                r.update(ans_result[0])
                            else:
                                r.update({'custom_pre_screen_ans' : ""})
                            quest_dict["questions"].append(r)
                    profile_dict['question_answers'] = quest_dict['questions']
                    if len(professional_status) > 0:
                        profile_dict.update({'applied_status' : "Applied"})
                        profile_dict.update({'feedback' : professional_status[0]['feedback']})
                        profile_dict.update({'invite_to_interview' : professional_status[0]['invite_to_interview']})
                        profile_dict.update({'application_status' : professional_status[0]['application_status']})
                        profile_dict.update({'custom_notes' : professional_status[0]['custom_notes']})
                    else:
                        profile_dict.update({'feedback' : ''})
                        profile_dict.update({'applied_status' : "Not Applied"})
                        profile_dict.update({'invite_to_interview' : 'No'})
                        profile_dict.update({'application_status' : ''})
                        profile_dict.update({'custom_notes' : ''})
                    profile_dict.update({'job_id': job_id})
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"User details fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Selected Professional Details", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Selected Professional Details",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Selected Professional Details, {str(e)}")
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
                else:
                    result_json = api_json_response_format(False,"User profile not found",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in fetching selected professional's details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Selected Professional Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Selected Professional Details Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Selected Professional Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    

def create_dict1(row):
    return {row['type']: row['name']}

def fetch_filter_params():
    data1 = []
    query = """
    SELECT 'skill' AS type, skill_name AS name FROM filter_skills WHERE is_active = %s
    UNION
    SELECT 'country' AS type,country AS name FROM filter_location WHERE is_active = %s
    """
    values = ('Y','Y',) 
    result = execute_query(query, values)

    for row in result:
        data1.append(create_dict1(row))

    merged_data = {
    'skill': [],
    'location': [],
    }

    for row in result:
        param_type = row['type']
        name = row['name'].strip()

        if param_type in merged_data:
            merged_data[param_type].append(name)

        if param_type == 'country':
            merged_data['location'].append(name)
    return merged_data


def candidates_dashboard_view():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            flag = 0
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id =  user_data['sub_user_id']
                owner_emp_id = user_data['user_id']    
                flag = 1      
            else:
                employer_id =  user_data['user_id']
                owner_emp_id = user_data['user_id'] 
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":  
                candidates_short_desc = []
                req_data = request.get_json()
                job_id = req_data["job_id"]
                job_status = req_data["job_status"]
                filter_parameters = fetch_filter_params()

                get_sub_users_query = 'select sub_user_id from sub_users where user_id = %s'
                get_sub_users_values = (owner_emp_id,)
                sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                sub_users = []
                if sub_users_dict:
                    for sub_user in sub_users_dict:
                        sub_users.append(sub_user['sub_user_id'])
                sub_users.append(owner_emp_id)

                if flag == 1:
                    query = "SELECT su.first_name, su.last_name, su.email_id, su.title as designation, su.pricing_category as emp_pricing_category, su.company_name FROM sub_users su WHERE su.sub_user_id = %s"
                    values = (employer_id,)
                    employer_name = execute_query(query, values)
                    # emp_query = "select u.pricing_category as emp_pricing_category, ep.company_name FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id WHERE u.user_id = %s"
                    # values = (owner_emp_id,)
                    # emp_details = execute_query(emp_query, values)
                    # if employer_name and emp_details:
                    #     employer_name[0].update({'emp_pricing_category' : emp_details[0]['emp_pricing_category']})
                    #     employer_name[0].update({'company_name' : emp_details[0]['company_name']})
                    # else:
                    #     employer_name[0].update({'emp_pricing_category' : ''})
                    #     employer_name[0].update({'company_name' : ''})
                else:
                    query = "SELECT u.first_name, u.last_name, u.email_id, u.pricing_category as emp_pricing_category, ep.designation, ep.company_name FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id WHERE u.user_id = %s"
                    values = (employer_id,)
                    employer_name = execute_query(query, values)
                if job_id == "" or job_id == None:
                    if flag == 1:
                        if user_data['user_role'] == 'employer_sub_admin':
                            query = "SELECT jp.id,jp.job_status, jp.job_title, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed FROM job_post jp WHERE jp.employer_id IN %s AND jp.job_status != 'drafted' and jp.job_status = %s  ORDER BY jp.created_at DESC"
                            values = (tuple(sub_users), job_status) # , ep.company_name,ep.designation, u.first_name, u.last_name, u.email_id 
                        else:
                            query = "SELECT jp.id,jp.job_status, jp.job_title, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed FROM job_post jp WHERE jp.employer_id = %s AND jp.job_status != 'drafted' and jp.job_status = %s  ORDER BY jp.created_at DESC"
                            values = (employer_id, job_status)
                        posted_job_list = execute_query(query, values)
                        if employer_id > 500000:
                            sub_user_details_query = "SELECT su.first_name, su.last_name, su.email_id, su.title as designation, su.pricing_category as emp_pricing_category, su.company_name FROM sub_users su WHERE su.sub_user_id = %s"
                            values = (employer_id,)
                            user_detail_dict = execute_query(sub_user_details_query, values)
                        else:
                            emp_query = "select u.first_name, u.last_name, u.email_id, ep.designation, u.pricing_category as emp_pricing_category, ep.company_name FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id WHERE u.user_id = %s"
                            values = (owner_emp_id,)
                            user_detail_dict = execute_query(emp_query, values)
                        if user_detail_dict:
                            for job in posted_job_list:
                                job.update({'first_name' : user_detail_dict[0]['first_name']})
                                job.update({'last_name' : user_detail_dict[0]['last_name']})
                                job.update({'designation' : user_detail_dict[0]['designation']})
                                job.update({'email_id' : user_detail_dict[0]['email_id']})
                                job.update({'emp_pricing_category' : user_detail_dict[0]['emp_pricing_category']})
                                job.update({'company_name' : user_detail_dict[0]['company_name']})
                        else:
                            for job in posted_job_list:
                                job.update({'first_name' : ''})
                                job.update({'last_name' : ''})
                                job.update({'designation' : ''})
                                job.update({'email_id' : ''})
                                job.update({'emp_pricing_category' : ''})
                                job.update({'company_name' : ''})
                    else:
                        query = "SELECT jp.id, jp.employer_id, jp.job_status, jp.job_title, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed, COALESCE(ep.company_name, su.company_name), COALESCE(ep.designation, su.title) as designation, COALESCE(u.first_name, su.first_name) AS first_name, COALESCE(u.last_name, su.last_name) AS last_name, COALESCE(u.email_id, su.email_id) AS email_id FROM job_post jp LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN users u ON ep.employer_id = u.user_id LEFT JOIN sub_users su ON ep.employer_id = su.sub_user_id WHERE jp.employer_id IN %s AND jp.job_status != 'drafted' AND jp.job_status = %s ORDER BY jp.created_at DESC;"
                        values = (tuple(sub_users), job_status)
                        posted_job_list = execute_query(query, values)
                    
                    if len(posted_job_list) > 0:
                        job_id = posted_job_list[0]['id']
                    else:
                        profile_dict = {
                            'first_name': "",
                            'last_name': "",
                            'professional_id' : "",                                        
                            'email_id': "",
                            'pricing_category': "",
                            'country_code' : "",
                            'contact_number': "",
                            'city': "",
                            'state': "",
                            'country': "",
                            'profile_image': "",
                            'video_name': "",
                            'resume_name': "",
                            'cover_letter_name' : "",
                            'about': "",
                            'preferences': "",
                            'experience': [],
                            'education': [],
                            'skills': [],
                            'languages': [],
                            'additional_info': [],
                            'social_link': [],
                            'expert_notes' : [],
                            'question_answers' : [],
                            'job_list' : [], 
                            'job_id' : "",
                            'candidates_short_desc' : [],
                            'feedback' : "",
                            'filter_parameters' : filter_parameters
                        }
                        try:
                            temp_dict = {'Message': f"The candidates dashboard for employer {user_data['email_id']} was displayed successfully. No jobs have been posted."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Candidates Dashboard View", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Candidates Dashboard View",event_properties, temp_dict.get('Message'),user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Candidates Dashboard View, {str(e)}")
                        result_json = api_json_response_format(True,"No job posted",0,profile_dict)
                        return result_json
                else:
                    if job_status == "" or job_status == None:
                        query = "SELECT jp.id, jp.job_status, jp.job_title, jp.updated_at, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed FROM job_post jp WHERE jp.employer_id IN %s AND jp.job_status != 'drafted' ORDER BY jp.created_at DESC"
                        values = (tuple(sub_users),) #, ep.company_name,ep.designation, u.first_name, u.last_name, u.email_id
                    else:
                        query = "SELECT jp.id, jp.job_status, jp.job_title, jp.updated_at, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed FROM job_post jp WHERE jp.employer_id IN %s and jp.job_status != 'drafted' and jp.job_status = %s ORDER BY jp.created_at DESC"
                        values = (tuple(sub_users),job_status,)  #ep.company_name,ep.designation,u.first_name, u.last_name,u.email_id        JOIN employer_profile ep ON jp.employer_id = ep.employer_id JOIN users u ON ep.employer_id = u.user_id
                    posted_job_list = execute_query(query, values)
                    if employer_id > 500000:
                        sub_user_details_query = "SELECT su.first_name, su.last_name, su.email_id, su.title as designation, su.pricing_category as emp_pricing_category, su.company_name FROM sub_users su WHERE su.sub_user_id = %s"
                        values = (employer_id,)
                        user_detail_dict = execute_query(sub_user_details_query, values)
                    else:
                        emp_query = "select u.first_name, u.last_name, u.email_id, ep.designation, u.pricing_category as emp_pricing_category, ep.company_name FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id WHERE u.user_id = %s"
                        values = (owner_emp_id,)
                        user_detail_dict = execute_query(emp_query, values)
                    if user_detail_dict:
                        for job in posted_job_list:
                            job.update({'first_name' : user_detail_dict[0]['first_name']})
                            job.update({'last_name' : user_detail_dict[0]['last_name']})
                            job.update({'designation' : user_detail_dict[0]['designation']})
                            job.update({'email_id' : user_detail_dict[0]['email_id']})
                            job.update({'emp_pricing_category' : user_detail_dict[0]['emp_pricing_category']})
                            job.update({'company_name' : user_detail_dict[0]['company_name']})
                    else:
                        for job in posted_job_list:
                            job.update({'first_name' : ''})
                            job.update({'last_name' : ''})
                            job.update({'designation' : ''})
                            job.update({'email_id' : ''})
                            job.update({'emp_pricing_category' : ''})
                            job.update({'company_name' : ''})
                query = "SELECT professional_id, invite_to_interview, application_status, custom_notes FROM job_activity WHERE job_id = %s ORDER BY created_at ASC"
                values = (job_id,)
                professional_id_list = execute_query(query, values)
                
                if not len(professional_id_list) > 0:
                    profile_dict = {
                    'first_name': "",
                    'last_name': "",
                    'professional_id' : "",                                        
                    'email_id': "",
                    'pricing_category': "",
                    'country_code' : "",
                    'contact_number': "",
                    'city': "",
                    'state': "",
                    'country': "",
                    'profile_image': "",
                    'video_name': "",
                    'resume_name': "",
                    'cover_letter_name' : "",
                    'about': "",
                    'preferences': "",
                    'experience': [],
                    'education': [],
                    'skills': [],
                    'languages': [],
                    'additional_info': [],
                    'social_link': [],
                    'question_answers' : [],
                    'job_list' : posted_job_list, 
                    'job_id' : job_id,
                    'candidates_short_desc' : [],
                    'feedback' : "",
                    'filter_parameters' : filter_parameters
                    }
                    try:
                        temp_dict = {'Message': f"The candidates dashboard for employer {user_data['email_id']} was displayed successfully. No professional have been applied."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Candidates Dashboard View", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Candidates Dashboard View",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:
                        print(f"Error in mixpanel event logging: Candidates Dashboard View, {str(e)}")
                    result_json = api_json_response_format(True,"No records found",0,profile_dict)
                    return result_json
                # professional_id = professional_id_list[0]['professional_id']
                s3_picture_folder_name  = "professional/profile-pic/"
                s3_intro_video_folder_name = "professional/profile-video/" 
                recomeded = employer_candidate_recommended(job_id)
                ids = list(set(data["id"] for data in recomeded))
                id_list = []
                if len(ids) > 3:
                    id_list.append(ids[0])
                    id_list.append(ids[1])
                    id_list.append(ids[2])
                    id_tuple = tuple(id_list)
                else:
                    id_tuple = tuple(ids)
                processed_professional_ids = set()
                query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                values = (job_id,2,)
                sc_recommended_professional_id = execute_query(query, values)
                professional_ids_tuple = tuple(d['professional_id'] for d in sc_recommended_professional_id)
                combined_id_tuple = professional_ids_tuple + id_tuple
                unique_combined_tuple = tuple(set(combined_id_tuple))
                professional_ids = [item['professional_id'] for item in sc_recommended_professional_id]
                second_career_professional_ids = set()
                second_career_recommendations = []
                ai_recommendations = []

                if len(unique_combined_tuple) > 0:  
                    for data in unique_combined_tuple:
                        professional_id_1 = data
                        professional_id_list_values = [entry["professional_id"] for entry in professional_id_list]
                        if professional_id_1 not in second_career_professional_ids:
                            if professional_id_1 in professional_id_list_values and professional_id_1 not in processed_professional_ids:
                                query = "SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, p.professional_id, pe.id AS experience_id, pe.start_year, pe.end_year, pe.job_title, pe.created_at, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') THEN '0000-00' ELSE pe.end_year END DESC, CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN '0000-00' ELSE pe.start_year END DESC, pe.created_at DESC;"
                                values = (professional_id_1,)
                                exp_data = execute_query(query, values)
                                query = "SELECT custom_notes,applied_on,professional_resume,professional_cover_letter,feedback FROM job_activity WHERE job_id = %s and professional_id = %s"
                                values = (job_id,professional_id_1,)
                                applied_on = execute_query(query, values)
                                if len(applied_on) > 0:
                                    # date_object = datetime.strptime(str(applied_on[0]["applied_on"]), "%a, %d %b %Y %H:%M:%S %Z")
                                    # formatted_date = date_object.strftime("%d/%m/%Y")
                                    exp_data[0].update({"feedback" : str(applied_on[0]["feedback"])})
                                    exp_data[0].update({"applied_on" : str(applied_on[0]["applied_on"])})
                                    exp_data[0].update({"custom_notes" : str(applied_on[0]["custom_notes"])})
                                    s3_resume_key = s3_resume_folder_name + str(replace_empty_values1(applied_on[0]['professional_resume']))
                                    s3_cover_letter_key = s3_cover_letter_folder_name + str(replace_empty_values1(applied_on[0]['professional_cover_letter']))
                                else:
                                    exp_data[0].update({"feedback" : str(applied_on[0]["feedback"])})
                                    exp_data[0].update({"applied_on" : ""})
                                    exp_data[0].update({"custom_notes" : ""})
                                    s3_resume_key = s3_resume_folder_name
                                    s3_cover_letter_key = s3_cover_letter_folder_name 
                                s3_pic_key1 = s3_picture_folder_name + str(replace_empty_values1(exp_data[0]['profile_image']))
                                exp_data[0].update({"profile_image" : s3_pic_key1})
                                if data in professional_ids:
                                    query = 'select description from sc_recommendation where professional_id = %s and job_id = %s'
                                    values = (professional_id_1, job_id,)
                                    sc_recommended_notes= execute_query(query, values)
                                    if len(sc_recommended_notes) > 0:
                                        exp_data[0].update({"sc_recommended_notes" : sc_recommended_notes[0]['description']})
                                    else:
                                        exp_data[0].update({"sc_recommended_notes" : ""})
                                    exp_data[0].update({"recommended_by" : "2nd careers recommended"})
                                    recommeded_by = "2nd careers recommended"
                                else:
                                    recommeded_by = "AI recommended"
                                    exp_data[0].update({"sc_recommended_notes" : ""})
                                    exp_data[0].update({"recommended_by" : "AI recommended"})
                                exp_data[0].update({"employer_first_name" : employer_name[0]["first_name"]})
                                exp_data[0].update({"employer_last_name" : employer_name[0]["last_name"]})
                                exp_data[0].update({"employer_email_id" : employer_name[0]["email_id"]})
                                exp_data[0].update({"designation" : employer_name[0]["designation"]})
                                exp_data[0].update({"company_name" : employer_name[0]["company_name"]})
                                exp_data[0].update({"pricing_category" : employer_name[0]["emp_pricing_category"]})
                                query = 'select show_to_employer, expert_notes from professional_profile where professional_id = %s'
                                values = (professional_id_1,)
                                status = execute_query(query, values)
                                if len(status) > 0:
                                    if status[0]['show_to_employer'] == 'Y':
                                        exp_data[0].update({"expert_notes" : status[0]["expert_notes"]})
                                    else:
                                        exp_data[0].update({"expert_notes" : ""})
                                else:
                                    exp_data[0].update({"expert_notes" : ""})
                                if recommeded_by == "2nd careers recommended":
                                    # candidates_short_desc.append(exp_data[0])
                                    second_career_recommendations.append(exp_data[0])
                                else:
                                    ai_recommendations.append(exp_data[0])
                                processed_professional_ids.add(professional_id_1)
                candidates_short_desc = second_career_recommendations + ai_recommendations
                for data in professional_id_list:
                    professional_id_1 = data['professional_id']
                    if professional_id_1 not in processed_professional_ids:
                        query = "SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, p.professional_id, pe.id AS experience_id, pe.start_year, pe.end_year, pe.job_title, pe.created_at, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN (pe.end_year IS NULL OR pe.end_year = '') THEN '0000-00' ELSE pe.end_year END DESC, CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN '0000-00' ELSE pe.start_year END DESC, pe.created_at DESC;"
                        values = (professional_id_1,)
                        exp_data = execute_query(query, values)
                        query = "SELECT custom_notes,applied_on,professional_resume,professional_cover_letter,feedback FROM job_activity WHERE job_id = %s and professional_id = %s"
                        values = (job_id,professional_id_1,)
                        applied_on = execute_query(query, values)
                        if len(applied_on) >0:
                            # date_object = datetime.strptime(str(applied_on[0]["applied_on"]), "%a, %d %b %Y %H:%M:%S %Z")
                            # formatted_date = date_object.strftime("%d/%m/%Y")
                            exp_data[0].update({"feedback" : str(applied_on[0]["feedback"])})
                            exp_data[0].update({"applied_on" : str(applied_on[0]["applied_on"])})
                            exp_data[0].update({"custom_notes" : str(applied_on[0]["custom_notes"])})
                            s3_resume_key = s3_resume_folder_name + str(replace_empty_values1(applied_on[0]['professional_resume']))
                            s3_cover_letter_key = s3_cover_letter_folder_name + str(replace_empty_values1(applied_on[0]['professional_cover_letter']))
                        else:
                            exp_data[0].update({"feedback" : str(applied_on[0]["feedback"])})
                            exp_data[0].update({"applied_on" : ""})
                            exp_data[0].update({"custom_notes" : ""})
                            s3_resume_key = s3_resume_folder_name
                            s3_cover_letter_key = s3_cover_letter_folder_name 
                        s3_pic_key1 = s3_picture_folder_name + str(replace_empty_values1(exp_data[0]['profile_image']))
                        exp_data[0].update({"profile_image" : s3_pic_key1})
                        exp_data[0].update({"recommended_by" : "Default"})
                        exp_data[0].update({"employer_first_name" : employer_name[0]["first_name"]})
                        exp_data[0].update({"employer_last_name" : employer_name[0]["last_name"]})
                        exp_data[0].update({"employer_email_id" : employer_name[0]["email_id"]})
                        exp_data[0].update({"designation" : employer_name[0]["designation"]})
                        exp_data[0].update({"company_name" : employer_name[0]["company_name"]})
                        exp_data[0].update({"pricing_category" : employer_name[0]["emp_pricing_category"]})
                        exp_data[0].update({"sc_recommended_notes" : ""})
                        query = 'select show_to_employer, expert_notes from professional_profile where professional_id = %s'
                        values = (professional_id_1,)
                        status = execute_query(query, values)
                        if len(status) > 0:
                            if status[0]['show_to_employer'] == 'Y':
                                exp_data[0].update({"expert_notes" : status[0]["expert_notes"]})
                            else:
                                exp_data[0].update({"expert_notes" : ""})
                        else:
                            exp_data[0].update({"expert_notes" : ""})
                        candidates_short_desc.append(exp_data[0])
                        processed_professional_ids.add(professional_id_1)
                final_list = []
                list_map = map(int, processed_professional_ids)
                list_sorted = sorted(list_map)
                for id in list_sorted:
                    if id in professional_ids_tuple:
                        final_list.append(id)
                        first_id = list(final_list)
                        break
                    elif id in id_tuple and len(professional_ids_tuple) == 0:
                        final_list.append(id)
                        first_id = list(final_list)
                        break
                    else:
                        first_id = list(list_sorted)
                # for i in processed_professional_ids:
                #     if i in sc_recommended_professional_id:
                #         final_list.append(id)
                # first_id = list(processed_professional_ids)
                query = """SELECT 
                                    u.first_name, 
                                    u.last_name, 
                                    u.email_id, 
                                    u.country_code, 
                                    u.contact_number, 
                                    u.country, 
                                    u.state, 
                                    u.city, 
                                    u.pricing_category, 
                                    p.professional_id,
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
                                    -- Experience ordering
                                    CASE 
                                        WHEN pe.end_year = 'Present' THEN 9999 
                                        ELSE CAST(SUBSTRING_INDEX(pe.end_year, '-', 1) AS UNSIGNED) 
                                    END DESC, 
                                    CAST(SUBSTRING_INDEX(pe.end_year, '-', -1) AS UNSIGNED) DESC, 
                                    CASE 
                                        WHEN pe.end_month = 'Present' THEN 12 
                                        ELSE CAST(SUBSTRING_INDEX(pe.end_month, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    -- Education ordering
                                    CASE 
                                        WHEN ed.end_year = 'Present' THEN 9999 
                                        ELSE CAST(SUBSTRING_INDEX(ed.end_year, '-', 1) AS UNSIGNED) 
                                    END DESC, 
                                    CAST(SUBSTRING_INDEX(ed.end_year, '-', -1) AS UNSIGNED) DESC, 
                                    CASE 
                                        WHEN ed.end_month = 'Present' THEN 12 
                                        ELSE CAST(SUBSTRING_INDEX(ed.end_month, '-', 1) AS UNSIGNED) 
                                    END DESC;""" 
                values = (first_id[0],)
                profile_result = execute_query(query, values) 
                query = "SELECT professional_id, invite_to_interview, application_status, custom_notes, professional_resume, professional_cover_letter, feedback FROM job_activity WHERE job_id = %s and professional_id = %s"
                values = (job_id, first_id[0],)
                prof_job_details = execute_query(query, values)                           
                s3_pic_key = s3_picture_folder_name + str(replace_empty_values1(profile_result[0]['profile_image']))
                if len(prof_job_details) > 0:     
                    feedback = replace_empty_values1(prof_job_details[0]['feedback'])
                    s3_resume_key = s3_resume_folder_name + str(replace_empty_values1(prof_job_details[0]['professional_resume']))
                    s3_cover_letter_key = s3_cover_letter_folder_name + str(replace_empty_values1(prof_job_details[0]['professional_cover_letter']))
                else:
                    feedback = ""
                    s3_resume_key = s3_resume_folder_name
                    s3_cover_letter_key = s3_cover_letter_folder_name
                if not profile_result[0]['video_url'] == "":
                    if not profile_result[0]['video_url'] == None:
                        s3_video_key = s3_intro_video_folder_name + str(replace_empty_values1(profile_result[0]['video_url'])) 
                    else:
                        s3_video_key = ""              
                else:
                    s3_video_key = ""
                
                # filter_parameters = fetch_filter_params()
                profile_dict = {
                    'first_name': replace_empty_values1(profile_result[0]['first_name']),
                    'last_name': replace_empty_values1(profile_result[0]['last_name']),
                    'professional_id' : profile_result[0]['professional_id'],                                        
                    'email_id': replace_empty_values1(profile_result[0]['email_id']),
                    'pricing_category': replace_empty_values1(profile_result[0]['pricing_category']),
                    'country_code': replace_empty_values1(profile_result[0]['country_code']),
                    'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                    'city': replace_empty_values1(profile_result[0]['city']),
                    'state': replace_empty_values1(profile_result[0]['state']),
                    'country': replace_empty_values1(profile_result[0]['country']),
                    'profile_image': s3_pic_key,
                    'video_name': s3_video_key,
                    'resume_name': s3_resume_key,
                    'cover_letter_name' : s3_cover_letter_key,
                    'about': replace_empty_values1(profile_result[0]['about']),
                    'preferences': replace_empty_values1(profile_result[0]['preferences']),
                    'experience': {},
                    'education': {},
                    'skills': {},
                    'languages': {},
                    'additional_info': {},
                    'social_link': {},
                    'question_answers' : {},
                    'job_list' : posted_job_list, 
                    'job_id' : job_id,
                    'candidates_short_desc' : candidates_short_desc,
                    'feedback' : feedback,
                    'filter_parameters' : filter_parameters
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
                quest_dict = {"questions" : []}
                query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                values = (job_id,)
                result = execute_query(query, values)
                if len(result) > 0:
                        for r in result:
                            query = 'select custom_pre_screen_ans from pre_screen_ans where job_id = %s and pre_screen_ques_id = %s and professional_id = %s'
                            values = (job_id, r['id'],first_id[0],)
                            ans_result = execute_query(query, values)
                            if len(ans_result) > 0:
                                r.update(ans_result[0])
                            else:
                                r.update({'custom_pre_screen_ans' : ""})
                            quest_dict["questions"].append(r)
                profile_dict['question_answers'] = quest_dict['questions']
                if len(prof_job_details) > 0:
                    profile_dict.update({'applied_status' : "Applied"})
                    profile_dict.update({'invite_to_interview' : prof_job_details[0]['invite_to_interview']})
                    profile_dict.update({'application_status' : prof_job_details[0]['application_status']})
                    profile_dict.update({'custom_notes' : prof_job_details[0]['custom_notes']})
                else:
                    profile_dict.update({'applied_status' : "Applied"})
                    profile_dict.update({'invite_to_interview' : 'No'})
                    profile_dict.update({'application_status' : 'Default'})
                    profile_dict.update({'custom_notes' : ''})
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"The candidates dashboard for employer {user_data['email_id']} was displayed successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Candidates Dashboard View", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Candidates Dashboard View",event_properties, temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Candidates Dashboard View, {str(e)}")
                result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to fetch details on candidates dashboard view.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Candidates Dashboard View Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Candidates Dashboard View Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Candidates Dashboard View Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def applicants_view_mail():
    try:
        job_status = None
        professional_id = None
        employer_id = None
        total_res = {}
        data = request.json
        if not data:
            return api_json_response_format(False,str("Invalid Input."),202,{}) 
        if "job_id" not in data:
            return api_json_response_format(False,str("Job id is required."),202,{})  
        job_id = data.get("job_id")            
        filter_parameters = fetch_filter_params()
        if not job_id:
            return api_json_response_format(False,str("Job id not found."),202,{}) 
        query = '''SELECT employer_id,job_status FROM job_post WHERE id = %s'''
        values = (job_id,)
        res = execute_query(query, values) 

        if not res:
            return api_json_response_format(False,str("Job not found."),202,{})
        
        job_status = res[0]['job_status']
        employer_id = res[0]['employer_id']

        if employer_id < 500001:
            owner_emp_id = employer_id
            emp_role_id = 0
        else:
            get_owner_id = 'select user_id, role_id from sub_users where sub_user_id = %s'
            values = (employer_id,)
            owner_emp_id_dict = execute_query(get_owner_id, values)
            if owner_emp_id_dict:
                owner_emp_id = owner_emp_id_dict[0]['user_id']
                emp_role_id = owner_emp_id_dict[0]['role_id']

        get_sub_users_query = 'select sub_user_id from sub_users where user_id = %s'
        get_sub_users_values = (owner_emp_id,)
        sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
        sub_users = []
        if sub_users_dict:
            for sub_user in sub_users_dict:
                sub_users.append(sub_user['sub_user_id'])
        sub_users.append(owner_emp_id)

        if isUserExist("employer_profile", "employer_id", employer_id):
            query = '''SELECT ep.company_name, ep.designation, 
                    u.email_id, u.first_name, u.last_name,
                    jp.id, jp.job_status, jp.job_title, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed
                    FROM job_post jp
                    LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id
                    LEFT JOIN users u ON u.user_id = ep.employer_id
                    WHERE jp.job_status = %s AND jp.employer_id IN %s;'''
            values = (job_status, tuple(sub_users),)
            job_list = execute_query(query, values)
        elif isUserExist("sub_users", "sub_user_id", employer_id):
            if emp_role_id == 9:
                query = '''SELECT jp.id, jp.job_status, jp.job_title, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed
                        FROM job_post jp WHERE jp.job_status = %s AND jp.employer_id IN %s;'''
                values = (job_status,tuple(sub_users),)
            else:
                query = '''SELECT jp.id, jp.job_status, jp.job_title, jp.created_at AS posted_on, DATEDIFF(CURRENT_DATE, jp.closed_on) AS days_since_closed
                        FROM job_post jp WHERE jp.job_status = %s AND jp.employer_id = %s;'''
                values = (job_status,employer_id,)
            job_list = execute_query(query, values)
            query = "SELECT su.first_name, su.last_name, su.email_id, su.title as designation, su.user_id, su.pricing_category as emp_pricing_category, su.company_name FROM sub_users su WHERE su.sub_user_id = %s"
            values = (employer_id,)
            employer_details = execute_query(query, values)
            if employer_details:
                # emp_query = "select u.pricing_category as emp_pricing_category, ep.company_name FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id WHERE u.user_id = %s"
                # values = (employer_name[0]['user_id'],)
                # emp_details = execute_query(emp_query, values)
                # if emp_details:
                    for job in job_list:
                        job.update({'first_name' : employer_details[0]['first_name']})
                        job.update({'last_name' : employer_details[0]['last_name']})
                        job.update({'designation' : employer_details[0]['designation']})
                        job.update({'email_id' : employer_details[0]['email_id']})
                        job.update({'emp_pricing_category' : employer_details[0]['emp_pricing_category']})
                        job.update({'company_name' : employer_details[0]['company_name']})
        if not job_list:
            job_list = []

        recommended = employer_candidate_recommended(job_id)
        if not recommended:
            recommended = []
        ids = list(set(data["id"] for data in recommended))
        id_list = []
        if len(ids) > 3:
            id_list.append(ids[0])
            id_list.append(ids[1])
            id_list.append(ids[2])
            id_tuple = tuple(id_list)
        else:
            id_tuple = tuple(ids)

        query = '''SELECT professional_id, description FROM sc_recommendation WHERE job_id = %s AND user_role_id = %s;'''
        values = (job_id, 2)
        sc_recommended = execute_query(query, values) 
        
        if not sc_recommended:
            sc_recommended = []
        employer_designation = ''
        if isUserExist("sub_users", "sub_user_id", employer_id):
            # query = '''WITH FirstExperience AS (
            #             SELECT 
            #                 pe.professional_id, 
            #                 pe.job_title, 
            #                 pe.start_year, 
            #                 pe.end_year, 
            #                 pe.id,
            #                 pe.created_at,
            #                 ROW_NUMBER() OVER (PARTITION BY pe.professional_id ORDER BY pe.id) AS row_num
            #             FROM professional_experience pe
            #             )
            #             SELECT 
            #                 u.first_name, u.last_name, u.profile_image, u.city, u.country, u.pricing_category, u.state, u.user_id AS professional_id,
            #                 ex.job_title, ex.start_year, ex.end_year, ex.id AS experience_id, ex.created_at,
            #                 ja.applied_on, ja.custom_notes,
            #                 jp.feedback,
            #                 pp.expert_notes                         
            #             FROM job_post jp
            #             LEFT JOIN job_activity ja ON ja.job_id = jp.id
            #             LEFT JOIN users u ON u.user_id = ja.professional_id
            #             LEFT JOIN professional_profile pp ON pp.professional_id = u.user_id
            #             LEFT JOIN FirstExperience ex ON ex.professional_id = ja.professional_id AND ex.row_num = 1
            #             WHERE jp.id = %s
            #             ORDER BY ja.applied_on DESC;
            #             '''
            query = '''WITH FirstExperience AS (
                            SELECT 
                                pe.professional_id, 
                                pe.job_title, 
                                pe.start_year, 
                                pe.start_month, 
                                pe.end_year, 
                                pe.end_month, 
                                pe.id,
                                pe.created_at,
                                ROW_NUMBER() OVER (
                                    PARTITION BY pe.professional_id 
                                    ORDER BY 
                                        CASE 
                                            WHEN pe.end_year = 'Present' THEN 9999 
                                            ELSE CAST(SUBSTRING_INDEX(pe.end_year, '-', 1) AS UNSIGNED) 
                                        END DESC, 
                                        CAST(SUBSTRING_INDEX(pe.end_year, '-', -1) AS UNSIGNED) DESC, 
                                        CASE 
                                            WHEN pe.end_month = 'Present' THEN 12 
                                            ELSE CAST(SUBSTRING_INDEX(pe.end_month, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        CASE 
                                            WHEN pe.start_year = 'Present' THEN 9999 
                                            ELSE CAST(SUBSTRING_INDEX(pe.start_year, '-', 1) AS UNSIGNED) 
                                        END DESC, 
                                        CAST(SUBSTRING_INDEX(pe.start_year, '-', -1) AS UNSIGNED) DESC, 
                                        pe.created_at DESC
                                ) AS row_num
                            FROM professional_experience pe
                        )
                        SELECT 
                            u.first_name, u.last_name, u.profile_image, u.city, u.country, u.pricing_category, u.state, u.user_id AS professional_id,
                            ex.job_title, ex.start_year, ex.start_month, ex.end_year, ex.end_month, ex.id AS experience_id, ex.created_at,
                            ja.applied_on, ja.custom_notes,
                            jp.feedback,
                            ep.company_name, ep.designation,
                            u2.first_name AS employer_first_name, u2.last_name AS employer_last_name, u2.email_id AS employer_email_id, u2.pricing_category as emp_pricing_category
                        FROM job_post jp
                        LEFT JOIN job_activity ja ON ja.job_id = jp.id
                        LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id
                        LEFT JOIN users u2 ON u2.user_id = jp.employer_id
                        LEFT JOIN users u ON u.user_id = ja.professional_id
                        LEFT JOIN FirstExperience ex ON ex.professional_id = ja.professional_id AND ex.row_num = 1
                        WHERE jp.id = %s
                        ORDER BY ja.applied_on DESC;
                    '''
            values = (job_id,)
            candidates_short_desc = execute_query(query, values)
            query = "SELECT su.first_name AS employer_first_name, su.last_name AS employer_last_name, su.email_id as employer_email_id, su.title as designation, su.user_id, su.pricing_category as emp_pricing_category, su.company_name FROM sub_users su WHERE su.sub_user_id = %s"
            values = (employer_id,)
            employer_details = execute_query(query, values)
            if employer_details:
                # emp_query = "select u.pricing_category as emp_pricing_category, ep.company_name FROM users u JOIN employer_profile ep ON u.user_id = ep.employer_id WHERE u.user_id = %s"
                # values = (employer_name[0]['user_id'],)
                # emp_details = execute_query(emp_query, values)
                # if emp_details:
                    for obj in candidates_short_desc:
                        obj.update({'employer_first_name' : employer_details[0]['employer_first_name']})
                        obj.update({'employer_last_name' : employer_details[0]['employer_last_name']})
                        obj.update({'designation' : employer_details[0]['designation']})
                        employer_designation = {'designation' : employer_details[0]['designation']}
                        obj.update({'employer_email_id' : employer_details[0]['employer_email_id']})
                        obj.update({'emp_pricing_category' : employer_details[0]['emp_pricing_category']})
                        obj.update({'company_name' : employer_details[0]['company_name']})
        else:   #ep.company_name, ep.designation,   u2.first_name AS employer_first_name, u2.last_name AS employer_last_name, u2.email_id AS employer_email_id
            # query = '''WITH FirstExperience AS (
            #             SELECT 
            #                 pe.professional_id, 
            #                 pe.job_title, 
            #                 pe.start_year, 
            #                 pe.end_year, 
            #                 pe.id,
            #                 pe.created_at,
            #                 ROW_NUMBER() OVER (PARTITION BY pe.professional_id ORDER BY pe.id) AS row_num
            #             FROM professional_experience pe
            #             )
            #             SELECT 
            #                 u.first_name, u.last_name, u.profile_image, u.city, u.country, u.pricing_category, u.state, u.user_id AS professional_id,
            #                 ex.job_title, ex.start_year, ex.end_year, ex.id AS experience_id, ex.created_at,
            #                 ja.applied_on, ja.custom_notes,
            #                 jp.feedback,
            #                 ep.company_name, ep.designation,
            #                 u2.first_name AS employer_first_name, u2.last_name AS employer_last_name, u2.email_id AS employer_email_id, u2.pricing_category as emp_pricing_category
            #             FROM job_post jp
            #             LEFT JOIN job_activity ja ON ja.job_id = jp.id
            #             LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id
            #             LEFT JOIN users u2 ON u2.user_id = jp.employer_id
            #             LEFT JOIN users u ON u.user_id = ja.professional_id
            #             LEFT JOIN FirstExperience ex ON ex.professional_id = ja.professional_id AND ex.row_num = 1
            #             WHERE jp.id = %s
            #             ORDER BY ja.applied_on DESC;
            #             '''
            query = '''WITH FirstExperience AS (
                            SELECT 
                                pe.professional_id, 
                                pe.job_title, 
                                pe.start_year, 
                                pe.start_month, 
                                pe.end_year, 
                                pe.end_month, 
                                pe.id,
                                pe.created_at,
                                ROW_NUMBER() OVER (
                                    PARTITION BY pe.professional_id 
                                    ORDER BY 
                                        CASE 
                                            WHEN pe.end_year = 'Present' THEN 9999 
                                            ELSE CAST(SUBSTRING_INDEX(pe.end_year, '-', 1) AS UNSIGNED) 
                                        END DESC, 
                                        CAST(SUBSTRING_INDEX(pe.end_year, '-', -1) AS UNSIGNED) DESC, 
                                        CASE 
                                            WHEN pe.end_month = 'Present' THEN 12 
                                            ELSE CAST(SUBSTRING_INDEX(pe.end_month, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        CASE 
                                            WHEN pe.start_year = 'Present' THEN 9999 
                                            ELSE CAST(SUBSTRING_INDEX(pe.start_year, '-', 1) AS UNSIGNED) 
                                        END DESC, 
                                        CAST(SUBSTRING_INDEX(pe.start_year, '-', -1) AS UNSIGNED) DESC, 
                                        pe.created_at DESC
                                ) AS row_num
                            FROM professional_experience pe
                        )
                        SELECT 
                            u.first_name, u.last_name, u.profile_image, u.city, u.country, u.pricing_category, u.state, u.user_id AS professional_id,
                            ex.job_title, ex.start_year, ex.start_month, ex.end_year, ex.end_month, ex.id AS experience_id, ex.created_at,
                            ja.applied_on, ja.custom_notes,
                            jp.feedback,
                            ep.company_name, ep.designation,
                            u2.first_name AS employer_first_name, u2.last_name AS employer_last_name, u2.email_id AS employer_email_id, u2.pricing_category as emp_pricing_category
                        FROM job_post jp
                        LEFT JOIN job_activity ja ON ja.job_id = jp.id
                        LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id
                        LEFT JOIN users u2 ON u2.user_id = jp.employer_id
                        LEFT JOIN users u ON u.user_id = ja.professional_id
                        LEFT JOIN FirstExperience ex ON ex.professional_id = ja.professional_id AND ex.row_num = 1
                        WHERE jp.id = %s
                        ORDER BY ja.applied_on DESC;
                    '''
            values = (job_id,)
            candidates_short_desc = execute_query(query, values)  
        if candidates_short_desc:
            first_id = candidates_short_desc[0]['professional_id']
            employer_designation = candidates_short_desc[0]['job_title']
            for i in candidates_short_desc:
                professional_id = i["professional_id"]
                query = 'select show_to_employer, expert_notes from professional_profile where professional_id = %s'
                values = (professional_id,)
                status = execute_query(query, values)
                if len(status) > 0:
                    if status[0]['show_to_employer'] == 'Y':
                        i.update({"expert_notes" : status[0]["expert_notes"]})
                    else:
                        i.update({"expert_notes" : ""})
                else:
                    i.update({"expert_notes" : ""})

        if not professional_id:
            candidates_short_desc = []

        sc_recommended_dict = {row.get('professional_id'): row.get('description') for row in sc_recommended}

        for row in candidates_short_desc:
            if row.get('professional_id'):
                prof_id1 = row.get('professional_id')
                if prof_id1 in sc_recommended_dict and prof_id1 in id_tuple:
                    row["recommended_by"] = "recommended by second careers"
                    row['sc_recommended_notes'] = sc_recommended_dict[prof_id1]
                
                elif prof_id1 in sc_recommended_dict:
                    row["recommended_by"] = "recommended by second careers"
                    row['sc_recommended_notes'] = sc_recommended_dict[prof_id1]
                
                elif prof_id1 in id_tuple:
                    row['recommended_by'] = "AI recommended"
                    row['sc_recommended_notes'] = ""
                else:
                    row['recommended_by'] = "Default"
                    row['sc_recommended_notes'] = ""
            if row.get('profile_image'):
                row['profile_image'] = s3_picture_folder_name + str(replace_empty_values1(row['profile_image']))

        query = '''SELECT u.first_name, u.last_name, u.profile_image, u.contact_number, u.email_id, u.city, u.country,u.user_id AS professional_id,u.pricing_category,u.profile_percentage, u.country_code, u.state,
                   pp.professional_resume AS resume_name, pp.about, pp.video_url AS video_name, pp.preferences, pp.preferences, pp.upload_date AS resume_upload_date,
                   ed.id AS edu_id,ed.degree_level, ed.specialisation,ed.institute_name,ed.institute_location, ed.start_year AS education_start_year,ed.start_month AS education_start_month, ed.end_year AS education_end_year, ed.end_month AS education_end_month,
                   ex.company_name, ex.job_title, ex.job_description, ex.job_location, ex.start_month AS experience_start_month, ex.start_year AS experience_start_year, ex.end_month AS experience_end_month, ex.end_year AS experience_end_year, ex.id AS expid,
                   ai.title AS additional_info_title, ai.description AS additional_info_description, ai.id AS additional_info_id,
                   ps.skill_name, ps.skill_level, ps.id AS skill_id,
                   psl.title AS social_link_title, psl.url AS social_link_url, psl.id AS social_link_id,
                   ja.invite_to_interview, ja.application_status, ja.custom_notes, ja.professional_id AS professional_status, ja.professional_cover_letter AS cover_letter_name,
                   jp.feedback
                   FROM users u
                   LEFT JOIN professional_profile pp ON pp.professional_id = u.user_id
                   LEFT JOIN professional_experience ex ON ex.professional_id = u.user_id
                   LEFT JOIN professional_education ed ON ed.professional_id = u.user_id
                   LEFT JOIN professional_skill ps ON ps.professional_id = u.user_id
                   LEFT JOIN professional_social_link psl ON psl.professional_id = u.user_id
                   LEFT JOIN professional_additional_info ai ON ai.professional_id = u.user_id
                   LEFT JOIN job_activity ja ON ja.professional_id = u.user_id and ja.job_id = %s
                   LEFT JOIN job_post jp ON jp.id = ja.job_id
                   WHERE user_id = %s;
                   '''
        values = (job_id,first_id,)

        res = execute_query(query, values) 

        if not res:
            res=[{'professional_id': '', 'about':'', 'city': '', 'contact_number':'', 'country':'', 
            'country_code':'', 'email_id':'', 'first_name':'', 
            'last_name':'', 'preferences':'', 'pricing_category':'', 'country_code':'',
             'state':'',  'invite_to_interview':'','application_status':'',
            'custom_notes':'', 'feedback':'','profile_image':'', 'resume_name':'', 'cover_letter_name':'', 'video_name':''}]
        else:
            if res[0]["professional_status"]:
                total_res["applied_status"] = "Applied"
            else:
                total_res["applied_status"] = "Not Applied"

        keys_to_copy = [
            'professional_id', 'about', 'city', 'contact_number', 'country', 
            'email_id', 'first_name', 
            'last_name', 'preferences', 'pricing_category', 
             'state', 'invite_to_interview','application_status','country_code',
            'custom_notes', 'feedback'
        ]

        total_res.update({
            **{key: res[0][key] for key in keys_to_copy},
            'profile_image': s3_picture_folder_name + str(replace_empty_values1(res[0]['profile_image'])),
            'resume_name': s3_resume_folder_name + str(replace_empty_values1(res[0]['resume_name'])),
            'cover_letter_name': s3_cover_letter_folder_name + str(replace_empty_values1(res[0]['cover_letter_name'])),
            'video_name': s3_intro_video_folder_name + str(replace_empty_values1(res[0]['video_name'])) 
        })
        
        education = []
        experience = []
        additional_info = []
        skill = []
        social_link = []
        seen_education_ids = set()
        seen_experience_ids = set()
        seen_ai_ids = set()
        seen_skill_ids = set()
        seen_social_ids =set()

        for row in res:
            if row.get('edu_id'):
                edu_id = row.get('edu_id') 
                if edu_id not in seen_education_ids:
                    education.append({
                        **{key: row[key] for key in [
                            "degree_level", "specialisation", "institute_name", 
                            "institute_location"
                        ]},
                        "id": row['edu_id'],
                        "start_date": row['education_start_year'],
                        "end_date": row['education_end_year'],
                    })
                    seen_education_ids.add(edu_id) 

            if row.get('expid'):
                exp_id = row.get('expid') 
                if exp_id not in seen_experience_ids:
                    experience.append({
                        **{key: row[key] for key in [
                            "company_name", "job_title", "job_description", 
                            "job_location"
                        ]},
                        "id": exp_id,
                        "start_date": row['experience_start_year'],
                        "end_date": row['experience_end_year'],
                    })
                    seen_experience_ids.add(exp_id)

            if row.get('additional_info_id'):
                additional_info_id = row.get('additional_info_id') 
                if additional_info_id not in seen_ai_ids:
                    additional_info.append({
                        "title": row['additional_info_title'],
                        "description": row['additional_info_description'],
                        "id": row['additional_info_id']
                    })
                    seen_ai_ids.add(additional_info_id)

            if row.get('skill_id'):
                skill_id = row.get('skill_id') 
                if skill_id not in seen_skill_ids:
                    skill.append({
                        "skill_name": row['skill_name'],
                        "skill_level": row['skill_level'],
                        "id": row['skill_id']
                    })
                    seen_skill_ids.add(skill_id)

            if row.get('social_link_id'):
                social_link_id = row.get('social_link_id') 
                if social_link_id not in seen_social_ids:
                    social_link.append({
                        "title": row['social_link_title'],
                        "url": row['social_link_url'],
                        "id": row['social_link_id']
                    })
                    seen_social_ids.add(social_link_id)

        questions = []
        question_answers = []
        query = '''select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'''
        values = (job_id,)
        res = execute_query(query, values) 
        
        for row in res:
            questions.append(row['id'])
        

        if not questions:
            question_answers = []
        else:
            query = '''select psa.custom_pre_screen_ans, 
                       psq.custom_pre_screen_ques, psq.id
                       from pre_screen_ans psa
                       join pre_screen_ques psq ON psq.id = psa.pre_screen_ques_id
                       where psa.job_id = %s and psa.pre_screen_ques_id IN %s and psa.professional_id = %s;'''
            values = (job_id, tuple(questions), first_id,)
            question_answers = execute_query(query, values) 

        if not question_answers:
            question_answers = []
        
        # get_job_title_query = 'select job_title from job_post where id = %s'
        # values = (job_id,)
        # job_title_dict = execute_query(get_job_title_query, values)
        # if job_title_dict:
        #     job_title = job_title_dict[0]['job_title']
        total_res["question_answers"] = question_answers
        total_res["filter_parameters"] = filter_parameters
        total_res["job_id"] = job_id
        total_res["job_title"] = employer_designation
        total_res["job_list"] = job_list
        total_res["candidates_short_desc"] = candidates_short_desc
        total_res["education"] = education
        total_res["experience"] = experience
        total_res["additional_info"] = additional_info
        total_res["skills"] = skill
        total_res["social_link"] = social_link
        total_res.update({"job_status" : job_status})

        return api_json_response_format(True,str("Candidates detail fetched successfully."),200,total_res)

    except Exception as e:
        error = f'Error occured while retrieving data: {e}'
        return api_json_response_format(False,error,500,{})

def update_application_status():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'professional_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(professional_id)",204,{})  
            return result_json
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(job_id)",204,{})  
            return result_json
        if 'status' not in req_data or req_data['status'] == "":
            result_json = api_json_response_format(False,"Please fill in all the fields(status)",204,{})  
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                professional_id = req_data['professional_id']
                job_id = req_data['job_id']
                status = req_data['status']

                query = "select job_title from job_post where id = %s"
                values = (job_id,)
                job_title = execute_query(query, values)

                query = "select application_status from job_activity where job_id = %s and professional_id = %s"
                values = (job_id, professional_id,)
                application_status_dict = execute_query(query, values)
                if len(application_status_dict) > 0:
                    application_status = application_status_dict[0]['application_status']
  
                if status == 'shortlisted':
                    if application_status == 'Rejected':
                        query = "update job_activity set feedback = %s where job_id = %s and professional_id = %s"
                        values = ("", job_id, professional_id,)
                        update_query(query, values)
                    query = "update job_activity set application_status = %s WHERE professional_id = %s and job_id = %s"
                    values = ('Shortlisted', professional_id, job_id,)
                    update_status = update_query(query, values)
                    notification_msg = f"You profile has been shortlisted for the job {job_title[0]['job_title']}"
                    created_at = datetime.now()                    
                    query = "insert into user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                    values = (professional_id, notification_msg, created_at,)
                    update_notification = update_query(query, values)
                    background_runner.send_email_for_shortlisted_candidates(professional_id, job_id, user_data['user_id'])
                if status == 'rejected':
                    if req_data['feedback'] != '' or req_data['feedback'] is not None:
                        feedback = req_data['feedback']
                        query=  "update job_activity set feedback = %s where job_id = %s and professional_id = %s"
                        values = (feedback, job_id, professional_id,)
                        update_feedback = update_query(query, values)
                    query = "update job_activity set application_status = %s WHERE professional_id =  %s and job_id = %s"
                    values = ('Rejected', professional_id, job_id,)
                    update_status = update_query(query, values)
                if status == 'not reviewed':
                    if application_status == 'Rejected':
                        query = "update job_activity set feedback = %s where job_id = %s and professional_id = %s"
                        values = ("", job_id, professional_id,)
                        update_query(query, values)
                    query = "update job_activity set application_status = %s WHERE professional_id = %s and job_id = %s"
                    values = ('Not Reviewed', professional_id, job_id,)
                    update_status = update_query(query, values)

                if update_status > 0:
                    result_json = api_json_response_format(True,"Candidate status has been updated successfully!",0,{})
                else:
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})    
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def update_custom_notes():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'professional_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(professional_id)",204,{})  
            return result_json
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(job_id)",204,{})  
            return result_json
        if 'custom_notes' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(custom_notes)",204,{})  
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":  
                professional_id = req_data['professional_id']
                job_id = req_data['job_id']
                custom_notes = req_data['custom_notes']
                query = "update job_activity set custom_notes = %s WHERE professional_id =  %s and job_id = %s"
                values = (custom_notes, professional_id, job_id,)
                update_status = update_query(query, values)
                if update_status > 0:
                    result_json = api_json_response_format(True,"Candidate status has been updated successfully!",0,{})
                else:
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})    
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def filter_by_application_status():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'status' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(status)",204,{})  
            return result_json
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the fields(job_id)",204,{})  
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])  
            flag = 0
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data["user_id"]
                flag = 1
            else:
                employer_id = user_data["user_id"]
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "admin":  
                # employer_id = user_data["user_id"]
                job_id = req_data['job_id']
                if user_data['user_role'] == 'admin':
                    query = 'select employer_id from job_post where id = %s'
                    values = (job_id,)
                    employer_id_dict = execute_query(query, values)
                    if employer_id_dict:
                        employer_id = employer_id_dict[0]['employer_id']
                filter_status = req_data['status']
                candidates_short_desc = []
                processed_professional_ids = set()
                if filter_status == "Applied":
                    query = "select professional_id, invite_to_interview, application_status, custom_notes from job_activity WHERE job_id = %s order by id asc"
                    values = ( job_id,)
                    professional_id_list = execute_query(query, values)
                    if len(professional_id_list) > 0:
                        professional_id = professional_id_list[0]['professional_id']
                        for data in professional_id_list:
                            professional_id_1 = data['professional_id']
                            if professional_id_1 not in processed_professional_ids:
                                query = "SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, p.professional_id, pe.id AS experience_id, pe.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year IS NULL THEN 0 ELSE pe.end_year END DESC, CASE WHEN pe.end_month IS NULL THEN 0 ELSE pe.end_month END DESC"
                                values = (professional_id_1,)
                                exp_data = execute_query(query, values)
                                query = "SELECT custom_notes,applied_on,professional_resume,professional_cover_letter FROM job_activity WHERE job_id = %s and professional_id = %s"
                                values = (job_id,professional_id_1,)
                                applied_on = execute_query(query, values)
                                if len(applied_on) >0:
                                    # date_object = datetime.strptime(str(applied_on[0]["applied_on"]), "%a, %d %b %Y %H:%M:%S %Z")
                                    # formatted_date = date_object.strftime("%d/%m/%Y")
                                    exp_data[0].update({"applied_on" : str(applied_on[0]["applied_on"])})
                                    exp_data[0].update({"custom_notes" : str(applied_on[0]["custom_notes"])})
                                    s3_resume_key = s3_resume_folder_name + str(replace_empty_values1(applied_on[0]['professional_resume']))
                                    s3_cover_letter_key = s3_cover_letter_folder_name + str(replace_empty_values1(applied_on[0]['professional_cover_letter']))
                                else:
                                    exp_data[0].update({"applied_on" : ""})
                                    exp_data[0].update({"custom_notes" : ""})
                                    s3_resume_key = s3_resume_folder_name
                                    s3_cover_letter_key = s3_cover_letter_folder_name 
                                s3_pic_key1 = s3_picture_folder_name + str(replace_empty_values1(exp_data[0]['profile_image']))
                                exp_data[0].update({"profile_image" : s3_pic_key1})
                                exp_data[0].update({"recommended_by" : "Default"})
                                if employer_id < 500000:
                                    query = "SELECT company_name,designation from employer_profile where employer_id = %s"
                                    values = (employer_id,)
                                    company_name = execute_query(query, values)
                                else:
                                    query = "SELECT company_name,title as designation from sub_users where sub_user_id = %s"
                                    values = (employer_id,)
                                    company_name = execute_query(query, values)
                                if company_name:
                                    exp_data[0].update({"company_name" : company_name[0]["company_name"]})
                                    exp_data[0].update({"employer_designation" : company_name[0]["designation"]})
                                else:
                                    exp_data[0].update({"company_name" : ""})
                                    exp_data[0].update({"employer_designation" : ""})
                                if flag == 1:
                                    query = "SELECT first_name, last_name, email_id, pricing_category from sub_users where sub_user_id = %s"
                                    values = (user_data['sub_user_id'],)
                                else:
                                    if employer_id < 500000:
                                        query = "select first_name,last_name, email_id, pricing_category from users where user_id = %s"
                                        values = (employer_id,)
                                    else:
                                        query = "select first_name,last_name, email_id, pricing_category from sub_users where sub_user_id = %s"
                                        values = (employer_id,)
                                employer_name = execute_query(query, values)
                                if employer_name:
                                    exp_data[0].update({"employer_first_name" : employer_name[0]["first_name"]})
                                    exp_data[0].update({"employer_last_name" : employer_name[0]["last_name"]})
                                    exp_data[0].update({"employer_email_id" : employer_name[0]["email_id"]})
                                    exp_data[0].update({"pricing_category" : employer_name[0]["pricing_category"]})
                                exp_data[0].update({"sc_recommended_notes" : ""})
                                query = 'select show_to_employer, expert_notes from professional_profile where professional_id = %s'
                                values = (professional_id_1,)
                                status = execute_query(query, values)
                                if len(status) > 0:
                                    if status[0]['show_to_employer'] == 'Y':
                                        exp_data[0].update({"expert_notes" : status[0]["expert_notes"]})
                                    else:
                                        exp_data[0].update({"expert_notes" : ""})
                                else:
                                    exp_data[0].update({"expert_notes" : ""})
                                candidates_short_desc.append(exp_data[0])
                                processed_professional_ids.add(professional_id_1)
                                
                    else:
                        try:
                            temp_dict = {'Message': "Jobs filtered by application status and displayed successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Job Filter By Application Status", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Job Filter By Application Status",event_properties,temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Job Filter By Application Status, {str(e)}")
                        result_json = api_json_response_format(True,"No records found",0,{})
                        return result_json
                
                elif filter_status == "Recommended":
                    second_career_recommendations = []
                    ai_recommendations = []
                    recomeded = employer_candidate_recommended(job_id)
                    ids = list(set(data["id"] for data in recomeded))
                    id_list = []
                    if len(ids) > 3:
                        id_list.append(ids[0])
                        id_list.append(ids[1])
                        id_list.append(ids[2])
                        id_tuple = tuple(id_list)
                    else:
                        id_tuple = tuple(ids)
                    processed_professional_ids = set()
                    query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                    values = (job_id,2,)
                    sc_recommended_professional_id = execute_query(query, values)
                    professional_ids_tuple = tuple(d['professional_id'] for d in sc_recommended_professional_id)
                    combined_id_tuple = id_tuple + professional_ids_tuple
                    unique_combined_tuple = tuple(set(combined_id_tuple))
                    professional_ids = [item['professional_id'] for item in sc_recommended_professional_id]
                    if len(unique_combined_tuple) > 0:  
                        query = "select professional_id, invite_to_interview, application_status, custom_notes from job_activity WHERE job_id = %s order by id asc"
                        values = ( job_id,)
                        professional_id_list = execute_query(query, values)
                        professional_id_list_values = []
                        if len(professional_id_list) > 0:
                            professional_id = professional_id_list[0]['professional_id']
                            professional_id_list_values = [entry["professional_id"] for entry in professional_id_list]
                        for data in unique_combined_tuple:
                            professional_id_1 = data
                            if professional_id_1 in professional_id_list_values and professional_id_1 not in processed_professional_ids:
                                query = "SELECT u.first_name, u.last_name, u.country, u.state, u.city, p.professional_id, pe.id AS experience_id, pe.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year IS NULL THEN 0 ELSE pe.end_year END DESC, CASE WHEN pe.end_month IS NULL THEN 0 ELSE pe.end_month END DESC"
                                values = (professional_id_1,)
                                exp_data = execute_query(query, values)
                                query = "SELECT custom_notes,applied_on,professional_resume,professional_cover_letter FROM job_activity WHERE job_id = %s and professional_id = %s"
                                values = (job_id,professional_id_1,)
                                applied_on = execute_query(query, values)
                                if len(applied_on) >0:
                                    # date_object = datetime.strptime(str(applied_on[0]["applied_on"]), "%a, %d %b %Y %H:%M:%S %Z")
                                    # formatted_date = date_object.strftime("%d/%m/%Y")
                                    exp_data[0].update({"applied_on" : str(applied_on[0]["applied_on"])})
                                    exp_data[0].update({"custom_notes" : str(applied_on[0]["custom_notes"])})
                                    s3_resume_key = s3_resume_folder_name + str(replace_empty_values1(applied_on[0]['professional_resume']))
                                    s3_cover_letter_key = s3_cover_letter_folder_name + str(replace_empty_values1(applied_on[0]['professional_cover_letter']))
                                else:
                                    exp_data[0].update({"applied_on" : ""})
                                    exp_data[0].update({"custom_notes" : ""})
                                    s3_resume_key = s3_resume_folder_name
                                    s3_cover_letter_key = s3_cover_letter_folder_name 
                                s3_pic_key1 = s3_picture_folder_name + str(replace_empty_values1(exp_data[0]['profile_image']))
                                exp_data[0].update({"profile_image" : s3_pic_key1})
                                if data in professional_ids:
                                    query = 'select description from sc_recommendation where professional_id = %s and job_id = %s'
                                    values = (professional_id_1, job_id,)
                                    sc_recommended_notes= execute_query(query, values)
                                    if len(sc_recommended_notes) > 0:
                                        exp_data[0].update({"sc_recommended_notes" : sc_recommended_notes[0]['description']})
                                    else:
                                        exp_data[0].update({"sc_recommended_notes" : ""})
                                    exp_data[0].update({"recommended_by" : "2nd careers recommended"})
                                    recommended_by = "2nd careers recommended"
                                else:
                                    recommended_by = "AI recommended"
                                    exp_data[0].update({"sc_recommended_notes" : ""})
                                    exp_data[0].update({"recommended_by" : "AI recommended"})
                                if employer_id < 500000:
                                    query = "SELECT company_name,designation from employer_profile where employer_id = %s"
                                    values = (employer_id,)
                                    company_name = execute_query(query, values)
                                else:
                                    query = "SELECT company_name,title as designation from sub_users where sub_user_id = %s"
                                    values = (employer_id,)
                                    company_name = execute_query(query, values)
                                if company_name:
                                    exp_data[0].update({"company_name" : company_name[0]["company_name"]})
                                    exp_data[0].update({"employer_designation" : company_name[0]["designation"]})
                                else:
                                    exp_data[0].update({"company_name" : ""})
                                    exp_data[0].update({"employer_designation" : ""})
                                if flag == 1:
                                    query = "SELECT first_name, last_name, email_id, pricing_category from sub_users where sub_user_id = %s"
                                    values = (user_data['sub_user_id'],)
                                else:
                                    if employer_id < 500000:
                                        query = "select first_name,last_name, email_id, pricing_category from users where user_id = %s"
                                        values = (employer_id,)
                                    else:
                                        query = "select first_name,last_name, email_id, pricing_category from sub_users where sub_user_id = %s"
                                        values = (employer_id,)
                                employer_name = execute_query(query, values)
                                if employer_name:
                                    exp_data[0].update({"employer_first_name" : employer_name[0]["first_name"]})
                                    exp_data[0].update({"employer_last_name" : employer_name[0]["last_name"]})
                                    exp_data[0].update({"employer_email_id" : employer_name[0]["email_id"]})
                                query = 'select show_to_employer, expert_notes from professional_profile where professional_id = %s'
                                values = (professional_id_1,)
                                status = execute_query(query, values)
                                if len(status) > 0:
                                    if status[0]['show_to_employer'] == 'Y':
                                        exp_data[0].update({"expert_notes" : status[0]["expert_notes"]})
                                    else:
                                        exp_data[0].update({"expert_notes" : ""})
                                else:
                                    exp_data[0].update({"expert_notes" : ""})
                                if recommended_by == "AI recommended":
                                    ai_recommendations.append(exp_data[0])
                                else:
                                    second_career_recommendations.append(exp_data[0])
                                processed_professional_ids.add(professional_id_1)

                else :
                    query = "select professional_id, invite_to_interview, application_status, custom_notes from job_activity WHERE application_status = %s and job_id = %s order by id asc"
                    values = (filter_status, job_id,)
                    professional_id_list = execute_query(query, values)
                    if len(professional_id_list) > 0:
                        professional_id = professional_id_list[0]['professional_id']
                        for data in professional_id_list:
                            professional_id_1 = data['professional_id']
                            if professional_id_1 not in processed_professional_ids:
                                query = "SELECT u.first_name, u.last_name, u.country, u.state, u.city, p.professional_id, pe.id AS experience_id, pe.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year IS NULL THEN 0 ELSE pe.end_year END DESC, CASE WHEN pe.end_month IS NULL THEN 0 ELSE pe.end_month END DESC"
                                values = (professional_id_1,)
                                exp_data = execute_query(query, values)
                                query = "SELECT custom_notes,applied_on,professional_resume,professional_cover_letter FROM job_activity WHERE job_id = %s and professional_id = %s"
                                values = (job_id,professional_id_1,)
                                applied_on = execute_query(query, values)
                                if len(applied_on) >0:
                                    # date_object = datetime.strptime(str(applied_on[0]["applied_on"]), "%a, %d %b %Y %H:%M:%S %Z")
                                    # formatted_date = date_object.strftime("%d/%m/%Y")
                                    exp_data[0].update({"applied_on" : str(applied_on[0]["applied_on"])})
                                    exp_data[0].update({"custom_notes" : str(applied_on[0]["custom_notes"])})
                                    s3_resume_key = s3_resume_folder_name + str(replace_empty_values1(applied_on[0]['professional_resume']))
                                    s3_cover_letter_key = s3_cover_letter_folder_name + str(replace_empty_values1(applied_on[0]['professional_cover_letter']))
                                else:
                                    exp_data[0].update({"applied_on" : ""})
                                    exp_data[0].update({"custom_notes" : ""})
                                    s3_resume_key = s3_resume_folder_name
                                    s3_cover_letter_key = s3_cover_letter_folder_name 
                                s3_pic_key1 = s3_picture_folder_name + str(replace_empty_values1(exp_data[0]['profile_image']))
                                exp_data[0].update({"profile_image" : s3_pic_key1})
                                exp_data[0].update({"recommended_by" : "Default"})
                                if employer_id < 500000:
                                    query = "SELECT company_name,designation from employer_profile where employer_id = %s"
                                    values = (employer_id,)
                                    company_name = execute_query(query, values)
                                else:
                                    query = "SELECT company_name,title as designation from sub_users where sub_user_id = %s"
                                    values = (employer_id,)
                                    company_name = execute_query(query, values)
                                if company_name:
                                    exp_data[0].update({"company_name" : company_name[0]["company_name"]})
                                    exp_data[0].update({"employer_designation" : company_name[0]["designation"]})
                                else:
                                    exp_data[0].update({"company_name" : ""})
                                    exp_data[0].update({"employer_designation" : ""})
                                if flag == 1:
                                    query = "SELECT first_name, last_name, email_id, pricing_category from sub_users where sub_user_id = %s"
                                    values = (user_data['sub_user_id'],)
                                else:
                                    if employer_id < 500000:
                                        query = "select first_name,last_name, email_id, pricing_category from users where user_id = %s"
                                        values = (employer_id,)
                                    else:
                                        query = "select first_name,last_name, email_id, pricing_category from sub_users where sub_user_id = %s"
                                        values = (employer_id,)
                                employer_name = execute_query(query, values)
                                if employer_name:
                                    exp_data[0].update({"employer_first_name" : employer_name[0]["first_name"]})
                                    exp_data[0].update({"employer_last_name" : employer_name[0]["last_name"]})
                                    exp_data[0].update({"employer_email_id" : employer_name[0]["email_id"]})
                                exp_data[0].update({"sc_recommended_notes" : ""})
                                query = 'select show_to_employer, expert_notes from professional_profile where professional_id = %s'
                                values = (professional_id_1,)
                                status = execute_query(query, values)
                                if len(status) > 0:
                                    if status[0]['show_to_employer'] == 'Y':
                                        exp_data[0].update({"expert_notes" : status[0]["expert_notes"]})
                                    else:
                                        exp_data[0].update({"expert_notes" : ""})
                                else:
                                    exp_data[0].update({"expert_notes" : ""})
                                candidates_short_desc.append(exp_data[0])
                                processed_professional_ids.add(professional_id_1)
                    else:
                        try:
                            temp_dict = {'Message': "Jobs filtered by application status and displayed successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Job Filter By Application Status", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Job Filter By Application Status",event_properties,temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Job Filter By Application Status, {str(e)}")
                        result_json = api_json_response_format(True,"No records found",0,{})
                        return result_json
                    
                list_map = map(int, processed_professional_ids)
                list_sorted = sorted(list_map)
                if not list(processed_professional_ids) == []:
                    final_list = []
                    if filter_status == "Recommended":
                        candidates_short_desc = second_career_recommendations + ai_recommendations
                        for id in list_sorted:
                            if id in professional_ids_tuple:
                                final_list.append(id)
                                first_id = list(final_list)
                                break
                            elif id in id_tuple and len(professional_ids_tuple) == 0:
                                final_list.append(id)
                                first_id = list(final_list)
                                break
                            else:
                                first_id = list(list_sorted)
                    else:
                        first_id = list(list_sorted)
                    query = "SELECT u.first_name, u.last_name, u.email_id, u.country_code, u.contact_number, u.country, u.state, u.city, p.professional_id,p.about,p.preferences, p.video_url, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year IS NULL THEN 0 ELSE pe.end_year END DESC"                
                    values = (first_id[0],)
                    profile_result = execute_query(query, values)
                                
                    profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                    intro_video_name = replace_empty_values1(profile_result[0]['video_url'])           
                    s3_pic_key = s3_picture_folder_name + str(profile_image_name)
                    if not profile_result[0]['video_url'] == "":
                        if not profile_result[0]['video_url'] == None:
                            s3_video_key = s3_intro_video_folder_name + str(replace_empty_values1(profile_result[0]['video_url'])) 
                        else:
                            s3_video_key = ""              
                    else:
                        s3_video_key = ""               
                    profile_dict = {
                        'first_name': replace_empty_values1(profile_result[0]['first_name']),
                        'last_name': replace_empty_values1(profile_result[0]['last_name']),
                        'professional_id' : profile_result[0]['professional_id'],                                        
                        'email_id': replace_empty_values1(profile_result[0]['email_id']),
                        'country_code': replace_empty_values1(profile_result[0]['country_code']),
                        'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                        'city': replace_empty_values1(profile_result[0]['city']),
                        'state': replace_empty_values1(profile_result[0]['state']),
                        'country': replace_empty_values1(profile_result[0]['country']),
                        'profile_image': s3_pic_key,
                        'video_name': s3_video_key,
                        'resume_name': s3_resume_key,
                        'cover_letter_name' : s3_cover_letter_key,
                        'about': replace_empty_values1(profile_result[0]['about']),
                        'preferences': replace_empty_values1(profile_result[0]['preferences']),
                        'experience': {},
                        'education': {},
                        'skills': {},
                        'languages': {},
                        'additional_info': {},
                        'social_link': {},
                        'question_answers' : {},
                        'invite_to_interview' : professional_id_list[0]['invite_to_interview'],
                        'application_status' : professional_id_list[0]['application_status'],
                        'custom_notes' : professional_id_list[0]['custom_notes'],
                        'job_id' : job_id,
                        'candidates_short_desc' : candidates_short_desc
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
                    quest_dict = {"questions" : []}
                    query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                    values = (job_id,)
                    result = execute_query(query, values)
                    if len(result) > 0:
                        for r in result:
                            query = 'select custom_pre_screen_ans from pre_screen_ans where job_id = %s and pre_screen_ques_id = %s and professional_id = %s'
                            values = (job_id, r['id'],professional_id,)
                            ans_result = execute_query(query, values)
                            if len(ans_result) > 0:
                                r.update(ans_result[0])
                            else:
                                r.update({"custom_pre_screen_ans" : ""})
                            quest_dict["questions"].append(r)
                    # job.update(quest_dict)
                    profile_dict['question_answers'] = quest_dict['questions']
                    if len(professional_id_list) > 0:
                        profile_dict.update({'applied_status' : "Applied"})
                        profile_dict.update({'invite_to_interview' : professional_id_list[0]['invite_to_interview']})
                        profile_dict.update({'application_status' : professional_id_list[0]['application_status']})
                        profile_dict.update({'custom_notes' : professional_id_list[0]['custom_notes']})
                    else:
                        profile_dict.update({'applied_status' : "Applied"})
                        profile_dict.update({'invite_to_interview' : 'No'})
                        profile_dict.update({'application_status' : 'Default'})
                        profile_dict.update({'custom_notes' : ''})
                    try:
                        temp_dict = {'Message': "Jobs filtered by application status and displayed successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Job Filter By Application Status", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Job Filter By Application Status",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Job Filter By Application Status, {str(e)}")
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
                else:
                   result_json = api_json_response_format(True, "No records found", 0, {})  
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})     

    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in filtering jobs based on application status."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Job Filter By Application Status Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Job Filter By Application Status Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Job Filter By Application Status Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.strftime('%d-%m-%Y')
        return json.JSONEncoder.default(self, obj)

def replace_empty_values(data):
    return [{k: '' if v in ('N/A', None) else v for k, v in item.items()} for item in data]

def format_profile(profile_data):
    profile_keys = ['id', 'job_title', 'job_type', 'job_overview', 'job_desc', 'responsibilities',
                    'additional_info', 'skills', 'country', 'state', 'city', 'work_schedule',
                    'workplace_type', 'is_paid', 'time_commitment', 'duration', 'calendly_link',
                    'currency', 'benefits', 'required_resume', 'required_cover_letter',
                    'required_background_check', 'required_subcontract', 'is_application_deadline',
                    'application_deadline_date', 'is_active', 'share_url', 'created_at', 'profile_image']

    return {key: profile_data.get(key, '') for key in profile_keys}

def get_job_detail(job_id):
    try:
        query_job_details = """
        SELECT 
            jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
            jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, 
            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration,
            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
            jp.required_background_check, jp.required_subcontract, jp.is_application_deadline,
            jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, u.profile_image 
        FROM 
            job_post jp 
        LEFT JOIN 
            users u ON jp.employer_id = u.user_id 
        LEFT JOIN 
            sub_users su ON jp.employer_id = su.sub_user_id
        WHERE jp.job_status = %s and jp.id = %s
        """

        values_job_details = ('opened', job_id,)
        job_details = execute_query(query_job_details, values_job_details)
        cleaned_job_details = replace_empty_values(job_details)
        
        profiles = [format_profile(profile) for profile in cleaned_job_details]

        # with open('job_data.json', "w") as outfile:
        #     json.dump(profiles, outfile, indent=4, cls=CustomEncoder)  # Apply the custom encoder
        
        return profiles
    except Exception as error:
        print("Error:", error)
        return (False, str(error), 500, {})

def employer_candidate_recommended(job_id):
    try:
        data = get_job_detail(job_id)
        # f = open('job_data.json')
        # data = json.load(f)

        out = process_quries_search(OPENAI_API_KEY,data)
        client = meilisearch.Client(
            url=os.environ.get("MEILI_HTTP_ADDR")
        )
        embeddings = OpenAIEmbeddings(deployment="text-embedding-ada-002")
        embedders = {
            "adra": {
                "source": "userProvided",
                "dimensions": 1536,
            }
        }
        embedder_name = "adra"
        index_name = PROFILE_INDEX
        vector_store = Meilisearch(client=client, embedding=embeddings,embedders=embedders,
            index_name=index_name)
        
        if out["is_error"]:    
            print(f"recommendation prompt query error : {out['result']}")        
            return
        else:
            query = out["result"]

        # Make similarity search
        #query = out

        results = vector_store.similarity_search_with_score(
            query=query,
            embedder_name = "adra",
        )
        professional_details = []
        for doc, _ in results:
            page_content = doc.page_content
            professional_details.append(json.loads(page_content))

        
        return professional_details

        # embedding_vector = embeddings.embed_query(query)
        # docs_and_scores = vector_store.similarity_search_by_vector( embedding_vector, embedder_name=embedder_name
        # )
        # print(docs_and_scores[0])
    except Exception as error:
        print(error)        
        return (False,str(error),500,{})          
    
def process_quries_search(openai_api_key,l_query_txt):
    global g_resume_path
    global g_openai_token_limit      
    result = {}

    try:
        if s3_exists(BUCKET_NAME,"professional_recommend_prompt.json"):     
            s3_resource = s3_obj.get_s3_resource()
            obj = s3_resource.Bucket(BUCKET_NAME).Object("professional_recommend_prompt.json")
            # print(obj)
            json_file_content = obj.get()['Body'].read().decode('utf-8')        
            prompt_json = json.loads(json_file_content)
            level_1 = prompt_json["level_1"]
            level_1_prompt = level_1["prompt"]      
            level_1_prompt = level_1_prompt.replace("{{data}}", "{{"+str(l_query_txt)+"}}")  
            openai_level_1_res = get_openai_summary(openai_api_key,level_1_prompt) 
            if not "error_code" in openai_level_1_res:
                chatbot_level_1_text = openai_level_1_res["summary"] 
                del openai_level_1_res
                result["is_error"] = False
                result["result"] = chatbot_level_1_text
            else:
                result["is_error"] = True
                result["result"] = str(openai_level_1_res["message"] )
    except Exception as error:
        print(f"process_quries_search in employer profile recommend error : {error}")
        result["is_error"] = True
        result["result"] = str(error)   
    finally:        
        return result
    
def get_openai_summary(l_openai_api_key,req_prompt): 
    result = {}    
    global openai_api_key
            
    openai_api_key = l_openai_api_key
    OpenAI.api_key = openai_api_key    

    try:                 
        req_messages = [{"role": "user", "content": req_prompt}]
        response = process_openai_completion(req_messages,OpenAI.api_key)
        # print(f"process_openai_completion response {response}")
        result["data_id"] = str(response.id)            
        result["summary"] = str(response.choices[0].message.content)
    except Exception as error:       
        print("Error in get_openai_summary(): "+str(error))
        result = api_json_response_format(False,str(error),500,{}) 
    finally:        
        return result

def process_openai_completion(req_messages,openai_api_key):
    try:
        global g_summary_model_name
        print(f"g_summary_model_name : {g_summary_model_name}, g_openai_completion_token_limit : {g_openai_completion_token_limit}, req_messages: {req_messages}")

        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        return client.chat.completions.create(
                                    model=g_summary_model_name,
                                    messages=req_messages,
                                    temperature=0,
                                    max_tokens=g_openai_completion_token_limit,
                                    top_p=1,
                                    frequency_penalty=0,
                                    presence_penalty=0
                                )
    except Exception as error:       
        print("Error in process_openai_completion(): "+str(error)) 

def s3_exists(s3_bucket, s3_key):
    try:
        s3_cient = s3_obj.get_s3_client()
        s3_cient.head_object(Bucket=s3_bucket,Key=s3_key)
        return True
    except Exception as e:
        print("s3_exists error : "+str(e))
        return False

def filter_professionals():
    try:
        result_json = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "admin":
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Job_id required",204,{})
                    return result_json
                job_id = req_data['job_id']
                location = req_data["location"]
                skill_value = req_data["skills"]
                # plan = req_data['plan']
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                skill_list = ["Business Strategy", "Change Management", "Conflict Resolution", "Financial Management", "Human Resource Management", "Operations Management", "Organizational Development", "Strategic Planning",  "Supply Chain Management", "Talent Management", "Direct Sales", "Leadership Skills", "Market Research", "Negotiation", "Presentation", "Product Knowledge", "Recruiting", "Sales and Budget Forecasting",    "Sales Strategy and Planning", "Upselling", "Agile Methodologies", "Budgeting", "Contract Management Skills", "Earned Value Management", "Process Improvement", "Risk Assessment", "Analytics", "Data Analysis",  "Metrics and KPIs", "Project Management", "Revenue Expansion", "SaaS Knowledge", "Salesforce", "Team Leadership", "Marketing Skills", "Keyword Research", "Algorithm Design", "Application Programming Interfaces (APIs)", "Database Design", "Debugging", "Mobile Application Development", "Quality Assurance (QA)"]
                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])

                query_job_details = "SELECT DISTINCT ja.professional_id, ja.invite_to_interview, ja.application_status, ja.feedback, ja.custom_notes, u.city, u.country FROM job_activity ja INNER JOIN professional_skill ps ON ja.professional_id = ps.professional_id INNER JOIN users u ON ja.professional_id = u.user_id WHERE ja.job_id = %s"        
                conditions = []
                values_job_details = [job_id]

                if req_data['skills']:
                    if "Others" in req_data['skills']:
                        conditions.append("ps.skill_name NOT IN %s")
                        values_job_details.append(tuple(skill_list),)
                    else:
                        conditions.append("ps.skill_name IN %s")
                        values_job_details.append(tuple(skill_value),)
                # if req_data['plan']:
                #     conditions.append("u.pricing_category IN %s")
                #     values_job_details.append(tuple(plan,))
                # if country:
                #     if "Others" in country:
                #         conditions.append("u.country NOT IN %s")
                #         values_job_details.append(tuple(country_list),)
                #     else:
                #         conditions.append("u.country IN %s")
                #         values_job_details.append(tuple(country),)
                if country:
                    conditions.append("u.country IN %s")
                    values_job_details.append(tuple(country),)
                
                if city:
                    conditions.append("u.city IN %s")
                    values_job_details.append(tuple(city),)

                if conditions:
                    if len(conditions) == 1:
                        query_job_details += " AND " + conditions[0]
                    else:
                        query_job_details += " AND (" + " AND ".join(conditions) + ")"
                
                prof_details = replace_empty_values(execute_query(query_job_details, values_job_details))
                prof_id_list = []
                candidates_short_desc = []
                s3_picture_folder_name  = "professional/profile-pic/"
                s3_intro_video_folder_name = "professional/profile-video/"
                if len(prof_details) > 0:
                    for professional in prof_details:
                        prof_id_list.append(professional['professional_id'])
                else:
                    try:
                        temp_dict = {'Message': "No profile found"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Employer Filter_Professionals", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Employer Filter_Professionals",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Employer Filter_Professionals, {str(e)}")
                    result_json = api_json_response_format(True, "No profile matches",0,{})
                    return result_json
                first_id = prof_id_list[0]
                for id in prof_id_list:
                    query = "SELECT u.first_name, u.last_name, u.country, u.state, u.pricing_category, u.city, p.professional_id, pe.id AS experience_id, pe.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year IS NULL THEN 0 ELSE pe.end_year END DESC, CASE WHEN pe.end_month IS NULL THEN 0 ELSE pe.end_month END DESC"
                    values = (id,)
                    exp_data = execute_query(query, values)
                    query = "SELECT custom_notes,applied_on,professional_resume,professional_cover_letter,feedback FROM job_activity WHERE job_id = %s and professional_id = %s"
                    values = (job_id,id,)
                    applied_on = execute_query(query, values)
                    if len(applied_on) > 0:
                        # date_object = datetime.strptime(str(applied_on[0]["applied_on"]), "%a, %d %b %Y %H:%M:%S %Z")
                        # formatted_date = date_object.strftime("%d/%m/%Y")
                        exp_data[0].update({"feedback" : str(applied_on[0]["feedback"])})
                        exp_data[0].update({"applied_on" : str(applied_on[0]["applied_on"])})
                        exp_data[0].update({"custom_notes" : str(applied_on[0]["custom_notes"])})
                        s3_resume_key = s3_resume_folder_name + str(replace_empty_values1(applied_on[0]['professional_resume']))
                        s3_cover_letter_key = s3_cover_letter_folder_name + str(replace_empty_values1(applied_on[0]['professional_cover_letter']))
                    else:
                        exp_data[0].update({"feedback" : str(applied_on[0]["feedback"])})
                        exp_data[0].update({"applied_on" : ""})
                        exp_data[0].update({"custom_notes" : ""})
                        s3_resume_key = s3_resume_folder_name
                        s3_cover_letter_key = s3_cover_letter_folder_name 
                    s3_pic_key1 = s3_picture_folder_name + str(replace_empty_values1(exp_data[0]['profile_image']))
                    exp_data[0].update({"profile_image" : s3_pic_key1})
                    exp_data[0].update({"recommended_by" : "Default"})
                    candidates_short_desc.append(exp_data[0])
                query = "SELECT u.first_name, u.last_name, u.email_id, u.country_code, u.contact_number, u.country, u.state, u.city, u.pricing_category, p.professional_id,p.about,p.preferences, p.video_url, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year IS NULL THEN 0 ELSE pe.end_year END DESC"                
                values = (first_id,)
                profile_result = execute_query(query, values) 
                query = "SELECT professional_id, invite_to_interview, application_status, custom_notes, feedback FROM job_activity WHERE job_id = %s and professional_id = %s"
                values = (job_id, first_id,)
                prof_job_details = execute_query(query, values)                           
                s3_pic_key = s3_picture_folder_name + str(replace_empty_values1(profile_result[0]['profile_image']))
                if not profile_result[0]['video_url'] == "":
                    if not profile_result[0]['video_url'] == None:
                        s3_video_key = s3_intro_video_folder_name + str(replace_empty_values1(profile_result[0]['video_url'])) 
                    else:
                        s3_video_key = ""              
                else:
                    s3_video_key = ""              
                profile_dict = {
                    'first_name': replace_empty_values1(profile_result[0]['first_name']),
                    'last_name': replace_empty_values1(profile_result[0]['last_name']),
                    'professional_id' : profile_result[0]['professional_id'],                                        
                    'email_id': replace_empty_values1(profile_result[0]['email_id']),
                    'country_code': replace_empty_values1(profile_result[0]['country_code']),
                    'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                    'city': replace_empty_values1(profile_result[0]['city']),
                    'state': replace_empty_values1(profile_result[0]['state']),
                    'country': replace_empty_values1(profile_result[0]['country']),
                    'pricing_category': replace_empty_values1(profile_result[0]['pricing_category']),
                    'profile_image': s3_pic_key,
                    'video_name': s3_video_key,
                    'resume_name': s3_resume_key,
                    'cover_letter_name' : s3_cover_letter_key,
                    'about': replace_empty_values1(profile_result[0]['about']),
                    'preferences': replace_empty_values1(profile_result[0]['preferences']),
                    'experience': {},
                    'education': {},
                    'skills': {},
                    'languages': {},
                    'additional_info': {},
                    'social_link': {},
                    'question_answers' : {}, 
                    'job_id' : job_id,
                    'candidates_short_desc' : candidates_short_desc
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
                quest_dict = {"questions" : []}
                query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                values = (job_id,)
                result = execute_query(query, values)
                if len(result) > 0:
                        for r in result:
                            query = 'select custom_pre_screen_ans from pre_screen_ans where job_id = %s and pre_screen_ques_id = %s and professional_id = %s'
                            values = (job_id, r['id'],first_id,)
                            ans_result = execute_query(query, values)
                            if len(ans_result) > 0:
                                r.update(ans_result[0])
                            else:
                                r.update({'custom_pre_screen_ans' : ""})
                            quest_dict["questions"].append(r)
                profile_dict['question_answers'] = quest_dict['questions']
                if len(prof_job_details) > 0:
                    profile_dict.update({'applied_status' : "Applied"})
                    profile_dict.update({'feedback' : prof_job_details[0]['feedback']})
                    profile_dict.update({'invite_to_interview' : prof_job_details[0]['invite_to_interview']})
                    profile_dict.update({'application_status' : prof_job_details[0]['application_status']})
                    profile_dict.update({'custom_notes' : prof_job_details[0]['custom_notes']})
                else:
                    profile_dict.update({'feedback' : ''})
                    profile_dict.update({'applied_status' : "Applied"})
                    profile_dict.update({'invite_to_interview' : 'No'})
                    profile_dict.update({'application_status' : 'Default'})
                    profile_dict.update({'custom_notes' : ''})
                profile_dict['question_answers'] = quest_dict['questions']
                try:
                    temp_dict = {'Message': "Professionals filtered successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Employer Filter_Professionals", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Employer Filter_Professionals",event_properties, temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Employer Filter_Professionals, {str(e)}")
                result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in filtering professionals by employer."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Filter_Professionals Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Filter_Professionals Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Filter_Professionals Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_other_professional_skills():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:  
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])           
            if user_data["user_role"] == "employer" or user_data["user_role"] == "admin" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                query = 'SELECT DISTINCT(skill_name) FROM `professional_skill` where skill_name != ""'
                values = ()
                result = execute_query(query, values)
                result = replace_empty_values(result)
                result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,result)
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def delete_job_post():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id =  user_data['user_id']
            else:        
                employer_id = user_data['user_id']
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                # employer_id = user_data["user_id"]                                       
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                job_id = req_data["job_id"]
                query = 'select job_title from job_post where id = %s'
                values = (job_id,)
                job_title = execute_query(query,values)
                if isUserExist("employer_profile","employer_id",employer_id):
                    if isUserExist("job_post","id",job_id): 
                        query = 'delete from pre_screen_ans where job_id = %s'
                        values = (job_id,)
                        answer_deletion = update_query(query,values)
                        query = 'delete from pre_screen_ques where job_id = %s'
                        values = (job_id,)
                        question_deletion = update_query(query,values)
                        query = 'delete from view_count where job_id = %s'
                        values = (job_id,)
                        view_count = update_query(query,values)
                        query = 'delete from ai_recommendation where job_id = %s'
                        values = (job_id,)
                        ai_deletion = update_query(query,values)
                        query = 'delete from sc_recommendation where job_id = %s'
                        values = (job_id,)
                        sc_deletion = update_query(query,values)
                        query = 'delete from assigned_jobs where job_id = %s'
                        values = (job_id,)
                        assigned_deletion = update_query(query,values)
                        query = 'delete from home_page_jobs where job_id = %s'
                        values = (job_id,)
                        home_page_deletion = update_query(query,values)
                        query = 'delete from invited_jobs where job_id = %s'
                        values = (job_id,)
                        invited_deletion = update_query(query,values)
                        query = 'delete from saved_job where job_id = %s'
                        values = (job_id,)
                        job_saved_deletion = update_query(query,values)
                        query = 'delete from job_activity where job_id = %s'
                        values = (job_id,)
                        job_activity_deletion = update_query(query,values)
                        query = 'delete from job_post where id = %s'
                        values = (job_id,)
                        job_post_deletion = update_query(query,values)
                        if answer_deletion > 0 and question_deletion > 0 and view_count > 0 and job_activity_deletion > 0 and job_post_deletion > 0 and job_saved_deletion > 0 and ai_deletion > 0 and sc_deletion > 0 and assigned_deletion > 0 and home_page_deletion > 0 and invited_deletion > 0:
                            try:
                                temp_dict = {'Job Title' : job_title[0]['job_title'],
                                            'Message': f"Job '{job_title[0]['job_title']}' deleted successfully."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Employer Delete Job", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Employer Delete Job",event_properties, temp_dict.get('Message'),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Employer Delete Job, {str(e)}")
                            result_json = api_json_response_format(True,"The job post has been updated successfully!",0,{})
                        else:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'Job Title' : job_title[0]['job_title'],
                                            'Message': f"An error occurred while deleting the job '{job_title[0]['job_title']}'."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Employer Delete Job Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Employer Delete Job Error",event_properties, temp_dict.get('Message'),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Employer Delete Job Error, {str(e)}")
                            result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                    else:                        
                        result_json = api_json_response_format(False,"Job not found",204,{})
                else:                        
                    result_json = api_json_response_format(False,"Employer profile Not Found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in deleting employer job post."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Employer Delete Job Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Employer Delete Job Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Employer Delete Job Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
      
def job_post_draft():
    try:
        key_id = 0
        result_json = {}       
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            flag = 0
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data['sub_user_id']
                flag = 1
            else:
                employer_id = user_data['user_id']                                         
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                req_data = request.get_json()
                job_title = ""
                job_overview = ""
                job_type = ""
                work_schedule = ""
                workplace_type = ""
                country = ""
                city = ""
                time_commitment = ""
                duration = ""
                is_paid = ""
                time_zone = ""
                is_active = ""
                specialisation = ""
                skills = ""
                job_desc = ""
                required_resume = ""
                required_cover_letter = ""
                required_background_check = ""
                required_subcontract = ""
                job_status = ""
                pre_screen_ques = ""
                receive_notification = ""
    
                if 'key_id'  in req_data:
                    key_id = req_data['key_id']
                if 'job_title' in req_data:
                    job_title = req_data['job_title'] 
                if 'job_type' in req_data:
                    job_type = req_data['job_type']
                if 'work_schedule' in req_data:
                    work_schedule = req_data['work_schedule']
                if 'workplace_type' in req_data:
                    workplace_type = req_data['workplace_type']
                if 'country' in req_data:    
                    country = req_data['country']
                if 'city' in req_data:
                    city = req_data['city']
                if 'time_zone' in req_data:
                    time_zone = req_data['time_zone']
                if 'skills' in req_data:
                    skills = req_data['skills']
                if 'specialisation' in req_data:
                    specialisation = req_data['specialisation']
                if 'job_desc' in req_data:
                    job_desc = req_data['job_desc']
                if 'required_resume' in req_data:
                    required_resume = req_data['required_resume']
                if 'required_cover_letter' in req_data:
                    required_cover_letter = req_data['required_cover_letter']
                if 'required_background_check' in req_data:
                    required_background_check = req_data['required_background_check']
                if 'required_subcontract' in req_data:
                    required_subcontract = req_data['required_subcontract']
                if 'time_commitment' in req_data:
                    time_commitment = req_data['time_commitment']
                if 'duration' in req_data:
                    duration = req_data['duration']
                if 'job_status' in req_data:
                    job_status = req_data['job_status']
                if 'is_paid' in req_data:
                    is_paid = req_data['is_paid']                
                if 'is_active' in req_data:
                    is_active = req_data['is_active']
                if 'job_overview' in req_data:
                    job_overview = req_data["job_overview"]
                if 'pre_screen_ques' in req_data:
                    pre_screen_ques = req_data["pre_screen_ques"] 
                if 'receive_notification' in req_data:
                    receive_notification = req_data["receive_notification"]
                created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

                if not key_id == 0:
                    if isUserExist("job_post", "id", key_id):                
                        query = 'update job_post set job_title=%s, job_type=%s, work_schedule=%s,job_overview=%s,workplace_type=%s,country=%s, city=%s,timezone=%s, specialisation=%s, required_subcontract=%s, skills=%s, job_desc=%s, required_resume=%s, required_cover_letter=%s, required_background_check=%s,time_commitment=%s, receive_notification=%s duration=%s, job_status=%s, is_paid=%s, is_active=%s where id=%s'
                        values = (job_title, job_type, work_schedule, job_overview,workplace_type,country, city,time_zone, specialisation, required_subcontract, skills, job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, duration, job_status, is_paid, is_active, key_id,)
                        result = update_query_last_index(query, values)
                        if result['row_count'] > 0:
                            if isUserExist("pre_screen_ques", "job_id", key_id):
                                for ques in pre_screen_ques:
                                    if 'id' in ques:
                                        query = 'update pre_screen_ques set custom_pre_screen_ques=%s where job_id=%s and id=%s'
                                        values = (ques['custom_pre_screen_ques'],key_id,ques['id'],)
                                        result = update_query_last_index(query, values)
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Job Title' : job_title,
                                        'Job Drafted Status' : "Success",
                                        'Message' : f"The job {job_title} has been drafted successfully."
                                    }
                                event_name = "Employer Job Draft"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, temp_dict.get('Message'),user_data)
                            except Exception as e:  
                                print("Error in employer draft job mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(True,"Your draft has been saved successfully",0,{})
                        else:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Job Title' : job_title,
                                        'Job Drafted Status' : "Failure",
                                        'Message' : "An error occurred while drafting the job."
                                    }
                                message = f"An error occurred for {user_data['email_id']} while drafting the job."
                                event_name = "Employer Job Draft Error"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message,user_data)
                            except Exception as e:  
                                print("Error in employer draft job mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                    else:             
                        created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                        query = 'insert into job_post (employer_id, job_title, job_type, work_schedule,job_overview,workplace_type,country, city,timezone, specialisation, required_subcontract, skills,job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, duration, job_status, is_paid, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                        values = (employer_id, job_title, job_type, work_schedule,job_overview, workplace_type,country, city,time_zone, specialisation, required_subcontract, skills, job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, duration, job_status, is_paid, is_active, created_at,)
                        result = update_query_last_index(query, values)
                        if result['row_count'] > 0:
                            key_id = result["last_index"]
                            for ques in pre_screen_ques:
                                query = 'insert into pre_screen_ques (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                                values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                                result = update_query_last_index(query, values)
                                try:
                                    event_properties = {    
                                            '$distinct_id' : user_data["email_id"], 
                                            '$time': int(time.mktime(dt.now().timetuple())),
                                            '$os' : platform.system(),          
                                            'Email' : user_data["email_id"],
                                            'Job Title' : job_title,
                                            'Job Drafted Status' : "Success",
                                            'Message' : "The job has been drafted successfully."
                                        }
                                    message = f"The job has been drafted successfully for {user_data['email_id']}."
                                    event_name = "Employer Job Draft"
                                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                                except Exception as e:  
                                    print("Error in employer draft job mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(True,"Your draft has been saved successfully",0,{})
                        else:
                            try:
                                event_properties = {    
                                        '$distinct_id' : user_data["email_id"], 
                                        '$time': int(time.mktime(dt.now().timetuple())),
                                        '$os' : platform.system(),          
                                        'Email' : user_data["email_id"],
                                        'Job Title' : job_title,
                                        'Job Drafted Status' : "Failure",
                                        'Message' : "An error occurred while drafting the job."
                                    }
                                message = f"An error occurred while drafting the job for {user_data['email_id']}."
                                event_name = "Employer Job Draft Error"
                                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                            except Exception as e:  
                                print("Error in employer draft job mixpanel_event_log : %s",str(e))
                            result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                else:
                    created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                    query = 'insert into job_post (employer_id, job_title, job_type, work_schedule,job_overview,workplace_type,country, city,timezone, specialisation, required_subcontract, skills,job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, duration, job_status, is_paid, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    values = (employer_id, job_title, job_type, work_schedule,job_overview, workplace_type,country, city,time_zone, specialisation, required_subcontract, skills, job_desc, required_resume, required_cover_letter, required_background_check,time_commitment, receive_notification, duration, job_status, is_paid, is_active, created_at,)
                    result = update_query_last_index(query, values)
                    if result['row_count'] > 0:
                        key_id = result["last_index"]
                        for ques in pre_screen_ques:
                            query = 'insert into pre_screen_ques (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                            values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                            result = update_query_last_index(query, values)
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Job Title' : job_title,
                                    'Job Drafted Status' : "Success",
                                    'Message' : "The job has been drafted successfully."
                                }
                            message = f"The job has been drafted successfully for {user_data['email_id']}."
                            event_name = "Employer Job Draft"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in employer draft job mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(True,"Your draft has been saved successfully",0,{})
                    else:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Post Title' : job_title,
                                    'Job Drafted Status' : "Failure",
                                    'Message' : "An error occurred while drafting the job."
                                }
                            message = f"An error occurred for {user_data['email_id']} while drafting the job."
                            event_name = "Employer Job Draft Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in employer draft job mixpanel_event_log : %s",str(e))
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
                    'Job Drafted Status' : "Failure",
                    'Error' : str(error)
                }
            message = f"{user_data['email_id']} Job Draft Error"
            event_name = "Employer Job Draft Error"
            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
        except Exception as e:  
            print("Error in employer draft job mixpanel_event_log : %s",str(e))
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_payment_status_employer():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            flag = 0
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])      
                employer_id = user_data["user_id"]
                flag = 1
            else:
                employer_id = user_data["user_id"]
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                email_id = user_data["email_id"]
                # query = 'select payment_status from users where user_id = %s'
                # values = (employer_id,)
                # ps = execute_query(query,values)
                if flag == 1:
                    query = "SELECT DATEDIFF(NOW(), created_at) AS days_passed, pricing_category,payment_status FROM sub_users WHERE sub_user_id = %s;"
                    values=  (user_data['sub_user_id'],)
                else:
                    query = "SELECT DATEDIFF(NOW(), created_at) AS days_passed, pricing_category,payment_status FROM users WHERE user_id = %s;"
                    values=  (employer_id,)
                detail = execute_query(query, values)
                if len(detail) > 0:
                    if detail[0]['days_passed'] > 90 and detail[0]['days_passed'] < 180 and detail[0]['pricing_category'] == 'trialing':
                        result_json = api_json_response_format(False, "Your trial period for posting jobs has expired. Please upgrade your plan to continue.",205,{})
                    elif detail[0]['days_passed'] > 180 and detail[0]['pricing_category'] == 'trialing':
                        result_json = api_json_response_format(False,f"Trial period of {email_id} has ended. please Subcribe to continue.",300,{})
                    else:
                        jobs_left_query = 'select no_of_jobs, user_plan, total_jobs from user_plan_details where user_id = %s'
                        values = (employer_id,)
                        rs = execute_query(jobs_left_query, values)
                        if len(rs) > 0 :
                            if rs[0]['no_of_jobs'] == 0 and detail[0]['payment_status'] == "complete":
                                result_json = api_json_response_format(False,"You have reached your job postings limit. Please upgrade your plan to post jobs.",0,{})
                            elif rs[0]['no_of_jobs'] == 0 and detail[0]['payment_status'] == "unpaid":
                                result_json = api_json_response_format(False,"You have reached your job postings limit. Please upgrade your plan to post jobs.",204,{})
                            else:
                                if flag == 1:
                                    query = 'select payment_status from sub_users where sub_user_id = %s'
                                    values = (user_data['sub_user_id'],)
                                else:
                                    query = 'select payment_status from users where user_id = %s'
                                    values = (employer_id,)
                                ps = execute_query(query,values)
                                if len(ps) >0:
                                    payment_status = ps[0]["payment_status"]
                                    if payment_status =="canceled":
                                        result_json = api_json_response_format(False,f"Trial period of {email_id} has ended. please Subcribe to continue.",300,{})   
                                    else:
                                        result_json = api_json_response_format(False,f"Trial period",0,{})
                # if len(ps) >0:
                #     payment_status = ps[0]["payment_status"]
                #     if payment_status =="canceled" or payment_status =="incomplete":
                #         result_json = api_json_response_format(False,f"Trial period of {email_id} has ended. please Subcribe to continue.",300,{})   
                #     else:
                #         result_json = api_json_response_format(False,f"Trial period",0,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json


def edit_employer_job_post():
    try:
        result_json = {}       
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id =  user_data['sub_user_id']
            else:
                employer_id =  user_data['user_id']           
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":                                        
                req_data = request.get_json()  
                if 'key_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json      
                if 'job_title' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_overview' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_type' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'work_schedule' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'workplace_type' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'time_commitment' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'is_paid' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'time_zone' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'is_active' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'specialisation' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'skills' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_desc' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_resume' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_cover_letter' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_subcontract' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json  
                if 'receive_notification' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json              
                if 'job_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'required_background_check' in req_data:
                    required_background_check = req_data['required_background_check']
                else:
                    required_background_check = 'N'
                if 'pre_screen_ques' in req_data:
                    pre_screen_ques = req_data["pre_screen_ques"]
                else:
                    pre_screen_ques = ""

                created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                query = 'SELECT skill_name FROM `filter_skills` where is_active = %s'
                values = ('Y',)
                skill_list = execute_query(query, values)
                temp_skill_list = []
                for i in skill_list:
                    temp_skill_list.append(i['skill_name'])
                txt = req_data["skills"]
                arr = txt.split(",")
                for s in arr :
                    if s not in temp_skill_list:
                        s = s.lower()
                        query = "select count(skill_name) as count from filter_skills where skill_name = %s"
                        values = (s,)
                        count_skill = execute_query(query, values)
                        if count_skill[0]['count'] == 0:
                            query = "insert into filter_skills (skill_name, is_active, created_at) values (%s,%s,%s)"
                            values = (s, 'N', created_at,)
                            rslt = update_query(query, values)
                
                query = 'SELECT specialisation_name FROM `filter_specialisation` where is_active = %s'
                values = ('Y',)
                specialisation_list = execute_query(query, values)
                m = req_data['specialisation']
                temp_specialisation_list = []
                for i in specialisation_list:
                    temp_specialisation_list.append(i['specialisation_name'])
                if m not in temp_specialisation_list:
                    m = m.lower()
                    query = "select count(specialisation_name) as count from filter_specialisation where specialisation_name = %s"
                    values = (m,)
                    count_specialisation = execute_query(query, values)
                    if count_specialisation[0]['count'] == 0:
                        query = "insert into filter_specialisation (specialisation_name, is_active, created_at) values (%s,%s,%s)"
                        values = (m, 'N', created_at,)
                        rslt = update_query(query, values)

                # Extract job details
                key_id = req_data['key_id']
                job_title = req_data['job_title'] 
                job_type = req_data['job_type']
                work_schedule = req_data['work_schedule']
                workplace_type = req_data['workplace_type']    
                country = req_data['country']
                city = req_data['city']
                time_zone = req_data['time_zone']
                skills = req_data['skills']
                specialisation = req_data['specialisation']
                job_desc = req_data['job_desc']
                required_resume = req_data['required_resume']
                required_cover_letter = req_data['required_cover_letter']
                required_subcontract = req_data['required_subcontract']
                time_commitment = req_data['time_commitment']
                receive_notification = req_data['receive_notification']
                if 'duration' in req_data:
                    duration = req_data['duration']
                else:
                    duration = ''
                job_status = req_data['job_status']
                is_paid = req_data['is_paid']
                is_active = req_data['is_active']
                # employer_id = user_data["user_id"]
                job_overview = req_data["job_overview"]

                if not key_id == 0:
                    if isUserExist("job_post", "id", key_id):             
                        query = 'update job_post set job_title=%s, job_type=%s, work_schedule=%s,job_overview=%s,workplace_type=%s,country=%s, city=%s, specialisation=%s, required_subcontract=%s, skills=%s, job_desc=%s, required_resume=%s, required_cover_letter=%s, required_background_check=%s,time_commitment=%s, receive_notification=%s, timezone=%s, duration=%s, job_status=%s, is_paid=%s, is_active=%s where id=%s'
                        values = (job_title, job_type, work_schedule, job_overview,workplace_type,country, city, specialisation, required_subcontract, skills, job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, receive_notification, time_zone, duration, job_status, is_paid, is_active, key_id,)
                        result = update_query_last_index(query, values)
                        if pre_screen_ques != "": 
                            if isUserExist("pre_screen_ques", "job_id", key_id):
                                query = "delete from pre_screen_ques where job_id = %s"
                                values = (key_id,)
                                del_job = update_query(query, values)
                                for ques in pre_screen_ques:
                                    query = 'insert into pre_screen_ques  (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                                    values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                                    result = update_query_last_index(query, values)
                            else:
                                for ques in pre_screen_ques:
                                    query = 'insert into pre_screen_ques  (custom_pre_screen_ques,job_id,created_at)values (%s,%s,%s)'
                                    values = (ques['custom_pre_screen_ques'], key_id,created_at,)
                                    result = update_query_last_index(query, values)
                        job_details = {
                            "id": key_id,
                            "employer_id": employer_id,
                            "job_title": req_data['job_title'],
                            "job_type": req_data['job_type'],
                            "work_schedule": req_data['work_schedule'],
                            "job_overview": req_data["job_overview"],
                            "workplace_type": req_data['workplace_type'],
                            "country": req_data['country'],
                            "city": req_data['city'],
                            "specialisation": req_data['specialisation'],
                            "required_subcontract": req_data['required_subcontract'],
                            "skills": req_data['skills'],
                            "job_desc": req_data['job_desc'],
                            "required_resume": req_data['required_resume'],
                            "required_cover_letter": req_data['required_cover_letter'],
                            "required_background_check": required_background_check,
                            "time_commitment": req_data['time_commitment'],
                            "time_zone": req_data['time_zone'],
                            "duration": req_data.get('duration', ''),
                            "job_status": req_data['job_status'],
                            "is_paid": req_data['is_paid'],
                            "is_active": req_data['is_active'],
                            "receive_notification" : req_data['receive_notification'],
                            "created_at": created_at
                        }
                        store_in_meilisearch([job_details])
                        background_runner.get_job_details(key_id)
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Job Title' : job_title,
                                    'Job Type' : job_type,
                                    'Workplace Type' : workplace_type,
                                    'Work Schedule' : work_schedule,
                                    'Specialisation' : specialisation,
                                    'Job City' : city,
                                    'Job Country' : country,
                                    'Job Timezone' : time_zone,
                                    'Post Edited Status' : "Success"
                                }
                            event_name = "Job Post Edit"
                            message = f"{user_data['email_id']} edit job post successfully."
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                        except Exception as e:  
                            print("Error in editing employer job post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(True, "Job edited successfully", 0, {})
                    else:
                        try:
                            event_properties = {    
                                    '$distinct_id' : user_data["email_id"], 
                                    '$time': int(time.mktime(dt.now().timetuple())),
                                    '$os' : platform.system(),          
                                    'Email' : user_data["email_id"],
                                    'Job Title' : job_title,
                                    'Job Type' : job_type,
                                    'Workplace Type' : workplace_type,
                                    'Work Schedule' : work_schedule,
                                    'Specialisation' : specialisation,
                                    'Job City' : city,
                                    'Job Country' : country,
                                    'Job Timezone' : time_zone,
                                    'Post Edited Status' : "Failure",
                                    'Error' : "No job found Error"
                                }
                            message = f"{user_data['email_id']} unable to edit job post"
                            event_name = "Job Post Edit Error"
                            background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties,message, user_data)
                        except Exception as e:  
                            print("Error in editing employer job post mixpanel_event_log : %s",str(e))
                        result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.", 500, {})
            else:
                try:
                    event_properties = {    
                            '$distinct_id' : user_data["email_id"], 
                            '$time': int(time.mktime(dt.now().timetuple())),
                            '$os' : platform.system(),          
                            'Email' : user_data["email_id"],
                            'Job Title' : job_title,
                            'Job Type' : job_type,
                            'Workplace Type' : workplace_type,
                            'Work Schedule' : work_schedule,
                            'Specialisation' : specialisation,
                            'Job City' : city,
                            'Job Country' : country,
                            'Job Timezone' : time_zone,
                            'Post Edited Status' : "Failure",
                            'Error' : "Unauthorized User Error"
                        }
                    message = f"unable to edit job post due to unauthorized User Error for {user_data['email_id']}"
                    event_name = "Job Post Edit Error"
                    background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
                except Exception as e:  
                    print("Error in editing employer job post mixpanel_event_log : %s",str(e))
                result_json = api_json_response_format(False, "Unauthorized User", 401, {})
        else:
            try:
                event_properties = {    
                        '$distinct_id' : user_data["email_id"], 
                        '$time': int(time.mktime(dt.now().timetuple())),
                        '$os' : platform.system(),          
                        'Email' : user_data["email_id"],
                        'Job Title' : job_title,
                        'Job Type' : job_type,
                        'Workplace Type' : workplace_type,
                        'Work Schedule' : work_schedule,
                        'Specialisation' : specialisation,
                        'Job City' : city,
                        'Job Country' : country,
                        'Job Timezone' : time_zone,
                        'Post Edited Status' : "Failure",
                        'Error' : "Token Error"
                    }
                message = f"Unable to edit job post due to Token error for {user_data['email_id']}"
                event_name = "Job Post Edit Error"
                background_runner.mixpanel_event_async(user_data["email_id"],event_name,event_properties, message, user_data)
            except Exception as e:  
                print("Error in editing employer job post mixpanel_event_log : %s",str(e))
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:        
        return result_json

def pool_dashboard_meilisearch():
    """
    Perform search operation to fetch professional profiles based on search criteria using Meilisearch.
    """
    try:
        result_json = {}
        token_result = get_user_token(request)
        if token_result["status_code"] != 200:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
            return result_json

        user_data = get_user_data(token_result["email_id"])
        if not user_data['is_exist']:
            user_data = get_sub_user_data(token_result['email_id'])

        user_role = user_data["user_role"]
        if user_role not in ["employer", "partner", "employer_sub_admin", "recruiter"]:
            result_json = api_json_response_format(False, "Unauthorized User", 401, {})
            return result_json

        req_data = request.get_json()
        if 'page_number' not in req_data or 'search_text' not in req_data:
            result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
            return result_json

        search_text = req_data['search_text']
        page_number = req_data["page_number"]

        offset = (page_number - 1) * 5
        profiles_to_fetch = 5

        if search_text.startswith("2C"):
            split_txt = search_text.split("-")
            if len(split_txt) > 2:
                search_text = split_txt[2]

        client = Client(MEILISEARCH_CLOUD_URL, MEILISEARCH_MASTER_KEY)
        index = client.index(MEILISEARCH_PROFESSIONAL_INDEX)

        filters = [
            "(email_active = 'Y')",
            "(user_role_fk = 3)",
            "((about != '' AND about IS NOT NULL) OR (education_id IS NOT EMPTY) OR (experience_id IS NOT EMPTY) OR (skill_id IS NOT EMPTY))"
        ]
        final_filters = " AND ".join(filters)
        # results = index.search(
        #     search_text,
        #     {
        #         'filter': final_filters,
        #         'sort': ['user_id:asc'],
        #         'limit': profiles_to_fetch,
        #         'offset' : offset
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
                "limit": profiles_to_fetch,
                "offset": offset,
                'showRankingScore': True
            }
        )
        fetched_data = results['hits']
        count = results['estimatedTotalHits']
        if not fetched_data:
            final_dict = {"final_result": [], "total_professionals": 0}
            result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
            # result_json = api_json_response_format(False, "No professionals found!", 200, {"final_result": [], "total_professionals": 0})
            return result_json

        user_ids = tuple(hit['user_id'] for hit in fetched_data)
        query = """
                    SELECT 
                        p.id, 
                        p.professional_id, 
                        p.about,

                        -- Aggregate experiences into a JSON array
                        (
                            SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'experience_id', ordered_exp.id, 
                                    'company_name', ordered_exp.company_name, 
                                    'job_title', ordered_exp.job_title,
                                    'start_date', ordered_exp.start_year,
                                    'end_date', ordered_exp.end_year,
                                    'job_description', ordered_exp.job_description,
                                    'job_location', ordered_exp.job_location
                                )
                            )
                            FROM (
                                SELECT * 
                                FROM professional_experience 
                                WHERE professional_id = p.professional_id
                                ORDER BY 
                                    CASE 
                                        WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                        ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC
                            ) AS ordered_exp
                        ) AS experiences,

                        -- Aggregate education into a JSON array
                        (
                            SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'education_id', ordered_ed.id,
                                    'institute_name', ordered_ed.institute_name,
                                    'institute_location', ordered_ed.institute_location,
                                    'degree_level', ordered_ed.degree_level,
                                    'specialisation', ordered_ed.specialisation,
                                    'start_date', ordered_ed.start_year,
                                    'end_date', ordered_ed.end_year
                                )
                            )
                            FROM (
                                SELECT * 
                                FROM professional_education 
                                WHERE professional_id = p.professional_id
                                ORDER BY 
                                    CASE 
                                        WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                        ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    CASE 
                                        WHEN start_year = 'Present' THEN 9999 -- Treat "Present" as the highest value for start_year as well
                                        ELSE CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED)
                                    END DESC
                            ) AS ordered_ed
                        ) AS education,

                        -- Aggregate skills into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'skill_id', ps.id,
                                'skill_name', ps.skill_name,
                                'skill_level', ps.skill_level
                            )
                        )
                        FROM professional_skill AS ps 
                        WHERE ps.professional_id = p.professional_id
                        ) AS skills,

                        -- Aggregate languages into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'language_known', pl.language_known
                            )
                        )
                        FROM professional_language AS pl 
                        WHERE pl.professional_id = p.professional_id
                        ) AS languages,

                        -- Aggregate additional info into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'description', pad.description
                            )
                        )
                        FROM professional_additional_info AS pad 
                        WHERE pad.professional_id = p.professional_id
                        ) AS additional_info
                    FROM 
                        professional_profile AS p
                    JOIN 
                        users AS u ON p.professional_id = u.user_id AND u.email_active = 'Y'
                    WHERE
                        p.professional_id IN %s
                    GROUP BY 
                        p.id, p.professional_id, p.about
                    ORDER BY 
                        p.id ASC;
                """
        profiles_result = execute_query(query, (user_ids,))

        final_result = []
        for row in profiles_result:
            profile_dict = {
                'id': row['id'],
                'professional_id': f"2C-PR-{row['professional_id']}",
                'about': replace_empty_values1((row['about']) if row['about'] else []),
                'experience': json.loads(row['experiences']) if row['experiences'] else [],
                'education': json.loads(row['education']) if row['education'] else [],
                'skills': json.loads(row['skills']) if row['skills'] else []
            }
            final_result.append(profile_dict)

        final_result = [
            profile for profile in final_result 
            if not (
                profile['about'] == [] and 
                profile['education'] == [] and 
                profile['experience'] == [] and 
                profile['skills'] == []
            )
        ]

        if count > 0:
            final_dict = {"final_result": final_result, "total_professionals": count}
            result_json = api_json_response_format(True, "Details fetched successfully!", 200, final_dict)
        else:
            final_dict = {"final_result": [], "total_professionals": 0}
            result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)

    except Exception as error:
        print(f"Error: {error}")
        result_json = api_json_response_format(False, str(error), 500, error)

    finally:
        return result_json

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

def get_individual_user_detail(user_id):
    try:
        result_json = {}
        prof_details = []
        final_result = []
        query_job_details = """
                    SELECT 
                        p.id, 
                        p.professional_id, 
                        p.about,
                        
                        (
                            SELECT CAST(ROUND(pd2.ranking_score, 4) AS DECIMAL(10,4))
                            FROM pool_dashboard_search_data pd2
                            WHERE pd2.user_id = p.professional_id
                            ORDER BY pd2.updated_at DESC
                            LIMIT 1
                        ) AS ranking_score,
                        -- Aggregate experiences into a JSON array
                        (
                            SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'experience_id', ordered_exp.id, 
                                    'company_name', ordered_exp.company_name, 
                                    'job_title', ordered_exp.job_title,
                                    'start_date', ordered_exp.start_year,
                                    'end_date', ordered_exp.end_year,
                                    'job_description', ordered_exp.job_description,
                                    'job_location', ordered_exp.job_location
                                )
                            )
                            FROM (
                                SELECT * 
                                FROM professional_experience 
                                WHERE professional_id = p.professional_id
                                ORDER BY 
                                    CASE 
                                        WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                        ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC
                            ) AS ordered_exp
                        ) AS experiences,

                        -- Aggregate education into a JSON array
                        (
                            SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'education_id', ordered_ed.id,
                                    'institute_name', ordered_ed.institute_name,
                                    'institute_location', ordered_ed.institute_location,
                                    'degree_level', ordered_ed.degree_level,
                                    'specialisation', ordered_ed.specialisation,
                                    'start_date', ordered_ed.start_year,
                                    'end_date', ordered_ed.end_year
                                )
                            )
                            FROM (
                                SELECT * 
                                FROM professional_education 
                                WHERE professional_id = p.professional_id
                                ORDER BY 
                                    CASE 
                                        WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                        ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    CASE 
                                        WHEN start_year = 'Present' THEN 9999 -- Treat "Present" as the highest value for start_year as well
                                        ELSE CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED)
                                    END DESC
                            ) AS ordered_ed
                        ) AS education,

                        -- Aggregate skills into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'skill_id', ps.id,
                                'skill_name', ps.skill_name,
                                'skill_level', ps.skill_level
                            )
                        )
                        FROM professional_skill AS ps 
                        WHERE ps.professional_id = p.professional_id
                        ) AS skills,

                        -- Aggregate languages into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'language_known', pl.language_known
                            )
                        )
                        FROM professional_language AS pl 
                        WHERE pl.professional_id = p.professional_id
                        ) AS languages,

                        -- Aggregate additional info into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'description', pad.description
                            )
                        )
                        FROM professional_additional_info AS pad 
                        WHERE pad.professional_id = p.professional_id
                        ) AS additional_info
                    FROM 
                        professional_profile AS p
                    JOIN 
                        users AS u ON p.professional_id = u.user_id AND u.email_active = 'Y'
                    WHERE
                        p.professional_id = %s
                    GROUP BY 
                        p.id, p.professional_id, p.about
                    ORDER BY 
                        p.id ASC;
                """
        values_job_details = (user_id,)
        user_detail = replace_empty_values(execute_query(query_job_details, values_job_details))
        total_count = 0
        if user_detail:
            prof_details.append(user_detail[0])
            total_count = len(user_detail)
        if prof_details:
            for row in prof_details:
                profile_dict = {
                    'id': row['id'],
                    'professional_id': f"2C-PR-{row['professional_id']}",
                    'about': replace_empty_values1((row['about']) if row['about'] else []),
                    'experience': json.loads(row['experiences']) if row['experiences'] else [],
                    'education': json.loads(row['education']) if row['education'] else [],
                    'skills': json.loads(row['skills']) if row['skills'] else [],
                    'ranking_score' : row['ranking_score'] if row['ranking_score'] is not None else None
                }
                final_result.append(profile_dict)

        final_result = [
            profile for profile in final_result 
            if not (
                profile['about'] == [] and 
                profile['education'] == [] and 
                profile['experience'] == [] and 
                profile['skills'] == []
            )
        ]

        if total_count > 0:
            final_dict = {"final_result": final_result, "total_professionals": total_count}
        else:
            final_dict = {"final_result": [], "total_professionals": 0}
        result_json = {"user_detail" : final_dict}
    except Exception as e:
        print(f"Error in get_individual_user_detail(): {e}")
        result_json = {"error": str(e)}
    finally:
        return result_json
    

def pool_dashboard_search_filter():
    """
    Perform search operation to fetch professional profiles based on search criteria using Meilisearch.
    """
    try:
        result_json = {}
        token_result = get_user_token(request)
        if token_result["status_code"] != 200:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
            return result_json

        user_data = get_user_data(token_result["email_id"])
        if not user_data['is_exist']:
            user_data = get_sub_user_data(token_result['email_id'])

        user_role = user_data["user_role"]
        if user_role not in ["employer", "partner", "employer_sub_admin", "recruiter"]:
            result_json = api_json_response_format(False, "Unauthorized User", 401, {})
            return result_json
        
        req_data = request.get_json()

        user_id = user_data['user_id']
        
        search_text = req_data["search_text"]
        # if search_text.startswith("2C"):
        #     split_txt = search_text.split("-")
        #     if len(split_txt) > 2:
        #         search_text = int(split_txt[2])
        #     individual_user_detail = get_individual_user_detail(search_text)
        #     if "error" not in individual_user_detail:
        #         result_json = api_json_response_format(True, "User details fetched successfully", 0, individual_user_detail['user_detail'])
        #         return result_json
        #     else:
        #         result_json = api_json_response_format(False, individual_user_detail["error"], 500, {})
        #         return result_json
            
        get_search_query_flag = 'select attribute_value from payment_config where attribute_name = %s'
        get_search_query_flag_values = ('pool_dashboard_search_query_flag',)
        search_query_flag_dict = execute_query(get_search_query_flag, get_search_query_flag_values)
        search_query_flag = 'N'
        if search_query_flag_dict:
            search_query_flag = search_query_flag_dict[0]['attribute_value']
        if search_text != '' and search_query_flag == 'Y':
            if search_text.startswith("2C"):
                split_txt = search_text.split("-")
                if len(split_txt) > 2:
                    search_text = str(split_txt[2])
            else:
                search_query_value = get_search_query(search_text)
                search_text = search_query_value['search_text']
        
        get_flag_query = 'select attribute_value from payment_config where attribute_name = %s'
        get_flag_values = ('pool_dashboard_open_ai_flag',)
        flag_value_dict = execute_query(get_flag_query, get_flag_values)
        open_ai_flag = 'N'
        if flag_value_dict:
            open_ai_flag = flag_value_dict[0]['attribute_value']
        if open_ai_flag == 'Y':
            result_json = pool_dashboard_search_filter_new(request)
            return result_json

        if 'page_number' not in req_data or 'search_text' not in req_data:
            result_json = api_json_response_format(False, "Please fill in all the required fields.", 204, {})
            return result_json

        page_number = req_data["page_number"]

        if search_text.startswith("2C"):
            split_txt = search_text.split("-")
            if len(split_txt) > 2:
                search_text = split_txt[2]

        check_entries_in_db = "select count(id) from pool_dashboard_search_data where employer_id = %s"
        values = (user_id,)
        is_entries_presents_dict = execute_query(check_entries_in_db, values)

        page_number = req_data["page_number"]
        offset = (page_number - 1) * 10
        location = req_data["location"]
        gender = req_data['gender']
        industry_sector = req_data['industry_sector']
        sector = req_data['sector']
        job_type = req_data['job_type']
        willing_to_relocate = req_data['willing_to_relocate']
        location_preference = req_data['location_preference']
        functional_specification = req_data['functional_specification']
        flag = req_data["flag"]
        pagination_flag = req_data["pagination_flag"]

        country = []
        city = []
        if len(location) != 0:
            for i in range(len(location)):
                res = location[i].split("&&&&&")
                country.append(res[1])
                city.append(res[0])
        
        client = Client(MEILISEARCH_CLOUD_URL, MEILISEARCH_MASTER_KEY)
        index = client.index(MEILISEARCH_PROFESSIONAL_INDEX)

        country_filter = country
        city_filter = city
        gender_filter = gender
        industry_sector_filter =  industry_sector
        sector_filter =  sector
        job_type_filter =  job_type
        willing_to_relocate_filter = willing_to_relocate
        location_preference_filter = location_preference  
        functional_specification_filter =  functional_specification

        functional_specification_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite and Board"]

        filters = []

        query = "select count(user_id) as total_count from users where user_role_fk = %s"
        values = (3,)
        prof_count = execute_query(query, values)

        if prof_count:
            total_limit = prof_count[0]['total_count']

        else:
            total_limit = 2000
        
        if flag != 'filter_in_search':
            if page_number == 1 and pagination_flag != 'first_page':
                if is_entries_presents_dict:
                    if is_entries_presents_dict[0]['count(id)'] > 0:
                        delete_query = "delete from pool_dashboard_search_data where employer_id = %s"
                        values = (user_id,)
                        update_query(delete_query, values)

                if country_filter:
                    country_query = " OR ".join([f"country = '{country}'" for country in country_filter])
                    filters.append(f"({country_query})")

                if city_filter:
                    city_query = " OR ".join([f"city = '{city}'" for city in city_filter])
                    filters.append(f"({city_query})")
                                
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
                # filters.append("((about != '' AND about IS NOT NULL) OR (education_id IS NOT EMPTY) OR (experience_id IS NOT EMPTY) OR (skill_id IS NOT EMPTY))")
                final_filters = " AND ".join(filters)

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
                        'showRankingScore': True
                    }
                )
                ranking_score_limit = 0
                if results['hits']:
                    if results['hits'][0]['_rankingScore'] > 0.15:
                        ranking_score_limit = results['hits'][0]['_rankingScore'] - 0.15
                        
                
                # if results['hits']:
                #     top_score = results['hits'][0]['_rankingScore']
                #     ranking_score_limit = top_score * 0.50   # keep top 50%
                # else:
                #     ranking_score_limit = 0

                # if results['hits']:
                #     # Sort scores high → low
                #     scores = sorted([hit['_rankingScore'] for hit in results['hits']], reverse=True)
                    
                #     # Take cutoff score based on percentile (example: keep top 80%)
                #     cutoff_index = int(len(scores) * 0.80)  # adjust 0.80 if needed
                    
                #     ranking_score_limit = scores[cutoff_index]
                # else:
                #     ranking_score_limit = 0

                # filtered_results = [
                #     hit for hit in results['hits']
                #     if hit['_rankingScore'] >= ranking_score_limit
                # ]


                filtered_results = [hit for hit in results['hits'] if hit['_rankingScore'] >= ranking_score_limit]

               # # filtered_results = [hit for hit in results['hits'] if ranking_score_limit <= hit.get('_rankingScore', 0) <= 1]
                # RANK_THRESHOLD = 0.20

                # filtered_results = [
                #     hit for hit in results['hits']
                #     if hit.get('_rankingScore', 0) >= RANK_THRESHOLD
                # ]

                # ranking_score_limit = 0
                # if results['hits']:
                #     ranking_score_limit = results['hits'][0]['_rankingScore'] - 0.15
                # filtered_results = [hit for hit in results['hits'] if ranking_score_limit <= hit.get('_rankingScore', 0) <= 1]
                # filtered_results = [
                #                 profile for profile in filtered_results 
                #                 if not (
                #                     (profile['about'] == [] or profile['about'] != '') and 
                #                     profile['education_id'] == [] and 
                #                     profile['experience_id'] == [] and 
                #                     profile['skill_id'] == []
                #                 )
                #             ]
                

                filtered_results = [
                                        profile for profile in filtered_results 
                                    if (profile['about'] and profile['about'].strip()) or 
                                    (profile['education_id'] and profile['education_id'] != "[]") or 
                                    (profile['experience_id'] and profile['experience_id'] != "[]") or 
                                    (profile['skill_id'] and profile['skill_id'] != "[]")
                                ]

                result_user_ids = [id['user_id'] for id in filtered_results] 
                if result_user_ids:
                    get_users_details_query = """SELECT u.user_id, u.country, u.city, u.gender, u.is_active, p.functional_specification, p.sector, p.industry_sector, 
                                                p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.years_of_experience 
                                                FROM users AS u 
                                                LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id 
                                                WHERE u.user_id IN %s;"""
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
                        record_insert_query = 'INSERT INTO pool_dashboard_search_data (employer_id, user_id, professional_id, gender, city, country, functional_specification, industry_sector, job_type, sector, location_preference, willing_to_relocate, is_active, flag, ranking_score) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        professional_id = '2C-PR-' + str(record['user_id'])
                        values = (user_id, record['user_id'], professional_id, record['gender'], record['city'], record['country'],
                                    record['functional_specification'], record['industry_sector'], record['job_type'], record['sector'], record['location_preference'], 
                                    record['willing_to_relocate'], record['is_active'], 'sample', ranking_score,)
                        update_query(record_insert_query, values)
            get_count_query = 'select count(user_id) as count from pool_dashboard_search_data where employer_id = %s'
            count_values = (user_id,)
            total_count_dict = execute_query(get_count_query, count_values)
            if total_count_dict:
                total_count = total_count_dict[0]['count']
            else:
                total_count = 0
            get_user_ids = 'select user_id from pool_dashboard_search_data where employer_id = %s ORDER BY ranking_score DESC, user_id DESC LIMIT 10 OFFSET %s'
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
                    values_job_details.append((user_id,))
            # ------
            #         if req_data['functional_specification']:
            #             if "Others" in req_data['functional_specification']:
            #                 conditions.append("functional_specification NOT IN %s")
            #                 values_job_details.append(tuple(functional_specification_list),)
            #             else:
            #                 conditions.append("functional_specification IN %s")
            #                 values_job_details.append(tuple(req_data['functional_specification']),)
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
                        conditions.append("country IN %s")
                        values_job_details.append(tuple(country),)    
                    if city:
                        conditions.append("city IN %s")
                        values_job_details.append(tuple(city),)
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
                    if req_data['location_preference']:
                        conditions.append("location_preference IN %s")
                        values_job_details.append(tuple(req_data['location_preference'],))
                    search_query = '''SELECT user_id, gender, city, country, functional_specification, industry_sector, job_type,
                                    sector, location_preference, willing_to_relocate, is_active, flag, ranking_score, created_at
                                    FROM pool_dashboard_search_data
                                    WHERE employer_id IN %s'''
                    if conditions:
                        search_query += " AND " + " AND ".join(conditions)
                    new_query = search_query + " LIMIT 10 OFFSET %s"
                    val_detail = values_job_details
                    total_count_query = "SELECT subquery.user_id, COUNT(*) AS total_count FROM (" + search_query + ") AS subquery GROUP BY subquery.user_id"
                    total_count_values = (val_detail)
                    total_count = execute_query(total_count_query, total_count_values)
                    if len(total_count) > 0:
                        total_count = len(total_count)
                    else:
                        total_count = 0
                    values_job_details = values_job_details + [offset]
                    professional_details = replace_empty_values(execute_query(new_query, values_job_details))
                    if professional_details:
                        result_user_ids = [id['user_id'] for id in professional_details]
                    else:
                        result_user_ids = []
            else:
                result_json = api_json_response_format(True,"No records found",0,{})
                return result_json
        prof_details = []
        if result_user_ids:
            for temp_id in result_user_ids:
                query_job_details = """
                    SELECT 
                        p.id, 
                        p.professional_id, 
                        p.about,
                        (
                            SELECT CAST(ROUND(pd.ranking_score, 4) AS DECIMAL(10,4))
                            FROM pool_dashboard_search_data pd
                            WHERE pd.user_id = p.professional_id
                            ORDER BY pd.updated_at DESC
                            LIMIT 1
                        ) AS ranking_score,
                        -- Aggregate experiences into a JSON array
                        (
                            SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'experience_id', ordered_exp.id, 
                                    'company_name', ordered_exp.company_name, 
                                    'job_title', ordered_exp.job_title,
                                    'start_date', ordered_exp.start_year,
                                    'end_date', ordered_exp.end_year,
                                    'job_description', ordered_exp.job_description,
                                    'job_location', ordered_exp.job_location
                                )
                            )
                            FROM (
                                SELECT * 
                                FROM professional_experience 
                                WHERE professional_id = p.professional_id
                                ORDER BY 
                                    CASE 
                                        WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                        ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC
                            ) AS ordered_exp
                        ) AS experiences,

                        -- Aggregate education into a JSON array
                        (
                            SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'education_id', ordered_ed.id,
                                    'institute_name', ordered_ed.institute_name,
                                    'institute_location', ordered_ed.institute_location,
                                    'degree_level', ordered_ed.degree_level,
                                    'specialisation', ordered_ed.specialisation,
                                    'start_date', ordered_ed.start_year,
                                    'end_date', ordered_ed.end_year
                                )
                            )
                            FROM (
                                SELECT * 
                                FROM professional_education 
                                WHERE professional_id = p.professional_id
                                ORDER BY 
                                    CASE 
                                        WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                        ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                    END DESC,
                                    CASE 
                                        WHEN start_year = 'Present' THEN 9999 -- Treat "Present" as the highest value for start_year as well
                                        ELSE CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED)
                                    END DESC
                            ) AS ordered_ed
                        ) AS education,

                        -- Aggregate skills into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'skill_id', ps.id,
                                'skill_name', ps.skill_name,
                                'skill_level', ps.skill_level
                            )
                        )
                        FROM professional_skill AS ps 
                        WHERE ps.professional_id = p.professional_id
                        ) AS skills,

                        -- Aggregate languages into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'language_known', pl.language_known
                            )
                        )
                        FROM professional_language AS pl 
                        WHERE pl.professional_id = p.professional_id
                        ) AS languages,

                        -- Aggregate additional info into a JSON array
                        (SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'description', pad.description
                            )
                        )
                        FROM professional_additional_info AS pad 
                        WHERE pad.professional_id = p.professional_id
                        ) AS additional_info
                    FROM 
                        professional_profile AS p
                    JOIN 
                        users AS u ON p.professional_id = u.user_id AND u.email_active = 'Y'
                    WHERE
                        p.professional_id = %s
                    GROUP BY 
                        p.id, p.professional_id, p.about
                    ORDER BY 
                        p.id ASC;
                """
                values_job_details = (temp_id,)
                temp_result = replace_empty_values(execute_query(query_job_details, values_job_details))
                if temp_result:
                    prof_details.append(temp_result[0])
        else:
            prof_details = []

        if not prof_details:
            final_dict = {"final_result": [], "total_professionals": 0}
            result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
            return result_json

        final_result = []
        for row in prof_details:
            profile_dict = {
                'id': row['id'],
                'professional_id': f"2C-PR-{row['professional_id']}",
                'about': replace_empty_values1((row['about']) if row['about'] else []),
                'experience': json.loads(row['experiences']) if row['experiences'] else [],
                'education': json.loads(row['education']) if row['education'] else [],
                'skills': json.loads(row['skills']) if row['skills'] else [],
                'ranking_score' : row['ranking_score'] if row['ranking_score'] is not None else None
            }
            final_result.append(profile_dict)

        # final_result = [
        #     profile for profile in final_result 
        #     if not (
        #         profile['about'] == [] and 
        #         profile['education'] == [] and 
        #         profile['experience'] == [] and 
        #         profile['skills'] == []
        #     )
        # ]

        final_result = [
            profile for profile in final_result 
            if (profile['about'] and profile['about'].strip()) or 
            (profile['education'] and profile['education'] != "[]") or 
            (profile['experience'] and profile['experience'] != "[]") or 
            (profile['skills'] and profile['skills'] != "[]")
        ]

        if req_data['search_text'].startswith("2C"):
            # split_txt = search_text.split("-")
            # if len(split_txt) > 2:
            #     search_text = int(split_txt[2])
            for f in final_result:
                if f['professional_id'] == req_data['search_text']:
                    individual_user_detail = get_individual_user_detail(search_text)
                    if "error" not in individual_user_detail:
                        result_json = api_json_response_format(True, "User details fetched successfully", 0, individual_user_detail['user_detail'])
                        return result_json
                    else:
                        result_json = api_json_response_format(False, individual_user_detail["error"], 500, {})
                        return result_json

        if total_count > 0 and len(final_result)> 0 :
            final_dict = {"final_result": final_result, "total_professionals": total_count}
            result_json = api_json_response_format(True, "Details fetched successfully!", 200, final_dict)
        else:
            final_dict = {"final_result": [], "total_professionals": 0}
            result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
    except Exception as error:
        print(f"Error: {error}")
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:
        return result_json

def match_profiles(search_text, user_profiles):
    batch_size = 30
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
        # prompt = prompt.replace("{{batch}}", str(batch))
        # prompt = prompt.replace("{{search_text}}", search_text)
    
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
            # print(clean_code)
            batch_results = json.loads(clean_code)["user_details"]
            filtered_profiles.extend(batch_results)
        except Exception as e:
            print(f"Error in match_profiles(): {e}")
            return {"error": str(e)}
    return {"user_details": filtered_profiles}

def pool_dashboard_search_filter_new(request):
    try:
        result_json = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            req_data = request.get_json()
            if 'page_number' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            user_id = user_data['user_id']
            check_entries_in_db = "select count(id) from pool_dashboard_search_data where employer_id = %s"
            values = (user_id,)
            is_entries_presents_dict = execute_query(check_entries_in_db, values)
                
            page_number = req_data["page_number"]
            offset = (page_number - 1) * 10
            location = req_data["location"]
            gender = req_data['gender']
            industry_sector = req_data['industry_sector']
            sector = req_data['sector']
            job_type = req_data['job_type']
            willing_to_relocate = req_data['willing_to_relocate']
            location_preference = req_data['location_preference']
            functional_specification = req_data['functional_specification']
            flag = req_data['flag']
            pagination_flag = req_data['pagination_flag']
            search_text = req_data["search_text"]
            # if search_text.startswith("2C"):
            #     split_txt = search_text.split("-")
            #     if len(split_txt) > 2:
            #         search_text = str(split_txt[2])

            get_search_query_flag = 'select attribute_value from payment_config where attribute_name = %s'
            get_search_query_flag_values = ('pool_dashboard_search_query_flag',)
            search_query_flag_dict = execute_query(get_search_query_flag, get_search_query_flag_values)
            search_query_flag = 'N'
            if search_query_flag_dict:
                search_query_flag = search_query_flag_dict[0]['attribute_value']
            if search_text != '' and search_query_flag == 'Y':
                if search_text.startswith("2C"):
                    split_txt = search_text.split("-")
                    if len(split_txt) > 2:
                        search_text = str(split_txt[2])
                else:
                    search_query_value = get_search_query(search_text)
                    search_text = search_query_value['search_text']

            client = Client(MEILISEARCH_CLOUD_URL, MEILISEARCH_MASTER_KEY)
            index = client.index(MEILISEARCH_PROFESSIONAL_INDEX)

            if len(location) != 0:
                for i in range(len(location)):
                    res = location[i].split("&&&&&")
                    country.append(res[1])
                    city.append(res[0])

            functional_specification_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite and Board"]
            if flag != 'filter_in_search':
                if page_number == 1 and pagination_flag != 'first_page':
                    if is_entries_presents_dict:
                        if is_entries_presents_dict[0]['count(id)'] > 0:
                            delete_query = "delete from pool_dashboard_search_data where employer_id = %s"
                            values = (user_id,)
                            update_query(delete_query, values)
                    country_filter = country
                    city_filter = city
                    gender_filter = gender
                    industry_sector_filter =  industry_sector
                    sector_filter =  sector
                    job_type_filter =  job_type
                    willing_to_relocate_filter = willing_to_relocate
                    location_preference_filter = location_preference  
                    functional_specification_filter =  functional_specification

                    filters = []

                    if country_filter:
                        country_query = " OR ".join([f"country = '{country}'" for country in country_filter])
                        filters.append(f"({country_query})")

                    if city_filter:
                        city_query = " OR ".join([f"city = '{city}'" for city in city_filter])
                        filters.append(f"({city_query})")

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
                            'showRankingScore': True
                        }
                    )
                    ranking_score_limit = 0
                    if results['hits']:
                        if results['hits'][0]['_rankingScore'] > 0.15:
                            ranking_score_limit = results['hits'][0]['_rankingScore'] - 0.15

                    filtered_results = [hit for hit in results['hits'] if ranking_score_limit <= hit.get('_rankingScore', 0) <= 1]

                    # filtered_results = [hit for hit in results['hits'] if 0.5 <= hit.get('_rankingScore', 0) <= 1]
                    filtered_results = [
                                    profile for profile in filtered_results 
                                    if (profile['about'] and profile['about'].strip()) or 
                                    (profile['education_id'] and profile['education_id'] != "[]") or 
                                    (profile['experience_id'] and profile['experience_id'] != "[]") or 
                                    (profile['skill_id'] and profile['skill_id'] != "[]")
                                ]
                    if search_text != "":
                        open_ai_result = match_profiles(search_text, filtered_results)
                    else:
                        open_ai_result = {"user_details": filtered_results}

                    # open_ai_result = match_profiles(search_text, filtered_results)
                    if "error" in open_ai_result:
                        result_json = api_json_response_format(False, "Error in match_profiles()", 500, {})
                        return result_json
                    final_data = open_ai_result['user_details']
                    resultant_search_ids = [id['user_id'] for id in final_data]
                    if resultant_search_ids:
                        get_users_details_query = """SELECT u.user_id, u.country, u.city, u.gender, u.is_active, p.functional_specification, p.sector, p.industry_sector, 
                                                p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, p.years_of_experience 
                                                FROM users AS u 
                                                LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id 
                                                WHERE u.user_id IN %s;"""
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
                            record_insert_query = 'INSERT INTO pool_dashboard_search_data (employer_id, user_id, professional_id, gender, city, country, functional_specification, industry_sector, job_type, sector, location_preference, willing_to_relocate, is_active, flag, ranking_score) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                            professional_id = '2C-PR-' + str(record['user_id'])
                            values = (user_id, record['user_id'], professional_id, record['gender'], record['city'], record['country'],
                                        record['functional_specification'], record['industry_sector'], record['job_type'], record['sector'], record['location_preference'], 
                                        record['willing_to_relocate'], record['is_active'], 'sample', ranking_score,)
                            update_query(record_insert_query, values)

                get_count_query = 'select count(user_id) as count from pool_dashboard_search_data where employer_id = %s'
                count_values = (user_id,)
                total_count_dict = execute_query(get_count_query, count_values)
                if total_count_dict:
                    total_count = total_count_dict[0]['count']
                else:
                    total_count = 0
                get_user_ids = 'select user_id from pool_dashboard_search_data where employer_id = %s ORDER BY ranking_score DESC LIMIT 10 OFFSET %s'
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
                        values_job_details.append((user_id,))
                        # if req_data['functional_specification']:
                        #     if "Others" in req_data['functional_specification']:
                        #         conditions.append("functional_specification NOT IN %s")
                        #         values_job_details.append(tuple(functional_specification_list),)
                        #     else:
                        #         conditions.append("functional_specification IN %s")
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
                            conditions.append("country IN %s")
                            values_job_details.append(tuple(country),)    
                        if city:
                            conditions.append("city IN %s")
                            values_job_details.append(tuple(city),)
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
                        if req_data['location_preference']:
                            conditions.append("location_preference IN %s")
                            values_job_details.append(tuple(req_data['location_preference'],))
                        search_query = '''SELECT user_id, gender, city, country, functional_specification, industry_sector, job_type,
                                    sector, location_preference, willing_to_relocate, is_active, flag, created_at
                                    FROM pool_dashboard_search_data
                                    WHERE employer_id IN %s'''
                        if conditions:
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
                        values_job_details = values_job_details + [offset]
                        professional_details = replace_empty_values(execute_query(new_query, values_job_details))
                        if professional_details:
                            result_user_ids = [id['user_id'] for id in professional_details]
                        else:
                            result_user_ids = []
                else:
                    result_json = api_json_response_format(True,"No records found",0,{})
                    return result_json

            prof_details = []
            if result_user_ids:
                for temp_id in result_user_ids:
                    query_job_details = """
                        SELECT 
                            p.id, 
                            p.professional_id, 
                            p.about,
                            (
                                SELECT CAST(ROUND(pd.ranking_score, 4) AS DECIMAL(10,4))
                                FROM pool_dashboard_search_data pd
                                WHERE pd.user_id = p.professional_id
                                ORDER BY pd.updated_at DESC
                                LIMIT 1
                            ) AS ranking_score,
                            -- Aggregate experiences into a JSON array
                            (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'experience_id', ordered_exp.id, 
                                        'company_name', ordered_exp.company_name, 
                                        'job_title', ordered_exp.job_title,
                                        'start_date', ordered_exp.start_year,
                                        'end_date', ordered_exp.end_year,
                                        'job_description', ordered_exp.job_description,
                                        'job_location', ordered_exp.job_location
                                    )
                                )
                                FROM (
                                    SELECT * 
                                    FROM professional_experience 
                                    WHERE professional_id = p.professional_id
                                    ORDER BY 
                                        CASE 
                                            WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                            ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC
                                ) AS ordered_exp
                            ) AS experiences,

                            -- Aggregate education into a JSON array
                            (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'education_id', ordered_ed.id,
                                        'institute_name', ordered_ed.institute_name,
                                        'institute_location', ordered_ed.institute_location,
                                        'degree_level', ordered_ed.degree_level,
                                        'specialisation', ordered_ed.specialisation,
                                        'start_date', ordered_ed.start_year,
                                        'end_date', ordered_ed.end_year
                                    )
                                )
                                FROM (
                                    SELECT * 
                                    FROM professional_education 
                                    WHERE professional_id = p.professional_id
                                    ORDER BY 
                                        CASE 
                                            WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                            ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        CASE 
                                            WHEN start_year = 'Present' THEN 9999 -- Treat "Present" as the highest value for start_year as well
                                            ELSE CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED)
                                        END DESC
                                ) AS ordered_ed
                            ) AS education,

                            -- Aggregate skills into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'skill_id', ps.id,
                                    'skill_name', ps.skill_name,
                                    'skill_level', ps.skill_level
                                )
                            )
                            FROM professional_skill AS ps 
                            WHERE ps.professional_id = p.professional_id
                            ) AS skills,

                            -- Aggregate languages into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'language_known', pl.language_known
                                )
                            )
                            FROM professional_language AS pl 
                            WHERE pl.professional_id = p.professional_id
                            ) AS languages,

                            -- Aggregate additional info into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'description', pad.description
                                )
                            )
                            FROM professional_additional_info AS pad 
                            WHERE pad.professional_id = p.professional_id
                            ) AS additional_info
                        FROM 
                            professional_profile AS p
                        JOIN 
                            users AS u ON p.professional_id = u.user_id AND u.email_active = 'Y'
                        WHERE
                            p.professional_id = %s
                        GROUP BY 
                            p.id, p.professional_id, p.about
                        ORDER BY 
                            p.id ASC;
                    """
                    values_job_details = (temp_id,)
                    temp_result = replace_empty_values(execute_query(query_job_details, values_job_details))
                    if temp_result:
                        prof_details.append(temp_result[0])
            else:
                prof_details = []

            if not prof_details:
                final_dict = {"final_result": [], "total_professionals": 0}
                result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
                return result_json

            final_result = []
            for row in prof_details:
                profile_dict = {
                    'id': row['id'],
                    'professional_id': f"2C-PR-{row['professional_id']}",
                    'about': replace_empty_values1((row['about']) if row['about'] else []),
                    'experience': json.loads(row['experiences']) if row['experiences'] else [],
                    'education': json.loads(row['education']) if row['education'] else [],
                    'skills': json.loads(row['skills']) if row['skills'] else [],
                    'ranking_score' : row['ranking_score'] if row['ranking_score'] is not None else None
                }
                final_result.append(profile_dict)

            final_result = [
                profile for profile in final_result 
                if not (
                    profile['about'] == [] and 
                    profile['education'] == [] and 
                    profile['experience'] == [] and 
                    profile['skills'] == []
                )
            ]

            if req_data['search_text'].startswith("2C"):
            # split_txt = search_text.split("-")
            # if len(split_txt) > 2:
            #     search_text = int(split_txt[2])
                for f in final_result:
                    if f['professional_id'] == req_data['search_text']:
                        individual_user_detail = get_individual_user_detail(search_text)
                        if "error" not in individual_user_detail:
                            result_json = api_json_response_format(True, "User details fetched successfully", 0, individual_user_detail['user_detail'])
                            return result_json
                        else:
                            result_json = api_json_response_format(False, individual_user_detail["error"], 500, {})
                            return result_json

            if total_count > 0:
                final_dict = {"final_result": final_result, "total_professionals": total_count}
                result_json = api_json_response_format(True, "Details fetched successfully!", 200, final_dict)
            else:
                final_dict = {"final_result": [], "total_professionals": 0}
                result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(f"Error: {error}")
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:
        return result_json

def pool_dashboard_search():
    """
    Perform search operation to fetch professional profiles based on search criteria.
    """
    try:
        result_json = {}
        token_result = get_user_token(request)                                      
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
            user_role = user_data["user_role"]
            if user_role == "employer" or user_role == "partner" or user_role == "employer_sub_admin" or user_role == "recruiter":
                req_data = request.get_json()
                if 'id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json            
                if 'search_text' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                search_text = req_data['search_text']
                current_id = req_data['id']
                profiles_to_fetch = 5
                if search_text.startswith("2C"):
                    split_txt = search_text.split("-")
                    if len(split_txt) > 2:
                        search_text = (search_text.split("-")[2])
                search_term = f"%{search_text}%"

                base_query = """
                        SELECT 
                            p.id, 
                            p.professional_id, 
                            p.about,

                            -- Aggregate experiences into a JSON array
                            (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'experience_id', ordered_exp.id, 
                                        'company_name', ordered_exp.company_name, 
                                        'job_title', ordered_exp.job_title,
                                        'start_date', ordered_exp.start_year,
                                        'end_date', ordered_exp.end_year,
                                        'job_description', ordered_exp.job_description,
                                        'job_location', ordered_exp.job_location
                                    )
                                )
                                FROM (
                                    SELECT * 
                                    FROM professional_experience 
                                    WHERE professional_id = p.professional_id
                                    ORDER BY 
                                        CASE 
                                            WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                            ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC
                                ) AS ordered_exp
                            ) AS experiences,

                            -- Aggregate education into a JSON array
                            (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'education_id', ordered_ed.id,
                                        'institute_name', ordered_ed.institute_name,
                                        'institute_location', ordered_ed.institute_location,
                                        'degree_level', ordered_ed.degree_level,
                                        'specialisation', ordered_ed.specialisation,
                                        'start_date', ordered_ed.start_year,
                                        'end_date', ordered_ed.end_year
                                    )
                                )
                                FROM (
                                    SELECT * 
                                    FROM professional_education 
                                    WHERE professional_id = p.professional_id
                                    ORDER BY 
                                        -- First order by end_date, treating future dates as the latest
                                        CASE 
                                            WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                            ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        -- Then by start_date (if end_date is the same, order by start_date)
                                        CASE 
                                            WHEN start_year = 'Present' THEN 9999 -- Treat "Present" as the highest value for start_year as well
                                            ELSE CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED)
                                        END DESC
                                ) AS ordered_ed
                            ) AS education,

                            -- Aggregate skills into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'skill_id', ps.id,
                                    'skill_name', ps.skill_name,
                                    'skill_level', ps.skill_level
                                )
                            )
                            FROM professional_skill AS ps 
                            WHERE ps.professional_id = p.professional_id
                            ) AS skills,

                            -- Aggregate languages into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'language_known', pl.language_known
                                )
                            )
                            FROM professional_language AS pl 
                            WHERE pl.professional_id = p.professional_id
                            ) AS languages,

                            -- Aggregate additional info into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'description', pad.description
                                )
                            )
                            FROM professional_additional_info AS pad 
                            WHERE pad.professional_id = p.professional_id
                            ) AS additional_info
                        FROM 
                            professional_profile AS p
                        JOIN 
    			            users AS u ON p.professional_id = u.user_id AND u.email_active = 'Y'
                        WHERE
                            (p.about LIKE %s
                            OR p.professional_id LIKE %s
                            OR u.city LIKE %s
                            OR u.country LIKE %s
                            OR EXISTS (
                                SELECT 1 FROM professional_skill ps WHERE ps.professional_id = p.professional_id AND ps.skill_name LIKE %s
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_experience pe WHERE pe.professional_id = p.professional_id AND (pe.job_title LIKE %s OR pe.job_description LIKE %s)
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_education ed WHERE ed.professional_id = p.professional_id AND ed.specialisation LIKE %s
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_language pl WHERE pl.professional_id = p.professional_id AND pl.language_known LIKE %s
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_additional_info pad WHERE pad.professional_id = p.professional_id AND pad.description LIKE %s
                            )
                        ) 
                        GROUP BY 
                            p.id, p.professional_id, p.about
                        ORDER BY 
                            p.id ASC;
                        """
                # total_count_values = ((search_term,) * 8)
                # total_count = execute_query(base_query, total_count_values)
                profiles_query = base_query #+ "GROUP BY p.id, p.professional_id, p.about ORDER BY p.id ASC" #AND p.id > %s
                profiles_values = (search_term,) * 10
                profiles_result = execute_query(profiles_query, profiles_values)

                if len(profiles_result) > 0:
                    final_result = []
                    for row in profiles_result:
                        profile_dict = {
                            'id': row['id'],
                            'professional_id': f"2C-PR-{row['professional_id']}",
                            'about': replace_empty_values1((row['about']) if row['about'] else []),
                            'experience': json.loads(row['experiences']) if row['experiences'] else [],
                            'education': json.loads(row['education']) if row['education'] else [],
                            'skills': json.loads(row['skills']) if row['skills'] else []
                        }
                        final_result.append(profile_dict)
                    final_result = [
                        profile for profile in final_result 
                        if not (
                            profile['about'] == [] and 
                            profile['education'] == [] and 
                            profile['experience'] == [] and 
                            profile['skills'] == []
                        )
                    ]
                    count = len(final_result)
                    new_list = []
                    flag = 0
                    if len(final_result) > 0:
                        for j in final_result:
                            if flag < 5:
                                if j['id'] > current_id:
                                    new_list.append(j)
                                    flag = flag + 1
                            else:
                                break
                    # print(len(final_result))
                    if count > 0:
                        final_dict = {"final_result": new_list, "total_professionals": count}
                        result_json = api_json_response_format(True, "Details fetched successfully!", 200, final_dict)
                    else:
                        final_dict = {"final_result": [], "total_professionals": 0}
                        result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
                else:
                    final_dict = {"final_result": [], "total_professionals": 0}
                    result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
            else:
                result_json = api_json_response_format(False, "Unauthorized User", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        print(f"Error: {error}")
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:        
        return result_json
      
def fetch_employer_jobs():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            req_data = request.get_json()
            if 'professional_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            temp_professional_id = str(req_data['professional_id']) 
            temp_id = temp_professional_id.split('-')
            if len(temp_id) > 0:
                professional_id = temp_id[2]
            else:
                result_json = api_json_response_format(False, "Invalid professional id", 500, {})
                return result_json
            user_data = get_user_data(token_result["email_id"])
            owner_emp_id = ''
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data["sub_user_id"]
                owner_emp_id = user_data['user_id']
            else:
                employer_id = user_data['user_id']         
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                if user_data["user_role"] == "recruiter":
                    query = "SELECT jp.id, jp.job_title, CASE WHEN EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s ) THEN 'Applied' ELSE 'Not Applied' END AS job_status FROM job_post jp WHERE jp.employer_id = %s and jp.job_status ='opened';"
                    values = (professional_id, employer_id,)
                else:
                    sub_user_jobs = 'select sub_user_id from sub_users where user_id = %s'
                    if user_data['user_role'] == 'employer':
                        sub_user_job_values = (employer_id,)
                    else:
                        sub_user_job_values = (owner_emp_id,)
                    sub_users_dict = execute_query(sub_user_jobs, sub_user_job_values)
                    sub_user_list = []
                    if sub_users_dict:
                        sub_user_list = [s['sub_user_id'] for s in sub_users_dict]
                    if user_data['user_role'] == 'employer':
                        sub_user_list.append(employer_id)
                    else:
                        sub_user_list.append(owner_emp_id)
                    placeholders = ', '.join(['%s'] * len(sub_user_list))
                    query = f"SELECT jp.id, jp.job_title, CASE WHEN EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s ) THEN 'Applied' ELSE 'Not Applied' END AS job_status FROM job_post jp WHERE jp.employer_id IN ({placeholders}) and jp.job_status ='opened';"
                    values = (professional_id, *sub_user_list,)
                job_details = execute_query(query, values)
                for job in job_details:
                    job.update({'professional_id' : professional_id})
                result_json = api_json_response_format(True,"Jobs fetched successfully!",0,replace_empty_values(job_details))
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def invite_by_employer():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data["user_id"]    
            else:
                employer_id = user_data['user_id'] 
            # employer_id = user_data["user_id"]
            req_data = request.get_json()
            if 'professional_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'job_id' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'invite_from' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            invite_from = req_data['invite_from']
            employer_feedback = ''
            if 'employer_feedback' in req_data:
                employer_feedback = req_data['employer_feedback']
            professional_id = req_data['professional_id']
            job_id = req_data['job_id']

            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                query = 'select count(id) as count from invited_jobs where job_id = %s and professional_id = %s and employer_id = %s and is_invite_sent = %s' #invite_mode != %s
                values = (job_id, professional_id, employer_id, 'Y',)    #"from BFA"
                count_status = execute_query(query, values)
                if len(count_status) > 0:
                    if count_status[0]['count'] > 0:
                        result_json = api_json_response_format(False,"Job invitation already sent",204,{})
                        return result_json
                else:
                    result_json = api_json_response_format(False,"Something went wrong. Please try again.",204,{})
                    return result_json
                query = 'select job_title from job_post where id = %s'
                values = (job_id,)
                job_title_dict = execute_query(query, values)
                job_title = job_title_dict[0]['job_title'] if job_title_dict else ''
                if invite_from == 'pool_page':
                    query = 'select count(id) as count from invited_jobs where job_id = %s and professional_id = %s and employer_id = %s and is_invite_sent = %s and invite_mode = %s'
                    values = (job_id, professional_id, employer_id, 'N', "from BFA")
                    count_status = execute_query(query, values)
                    if len(count_status) > 0:
                        if count_status[0]['count'] > 0:
                            query = 'update invited_jobs set employer_feedback = %s, is_invite_sent = %s where job_id = %s and employer_id = %s and professional_id = %s'
                            values = (employer_feedback, 'Y', job_id, employer_id, professional_id,)
                            invited_job_details = update_query(query, values)
                            # result_json = api_json_response_format(False,"Job invitation already sent",204,{})
                            # return result_json
                        else:
                            query = 'insert into invited_jobs (job_id, professional_id, employer_id, employer_feedback, invite_mode, is_invite_sent) values (%s,%s,%s,%s,%s,%s)'
                            values = (job_id, professional_id, employer_id, employer_feedback, 'from pool page', 'Y',)
                            invited_job_details = update_query(query, values)
                    else:
                        result_json = api_json_response_format(False,"Something went wrong. Please try again.",204,{})
                        return result_json
                    # query = 'insert into invited_jobs (job_id, professional_id, employer_id, employer_feedback, invite_mode, is_invite_sent) values (%s,%s,%s,%s,%s,%s)'
                    # values = (job_id, professional_id, employer_id, employer_feedback, 'from pool page', 'Y',)
                    # invited_job_details = update_query(query, values)
                else:
                    query = 'update invited_jobs set employer_feedback = %s, is_invite_sent = %s where job_id = %s and employer_id = %s and professional_id = %s'
                    values = (employer_feedback, 'Y', job_id, employer_id, professional_id,)
                    invited_job_details = update_query(query, values)
                if invited_job_details > 0:
                    # query = 'select is_invite_sent from invited_jobs where professional_id = %s and job_id = %s'
                    # values = (professional_id, job_id,)
                    # invite_sent = execute_query(query, values)
                    # if len(invite_sent) > 0:
                    #     if invite_sent[0]['is_invite_sent'] == 'Y':
                    #         result_json = api_json_response_format(False,"Job invitation already sent",204,{})
                    #         return result_json
                    query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                    created_at = datetime.now()                    
                    values = (professional_id, f"You have been invited to apply for the position of '{job_title}'. We believe your profile is a great match for this opportunity.", created_at,)
                    update_query(query,values)
                    background_runner.send_email_job_apply_invite(professional_id, employer_id, job_id)
                    result_json = api_json_response_format(True,"Job apply invitation sent successfully!",0,{})
                else:
                    result_json = api_json_response_format(False,"Failed to send job apply invitation",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def best_fit_applicants():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data['user_id']
            else:
                employer_id = user_data['user_id']
            user_role = ""
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "admin":
                user_role = user_data["user_role"]
                final_result = []
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False, "Please fill in all the required fields(job_id).", 409, {})
                    return result_json
                
                job_id = req_data['job_id']

                if user_data['user_role'] == 'admin':
                    get_employer_id_query = "select employer_id from job_post where id = %s"
                    get_employer_id_values = (job_id,)
                    employer_id = execute_query(get_employer_id_query, get_employer_id_values)
                    if employer_id:
                        employer_id = employer_id[0]['employer_id']
                    else:
                        result_json = api_json_response_format(False, "Employer ID not found for the job.", 404, {})
                        return result_json
                employer_id_list = []
                get_sub_users_id = "select sub_user_id from sub_users where user_id = %s"
                sub_user_values = (employer_id,)
                sub_users_dict = execute_query(get_sub_users_id, sub_user_values)
                if sub_users_dict:
                    employer_id_list = [s['sub_user_id'] for s in sub_users_dict]
                employer_id_list.append(employer_id)
                get_job_title_query = "select job_title from job_post where id = %s"
                job_title_dict = execute_query(get_job_title_query, (job_id,))
                if job_title_dict:
                    job_title = job_title_dict[0]['job_title']
                else:
                    job_title = ''

                recomeded = employer_candidate_recommended(job_id)
                ids = list(set(data["id"] for data in recomeded))
                # if user_data['user_role'] == 'admin':
                for i in ids:
                    query = 'select count(id) from invited_jobs where job_id = %s and employer_id IN %s and professional_id = %s'
                    values = (job_id, tuple(employer_id_list), i,)
                    count = execute_query(query, values)
                    if count:
                        if count[0]['count(id)'] == 0:
                            query = 'insert into invited_jobs (job_id, professional_id, employer_id, invite_mode) values (%s,%s,%s,%s)'
                            values = (job_id, i, employer_id, 'from BFA',)
                            update_query(query, values)
                        
                query = '''SELECT professional_id FROM invited_jobs ij WHERE job_id = %s AND employer_id IN %s AND invite_mode = %s 
                            AND professional_id NOT IN (101089, 101091, 101070) 
                            AND NOT EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.professional_id = ij.professional_id AND ja.job_id = ij.job_id ) LIMIT 10'''
                values = (job_id, tuple(employer_id_list), 'from BFA',)
                professional_id_dict = execute_query(query, values)
                new_id_list = []
                if professional_id_dict:
                    new_id_list = [j['professional_id'] for j in professional_id_dict]
                else:
                    new_id_list = []
                
                query = 'SELECT professional_id FROM invited_jobs ij WHERE job_id = %s AND employer_id IN %s AND invite_mode = %s'
                values = (job_id, tuple(employer_id_list), 'from 2ndC',)
                prof_id_dict_2 = execute_query(query, values)
                if prof_id_dict_2:
                    for j in prof_id_dict_2:
                        new_id_list.append(j['professional_id'])

                # else:
                #     new_id_list = ids
                
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
                        event_properties = background_runner.process_dict(user_data["email_id"], "Best Fit Applicants", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Best Fit Applicants",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: {user_role} Best Fit Applicants, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, final_dict)
                    return result_json
                for obj in final_result:
                    temp_id = obj['professional_id'].split('-')
                    if len(temp_id) > 0:
                        temp_professional_id = temp_id[2]
                        get_invited_mode_query = 'select invite_mode from invited_jobs where professional_id = %s and job_id = %s and is_invite_sent = %s'
                        values = (temp_professional_id, job_id, 'N',)
                        invited_mode_dict = execute_query(get_invited_mode_query, values)
                        if invited_mode_dict:
                            obj.update({'invite_mode' : invited_mode_dict[0]['invite_mode'] })
                        else:
                            obj.update({'invite_mode' : ''})

                        if obj['invite_mode'] == 'from 2ndC':
                            get_admin_description_query = 'select employer_feedback from invited_jobs where professional_id = %s and job_id = %s and invite_mode = %s'
                            values = (int(temp_professional_id), job_id, 'from 2ndC',)
                            admin_description_dict = execute_query(get_admin_description_query, values)
                            if admin_description_dict:
                                obj.update({'recommendation_notes' : admin_description_dict[0]['employer_feedback'] })
                            else:
                                obj.update({'recommendation_notes' : ''})
                    else:
                        obj.update({'invite_mode' : ''})
                        obj.update({'recommendation_notes' : ''})
                final_result = sorted(final_result, key=lambda x: (x['invite_mode'] != 'from 2ndC'))
                final_dict = {
                    "final_result": final_result,
                    "job_title" : job_title
                }
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': "Professionals details displayed successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Best Fit Applicants", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Best Fit Applicants",event_properties, temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: {user_role} Best Fit applicants, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, final_dict)
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching professional details on the {user_role} Best Fit Applicants."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Best Fit Applicants Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Best Fit Applicants Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: {user_role} Best Fit Applicants Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:
        return result_json


def invited_applicants_view():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data['user_id']
            else:
                employer_id = user_data['user_id']
            user_role = ""     
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "admin":
                user_role = user_data["user_role"]
                final_result = []
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False, "Please fill in all the required fields(job_id).", 409, {})
                    return result_json
                
                job_id = req_data['job_id']
                if user_data['user_role'] == 'admin':
                    get_emp_id_query = 'select employer_id from job_post where id = %s'
                    values = (job_id,)
                    get_emp_id_result = execute_query(get_emp_id_query, values)
                    if get_emp_id_result:
                        employer_id = get_emp_id_result[0]['employer_id']
                    if employer_id > 500000:
                        get_owner_emp_id = 'select user_id from sub_users where sub_user_id = %s'
                        values = (employer_id,)
                        get_owner_emp_id_result = execute_query(get_owner_emp_id, values)
                        if get_owner_emp_id_result:
                            employer_id = get_owner_emp_id_result[0]['user_id']
                    
                get_job_title_query = "select job_title from job_post where id = %s"
                job_title_dict = execute_query(get_job_title_query, (job_id,))
                if job_title_dict:
                    job_title = job_title_dict[0]['job_title']
                else:
                    job_title = ''
                query = "SELECT ij.*, CASE WHEN EXISTS (SELECT 1 FROM job_activity ja WHERE ja.professional_id = ij.professional_id AND ja.job_id = ij.job_id ) THEN 'applied' ELSE 'not applied' END AS applied_status FROM invited_jobs ij WHERE ij.job_id = %s AND ij.employer_id = %s AND ij.is_invite_sent = %s ORDER BY ij.id DESC;"
                values = (job_id, employer_id, 'Y',)
                details = execute_query(query, values)
                
                for d in details:
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
                    values = (d['professional_id'], d['professional_id'], d['professional_id'],)
                    profile_result = execute_query(query, values)

                    if len(profile_result) == 0:
                        continue

                    profile_dict = {
                        'id': profile_result[0]['id'],
                        'professional_id': "2C-PR-" + str(profile_result[0]['professional_id']),
                        'about': replace_empty_values1(profile_result[0]['about']),
                        'experience': [],
                        'education': [],
                        'skills': [],
                        'invited_mode' : d['invite_mode'],
                        'feedback' : d['employer_feedback'],
                        'applied_status' : d['applied_status']
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
                    if profile_dict['skills'] or profile_dict['education'] or profile_dict['experience']:
                        final_result.append(profile_dict)
                if not final_result:
                    final_dict = {
                        "final_result": [],
                        "job_title" : job_title,
                        "total_professionals": 0
                    }
                    try:
                        temp_dict = {'Message': "There is no professionals to display."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Invited Applicants View", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Invited Applicants View",event_properties, temp_dict.get("Message"),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: {user_role} Invited Applicants View, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, final_dict)
                    return result_json
                
                final_dict = {
                    "final_result": final_result,
                    "job_title" : job_title
                }
                try:
                    temp_dict = {'Message': "Professionals details displayed successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Invited Applicants View", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Invited Applicants View",event_properties,temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: {user_role} Invited Applicants View, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, final_dict)
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching professional details on the {user_role} Invited Applicants View."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Invited Applicants View Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Invited Applicants View Error",event_properties, temp_dict.get("Message"),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: {user_role} Invited Applicants View Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:
        return result_json  

def invited_applicants_search():
    try:
        result_json = {}
        token_result = get_user_token(request)                                      
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id = user_data['user_id']
            else:
                employer_id = user_data['user_id']
            user_role = user_data["user_role"]
            if user_role == "employer" or user_role == "partner" or user_role == "employer_sub_admin" or user_role == "recruiter" or user_data["user_role"] == "admin":
                # employer_id = user_data['user_id']
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json            
                if 'search_text' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'search_req_from' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                
                job_id = req_data['job_id']
                
                if user_data['user_role'] == 'admin':
                    get_emp_id_query = 'select employer_id from job_post where id = %s'
                    values = (job_id,)
                    get_emp_id_result = execute_query(get_emp_id_query, values)
                    if get_emp_id_result:
                        employer_id = get_emp_id_result[0]['employer_id']
                    if employer_id > 500000:
                        get_owner_emp_id = 'select user_id from sub_users where sub_user_id = %s'
                        values = (employer_id,)
                        get_owner_emp_id_result = execute_query(get_owner_emp_id, values)
                        if get_owner_emp_id_result:
                            employer_id = get_owner_emp_id_result[0]['user_id']

                search_text = req_data['search_text']
                search_req_from = req_data['search_req_from']

                if search_text.startswith("2C"):
                    split_txt = search_text.split("-")
                    if len(split_txt) > 2:
                        search_text = (search_text.split("-")[2])
                search_term = f"%{search_text}%"

                if search_req_from == 'best_fit_applicants':
                    query = 'select professional_id from invited_jobs ij where ij.job_id = %s and ij.employer_id = %s and ij.invite_mode = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.professional_id = ij.professional_id AND ja.job_id = ij.job_id )'
                    values = (job_id, employer_id, 'from BFA',)
                    details = execute_query(query, values)
                else:
                    query = "SELECT ij.*, CASE WHEN EXISTS (SELECT 1 FROM job_activity ja WHERE ja.professional_id = ij.professional_id AND ja.job_id = ij.job_id ) THEN 'applied' ELSE 'not applied' END AS applied_status FROM invited_jobs ij WHERE ij.job_id = %s AND ij.employer_id = %s AND ij.is_invite_sent = %s ORDER BY ij.id DESC;"
                    values = (job_id, employer_id, 'Y',)
                    details = execute_query(query, values)
                    invite_mode_map = {row['professional_id']: row.get('invite_mode', '') for row in details}
                    applied_status_map = {row['professional_id']: row.get('applied_status', '') for row in details}
                professional_id_list = [id['professional_id'] for id in details]
                id_placeholders = ', '.join(['%s'] * len(professional_id_list))
                base_query = f"""
                        SELECT 
                            p.id, 
                            p.professional_id, 
                            p.about,

                            -- Aggregate experiences into a JSON array
                            (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'experience_id', ordered_exp.id, 
                                        'company_name', ordered_exp.company_name, 
                                        'job_title', ordered_exp.job_title,
                                        'start_date', ordered_exp.start_year,
                                        'end_date', ordered_exp.end_year,
                                        'job_description', ordered_exp.job_description,
                                        'job_location', ordered_exp.job_location
                                    )
                                )
                                FROM (
                                    SELECT * 
                                    FROM professional_experience 
                                    WHERE professional_id = p.professional_id
                                    ORDER BY 
                                        CASE 
                                            WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                            ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED) DESC
                                ) AS ordered_exp
                            ) AS experiences,

                            -- Aggregate education into a JSON array
                            (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'education_id', ordered_ed.id,
                                        'institute_name', ordered_ed.institute_name,
                                        'institute_location', ordered_ed.institute_location,
                                        'degree_level', ordered_ed.degree_level,
                                        'specialisation', ordered_ed.specialisation,
                                        'start_date', ordered_ed.start_year,
                                        'end_date', ordered_ed.end_year
                                    )
                                )
                                FROM (
                                    SELECT * 
                                    FROM professional_education 
                                    WHERE professional_id = p.professional_id
                                    ORDER BY 
                                        -- First order by end_date, treating future dates as the latest
                                        CASE 
                                            WHEN end_year = 'Present' THEN 9999 -- Treat "Present" as the highest value
                                            ELSE CAST(SUBSTRING_INDEX(end_year, '-', 1) AS UNSIGNED) 
                                        END DESC,
                                        -- Then by start_date (if end_date is the same, order by start_date)
                                        CASE 
                                            WHEN start_year = 'Present' THEN 9999 -- Treat "Present" as the highest value for start_year as well
                                            ELSE CAST(SUBSTRING_INDEX(start_year, '-', 1) AS UNSIGNED)
                                        END DESC
                                ) AS ordered_ed
                            ) AS education,

                            -- Aggregate skills into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'skill_id', ps.id,
                                    'skill_name', ps.skill_name,
                                    'skill_level', ps.skill_level
                                )
                            )
                            FROM professional_skill AS ps 
                            WHERE ps.professional_id = p.professional_id
                            ) AS skills,

                            -- Aggregate languages into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'language_known', pl.language_known
                                )
                            )
                            FROM professional_language AS pl 
                            WHERE pl.professional_id = p.professional_id
                            ) AS languages,

                            -- Aggregate additional info into a JSON array
                            (SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'description', pad.description
                                )
                            )
                            FROM professional_additional_info AS pad 
                            WHERE pad.professional_id = p.professional_id
                            ) AS additional_info
                        FROM 
                            professional_profile AS p
                        WHERE 
                            p.professional_id IN ({id_placeholders}) AND
                            (p.about LIKE %s
                            OR p.professional_id LIKE %s
                            OR EXISTS (
                                SELECT 1 FROM professional_skill ps WHERE ps.professional_id = p.professional_id AND ps.skill_name LIKE %s
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_experience pe WHERE pe.professional_id = p.professional_id AND (pe.job_title LIKE %s OR pe.job_description LIKE %s)
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_education ed WHERE ed.professional_id = p.professional_id AND ed.specialisation LIKE %s
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_language pl WHERE pl.professional_id = p.professional_id AND pl.language_known LIKE %s
                            )
                            OR EXISTS (
                                SELECT 1 FROM professional_additional_info pad WHERE pad.professional_id = p.professional_id AND pad.description LIKE %s
                            )
                        ) 
                        GROUP BY 
                            p.id, p.professional_id, p.about
                        ORDER BY 
                            p.id ASC;
                        """
                # total_count_values = ((search_term,) * 8)
                # total_count = execute_query(base_query, total_count_values)
                profiles_query = base_query #+ "GROUP BY p.id, p.professional_id, p.about ORDER BY p.id ASC" #AND p.id > %s
                # profiles_values = (search_term,) * 8
                profiles_values = (*professional_id_list, *(search_term,) * 8)
                if id_placeholders:
                    profiles_result = execute_query(profiles_query, profiles_values)
                else:
                    profiles_result = []

                if len(profiles_result) > 0:
                    final_result = []
                    for row in profiles_result:
                        profile_dict = {
                            'id': row['id'],
                            'professional_id': f"2C-PR-{row['professional_id']}",
                            'about': replace_empty_values1((row['about']) if row['about'] else []),
                            'experience': json.loads(row['experiences']) if row['experiences'] else [],
                            'education': json.loads(row['education']) if row['education'] else [],
                            'skills': json.loads(row['skills']) if row['skills'] else []
                        }
                        if search_req_from == '':
                            profile_dict['invited_mode'] = invite_mode_map.get(row['professional_id'], '')
                            profile_dict['applied_status'] = applied_status_map.get(row['professional_id'], '')
                    
                        final_result.append(profile_dict)
                    final_result = [
                        profile for profile in final_result 
                        if not (
                            profile['about'] == [] and 
                            profile['education'] == [] and 
                            profile['experience'] == [] and 
                            profile['skills'] == []
                        )
                    ]
                    count = len(final_result)
                    if count > 0:
                        final_dict = {"final_result": final_result, "total_professionals": count}
                        result_json = api_json_response_format(True, "Details fetched successfully!", 200, final_dict)
                    else:
                        final_dict = {"final_result": [], "total_professionals": 0}
                        result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
                else:
                    final_dict = {"final_result": [], "total_professionals": 0}
                    result_json = api_json_response_format(False, "No professionals found!", 200, final_dict)
            else:
                result_json = api_json_response_format(False, "Unauthorized User", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        print(f"Error: {error}")
        result_json = api_json_response_format(False, str(error), 500, error)
    finally:        
        return result_json


# def store_customer_gst():
#     try:
#         result_json = {}
#         token_result = get_user_token(request)                                        
#         if token_result["status_code"] == 200:                                
#             user_data = get_user_data(token_result["email_id"])        
#             if user_data["is_exist"]:
#                 if user_data["user_role"] == "employer":
                    
#                     req_data = request.form
#                     name_of_the_form = req_data.get('name_of_the_form')
#                     first_name = req_data.get('first_name')
#                     last_name = req_data.get('last_name')
#                     address = req_data.get('address')
#                     country = req_data.get('country')
#                     GSTIN = req_data.get('GSTIN')
#                     employer_id = user_data['user_id']
#                     created_at = datetime.now()
#                     if not name_of_the_form:
#                         result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#                         return result_json

#                     if not first_name:
#                         result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#                         return result_json
#                     if not last_name:
#                         result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#                         return result_json
#                     if not address:
#                         result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#                         return result_json
#                     if not country:
#                         result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#                         return result_json
#                     if not GSTIN:
#                         result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
#                         return result_json
                
#                     query = "insert into store_customer_gst(employer_id, `name_of_the_form`,`first_name`,`last_name`, `address`, `country`,`GSTIN`, created_at) values(%s, %s, %s, %s, %s, %s, %s, %s)"
#                     values = (employer_id, name_of_the_form,first_name,last_name, address, country, GSTIN,created_at)
#                     res = update_query(query,values)
                    
#                 else:
#                     result_json = api_json_response_format(False,"Unauthorized user",401,{})
#             else:
#                 result_json = api_json_response_format(False,"Unauthorized user",401,{})
#         else:
#                 result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})

                
#     except Exception as error:
#         print(error)        
#         result_json = api_json_response_format(False,str(error),500,{})        
#     finally:        
#         return result_json
    
# def get_stored_customer_gst_details():
#     try:
#         result_json = {}
#         token_result = get_user_token(request)                                        
#         if token_result["status_code"] == 200:                                
#             user_data = get_user_data(token_result["email_id"])        
#             if user_data["is_exist"]:
#                 if user_data["user_role"] == "employer":
#                     employer_id = user_data['user_id']
#                     query = "SELECT `employer_id`,`name_of_the_form`,`first_name`, `last_name`, `address`, `country`, `GSTIN`, `created_at` FROM `store_customer_gst` where employer_id = %s"
#                     values = (employer_id,)
#                     res = execute_query(query, values)
#                     result_json = api_json_response_format(True, "details fetched successfully", 200, res)

#                 else:
#                     result_json = api_json_response_format(False,"Unauthorized user",401,{})
#             else:
#                 result_json = api_json_response_format(False,"Unauthorized user",401,{})
#         else:
#                 result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})

                
#     except Exception as error:
#         print(error)        
#         result_json = api_json_response_format(False,str(error),500,{})        
#     finally:        
#         return result_json

def new_home_dashboard_view():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            sub_user_id = ''
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result['email_id'])
                employer_id =  user_data['user_id']
                sub_user_id = user_data['sub_user_id']
                owner_emp_id = employer_id
            else:
                employer_id =  user_data['user_id']
                owner_emp_id = employer_id
            cost_to_extend = int(os.environ.get('COST_TO_EXTEND'))
            cost_per_job = int(os.environ.get('COST_PER_JOB'))

            req_data = request.get_json()
            if 'job_status' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            job_status = req_data["job_status"]
            get_sub_users_query = 'select sub_user_id from sub_users where user_id = %s'
            get_sub_users_values = (employer_id,)
            sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
            sub_users = []
            if sub_users_dict:
                for sub_user in sub_users_dict:
                    sub_users.append(sub_user['sub_user_id'])
            sub_users.append(employer_id)
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin":  
                # print(sub_users)
                job_stats_list = []
                query = """
                    SELECT 
                        jp.employer_id,
                        jp.id AS job_id,
                        jp.job_title,
                        jp.receive_notification,
                        jp.job_closed_status_flag,
                        jp.created_at AS posted_on,
                        COALESCE(vc.view_count, 0) AS view_count,
                        COALESCE(ja.applied_count, 0) AS applied_count,
                        COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                        COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                        GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left
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
                            SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count
                        FROM 
                            job_activity
                        GROUP BY 
                            job_id
                    ) ja ON jp.id = ja.job_id
                    WHERE 
                        jp.employer_id IN %s
                        AND jp.job_status = %s
                    ORDER BY 
                        jp.id DESC;
                """
                values = (tuple(sub_users),job_status,)
                job_stats_list = execute_query(query, values)
                if job_stats_list:
                    for job in job_stats_list:
                        job_id = job['job_id']
                        check_query = 'select user_id from assigned_jobs where job_id = %s'
                        check_values = (job_id,)
                        is_assigned = execute_query(check_query, check_values)
                        if is_assigned:
                            check_sub_users_query = 'SELECT first_name, last_name FROM sub_users WHERE sub_user_id = %s'
                            values = (is_assigned[0]['user_id'],)
                            assigned_user_name = execute_query(check_sub_users_query, values)
                            if not assigned_user_name:
                                check_users_query = 'SELECT first_name, last_name FROM users WHERE user_id = %s'
                                values = (is_assigned[0]['user_id'],)
                                assigned_user_name = execute_query(check_users_query, values)
                                if assigned_user_name:
                                    job['assigned_user_name'] = assigned_user_name[0]['first_name'] + " " +assigned_user_name[0]['last_name']
                                else:
                                    job['assigned_user_name'] = ''
                            else:
                                job['assigned_user_name'] = assigned_user_name[0]['first_name'] + " " +assigned_user_name[0]['last_name']
                        else:
                            job['assigned_user_name'] = ''
                        get_recruiters_query = 'select sub_user_id, first_name, last_name AS full_name from sub_users where user_id = %s'
                        values = (employer_id,)
                        recruiters = execute_query(get_recruiters_query, values)
                        if recruiters:
                            job.update({"recruiters" : recruiters})
                            # job.update({"employer_id" : employer_id})
                        else:
                            job.update({"recruiters" : []})
                            # job.update({"employer_id" : employer_id})
            elif user_data['user_role'] == 'recruiter':
                get_job_ids_query = 'SELECT aj.job_id, jp.job_status FROM assigned_jobs aj JOIN job_post jp ON aj.job_id = jp.id WHERE aj.user_id = %s AND jp.job_status = %s;'
                get_job_ids_values = (sub_user_id, job_status,)
                job_ids_dict = execute_query(get_job_ids_query, get_job_ids_values)
                get_posted_job_ids_query = 'select id from job_post where employer_id = %s and job_status = %s'
                get_posted_job_ids_values = (sub_user_id, job_status,)
                posted_job_ids_dict = execute_query(get_posted_job_ids_query, get_posted_job_ids_values)
                job_id_list = []
                for i in job_ids_dict:
                    job_id_list.append(i['job_id'])
                for j in posted_job_ids_dict:
                    job_id_list.append(j['id'])
                query = """
                    SELECT 
                        jp.id AS job_id,
                        jp.job_title,
                        jp.receive_notification,
                        jp.job_closed_status_flag,
                        jp.created_at AS posted_on,
                        COALESCE(vc.view_count, 0) AS view_count,
                        COALESCE(ja.applied_count, 0) AS applied_count,
                        COALESCE(CAST(ja.not_reviewed_count AS SIGNED), 0) AS not_reviewed_count,
                        COALESCE(CAST(ja.shortlist_count AS SIGNED), 0) AS shortlisted_count,
                        GREATEST(jp.days_left - DATEDIFF(CURDATE(), jp.created_at), 0) AS days_left
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
                            SUM(CASE WHEN application_status = 'Shortlisted' THEN 1 ELSE 0 END) AS shortlist_count
                        FROM 
                            job_activity
                        GROUP BY 
                            job_id
                    ) ja ON jp.id = ja.job_id
                    WHERE 
                        jp.id IN %s
                        AND jp.job_status = %s
                    ORDER BY 
                        jp.id DESC;
                """
                values = (tuple(job_id_list), job_status,)
                job_stats_list = execute_query(query, values)
                for j in job_stats_list:
                    j.update({"employer_id" : employer_id})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
                return result_json
            
            jobs_left_query ="SELECT COUNT(jp.id) AS opened_jobs_count, MAX(COALESCE(u.pricing_category, su.pricing_category)) AS pricing_category FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE COALESCE(u.user_id, su.sub_user_id) IN %s AND (jp.job_status = %s OR jp.job_status = %s);"
            values = (tuple(sub_users), 'opened', 'paused',)
            opened_jobs_dict = execute_query(jobs_left_query, values)

            total_jobs = 0 
            get_total_jobs = 'select * from user_plan_details where user_id = %s'
            values = (employer_id,)
            total_jobs_dict = execute_query(get_total_jobs, values)
            if total_jobs_dict:
                additional_jobs_count = int(total_jobs_dict[0]['additional_jobs_count'])
                total_jobs = total_jobs_dict[0]['total_jobs'] + additional_jobs_count

                assisted_jobs_allowed = int(total_jobs_dict[0]['assisted_jobs_allowed'])
                assisted_jobs_used	= int(total_jobs_dict[0]['assisted_jobs_used'])

                assisted_jobs = assisted_jobs_allowed - assisted_jobs_used


            opened_jobs = 0
            user_plan = 'trialing'
            
            if len(opened_jobs_dict) > 0:
                opened_jobs = opened_jobs_dict[0]['opened_jobs_count']
                user_plan = opened_jobs_dict[0]['pricing_category']
            # job_left = total_jobs - opened_jobs
            
            # Get jobs left from user_plan_details
            jobs_left_query = "SELECT no_of_jobs FROM user_plan_details WHERE user_id = %s"
            job_left_res = execute_query(jobs_left_query, (employer_id,))
            

            job_left = 0
            # Check the number of jobs left in the user's plan
            if(job_left_res and job_left_res[0]['no_of_jobs'] > 0):
                job_left = job_left_res[0]['no_of_jobs']

            result_list = {}
            
            check_customer_query = 'select id from stripe_customers where email = %s'
            values = (user_data['email_id'],)
            customer_result = execute_query(check_customer_query, values)

            user_payment_profile_query = "SELECT id FROM user_payment_profiles WHERE email_id = %s AND gateway = %s"
            values = (user_data['email_id'], 'stripe',)
            payment_profile_result = execute_query(user_payment_profile_query, values)

            if customer_result or payment_profile_result:
                from_stripe = "yes"
            else:
                from_stripe = "no"

            if user_data['user_role'] == 'employer_sub_admin' or user_data['user_role'] == 'recruiter':
                check_welcome_count = 'select welcome_count as emp_welcome_count from sub_users where sub_user_id = %s'
                values = (sub_user_id,)
            elif user_data['user_role'] == 'employer':
                check_welcome_count = 'select emp_welcome_count from employer_profile where employer_id = %s'
                values = (employer_id,)
            
            welcome_count_dict = execute_query(check_welcome_count, values)
            if welcome_count_dict:
                welcome_count = welcome_count_dict[0]['emp_welcome_count']
                if welcome_count == 0:
                    result_dict = {
                        "job_list": job_stats_list,
                        "jobs_left": job_left,
                        "cost_per_job": cost_per_job,
                        "cost_to_extend": cost_to_extend,
                        "total_jobs": total_jobs,
                        "assisted_jobs": assisted_jobs_allowed,
                        "assisted_jobs_left": assisted_jobs,
                        "user_plan": user_plan,
                        "email_id": user_data['email_id'],
                        "pricing_category": user_data['pricing_category'],
                        "payment_status": user_data['payment_status'],
                        "profile_image": s3_employer_picture_folder_name + str(user_data['profile_image']),
                        "from_stripe": from_stripe
                    }

                    try:
                        temp_dict = {'Jobs Left' : job_left,
                                    'Total Jobs' : total_jobs,
                                    'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties,temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 200, result_dict)
                    if user_data['user_role'] == 'recruiter' or user_data['user_role'] == 'employer_sub_admin':
                        query = 'update sub_users set welcome_count = %s where sub_user_id = %s'
                        values = (1, sub_user_id,)
                    else:
                        query = 'update employer_profile set emp_welcome_count = %s where employer_id = %s'
                        values = (1, employer_id,)
                    temp = update_query(query, values)
                else:
                    result_list = {
                        "job_list": job_stats_list,
                        "jobs_left": job_left,
                        "cost_per_job": cost_per_job,
                        "cost_to_extend": cost_to_extend,
                        "total_jobs": total_jobs,
                        "assisted_jobs": assisted_jobs_allowed,
                        "assisted_jobs_left": assisted_jobs,
                        "user_plan": user_plan,
                        "email_id": user_data["email_id"],
                        "pricing_category": user_data["pricing_category"],
                        "payment_status": user_data["payment_status"],
                        "profile_image": s3_employer_picture_folder_name + str(user_data["profile_image"]),
                        "from_stripe": from_stripe
                    }

                    try:
                        temp_dict = {'Jobs Left' : job_left,
                                    'Total Jobs' : total_jobs,
                                    'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties, temp_dict.get('Message'),user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, result_list)
            else:
                result_list = {
                        "job_list": job_stats_list,
                        "jobs_left": job_left,
                        "cost_per_job": cost_per_job,
                        "cost_to_extend": cost_to_extend,
                        "total_jobs": total_jobs,
                        "assisted_jobs": assisted_jobs_allowed,
                        "assisted_jobs_left": assisted_jobs,
                        "user_plan": user_plan,
                        "email_id": user_data['email_id'],
                        "pricing_category": user_data['pricing_category'],
                        "payment_status": user_data['payment_status'],
                        "profile_image": s3_employer_picture_folder_name + str(user_data['profile_image']),
                        "from_stripe": from_stripe
                    }

                try:
                    temp_dict = {'Jobs Left' : job_left,
                                'Total Jobs' : total_jobs,
                                'Message': f"User {user_data['email_id']}'s home dashboard details fetched successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page",event_properties, temp_dict.get('Message'),user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Get Employer Home Page, {str(e)}")
                print(result_list)
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, result_list)
            # else:
            #     result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching user {user_data['email_id']}'s home dashboard details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Get Employer Home Page Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Get Employer Home Page Error",event_properties, temp_dict.get('Message'),user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Get Employer Home Page Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

# def transfer_jobs_to_owner(employer_id: int, reason: str):
#     """Transfer jobs + maintain log"""
#     try:
#         # Get all jobs for this employer
#         query = """
#             SELECT job_id, user_id 
#             FROM assigned_jobs 
#             WHERE employer_id = %s
#         """
#         jobs = execute_query(query, (employer_id,))
        
#         print(f"[*] Found {len(jobs)} total jobs for employer {employer_id}")
        
#         # Identify which ones need to transfer
#         jobs_to_transfer = [job for job in jobs if job['user_id'] != employer_id]
        
#         print(f"[*] {len(jobs_to_transfer)} jobs need transfer from subusers")
        
#         if not jobs_to_transfer:
#             print(f"[*] All jobs already belong to owner")
#             return
        
#         subuser_list = list(set([job['user_id'] for job in jobs_to_transfer]))

#         # Transfer each job 
#         for job in jobs_to_transfer:
#             update_query(
#                 "UPDATE assigned_jobs SET user_id = %s WHERE job_id = %s",
#                 (employer_id, job['job_id'])
#             )
            
#             update_query(
#                 "UPDATE job_post SET employer_id = %s WHERE id = %s",
#                 (employer_id, job['job_id'])
#             )
#             # Log each transfer record
#             execute_query(
#                 """
#                 INSERT INTO job_transfer_logs 
#                 (employer_id, job_id, from_user_id, to_user_id, reason, transferred_at)
#                 VALUES (%s, %s, %s, %s, %s, NOW())
#                 """,
#                 (employer_id, job['job_id'], job['user_id'], employer_id, reason)
#             )
        
#         print(f"[*] Successfully transferred {len(jobs_to_transfer)} jobs to owner {employer_id}")

#         # Delete subusers if they have no more jobs
#         for subuser_id in subuser_list:
#             check_query = """
#                 SELECT COUNT(*) AS job_count 
#                 FROM assigned_jobs 
#                 WHERE user_id = %s
#             """
#             count_result = execute_query(check_query, (subuser_id,))
#             job_count = count_result[0]['job_count']
#             if job_count == 0:
#                 delete_query = "DELETE FROM sub_users WHERE sub_user_id = %s"
#                 update_query(delete_query, (subuser_id,))
#                 print(f"[*] Deleted subuser {subuser_id} as they have no more jobs")
#             else:
#                 print(f"[*] Subuser {subuser_id} has {job_count} remaining jobs; not deleting")
        
#     except Exception as e:
#         print(f"[x] Error: {str(e)}")
#         return 


def transfer_jobs_to_owner(employer_id: int, reason: str):
    """Transfer jobs + maintain log and delete ALL subusers"""
    try:
        # Get all jobs for this employer
        query = """
            SELECT job_id, user_id 
            FROM assigned_jobs 
            WHERE employer_id = %s
        """
        jobs = execute_query(query, (employer_id,))
        
        print(f"[*] Found {len(jobs)} total jobs for employer {employer_id}")
        
        # Identify which ones need to transfer
        jobs_to_transfer = [job for job in jobs if job['user_id'] != employer_id]
        
        print(f"[*] {len(jobs_to_transfer)} jobs need transfer from subusers")
        
        if not jobs_to_transfer:
            print(f"[*] All jobs already belong to owner")
        else:
            # Transfer each job 
            for job in jobs_to_transfer:
                update_query(
                    "UPDATE assigned_jobs SET user_id = %s WHERE job_id = %s",
                    (employer_id, job['job_id'])
                )
                
                update_query(
                    "UPDATE job_post SET employer_id = %s WHERE id = %s",
                    (employer_id, job['job_id'])
                )
                # Log each transfer record
                execute_query(
                    """
                    INSERT INTO job_transfer_logs 
                    (employer_id, job_id, from_user_id, to_user_id, reason, transferred_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (employer_id, job['job_id'], job['user_id'], employer_id, reason)
                )
            
            print(f"[*] Successfully transferred {len(jobs_to_transfer)} jobs to owner {employer_id}")

        # Delete ALL subusers for this employer (regardless of remaining jobs)
        subusers_query = """
            SELECT sub_user_id 
            FROM sub_users 
            WHERE employer_id = %s
        """
        subusers = execute_query(subusers_query, (employer_id,))
        
        if subusers:
            deleted_count = 0
            for sub in subusers:
                subuser_id = sub['sub_user_id']
                delete_query = "DELETE FROM sub_users WHERE sub_user_id = %s"
                update_query(delete_query, (subuser_id,))
                deleted_count += 1
                print(f"[*] Deleted subuser {subuser_id}")
            
            print(f"[*] Deleted all {deleted_count} subusers for employer {employer_id}")
        else:
            print(f"[*] No subusers found for employer {employer_id}")

    except Exception as e:
        print(f"[x] Error: {str(e)}")
        return
