# from flask import Flask,request

# import app.models.mysql_connector as db_con
import os
import re
from src import app
from src.models.mysql_connector import execute_query,update_query,update_query_last_index,update_many
from src.models.aws_resources import S3_Client
from flask import jsonify, request, redirect, url_for, session
import json
from src.controllers.jwt_tokens.jwt_token_required import get_user_token,get_jwt_access_token
from src.models.user_authentication import get_user_data,isUserExist,api_json_response_format, get_sub_user_data
from src.controllers.authentication.manual.authentication_process import calculate_professional_profile_percentage
from dotenv import load_dotenv
from flask import Response
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import docx2txt
import meilisearch
import random
from  openai import OpenAI
import fitz
import io
import time
import tiktoken
import boto3
from datetime import datetime,date, timedelta, timezone
from langchain_community.vectorstores import Meilisearch
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import JSONLoader
from io import BytesIO
from PyPDF2 import PdfReader
from src.controllers.payment.payment_process import create_trial_session
import stripe 
import uuid
import platform
import ast
from meilisearch import Client
from meilisearch.index import Index

g_resume_path = os.getcwd()
g_prompt_file_path = os.getcwd()
home_dir = "/home"
load_dotenv(home_dir+"/.env")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
g_summary_model_name = os.environ.get('SUMMARY_MODEL_NAME')
g_token_encoding_txt = os.environ.get('TOKEN_ENCODING_TEXT')
g_openai_token_limit =int(os.environ.get('OPENAI_TOKEN_LIMIT'))
g_openai_completion_token_limit =int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))

os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

meilisearch_url = os.environ.get("MEILI_HTTP_ADDR")
PROFILE_INDEX = os.environ.get("PROFILE_INDEX")
MEILISEARCH_PROFESSIONAL_INDEX = os.environ.get("MEILISEARCH_PROFESSIONAL_INDEX")
MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
JOB_POST_INDEX = os.environ.get("JOB_POST_INDEX")
BUCKET_NAME = os.environ.get('PROMPT_BUCKET')
S3_BUCKET_NAME = os.environ.get('CDN_BUCKET')
WEB_APP_URI = os.environ.get('WEB_APP_URI')
PROFESSIONAL_TRIAL_PERIOD = os.environ.get('PROFESSIONAL_TRIAL_PERIOD')
PROFESSIONAL_BASIC_PLAN_ID = os.environ.get('PROFESSIONAL_BASIC_PLAN_ID')
s3_picture_folder_name = "professional/profile-pic/"
s3_employer_picture_folder_name = "employer/logo/"
s3_intro_video_folder_name = "professional/profile-video/"
s3_resume_folder_name = "professional/resume/"
s3_sector_image_folder_name = "sector-image/"
s3_cover_letter_folder_name = "professional/cover-letter/"
s3_partner_learning_folder_name = "partner/learning-doc/"
s3_partner_cover_pic_folder_name = "partner/cover-pic/"
s3_partner_picture_folder_name = "partner/profile-pic/"
s3_sc_community_cover_pic_folder_name = "2ndcareers/cover-pic/"
s3_sc_community_audio_folder_name = "2ndcareers/audio/"
s3_employeer_logo_folder_name = "employer/logo/"
s3_trailing_cover_pic_folder_name = "2ndcareers/trailing-pic/"

s3_obj = S3_Client()

from flask_executor import Executor
from src.models.background_task import BackgroundTask

executor = Executor(app)
background_runner = BackgroundTask(executor)

# class DocumentWrapper:
#     def __init__(self, document):
#         self.document = document
#         self.page_content = json.dumps(document)  # Serialize the document to a JSON string
#         self.metadata = document  # Use the document itself as metadata

#     def to_dict(self):
#         return self.document

#class DocumentWrapper:
    # def __init__(self, document):
    #     self.document = document
    #     self.page_content = json.dumps(document)  # Serialize the document to a JSON string
    #     self.metadata = document  # Use the document itself as metadata
    #     self.id = document.get("user_id")
    #     # self.id = str(uuid.uuid4())  # Generate a unique ID for each document

    # # def to_dict(self):
    # #     return self.document
    # def to_dict(self):
    #     # Include page_content explicitly in the dictionary representation
    #     return {
    #         "page_content": self.page_content,
    #         **self.document,  # Include all other fields from the document
    #     }

class DocumentWrapper:
    def __init__(self, document):
        self.document = document
        self.page_content = json.dumps(document)  # Serialize the document to a JSON string
        self.metadata = document  # Use the document itself as metadata
        self.id = str(uuid.uuid4())  # Generate a unique ID for each document

    def to_dict(self):
        return self.document
    
def update_professional_profile():        
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])          
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()        
                if 'contact_number' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'country_code' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'country' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                # if 'city' not in req_data:
                #     result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                #     return result_json
                # contact_number = None
                # if 'contact_number' in req_data:
                #     contact_number = req_data['contact_number']   
                # country_code = None
                # if 'country_code' in req_data:
                #     country_code = req_data['country_code']
                country_code = req_data['country_code'] 
                contact_number = req_data['contact_number']
                country = req_data['country']
                city = req_data['city']
                gender = req_data['gender']
                years_of_experience = req_data['years_of_experience']
                functional_specification = req_data['functional_specification']
                willing_to_relocate = req_data['willing_to_relocate']
                industry_sector = req_data['industry_sector']
                sector = req_data['sector']
                job_type = req_data['job_type']
                location_preference = req_data['location_preference']
                mode_of_communication = req_data['mode_of_communication']
                user_id = user_data["user_id"]                                
                query = 'update users set country_code=%s, contact_number=%s,country=%s,city=%s,gender=%s where user_id=%s'
                values = (country_code,contact_number,country,city,gender,user_id,)
                row_count = update_query(query, values)  
                update_professional_profile_query = 'update professional_profile set years_of_experience=%s, functional_specification=%s, sector=%s, industry_sector=%s, job_type=%s, location_preference=%s, mode_of_communication=%s, willing_to_relocate=%s where professional_id=%s'
                values = (years_of_experience, functional_specification, sector, industry_sector, job_type, location_preference, mode_of_communication, willing_to_relocate, user_id,)
                profile_table_row_count = update_query(update_professional_profile_query, values)
                if row_count > 0 and profile_table_row_count > 0:
                    try:
                        temp_dict = {'Country' : country,
                                     'City' : city,
                                    'Country Code' : country_code,
                                    'Contact Number' : contact_number,
                                    'Message': 'Profile updated successfully'}
                        event_properties = background_runner.process_dict(user_data['email_id'], "Professional Profile Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Profile Updation",{**event_properties, "User Role": user_data['user_role']}, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Profile Updation, {str(e)}")
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{}) 
                else:
                    try:
                        temp_dict = {'Country' : country,
                                     'City' : city,
                                    'Country Code' : country_code,
                                    'Contact Number' : contact_number,
                                    'Message': 'Profile updation unsuccessfull'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Profile Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Profile Updation Error",event_properties,temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Profile Updation, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Profile updation unsuccessfull'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Profile Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Profile Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Profile Updation, {str(e)}")
        print(f"Exception in update_professional_profile(), {str(error)}")           
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def month_to_number(month_name):
    month_mapping = {
        "jan": "01", "january": "01",
        "feb": "02", "february": "02",
        "mar": "03", "march": "03",
        "apr": "04", "april": "04",
        "may": "05",
        "jun": "06", "june": "06",
        "jul": "07", "july": "07",
        "aug": "08", "august": "08",
        "sep": "09", "september": "09",
        "oct": "10", "october": "10",
        "nov": "11", "november": "11",
        "dec": "12", "december": "12"
    }
    return month_mapping.get(month_name.strip().lower(), None)
    
def get_professional_profile_data():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                              
                query = 'select first_name, last_name, email_id, country_code, contact_number, country, state, city,login_mode, pricing_category from users where user_id=%s'
                values = (professional_id,)
                profile_data_set = execute_query(query, values)
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': 'Profile details displayed successfully'}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Show Professional Profile Data", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Show Professional Profile Data",event_properties, temp_dict.get('Message'), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging : Show Professional Profile Data, {str(e)}")                  
                result_json = api_json_response_format(True,"User details displayed successfully",0,replace_empty_values(profile_data_set)) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Profile data not displayed.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Show Professional Profile Data Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Show Professional Profile Data Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Show Professional Profile Data Error, {str(e)}")
        print(f"Exception in get_professional_profile_data(), {str(error)}")
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
def extract_year_from_field(field_value):
    # Use regular expression to find and extract a 4-digit number representing the year
    match = re.search(r'\b\d{4}\b', field_value)
    if match:
        return match.group()
    else:
        return None

def professional_details_update(request):        
    result_json = {}
    text = ""
    try: 
        # f.save(g_resume_path + "/resume/"+f.filename)    
        token_result = get_user_token(request)                    
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"]) 
            professional_id = user_data["user_id"]
            s3_pro = s3_obj.get_s3_client()   

            req_data = request.form
            from_profile_dashboard = req_data.get('from_profile_dashboard')
            if from_profile_dashboard == 'true':
                get_resume_query = 'select professional_resume from professional_profile where professional_id = %s'
                get_resume_values = (professional_id,)
                get_resume_result_dict = execute_query(get_resume_query, get_resume_values)
                resume_name = ''
                if get_resume_result_dict:
                    resume_name = get_resume_result_dict[0]['professional_resume']
                # if resume_name == '':
                #     s3_pro = s3_obj.get_s3_client()   
                #     s3_pro.upload_fileobj(f, S3_BUCKET_NAME, s3_resume_folder_name+f.filename)    
            else:
                res = request.form
                resume_file = request.files['file']
                resume_name = resume_file.filename
                s3_pro.upload_fileobj(resume_file, S3_BUCKET_NAME, s3_resume_folder_name+resume_file.filename)                               


            if(resume_name == '' or not resume_name):
                result_json = api_json_response_format(True,"File not found! Please upload your resume.",204 ,{})
                return result_json

            if resume_name.endswith(".pdf"):
                if resume_name.endswith(".pdf"):
                    s3_file_name = s3_resume_folder_name+resume_name               
                    pdf_file = s3_pro.get_object(Bucket=S3_BUCKET_NAME, Key=s3_file_name)["Body"].read()                
                    reader = PdfReader(BytesIO(pdf_file))                
                    for page in reader.pages:
                        # print(f"Text: {page.extract_text()}")
                        text+=f"Text: {page.extract_text()}"                
                    resume_text = text
                    # doc = fitz.open(g_resume_path + "/resume/"+f.filename)
                    # for page in doc: 
                    #     text+=page.get_text() 
                else:
                    doc_file = s3_pro.get_object(Bucket=S3_BUCKET_NAME, Key=s3_resume_folder_name+resume_name)["Body"].read()        
                    text = BytesIO(doc_file)
                    text = docx2txt.process(text)
                    resume_text = text
                    # text = docx2txt.process(g_resume_path + "/resume/"+f.filename)  
                out = process_quries(OPENAI_API_KEY,text) 
                res=out.split("#####") 
                # print(res[0])
                if(res[1]=="False"):
                    data = json.loads(res[0])
                    print(user_data["user_role"])
                    if user_data["user_role"] == "professional" and not len(data["Work_Experience"]) == 0: 
                        is_exp = "Y"  
                        flag_user_exist = isUserExist("professional_experience","professional_id",professional_id)
                        if flag_user_exist:
                            query = "delete from professional_experience where professional_id = %s"
                            values = (professional_id,)  
                            update_query(query, values)
                        for experience in data["Work_Experience"]:
                            org_name = experience.get("Organization_Name", "")
                            job_title = experience.get("Job_Title", "")
                            start_month = experience.get("Start_Month", "")
                            start_year = experience.get("Start_Year", None)
                            end_month = experience.get("End_Month", "")
                            end_year = experience.get("End_Year", None)
                            is_now_working = "Y" if experience.get("Currently_Working", False) else "N"
                            work_desc = experience.get("Job_Description", "")
                            work_location = experience.get("Work_Location", "")
                            if start_year == "":
                                start_year = None
                            if end_year == "":
                                end_year = None
                            created_at = datetime.now()
                            query = 'insert into professional_experience (professional_id,is_experienced,company_name,job_title,start_year,end_year,start_month,end_month,is_currently_working,job_description,job_location,created_at) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                            values = (professional_id,is_exp,org_name,job_title,start_year,end_year,start_month,end_month,is_now_working,work_desc,work_location,created_at,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                result_json = api_json_response_format(True,"professional Experience updated successfully",0,{})
                            else:
                                result_json = api_json_response_format(False,"Could not update  professional Experience",500,{})
                    i=0
                    if user_data["user_role"] == "professional" and not len(data["Education"]) == 0: 
                        flag_user_exist = isUserExist("professional_education","professional_id",professional_id)
                        if flag_user_exist:
                            query = "delete from professional_education where professional_id = %s"
                            values = (professional_id,)  
                            update_query(query, values)
                        for education in data["Education"]:
                            institute_name = education.get("Institute_Name", "")
                            institute_location = education.get("Location", "")
                            degree_level = education.get("Degree", "")
                            specialisation = education.get("Major", "")
                            start_month = education.get("Start_Month", "")
                            end_month = education.get("End_Month", "")
                            start_year = education.get("Start_Year", "")
                            year_of_passing = education.get("End_Year", "")
                            is_pursuing = "Y" if education.get("Is_Pursuing", False) else "N"
                            created_at = datetime.now()
                            
                            if start_year == "":
                                start_year = None
                            if year_of_passing == "":
                                year_of_passing = None
                            query = 'insert into professional_education(institute_name,institute_location,degree_level,specialisation,start_year,start_month,end_month,end_year,is_pursuing,created_at,professional_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                            values = (institute_name,institute_location,degree_level,specialisation,start_year,start_month,end_month,year_of_passing,is_pursuing,created_at,professional_id,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                result_json = api_json_response_format(True,"professional Education updated successfully",0,{})
                            else:
                                result_json = api_json_response_format(False,"Could not update  professional Education",500,{})
                    i=0
                    if user_data["user_role"] == "professional" and not len(data["Skills"]) == 0: 
                        flag_user_exist = isUserExist("professional_skill","professional_id",professional_id)
                        if flag_user_exist:
                            query = "delete from professional_skill where professional_id = %s"
                            values = (professional_id,)  
                            update_query(query, values)
                        for i in range(len(data["Skills"])):
                            skill_name = data["Skills"][i]["Skill"]
                            skill_level = data["Skills"][i]["Skill_Level"]
                            created_at = datetime.now()
                            query = 'insert into professional_skill(skill_name,skill_level,created_at,professional_id) values(%s,%s,%s,%s)'
                            values = (skill_name, skill_level, created_at,professional_id,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                result_json = api_json_response_format(True,"Professional skills updated successfully",0,{})
                            else:
                                result_json = api_json_response_format(False,"Could not update professional skills",500,{})
                    i=0
                    if user_data["user_role"] == "professional" and not len(data["Languages"]) == 0: 
                        flag_user_exist = isUserExist("professional_language","professional_id",professional_id)
                        if flag_user_exist:
                            query = "delete from professional_language where professional_id = %s"
                            values = (professional_id,)  
                            update_query(query, values)
                        for i in range(len(data["Languages"])):
                            language_known = data["Languages"][i]["Language"]
                            language_level = data["Languages"][i]["Language_Level"]
                            created_at = datetime.now()
                            if not language_known == '':
                                query = 'insert into professional_language(professional_id, language_known,language_level,created_at) values(%s,%s,%s,%s) '
                                values = (professional_id, language_known, language_level, created_at,)
                                row_count = update_query(query,values)
                                if row_count > 0:
                                    result_json = api_json_response_format(True,"professional language updated successfully",0,{})
                                else:
                                    result_json = api_json_response_format(False,"Could not update professional language",500,{})
                            else:
                                result_json = api_json_response_format(True,"professional language updated successfully",0,{})
                    
                    i=0
                    if user_data["user_role"] == "professional" and not data["About"]=="": 
                            flag_user_exist = isUserExist("professional_profile","professional_id",professional_id)
                            if flag_user_exist:
                                query = "update professional_profile set about = %s where professional_id = %s"
                                values = (None,professional_id,)  
                                update_query(query, values)
                            about = data["About"]
                            # current_date = datetime.today()
                            # file_name = resume_name
                            # formatted_date = current_date.strftime("%Y/%m/%d")
                            created_at = datetime.now()
                            query = 'update professional_profile set about = %s where professional_id = %s'
                            values = (about, professional_id,)
                            row_count = update_query(query,values)
                            if row_count > 0:
                                result_json = api_json_response_format(True,"professional about updated successfully",0,{})
                            else:
                                result_json = api_json_response_format(False,"Could not update professional about",500,{})
                    i=0
                    if user_data["user_role"] == "professional" and not data["Social_Links"]=="": 
                            flag_user_exist = isUserExist("professional_social_link","professional_id",professional_id)
                            if flag_user_exist:
                                query = "delete from professional_social_link where professional_id = %s"
                                values = (professional_id,)  
                                update_query(query, values)
                            key_list = list(data["Social_Links"].keys())
                            for i in range(len(data["Social_Links"])):
                                title = key_list[i]
                                url = data["Social_Links"][title]
                                created_at = datetime.now()
                                if not url == "":
                                    query = 'insert into professional_social_link(professional_id, title, url, created_at) values(%s,%s,%s,%s) '
                                    values = (professional_id, title, url, created_at,)
                                    row_count = update_query(query,values)
                                    if row_count > 0:
                                        result_json = api_json_response_format(True,"professional about updated successfully",0,{})
                                    else:
                                        result_json = api_json_response_format(False,"Could not update professional about",500,{})
                                else:
                                    result_json = api_json_response_format(True,"professional about updated successfully",0,{})
                    i=0
                    if user_data["user_role"] == "professional" and "Additional_Information" in data:
                        additional_info = data["Additional_Information"]

                        # Check if there are existing records and delete them
                        flag_user_exist = isUserExist("professional_additional_info", "professional_id", professional_id)
                        if flag_user_exist:
                            query = "delete from professional_additional_info where professional_id = %s"
                            values = (professional_id,)
                            update_query(query, values)

                        # Insert Certificates_Earned
                        if "Certificates_Earned" in additional_info and additional_info["Certificates_Earned"]:
                            for certificate in additional_info["Certificates_Earned"]:
                                created_at = datetime.now()
                                title = "Certificates_Earned"
                                query = 'insert into professional_additional_info(professional_id, title, description, created_at) values(%s,%s,%s,%s)'
                                values = (professional_id, title, certificate, created_at,)
                                row_count = update_query(query, values)
                                if row_count > 0:
                                    result_json = api_json_response_format(True, "Professional additional information updated successfully", 0, {})
                                else:
                                    result_json = api_json_response_format(False, "Could not update professional additional information", 500, {})

                        # Insert Volunteering_Activities
                        if "Volunteering_Activities" in additional_info and additional_info["Volunteering_Activities"]:
                            for activity in additional_info["Volunteering_Activities"]:
                                created_at = datetime.now()
                                title = "Volunteering_Activities"
                                query = 'insert into professional_additional_info(professional_id, title, description, created_at) values(%s,%s,%s,%s)'
                                values = (professional_id, title, activity, created_at,)
                                row_count = update_query(query, values)
                                if row_count > 0:
                                    result_json = api_json_response_format(True, "Professional additional information updated successfully", 0, {})
                                else:
                                    result_json = api_json_response_format(False, "Could not update professional additional information", 500, {})
                        if "Board_Positions_Held" in additional_info and additional_info["Board_Positions_Held"]:
                            for position in additional_info["Board_Positions_Held"]:
                                created_at = datetime.now()
                                title = "Board_Positions_Held"
                                query = 'insert into professional_additional_info(professional_id, title, description, created_at) values(%s,%s,%s,%s)'
                                values = (professional_id, title, position, created_at,)
                                row_count = update_query(query, values)
                                if row_count > 0:
                                    result_json = api_json_response_format(True, "Professional additional information updated successfully", 0, {})
                                else:
                                    result_json = api_json_response_format(False, "Could not update professional additional information", 500, {})
                    query = 'update professional_profile set professional_resume = %s,upload_date = %s where professional_id = %s'
                    current_date = datetime.today()
                    file_name = resume_name
                    formatted_date = current_date.strftime("%Y/%m/%d")
                    values = (file_name,formatted_date,professional_id,)
                    update_query(query, values)
                    profile_percentage = show_percentage(professional_id)
                    update_prof_percentage = 'update users set profile_percentage = %s where user_id = %s'
                    values = (profile_percentage,professional_id,)
                    update_query(update_prof_percentage, values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Resume extracted successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Resume Extraction", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Resume Extraction",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Resume Extraction, {str(e)}")
                    result_json = api_json_response_format(True,"Resume extracted successfully!",0,{})
                    vector_search_init(professional_id)
                    background_runner.get_professional_details(professional_id)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Resume extraction unsuccessfull, error in Open-AI processing'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Resume Extraction Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Resume Extraction Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Resume Extraction Error, {str(e)}")
                    result_json = api_json_response_format(False,str(res[0]),500,{})
            else:
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': 'Resume extraction unsuccessfull. Unsupported file format'}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Professional Resume Extraction Error", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Professional Resume Extraction Error",event_properties, temp_dict.get('Message'), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Professional Resume Extraction Error, {str(e)}")
                result_json = api_json_response_format(True,"Sorry, unsupported file format. Please upload a pdf or doc not exceeding 10mb.",204 ,{})  
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Resume extraction unsuccessfull.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Resume Extraction Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Resume Extraction Error",event_properties,temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Resume Extraction Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{}) 
    finally:        
        return result_json

def professional_home_recommended_view():
    try:
        profile = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"]) 
            professional_id = user_data["user_id"]  
            profile_percentage = show_percentage(professional_id)
            if profile_percentage > 50:
                unapplied_ai_jobs_query = "SELECT count(job_id) as count FROM ai_recommendation WHERE professional_id = %s AND source = 'AI' AND job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s);"
                values = (professional_id, professional_id,)
                unapplied_jobs_count = execute_query(unapplied_ai_jobs_query, values)
                job_details = []
                if len(unapplied_jobs_count) > 0 and unapplied_jobs_count[0]['count'] == 0:
                    data = get_profile_search(professional_id)
                    out = process_quries_search(OPENAI_API_KEY,data)
                    print("recommended query :" + str(out))
                    result_json = api_json_response_format(False,out,200,{})
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
                    index_name = JOB_POST_INDEX
                    vector_store = Meilisearch(client=client, embedding=embeddings,embedders=embedders,
                        index_name=index_name)

                    if out["is_error"]:    
                        print(f"recommendation prompt query error : {out['result']}")        
                        return
                    else:
                        query = out["result"]
                    # print("similarity search query :" + str(query))
                    # Make similarity search
                    #query = out

                    results = vector_store.similarity_search_with_score(
                        query=query,
                        embedder_name = "adra",
                        k=15
                        
                    )
                    job_details1 = []
                    Willing_to_Relocate = data[0]['Willing_to_Relocate'].lower()
                    country = data[0]['Contact_Information']['Country'].lower()
                    city = data[0]['Contact_Information']['Address'].lower()
                    for doc, _ in results:
                        page_content = doc.page_content
                        page_content = json.loads(page_content)
                        if page_content['workplace_type'].lower() in ('hybrid', 'on-site'):
                            if Willing_to_Relocate == 'yes':
                                if page_content['country'].lower() == country:
                                    job_details1.append(page_content)
                            else:
                                if page_content['city'].lower() == city:
                                    job_details1.append(page_content)
                        else:
                            job_details1.append(page_content)
                    if len(job_details1) > 0:  
                        ids = list(set(data["id"] for data in job_details1))
                        # ids.append("Ex_Job_1")
                        admin_job_ids = [job_id for job_id in ids if str(job_id).startswith("Ex_")]
                        job_ids = [job_id for job_id in ids if not str(job_id).startswith("Ex_")]

                        #job_post
                        ids_list_new = []
                        if job_ids:
                            query = 'select id,job_status from job_post where id IN %s'
                            values = (tuple(job_ids),)
                            id_job_status = execute_query(query, values)
                            if len(id_job_status) > 0:
                                for i in id_job_status:
                                    if i['job_status'] == 'opened':
                                        ids_list_new.append(i['id'])
                        #admin_job_post
                        admin_ids_list_new = []
                        if admin_job_ids:
                            query = 'select job_reference_id AS id,lower(admin_job_status) AS job_status from admin_job_post where job_reference_id IN %s'
                            values = (tuple(admin_job_ids),)
                            id_admin_job_status = execute_query(query, values)
                            if len(id_admin_job_status) > 0:
                                for i in id_admin_job_status:
                                    if i['job_status'] == 'opened':
                                        admin_ids_list_new.append(i['id'])

                        id_list = []
                        combined_ids_list_new = ids_list_new + admin_ids_list_new
                        id_tuple = tuple(combined_ids_list_new)
                        # if len(combined_ids_list_new) > 3:
                        #     id_list.append(combined_ids_list_new[0])
                        #     id_list.append(combined_ids_list_new[1])
                        #     id_list.append(combined_ids_list_new[2])
                        #     id_tuple = tuple(id_list)
                        # else:
                        #     id_tuple = tuple(combined_ids_list_new)
                        query = 'SELECT sr.job_id, jp.job_status FROM sc_recommendation sr JOIN job_post jp ON sr.job_id = jp.id WHERE sr.professional_id = %s and user_role_id = %s AND jp.job_status = "opened" AND sr.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) LIMIT 2;'
                        values = (professional_id, 3, professional_id,)
                        sc_recommended_jobs_id = execute_query(query, values)
                    else:
                        query = 'SELECT sr.job_id, jp.job_status FROM sc_recommendation sr JOIN job_post jp ON sr.job_id = jp.id WHERE sr.professional_id = %s and user_role_id = %s AND jp.job_status = "opened" AND sr.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) LIMIT 2;'
                        values = (professional_id, 3, professional_id,)
                        sc_recommended_jobs_id = execute_query(query, values)
                        id_tuple = ()
                        admin_ids_list_new = []
                    query = "select job_id from ai_recommendation where professional_id = %s and job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s);"
                    values = (professional_id, professional_id,)
                    existing_ai_job_ids = execute_query(query, values)
                    
                    if len(existing_ai_job_ids) > 0:
                        existing_ai_job_ids_list = [k['job_id'] for k in existing_ai_job_ids]
                        # new_id_tuple = list(set(existing_ai_job_ids_list + id_tuple))
                        new_id_tuple = tuple(item for item in id_tuple if item not in existing_ai_job_ids_list)
                    else:
                        new_id_tuple = id_tuple

                    # if len(new_id_tuple) < 3:
                    #     need_ids_count = 3 - len(new_id_tuple)
                    # new_id_tuple = new_id_tuple + tuple(admin_ids_list_new)
                    
                    if len(new_id_tuple) > 0:
                        created_at = datetime.now()
                        values = []
                        for id in new_id_tuple:
                            values.append((professional_id, id, "AI", 3, created_at,))
                        query = "INSERT INTO ai_recommendation (professional_id, job_id, source, user_role_id, created_at) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE source = VALUES(source), created_at = VALUES(created_at)"
                        update_many(query, values)
                    if len(job_details1) == 0 and len(sc_recommended_jobs_id) == 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'No jobs posted.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Recommended Jobs View", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Recommended Jobs View",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Recommended Jobs View, {str(e)}")
                        profile_percentage = {'profile_percentage' : profile_percentage} 
                        profile.update(profile_percentage) 
                        result_json = api_json_response_format(True,"No new recommendations found",0,profile)
                        return result_json
                    else:
                        query = "SELECT ar.job_id, jp.job_status FROM ai_recommendation ar JOIN job_post jp ON ar.job_id = jp.id WHERE ar.professional_id = %s AND ar.source = 'AI' AND jp.job_status = 'opened' AND ar.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) AND ar.job_id NOT IN ( SELECT job_id FROM sc_recommendation WHERE professional_id = %s ) LIMIT 3;"
                        values = (professional_id, professional_id,professional_id,)
                        new_ai_rcmn_ids = execute_query(query, values)
                        new_ai_rcmn_ids_tuple = tuple(n['job_id'] for n in new_ai_rcmn_ids)

                        # For admin_job_post
                        new_ai_rcmn_admin_ids_tuple = ()
                        if len(new_ai_rcmn_ids_tuple) < 3:
                            need_admin_ids_count = 3 - len(new_ai_rcmn_ids_tuple)
                            query = "SELECT ar.job_id, ap.admin_job_status FROM ai_recommendation ar LEFT JOIN admin_job_post ap ON ar.job_id = ap.job_reference_id WHERE ar.professional_id = %s AND ar.source = 'AI' AND ap.admin_job_status= 'opened' LIMIT %s;"
                            new_ai_rcmn_admin_ids = execute_query(query, (professional_id,need_admin_ids_count))
                            new_ai_rcmn_admin_ids_tuple = tuple(n['job_id'] for n in new_ai_rcmn_admin_ids)

                        job_ids_tuple = tuple(d['job_id'] for d in sc_recommended_jobs_id)
                        combined_id_tuple = new_ai_rcmn_ids_tuple + job_ids_tuple
                        admin_id_list = list(job_ids_tuple)
                        new_unique_tuple = tuple(set(combined_id_tuple))

                        
                        # new_unique_list = []
                        # for i in unique_combined_tuple:
                        #     query = 'select count(job_id) from job_activity where job_id = %s and professional_id = %s'
                        #     values = (i, professional_id,)
                        #     rslt = execute_query(query, values)
                        #     if rslt[0]['count(job_id)'] == 0:
                        #         new_unique_list.append(i)
                        # new_unique_tuple = tuple(new_unique_list)
                        if len(new_unique_tuple) > 0:
                            query_job_details = """
                                    SELECT 
                                        jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
                                        jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, 
                                        jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.specialisation, jp.timezone,
                                        jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                                        jp.required_background_check, jp.required_subcontract, jp.is_application_deadline,
                                        jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at,
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(u.user_id, su.sub_user_id) AS user_id
                                    FROM 
                                        job_post jp 
                                    LEFT JOIN 
                                        users u ON jp.employer_id = u.user_id 
                                    LEFT JOIN
                                        sub_users su ON jp.employer_id = su.sub_user_id
                                    WHERE jp.job_status = %s and jp.is_active = %s and  jp.id IN %s ORDER by jp.id DESC LIMIT 3;
                                    """
                            values_job_details = ('Opened', 'Y', new_unique_tuple,)
                            job_details = replace_empty_values(execute_query(query_job_details, values_job_details))


                            if new_ai_rcmn_admin_ids_tuple:
                                query_admin_job_details = """SELECT ap.employer_id AS user_id, ap.`job_reference_id` AS id, ap.`job_title`, ap.`job_overview`, ap.`job_description` AS job_desc, 
                                        ap.`schedule` AS work_schedule, ap.`skills`, ap.`country`, ap.`state`, ap.`city`, ap.`company_name`, ap.`company_sector` AS sector, ap.`job_type`, 
                                        ap.`workplace_type`, ap.`Functional_Specification` AS specialisation, ap.`functional_specification_others`, ap.`apply_link`, 
                                        ap.`source`, ap.`seniority`, ap.`is_active`, ap.`pricing_category`, ap.`admin_job_status` AS job_status, ap.`created_at`,
                                        u.profile_image, NULL AS employer_type, NULL AS questions, NULL AS additional_info, NULL AS application_deadline_date, NULL AS applied_status, 
                                        NULL AS benefits, NULL AS calendly_link, NULL AS currency, NULL AS duration, NULL AS invited_by_employer, NULL AS invited_message, NULL AS is_application_deadline, NULL AS is_paid,
                                        NULL AS payment_status, NULL AS required_background_check, NULL AS required_cover_letter, NULL AS required_resume, NULL AS required_subcontract,
                                        NULL AS responsibilities, NULL AS share_url, NULL AS time_commitment, NULL AS timezone FROM admin_job_post ap LEFT JOIN users u ON ap.employer_id=u.user_id WHERE ap.job_reference_id IN %s ORDER by job_reference_id DESC;"""
                                values_admin_job_details = (new_ai_rcmn_admin_ids_tuple,)
                                admin_job_details = replace_empty_values(execute_query(query_admin_job_details, values_admin_job_details))
                                job_details.extend(admin_job_details)
                        
                        else:
                            if new_ai_rcmn_admin_ids_tuple:
                                query_admin_job_details = """SELECT ap.employer_id AS user_id, ap.`job_reference_id` AS id, ap.`job_title`, ap.`job_overview`, ap.`job_description` AS job_desc, 
                                        ap.`schedule` AS work_schedule, ap.`skills`, ap.`country`, ap.`state`, ap.`city`, ap.`company_name`, ap.`company_sector` AS sector, ap.`job_type`, 
                                        ap.`workplace_type`, ap.`Functional_Specification` AS specialisation, ap.`functional_specification_others`, ap.`apply_link`, 
                                        ap.`source`, ap.`seniority`, ap.`is_active`, ap.`pricing_category`, ap.`admin_job_status` AS job_status, ap.`created_at`,
                                        u.profile_image, NULL AS employer_type, NULL AS questions, NULL AS additional_info, NULL AS application_deadline_date, NULL AS applied_status, 
                                        NULL AS benefits, NULL AS calendly_link, NULL AS currency, NULL AS duration, NULL AS invited_by_employer, NULL AS invited_message, NULL AS is_application_deadline, NULL AS is_paid,
                                        NULL AS payment_status, NULL AS required_background_check, NULL AS required_cover_letter, NULL AS required_resume, NULL AS required_subcontract,
                                        NULL AS responsibilities, NULL AS share_url, NULL AS time_commitment, NULL AS timezone FROM admin_job_post ap LEFT JOIN users u ON ap.employer_id=u.user_id WHERE ap.job_reference_id IN %s ORDER by job_reference_id DESC LIMIT 3;"""
                                values_admin_job_details = (new_ai_rcmn_admin_ids_tuple,)
                                job_details = replace_empty_values(execute_query(query_admin_job_details, values_admin_job_details))
                                if len(job_details) == 0:
                                    profile_percentage = {'profile_percentage' : profile_percentage} 
                                    profile.update(profile_percentage) 
                                    result_json = api_json_response_format(True,"No new recommendations found",0,profile)
                                    return result_json
                            else:
                                profile_percentage = {'profile_percentage' : profile_percentage} 
                                profile.update(profile_percentage) 
                                result_json = api_json_response_format(True,"No new recommendations found",0,profile)
                                return result_json
                else:
                    unapplied_ai_jobs_query = "SELECT ar.job_id, jp.job_status FROM ai_recommendation ar JOIN job_post jp ON ar.job_id = jp.id WHERE ar.professional_id = %s AND ar.source = 'AI' AND jp.job_status = 'opened' AND ar.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) AND ar.job_id NOT IN ( SELECT job_id FROM sc_recommendation WHERE professional_id = %s ) LIMIT 3;"
                    unapplied_ai_jobs = execute_query(unapplied_ai_jobs_query, (professional_id, professional_id, professional_id,))
                    ai_id_list = [i['job_id'] for i in unapplied_ai_jobs]

                    ai_external_job_ids = []
                    if len(ai_id_list)<3:
                        need_ids_count = 3 - len(ai_id_list)
                        query = "SELECT ar.job_id, ap.admin_job_status FROM ai_recommendation ar LEFT JOIN admin_job_post ap ON ar.job_id = ap.job_reference_id WHERE ar.professional_id = %s AND ar.source = 'AI' AND lower(ap.admin_job_status)= 'opened' LIMIT %s;"
                        values = (professional_id,need_ids_count)
                        ai_external_recmnd_job = execute_query(query, values)
                        ai_external_job_ids = [ i['job_id'] for i in ai_external_recmnd_job]

                    admin_rcmnd_jobs_query = "SELECT sr.job_id, jp.job_status FROM sc_recommendation sr JOIN job_post jp ON sr.job_id = jp.id WHERE sr.professional_id = %s AND sr.user_role_id = %s AND jp.job_status = 'opened' AND sr.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) LIMIT 2;"
                    values = (professional_id, 3, professional_id,)
                    admin_recmnd_jobs = execute_query(admin_rcmnd_jobs_query, values)
                    admin_id_list = []
                    if len(admin_recmnd_jobs) > 0:
                        admin_id_list = [i['job_id'] for i in admin_recmnd_jobs]
                    
                    


                    recommended_id_list = admin_id_list + ai_id_list
                    if len(recommended_id_list) != 0:
                        query_job_details = """
                                        SELECT 
                                            jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
                                            jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, 
                                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.specialisation, jp.timezone,
                                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                                            jp.required_background_check, jp.required_subcontract, jp.is_application_deadline,
                                            jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at,
                                            COALESCE(u.profile_image, su.profile_image) AS profile_image,
                                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                            COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                            COALESCE(u.user_id, su.sub_user_id) AS user_id 
                                        FROM 
                                            job_post jp 
                                        LEFT JOIN 
                                            users u ON jp.employer_id = u.user_id 
                                        LEFT JOIN
                                            sub_users su ON jp.employer_id = su.sub_user_id
                                        WHERE jp.job_status = %s and jp.is_active = %s and  jp.id IN %s ORDER by jp.id;
                                        """
                        values_job_details = ('Opened', 'Y', recommended_id_list,)
                        job_details = replace_empty_values(execute_query(query_job_details, values_job_details))

                    if ai_external_job_ids:
                        ext_query_job_details = """SELECT ap.employer_id AS user_id, ap.`job_reference_id` AS id, ap.`job_title`, ap.`job_overview`, ap.`job_description` AS job_desc, 
                                        ap.`schedule` AS work_schedule, ap.`skills`, ap.`country`, ap.`state`, ap.`city`, ap.`company_name`, ap.`company_sector` AS sector, ap.`job_type`, 
                                        ap.`workplace_type`, ap.`Functional_Specification` AS specialisation, ap.`functional_specification_others`, ap.`apply_link`, 
                                        ap.`source`, ap.`seniority`, ap.`is_active`, ap.`pricing_category`, ap.`admin_job_status` AS job_status, ap.`created_at`,
                                        u.profile_image, NULL AS employer_type, NULL AS questions, NULL AS additional_info, NULL AS application_deadline_date, NULL AS applied_status, 
                                        NULL AS benefits, NULL AS calendly_link, NULL AS currency, NULL AS duration, NULL AS invited_by_employer, NULL AS invited_message, NULL AS is_application_deadline, NULL AS is_paid,
                                        NULL AS payment_status, NULL AS required_background_check, NULL AS required_cover_letter, NULL AS required_resume, NULL AS required_subcontract,
                                        NULL AS responsibilities, NULL AS share_url, NULL AS time_commitment, NULL AS timezone FROM admin_job_post ap LEFT JOIN users u ON ap.employer_id=u.user_id WHERE ap.job_reference_id IN %s ORDER by job_reference_id DESC;"""
                        values_ext_query_job_details = (tuple(ai_external_job_ids),)
                        ext_job_details = replace_empty_values(execute_query(ext_query_job_details, values_ext_query_job_details))
                        if ext_job_details:
                            job_details.extend(ext_job_details)

                if len(job_details) > 0:
                    id = job_details[0]["id"]
                    query = 'select * from  view_count where job_id = %s and professional_id = %s'
                    values = (id,professional_id,)
                    count = execute_query(query,values)
                    if not count and not str(id).startswith("Ex_"):
                        current_time = datetime.now()
                        query = "INSERT INTO view_count (job_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                        values = (id, professional_id, current_time,)
                        update_query(query,values)

                    for job in job_details:
                        raw_id = job.get('user_id') or job.get('id')
                        if not raw_id:   # handles None and ''
                            continue
                        try:
                            user_id = int(raw_id)
                        except (ValueError, TypeError):
                            continue
                        if user_id < 500000:
                            query = "select employer_type, sector, company_name from employer_profile where employer_id = %s"
                        else:
                            query = "select employer_type, sector, company_name from sub_users where sub_user_id = %s"
                        values = (user_id,)
                        # if job['user_id'] < 500000:
                        #     query = "select employer_type, sector, company_name from employer_profile where employer_id = %s"
                        # else:
                        #     query = "select employer_type, sector, company_name from sub_users where sub_user_id = %s"
                        # values = (job['user_id'],)
                        emp_details = execute_query(query, values)
                        if len(emp_details) > 0:
                            job.update({'employer_type': emp_details[0]['employer_type']})
                            job.update({'company_name': emp_details[0]['company_name']})
                            job.update({'sector': emp_details[0]['sector']})
                            txt = emp_details[0]['sector']
                            txt = txt.replace(", ", "_")
                            txt = txt.replace(" ", "_")
                            sector_name = txt + ".png"
                        job.update({'profile_image' : s3_employeer_logo_folder_name + job['profile_image']})
                        job.update({'sector_image' : s3_sector_image_folder_name + sector_name}) if 'sector_name' in locals() else job.update({'sector_image' : ""})
                        job.update({'profile_percentage' : profile_percentage})
                        query = 'select professional_resume from professional_profile where professional_id = %s'
                        values = (professional_id,)
                        rslt = execute_query(query, values)
                        resume_name = rslt[0]['professional_resume'] if rslt else ""
                        job.update({'user_resume' : resume_name})
                        quest_dict = {"questions" : []}
                        if not str(job['id']).startswith("Ex_"):
                            job_id = int(job['id'])
                            job['id'] = job_id
                            query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                            values = (job_id,)
                            result = execute_query(query, values)
                            if len(result) > 0:
                                for r in result:
                                    quest_dict["questions"].append(r)
                            job.update(quest_dict)
                            if job['created_at'] == "null" or job['created_at'] == None:
                                job['created_at'] = datetime.now()

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
                            job.update({'applied_status': job_applied_status})
                            query = 'select description from sc_recommendation where professional_id = %s and job_id = %s'
                            values = (professional_id, job_id,)
                            sc_recommended_notes= execute_query(query, values)
                            job.update({"recommended_by" : "2nd careers recommended"})
                            if job['id'] not in admin_id_list:
                                job.update({"sc_recommended_notes" : ""})
                                job.update({'recommended_by' : 'AI Recommendation'})
                            else:
                                if len(sc_recommended_notes) > 0:
                                    job.update({"sc_recommended_notes" : sc_recommended_notes[0]['description']})
                                else:
                                    job.update({"sc_recommended_notes" : ""})
                                job.update({'recommended_by' : '2nd Career Recommendation'})
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
                        else:
                            job.update({'recommended_by' : 'AI Recommendation'})
                    sorted_job_details = sorted(job_details, key=lambda x: (x['recommended_by'] != '2nd Career Recommendation', x['recommended_by']))
                    job_details_dict = {'job_details': sorted_job_details}
                    profile_percentage = {'profile_percentage' : profile_percentage}
                    profile.update(job_details_dict)  
                    profile.update(profile_percentage)              
                    data = fetch_filter_params()
                    profile.update(data)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Recommended jobs displayed successfully.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Recommended Jobs View", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Recommended Jobs View",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Recommended Jobs View, {str(e)}")
                    # background_runner.store_ai_recommended_jobs(profile, professional_id)
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                else:
                    profile_percentage = {'profile_percentage' : profile_percentage} 
                    profile.update(profile_percentage)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city']}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Recommended Jobs View", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Recommended Jobs View",event_properties,temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
                    result_json = api_json_response_format(False, "No new job recommendations found",200,profile)
                
                # else:
                #     try:
                #         temp_dict = {'Country' : user_data['country'],
                #                     'City' : user_data['city'],
                #                     'Message': 'No jobs posted.'}
                #         event_properties = background_runner.process_dict(user_data["email_id"], "Recommended Jobs View", temp_dict)
                #         background_runner.mixpanel_event_async(user_data['email_id'],"Recommended Jobs View",event_properties)
                #     except Exception as e:  
                #         print(f"Error in mixpanel event logging: Recommended Jobs View, {str(e)}")
                #     profile_percentage = {'profile_percentage' : profile_percentage} 
                #     profile.update(profile_percentage) 
                #     result_json = api_json_response_format(True,"No job posted",0,profile)
            
            else:
                profile_percentage = {'profile_percentage' : profile_percentage} 
                profile.update(profile_percentage) 
                notification_msg = "Please take a few moments to complete your profile to increase your chances of receiving better recommendations."
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': 'Jobs not recommended, User profile percentage is less than 50.'}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Recommended Jobs View", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Recommended Jobs View",event_properties, temp_dict.get('Message'), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
                result_json = api_json_response_format(True, notification_msg, 0, profile)
        else:
            result_json = api_json_response_format(False,"Unauthorized user",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in recommended jobs view.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Recommended Jobs View Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Recommended Jobs View Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

#def professional_home_recommended_view():
    try:
        token_result = get_user_token(request)
        profile = {}
        if token_result["status_code"] == 200:        
            user_data = get_user_data(token_result["email_id"])
            professional_id = user_data["user_id"]
            profile_percentage = show_percentage(professional_id)
            current_time = datetime.now()

            last_generation_query = """
                SELECT MAX(created_at) as last_generation_time
                FROM ai_recommendation
                WHERE professional_id = %s AND source = 'AI'
            """
            last_generation_result = execute_query(last_generation_query, (professional_id,))
            last_generation_time = last_generation_result[0]['last_generation_time'] if last_generation_result else None
            time_diff = current_time - last_generation_time if last_generation_time else timedelta(days=8)

            unapplied_ai_jobs_query = """
                SELECT job_id 
                FROM ai_recommendation 
                WHERE professional_id = %s AND source = 'AI'
                AND job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s) LIMIT 3;
            """
            unapplied_ai_jobs = execute_query(unapplied_ai_jobs_query, (professional_id, professional_id))
            
            def add_ai_recommendations(job_details):
                new_jobs_added = False
                flag = 0
                for job in job_details:
                    job_id = job.get('id')
                    if job_id:
                        check_query = """
                            SELECT COUNT(job_id) as count
                            FROM job_activity
                            WHERE professional_id = %s AND job_id = %s
                        """
                        rs = execute_query(check_query, (professional_id, job_id))
                        if rs and rs[0]['count'] == 0:
                            insert_query = """
                                INSERT INTO ai_recommendation (professional_id, job_id, source, user_role_id, created_at)
                                VALUES (%s, %s, 'AI', 3, %s)
                            """
                            updated_val = update_query(insert_query, (professional_id, job_id, current_time))
                            if updated_val > 0:
                                flag = flag + 1
                            new_jobs_added = True
                return flag

            if len(unapplied_ai_jobs) == 0 or time_diff > timedelta(days=7):
                data = get_profile_search(professional_id)
                query = process_quries_search(OPENAI_API_KEY, data)
                vector_store = Meilisearch(
                    client=meilisearch.Client(url=os.environ.get("MEILI_HTTP_ADDR")),
                    embedding=OpenAIEmbeddings(),
                    embedders={"adra": {"source": "userProvided", "dimensions": 1536}},
                    index_name=JOB_POST_INDEX,
                )
                results = vector_store.similarity_search_with_score(query=query, embedder_name="adra")
                job_details1 = [json.loads(doc.page_content) for doc, _ in results]
                unique_dict = {item["id"]: item for item in job_details1}
                job_details1 = list(unique_dict.values())
                for j in job_details1:
                    query = 'select profile_image, pricing_category from users where user_id = %s'
                    values = (j['employer_id'],)
                    user_details = execute_query(query, values)
                    if user_details:
                        j.update({'profile_image' : user_details[0]['profile_image']})
                        j.update({'pricing_category' : user_details[0]['pricing_category']})
                    else:
                        j.update({'profile_image' : ''})
                        j.update({'pricing_category' : ''})
                new_jobs_added = add_ai_recommendations(job_details1)

                if new_jobs_added == 0:
                    result_json = api_json_response_format(False, "No new job recommendations found.", 200, {})
                    return result_json
                # return api_json_response_format(True, "New AI job recommendations generated.", 200, {})

            elif len(unapplied_ai_jobs) < 3:
                data = get_profile_search(professional_id)
                query = process_quries_search(OPENAI_API_KEY, data)
                vector_store = Meilisearch(
                    client=meilisearch.Client(url=os.environ.get("MEILI_HTTP_ADDR")),
                    embedding=OpenAIEmbeddings(),
                    embedders={"adra": {"source": "userProvided", "dimensions": 1536}},
                    index_name=JOB_POST_INDEX,
                )
                results = vector_store.similarity_search_with_score(query=query, embedder_name="adra")
                job_details1 = [json.loads(doc.page_content) for doc, _ in results]
                unique_dict = {item["id"]: item for item in job_details1}
                job_details1 = list(unique_dict.values())
                for j in job_details1:
                    query = 'select profile_image, pricing_category from users where user_id = %s'
                    values = (j['employer_id'],)
                    user_details = execute_query(query, values)
                    if user_details:
                        j.update({'profile_image' : user_details[0]['profile_image']})
                        j.update({'pricing_category' : user_details[0]['pricing_category']})
                    else:
                        j.update({'profile_image' : ''})
                        j.update({'pricing_category' : ''})
                new_jobs_added = add_ai_recommendations(job_details1)

                if new_jobs_added == 0:
                    result_json = api_json_response_format(False, "No new job recommendations found.", 200, {})
                # return api_json_response_format(True, "New AI job recommendations generated.", 200, {})
            # else:
            unapplied_ai_jobs_query = "SELECT job_id FROM ai_recommendation WHERE professional_id = %s AND source = 'AI' AND job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s) AND job_id NOT IN (SELECT job_id FROM sc_recommendation WHERE professional_id = %s) LIMIT 3;"
            unapplied_ai_jobs = execute_query(unapplied_ai_jobs_query, (professional_id, professional_id))
                   
            admin_rcmnd_jobs_query = "SELECT job_id FROM sc_recommendation WHERE professional_id = %s AND job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s) LIMIT 2"
            values = (professional_id, professional_id,)
            admin_recmnd_jobs = execute_query(admin_rcmnd_jobs_query, values)
            admin_id_list = []
            if len(admin_recmnd_jobs) > 0:
                admin_id_list = [i['job_id'] for i in admin_recmnd_jobs]
            ai_id_list = [i['job_id'] for i in unapplied_ai_jobs]
            recommended_id_list = admin_id_list + ai_id_list
            if len(recommended_id_list) != 0:
                query_job_details = """
                                SELECT 
                                    jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
                                    jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, 
                                    jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.specialisation, jp.timezone,
                                    jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                                    jp.required_background_check, jp.required_subcontract, jp.is_application_deadline,
                                    jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, u.profile_image, u.user_id as employer_id, u.pricing_category 
                                FROM 
                                    job_post jp 
                                JOIN 
                                    users u ON jp.employer_id = u.user_id WHERE jp.job_status = %s and jp.is_active = %s and  jp.id IN %s ORDER by jp.id DESC;
                                """
                values_job_details = ('Opened', 'Y', recommended_id_list,)
                job_details1 = replace_empty_values(execute_query(query_job_details, values_job_details))

            if len(job_details1) > 0:
                id = job_details1[0]["id"]
                query = 'select * from  view_count where job_id = %s and professional_id = %s'
                values = (id,professional_id,)
                count = execute_query(query,values)
                if not count:
                    current_time = datetime.now()
                    query = "INSERT INTO view_count (job_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                    values = (id, professional_id, current_time,)
                    update_query(query,values)

                for job in job_details1:
                    query = "select employer_type, sector, company_name from employer_profile where employer_id = %s"
                    values = (job['employer_id'],)
                    emp_details = execute_query(query, values)
                    if len(emp_details) > 0:
                        job.update({'employer_type': emp_details[0]['employer_type']})
                        job.update({'company_name': emp_details[0]['company_name']})
                        job.update({'sector': emp_details[0]['sector']})
                        txt = emp_details[0]['sector']
                        txt = txt.replace(", ", "_")
                        txt = txt.replace(" ", "_")
                        sector_name = txt + ".png"
                    job.update({'profile_image' : s3_employeer_logo_folder_name + job['profile_image']})
                    job.update({'sector_image' : s3_sector_image_folder_name + sector_name})
                    job.update({'profile_percentage' : profile_percentage})
                    query = 'select professional_resume from professional_profile where professional_id = %s'
                    values = (professional_id,)
                    rslt = execute_query(query, values)
                    resume_name = rslt[0]['professional_resume'] if rslt else ""
                    job.update({'user_resume' : resume_name})
                    quest_dict = {"questions" : []}
                    job_id = int(job['id'])
                    job['id'] = job_id
                    query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                    values = (job_id,)
                    result = execute_query(query, values)
                    if len(result) > 0:
                        for r in result:
                            quest_dict["questions"].append(r)
                    job.update(quest_dict)
                    if job['created_at'] == "null" or job['created_at'] == None:
                        job['created_at'] = datetime.now()

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
                    job.update({'applied_status': job_applied_status})
                    query = 'select description from sc_recommendation where professional_id = %s and job_id = %s'
                    values = (professional_id, job_id,)
                    sc_recommended_notes= execute_query(query, values)
                    job.update({"recommended_by" : "2nd careers recommended"})
                    if job['id'] not in admin_id_list:
                        job.update({"sc_recommended_notes" : ""})
                        job.update({'recommended_by' : 'AI Recommendation'})
                    else:
                        if len(sc_recommended_notes) > 0:
                            job.update({"sc_recommended_notes" : sc_recommended_notes[0]['description']})
                        else:
                            job.update({"sc_recommended_notes" : ""})
                        job.update({'recommended_by' : '2nd Careers Recommendation'})
                    query = 'SELECT ij.employer_feedback, (SELECT COUNT(job_id) FROM invited_jobs WHERE professional_id = ij.professional_id AND job_id = ij.job_id) AS job_count FROM invited_jobs ij WHERE ij.professional_id = %s AND ij.job_id = %s;'
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
                sorted_job_details = sorted(job_details1, key=lambda x: (x['recommended_by'] != '2nd Career Recommendation', x['recommended_by']))
                job_details_dict = {'job_details': sorted_job_details}
                profile_percentage = {'profile_percentage' : profile_percentage}
                profile.update(job_details_dict)  
                profile.update(profile_percentage)              
                data = fetch_filter_params()
                profile.update(data)
                # background_runner.store_ai_recommended_jobs(profile, professional_id)
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
            else:
                result_json = api_json_response_format(False, "No jobs found.",200)
        else:
            result_json = api_json_response_format(False, "Invalid token", 401)
        # result_json = api_json_response_format(True, "Existing recommendations available.", 200, {})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
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

def process_quries_search(openai_api_key,l_query_txt):
    global g_resume_path
    global g_openai_token_limit  
    res_json = {}
    result = {}

    try:
        if s3_exists(BUCKET_NAME,"job_recommend_prompt.json"):     
            s3_resource = s3_obj.get_s3_resource()
            obj = s3_resource.Bucket(BUCKET_NAME).Object("job_recommend_prompt.json")
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
        print(f"process_quries_search  in professional job recommend error : {error}")
        result["is_error"] = True
        result["result"] = str(error)  
    finally:        
        return result
def vector_search_init(professional_id):
    try:
        profile = get_profile_search(professional_id)
        print("Loaded {} documents".format(len(profile)))

        index_name = PROFILE_INDEX

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

def vector_search_init_new(professional_id):
    try:
        profile = get_profile_search(professional_id)

        # Store documents in Meilisearch
        index_name = MEILISEARCH_PROFESSIONAL_INDEX
        client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
        index = client.index(index_name)
        index.add_documents(profile)

        print("Documents successfully stored in Meilisearch Cloud.")
    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as error:
        print(f"An error occurred while storing documents in Meilisearch: {error}")

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
            'willing_to_relocate':'',
            'country': '',
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
            profile_dict['willing_to_relocate'] = replace_empty_values1(profile_result[0]['willing_to_relocate'])
            profile_dict['country'] = replace_empty_values1(profile_result[0]['country'])
            

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

    
        # with open('data.json', "w") as outfile:
        #     json.dump(profiles, outfile, indent=4)

        return profiles
    except Exception as error:
        # Log the error for debugging purposes
        print("Error:", error)
        return (False, str(error), 500, {})
    
def format_profile(profile_data):
    def convert_date(date_obj):
        if isinstance(date_obj, date):
            return date_obj.strftime("%Y-%m-%d")  # Or any other date format you prefer
        return date_obj

    profile = {
        "user_id": profile_data['professional_id'],
        "email_active" : 'Y',
        "user_role_fk" : 3,
        "id": profile_data['professional_id'],
        "About": profile_data['about'],
        "Willing_to_Relocate": profile_data['willing_to_relocate'],
        "Additional_Information": profile_data['additional_info'],
        "Candidate_Name": f"{profile_data['first_name']} {profile_data['last_name']}",
        "Contact_Information": {
            "Address": profile_data['city'],
            "Email": profile_data['email_id'],
            "Phone_Number": profile_data['contact_number'],
            "Country": profile_data['country']
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

def get_openai_summary(l_openai_api_key,req_prompt): 
    result = {}    
    global openai_api_key
            
    openai_api_key = l_openai_api_key
    OpenAI.api_key = openai_api_key    

    try:                 
        req_messages = [{"role": "user", "content": req_prompt}]
        response = process_openai_completion(req_messages,OpenAI.api_key)
        print(f"process_openai_completion response {response}")
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
 

def Merge(dict1, dict2):
    res = {**dict1, **dict2}
    return res

#@profile
def process_quries(openai_api_key,l_query_txt):
    global g_resume_path
    global g_openai_token_limit  
    res_json = {}
    result = ""

    try:
        if s3_exists(BUCKET_NAME,"extraction_prompts.json"):     
            s3 = s3_obj.get_s3_resource()   

            obj = s3.Object(BUCKET_NAME, "extraction_prompts.json")
            prompt_json = json.load(obj.get()['Body']) 


            # obj = s3.Bucket(BUCKET_NAME).Object("extraction_prompts.json")


            # json_file_content = s3.get_object(Bucket=BUCKET_NAME, Key="extraction_prompts.json")["Body"].read()   
            # json_file_content = obj.get()['Body'].read()


            # json_file_content = obj.get()['Body'].read().decode('utf-8')  
            # prompt_json = json.load(json_file_content)
            level_1 = prompt_json["level_1"]
            level_1_prompt = level_1["prompt"]      
            level_1_prompt = level_1_prompt.replace("{{resume_content}}", "{{"+str(l_query_txt)+"}}")  
            openai_level_1_res = get_openai_summary(openai_api_key,level_1_prompt) 
            if not "error_code" in openai_level_1_res:
                print("no error in process_queries")
                chatbot_level_1_text = openai_level_1_res["summary"]
                chatbot_level_1_text = chatbot_level_1_text.strip('```json')
                chatbot_level_1_text = chatbot_level_1_text.strip('```')
                if "It looks like the document you uploaded isn't a resume. Please upload a resume so we can create your 2C profile" in chatbot_level_1_text:
                    is_error = "True"
                    result = "error: "+str(chatbot_level_1_text )+"#####"+is_error
                else:
                    res_json = json.loads(chatbot_level_1_text)
                    del openai_level_1_res
                    is_error = "False"
                    result = json.dumps(res_json)+"#####"+is_error
            else:
                print("error in process_queries")
                is_error = "True"
                result = "error: "+str(openai_level_1_res["message"] )+"#####"+is_error

    except Exception as error:
        is_error = "False"
        print(error)        
        result = "error: "+str(error)+"#####"+is_error 
    finally:        
        return result

def get_learning_attachment():
    try:             
        result_json = []                
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]  
                query = 'SELECT professional_resume FROM job_activity where professional_id = %s'
                values = (professional_id,)
                rs = execute_query(query,values)                         
                if len(rs) > 0:
                    pdf = rs[0]["professional_resume"]
                        
                    # return send_file(test, attachment_filename='document.pdf', as_attachment=True)
                else:             
                    result_json = api_json_response_format(False,"File not found. Please try again.",204,{}) 
            else:
                result_json = api_json_response_format(False,token_result["status"],401,{})     
        else:
            result_json = api_json_response_format(False,token_result["status"],401,{})     
    except Exception as error:
        print(error)
        result_json = api_json_response_format(False,str(error),500,{})            
    finally:
        return Response(
                    pdf,
                    mimetype="text/pdf",
                    headers={"Content-disposition":
                            "attachment; filename=attachment.pdf"})

# def update_dob_status():
#     try:
#         result_json = {}
#         token_result = get_user_token(request)                                        
#         if token_result["status_code"] == 200:                                
#             user_data = get_user_data(token_result["email_id"])            
#             if user_data["user_role"] == "professional":
#                 req_data = request.get_json()
#                 professional_id = user_data["user_id"]
#                 if 'show' not in req_data:
#                     result_json = api_json_response_format(False,"Show status is required",204,{})
#                     return result_json
#                 status = (req_data['show']).upper()
#                 status_dict = {"show_status" : status}
#                 query = 'update professional_profile set show_to_employer = %s where professional_id = %s'
#                 values = (status, professional_id)
#                 row_count = update_query(query, values)
#                 if row_count > 0:
#                     result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,status_dict)
#                 else:
#                     result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",401,{})
#             else:
#                 result_json = api_json_response_format(False,"Unauthorized user",401,{})
#         else:
#                 result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
#     except Exception as error:
#         print(error)        
#         result_json = api_json_response_format(False,str(error),500,{})        
#     finally:        
#         return result_json
    
def show_pages():
    try:
        result_json = {}
        data = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]
                file = open('/home/applied/Documents/new/second_careers_project/config_features.json')
                file_data = json.load(file)
                if file_data['learning'] == 'Y':
                    learning_result = professional_learning()   
                    data.append({"learning" : learning_result['data']})
                if file_data['community'] == 'Y':
                    community_result = professional_community()   
                    data.append({"community" : community_result['data']})
                if file_data['expert_notes'] == 'Y':
                    query = 'select expert_notes from professional_profile where professional_id = %s'
                    values = (professional_id,)
                    expert_notes_result = execute_query(query, values)   
                    data.append({"expert_notes" : expert_notes_result})
                result_json = api_json_response_format(True,"Flag updated successfully",0,data)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def professional_meilisearch():        
    result_json = {}
    try:   
        key_id = 0    
        profile={}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:  
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json() 
                if 'search_index' not in req_data:
                    result_json = api_json_response_format(False,"Search data required",204,{})  
                    return result_json  
                search_index = req_data['search_index']                           
                index_name = "Professional_search"
                client = meilisearch.Client(meilisearch_url)
                response = client.index(index_name).search(search_index)
                if response['hits'] != []:
                    for i in range(len(response['hits'])):
                        job_tiltle = response['hits'][i]['job_title']
                        job_type = response['hits'][i]['job_type']
                        description = response['hits'][i]['description']
                        location = response['hits'][i]['location']
                        query = 'select job_title, job_type, job_status, specialisation, skills, country, state, city, work_schedule, workplace_type, number_of_openings, time_commitment, duration, calendly_link, currency, benefits, required_resume, required_cover_letter, required_background_check, required_subcontract, is_application_deadline, application_deadline_date, is_paid, is_active,job_desc, created_at from job_post where job_title = %s OR job_type = %s OR job_desc = %s OR city = %s '
                        values = (job_tiltle,job_type,description,location,)
                        rs = execute_query(query,values)                         
                        if len(rs) > 0:
                            profile["job_title"] = rs[0]["job_title"]   
                            profile["job_type"] = rs[0]["job_type"]        
                            profile["country"] = rs[0]["country"]
                            profile["city"] = rs[0]["city"]
                            profile["work_schedule"] = rs[0]["work_schedule"]
                            profile["workplace_type"] = rs[0]["workplace_type"]
                            profile["is_paid"] = rs[0]["is_paid"]
                            profile["job_desc"] = rs[0]["job_desc"]
                            profile["created_at"] = rs[0]["created_at"]
                    res = not bool(profile)
                    if res : 
                        result_json = api_json_response_format(False,"Sorry, no results match your search criteria. Please try again.",204,{})   
                        return result_json
                    result_json = api_json_response_format(True,"Search results displayed successfully!",0,replace_empty_values(rs))
                else:
                    result_json = api_json_response_format(False,"No search result found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorised User.",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid token. Please try again.",401,{})
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
  
def update_professional_experience():        
    result_json = {}
    try:        
        experience_id = 0        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])                        
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                  
                req_data = request.get_json()  
                company_name,job_title,start_month,start_year,end_month,end_year,job_description,job_location="","","","","","","",""                                                   
                if 'experience_id' in req_data:
                    experience_id = req_data['experience_id']
                flag_user_exist = isUserExist("professional_experience","id",experience_id)                                
                if 'company_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_title' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'start_date' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                # if 'current_job' in req_data:
                #     current_job_status = "Present" 
                if 'end_date' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'job_location' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json    

                company_name = req_data['company_name']
                job_title = req_data['job_title'] 
                # start_month = req_data['start_date'].split('-')[1]
                # start_year = req_data['start_date'].split('-')[0]
                start_date = req_data['start_date']
                if req_data['end_date'] == "Present":
                    # end_month = "Present"
                    # end_year = "Present"
                    end_date = req_data['end_date']
                else:
                    # end_month = req_data['end_date'].split('-')[1]
                    # end_year = req_data['end_date'].split('-')[0]
                    end_date = req_data['end_date']

                job_description = req_data['job_description']
                job_location = req_data['job_location']                                        
                is_exp = 'N'       
                row_count = -1
                process_msg = ""
                if flag_user_exist:
                    is_exp = 'Y'
                    query = 'update professional_experience set is_experienced=%s,company_name=%s,job_title=%s,start_month=%s,start_year=%s,end_month=%s,end_year=%s,job_description=%s, job_location=%s where id = %s and professional_id = %s'
                    values = (is_exp,company_name,job_title,start_month,start_date,end_month,end_date,job_description,job_location,experience_id,professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                else:
                    created_at = datetime.now()
                    is_exp = 'Y'
                    query = 'insert into professional_experience (professional_id,is_experienced,company_name,job_title,start_month,start_year,end_month,end_year,job_location,job_description,created_at) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    values = (professional_id,is_exp,company_name,job_title,start_month,start_date,end_month,end_date,job_location,job_description,created_at,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                if row_count > 0:
                    query = 'select id, company_name,job_title,start_month,start_year,end_month,end_year,job_location,job_description from professional_experience where professional_id = %s'
                    values = (professional_id,)
                    rslt = execute_query(query,values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional experience updated successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Experience Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Experience Updation",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Experience Updation, {str(e)}")
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,process_msg,0,rslt)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Unable to update professional experience."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Experience Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Experience Updation Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Experience Updation Error, {str(e)}")
                    result_json = api_json_response_format(False, "Sorry! We had an issue with updating your profile. We request you to retry.")
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional experience.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Experience Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Experience Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Experience Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_experience_data():
    result_json = {}
    try:                
        req_data = request.get_json() 
        if 'experience_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        experience_id = req_data['experience_id']
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                               
                query = 'select id,job_title, company_name, job_location, start_month, start_year, end_month, end_year, job_description from professional_experience where professional_id = %s and id = %s'
                values = (professional_id, experience_id,)
                experience_data_set = execute_query(query, values)
                start_month = experience_data_set[0]['start_month']   
                start_year = experience_data_set[0]['start_year']       
                end_month = experience_data_set[0]['end_month']       
                end_year = experience_data_set[0]['end_year']       
                # start_date = format_date(start_year, start_month)
                if experience_data_set[0]['start_year'] != None: 
                    start_date = experience_data_set[0]['start_year']
                else:
                    start_date = ''
                if experience_data_set[0]['end_year'] != None:
                    if end_month == "Present" or end_year == "Present":  
                        end_date = "Present"
                    else:
                        # end_date = format_date(end_year, end_month)
                        end_date = experience_data_set[0]['end_year']
                else:
                    end_date = ''
                experience_data_set[0].pop("start_month")
                experience_data_set[0].pop("start_year")
                experience_data_set[0].pop("end_month")
                experience_data_set[0].pop("end_year")
                experience_data_set[0].update({"start_date" : start_date})
                experience_data_set[0].update({"end_date" : end_date})

                if len(experience_data_set) > 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional experience data displayed successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Experience", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Experience",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Experience, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,experience_data_set)
                else:       
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to display professional experience.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Experience Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Experience Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Experience Error, {str(e)}")      
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional experience.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Experience Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Experience Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional Experience Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def delete_professional_experience():
    result_json = {}
    try:                
        req_data = request.get_json()    
        if 'experience_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        experience_id = req_data['experience_id']
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                query = "select * from professional_experience where id = %s and professional_id = %s"                
                values = (experience_id,professional_id,)
                data_set = execute_query(query,values) 
                if(len(data_set) > 0):                
                    query = 'delete from professional_experience where id = %s and professional_id = %s'
                    values = (experience_id,professional_id,)
                    row_count = update_query(query, values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional experience data deleted successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Experience", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Experience",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Experience, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})                    
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Unable to delete professional experience.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Experience Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Experience Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Experience Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})  
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to delete professional experience.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Experience Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Experience Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:
                        print(f"Error in mixpanel event logging: Deleting Professional Experience Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",401,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to delete professional experience.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Experience Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Experience Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:
            print(f"Error in mixpanel event logging: Deleting Professional Experience Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def update_professional_education():        
    result_json = {}
    try:        
        education_id = 0
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()                
                if 'institute_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'institute_location' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'degree_level' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'specialisation' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json                
                if 'start_date' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'end_date' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'education_id' in req_data:
                    education_id = req_data['education_id']
                
                institute_name = req_data['institute_name']
                institute_location = req_data['institute_location']
                degree_level = req_data['degree_level']
                specialisation = req_data['specialisation']                
                # start_month = req_data['start_date'].split('-')[1]
                # start_year = req_data['start_date'].split('-')[0]
                # end_month = req_data['end_date'].split('-')[1]
                # end_year = req_data['end_date'].split('-')[0] 
                start_month = ''
                end_month = ''
                start_date = req_data['start_date']
                end_date = req_data['end_date']
                professional_id = user_data["user_id"]
                row_count = -1
                process_msg = ""
                if isUserExist("professional_education","id",education_id): 
                    query = 'update professional_education set institute_name = %s,institute_location=%s,degree_level=%s,specialisation=%s,start_month=%s,start_year=%s,end_month=%s,end_year=%s where id = %s and professional_id = %s'
                    values = (institute_name,institute_location,degree_level,specialisation,start_month,start_date,end_month,end_date,education_id,professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                else:
                    created_at = datetime.now()
                    query = 'insert into professional_education(institute_name,institute_location,degree_level,specialisation,start_month,start_year,end_month,end_year,created_at,professional_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    values = (institute_name,institute_location,degree_level,specialisation,start_month,start_date,end_month,end_date,created_at,professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                if row_count > 0:
                    query = 'select id, institute_name,institute_location,degree_level,specialisation,start_month,start_year,end_month,end_year from professional_education where professional_id = %s'
                    values = (professional_id,)
                    rslt = execute_query(query,values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional education updated successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Education Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Education Updation",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Education Updation, {str(e)}")
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,process_msg,0,rslt)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Unable to update professional education."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Education Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Education Updation Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Education Updation Error, {str(e)}")
                    result_json = api_json_response_format(False, "Sorry! We had an issue with updating your profile. We request you to retry.")
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional education.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Education Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Education Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Education Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_education_data():
    result_json = {}
    try:                
        req_data = request.get_json()                
        if 'education_id' not in req_data:
            result_json = api_json_response_format(False,"education_id required",204,{})  
            return result_json
        education_id = req_data["education_id"]
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                if isUserExist("professional_education","id",education_id):                
                    query = 'select id, institute_name, institute_location, degree_level, specialisation, start_month, start_year, end_month, end_year, is_pursuing from professional_education where professional_id = %s and id = %s'
                    values = (professional_id, education_id,)
                    education_data_set = execute_query(query, values)
                    start_month = education_data_set[0]['start_month']   
                    start_year = education_data_set[0]['start_year']       
                    end_month = education_data_set[0]['end_month']       
                    end_year = education_data_set[0]['end_year']       
                    # start_date = format_date(start_year, start_month) 
                    # end_date = format_date(end_year, end_month) 
                    if education_data_set[0]['start_year'] != None:
                        start_date = education_data_set[0]['start_year']
                    else:
                        start_date = ''
                    if education_data_set[0]['end_year'] != None:
                        end_date = education_data_set[0]['end_year'] 
                    else:
                        end_date = ''
                    education_data_set[0].pop("start_month")
                    education_data_set[0].pop("start_year")
                    education_data_set[0].pop("end_month")
                    education_data_set[0].pop("end_year")
                    education_data_set[0].update({"start_date" : start_date})
                    education_data_set[0].update({"end_date" : end_date})
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional education displayed successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Education", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Education",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Education, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(education_data_set))
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to display professional Education.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Education Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Education Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Education Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional Education.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Education Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Education Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional Education Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def delete_professional_education():
    result_json = {}
    try:                
        req_data = request.get_json()                
        if 'education_id' not in req_data:
            result_json = api_json_response_format(False,"education_id required",204,{})  
            return result_json
        education_id = req_data["education_id"]
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                query = "select * from professional_education where id = %s and professional_id = %s"                
                values = (education_id,professional_id,)
                data_set = execute_query(query,values) 
                if(len(data_set) > 0):                  
                    query = 'delete from professional_education where id = %s and professional_id = %s '
                    values = (education_id,professional_id,)
                    row_count = update_query(query, values)
                    if(row_count > 0):
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional education deleted successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Education", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Education",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Education, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})                    
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Unable to delete professional Education.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Education Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Education Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Education Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{}) 
                else:   
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to delete professional Education.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Education Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Education Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Deleting Professional Education Error, {str(e)}")          
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to delete professional Education.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Education Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Education Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:
            print(f"Error in mixpanel event logging: Deleting Professional Education Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def update_professional_skills():        
    result_json = {}
    try:   
        skill_id = 0     
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()
                if 'skill_name' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'skill_level' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                professional_id = user_data["user_id"]
                skill_name = req_data["skill_name"]
                skill_level = req_data['skill_level']
                if 'skill_id' in req_data:
                    skill_id = req_data['skill_id']
                row_count = -1
                process_msg = ""
                if isUserExist("professional_skill","id",skill_id): 
                    query = 'update professional_skill set skill_name = %s, skill_level = %s where id = %s and professional_id = %s'
                    values = (skill_name, skill_level, skill_id, professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                else:
                    created_at = datetime.now()
                    query = 'insert into professional_skill(skill_name,skill_level,created_at,professional_id) values(%s,%s,%s,%s)'
                    values = (skill_name, skill_level, created_at,professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                if row_count > 0:
                    query = 'select id, skill_name,skill_level from professional_skill where professional_id = %s'
                    values = (professional_id,)
                    rslt = execute_query(query,values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional skills updated successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Skills Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Skills Updation",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Skills Updation, {str(e)}")
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,process_msg,0,rslt)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Unable to update professional skills."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Skills Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Skills Updation Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Skills Updation Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional skills.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Skills Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Skills Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Skills Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_skills_data():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:  
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                if isUserExist("professional_skill","professional_id",professional_id):                
                    query = 'select id,skill_name, skill_level from professional_skill where professional_id = %s '
                    values = (professional_id,)
                    result = execute_query(query, values)
                    result = replace_empty_values(result)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional skills displayed successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Skills", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Skills",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Skills, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,result)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to display professional skills.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Skills Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Skills Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Skills Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional skills.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Skills Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Skills Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional Skills Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def delete_professional_skill():
    result_json = {}
    try:                
        req_data = request.get_json()
        if 'skill_id' not in req_data:
            result_json = api_json_response_format(False,"skill_id required",204,{})  
            return result_json
        skill_id = req_data['skill_id']
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                                
                query = 'select * from professional_skill where professional_id = %s and id = %s'
                values = (professional_id, skill_id,)
                result = execute_query(query, values,)
                if len(result) > 0:
                    query = 'delete from professional_skill where id = %s and professional_id = %s'
                    values = (skill_id, professional_id,)
                    row_count = update_query(query, values)
                    if(row_count > 0):
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional skills deleted successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Skills", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Skills",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Skills, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})                    
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Unable to delete professional skills.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Skills Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Skills Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Skills Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to delete professional skills.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Skills Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Skills Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Deleting Professional Skills Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",204,{})                
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to delete professional skills.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Skills Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Skills Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:
            print(f"Error in mixpanel event logging: Deleting Professional Skills Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_selected_professional_skill_data():
    result_json = {}
    try:        
        key_id = 2
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                if isUserExist("professional_profile","professional_id",professional_id):                
                    query = 'select skill_name, skill_level from professional_skill where id = %s'
                    values = (key_id,)
                    data_set = execute_query(query, values)                    
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",204,data_set)
                else:             
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
      
def update_professional_language():        
    result_json = {}
    try:        
        language_id = 0                         
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()
                if 'language_known' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'language_level' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                professional_id = user_data["user_id"]
                language_known = req_data["language_known"]
                language_level = req_data['language_level']
                if 'language_id' in req_data:
                    language_id = req_data['language_id']
                row_count = -1
                process_msg = ""
                if isUserExist("professional_language","id",language_id): 
                    query = 'update professional_language set language_known = %s, language_level = %s where id = %s and professional_id = %s'
                    values = (language_known, language_level, language_id, professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Language details updated successfully"
                else:
                    created_at = datetime.now()
                    query = 'insert into professional_language(professional_id,language_known,language_level,created_at) values(%s,%s,%s,%s)'
                    values = (professional_id, language_known, language_level, created_at,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                if row_count > 0:
                    query = "select id, language_known,language_level from professional_language where professional_id = %s"
                    values = (professional_id,)
                    rslt = execute_query(query, values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional languages updated successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Languages Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Languages Updation",event_properties,temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Languages Updation, {str(e)}")
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,process_msg,0,rslt)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Unable to update professional languages."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Languages Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Languages Updation Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Languages Updation Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional languages.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Languages Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Languages Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Languages Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_language_data():
    result_json = {}
    try:                
        req_data = request.get_json()
        if 'language_id' not in req_data:
            result_json = api_json_response_format(False,"language_id required",204,{})  
            return result_json
        language_id = req_data["language_id"]
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                if isUserExist("professional_profile","professional_id",professional_id):                
                    query = 'select id,language_known, language_level from professional_language where professional_id = %s and id = %s'
                    values = (professional_id, language_id,)
                    language_data_set = execute_query(query, values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional languages displayed successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Languages", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Languages",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Languages, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(language_data_set))
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to display professional languages.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Languages Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Languages Error",event_properties,temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Languages Error, {str(e)}")         
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional languages.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Languages Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Languages Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional Languages Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def delete_professional_language():
    result_json = {}
    try:                
        req_data = request.get_json()
        if 'language_id' not in req_data:
            result_json = api_json_response_format(False,"language_id required",204,{})  
            return result_json
        language_id = req_data["language_id"]
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                                
                query = 'select * from professional_language where professional_id = %s and id = %s'
                values = (professional_id, language_id,)
                result = execute_query(query, values)
                if len(result) > 0:                
                    query = 'delete from professional_language where professional_id = %s and id = %s'
                    values = (professional_id,language_id,)
                    row_count = update_query(query, values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional languages deleted successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Languages", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Languages",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Languages, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})                    
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Unable to delete professional languages.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Languages Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Languages Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Languages Error, {str(e)}")
                        result_json = api_json_response_format(True,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})   
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to delete professional languages.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Languages Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Languages Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Deleting Professional Languages Error, {str(e)}")            
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to delete professional languages.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Languages Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Languages Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:
            print(f"Error in mixpanel event logging: Deleting Professional Languages Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def update_professional_preferences():        
    result_json = {}
    try:             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()
                if 'preferences' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                professional_id = user_data["user_id"]
                preferences = req_data["preferences"]
                if isUserExist("professional_profile","professional_id",professional_id): 
                    query = 'update professional_profile set preferences = %s where professional_id = %s'
                    values = (preferences, professional_id,)
                    row_count = update_query(query,values)
                    if row_count > 0:
                        query = "select preferences from professional_profile where professional_id = %s"
                        values = (professional_id,)
                        rslt = execute_query(query, values)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional preferences updated successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Preferences Updation", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Preferences Updation",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Preferences Updation, {str(e)}")
                        background_runner.get_professional_details(user_data['user_id'])
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,rslt)
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': "Unable to update professional preferences."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Preferences Updation Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Preferences Updation Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Preferences Updation Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                else:
                    result_json = api_json_response_format(False,"User Profile Not Found",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional preferences.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Preferences Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Preferences Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Preferences Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def get_professional_preferences_data():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]                
                if isUserExist("professional_profile","professional_id",professional_id):
                    query = 'select id,preferences from professional_profile where professional_id = %s'
                    values = (professional_id,)
                    preference_data_set = execute_query(query, values) 
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional preferences displayed successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Preferences", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Preferences",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Preferences, {str(e)}")                  
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(preference_data_set))
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to display professional preferences.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Preferences Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Preferences Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Preferences Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional preferences.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Preferences Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Preferences Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional Preferences Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json    
    
def update_professional_about():        
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()
                if 'about' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                professional_id = user_data["user_id"]
                about = req_data["about"]
                if isUserExist("professional_profile","professional_id",professional_id): 
                    query = 'update professional_profile set about = %s where professional_id = %s'
                    values = (about, professional_id,)
                    row_count = update_query(query,values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional about updated successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional About Updation", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional About Updation",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional About Updation, {str(e)}")
                        background_runner.get_professional_details(user_data['user_id'])
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': "Unable to update professional about."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional About Updation Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional About Updation Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional About Updation Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
                else:                        
                    result_json = api_json_response_format(False,"Professional profile Not Found",204,{})                   
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional about.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional About Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional About Updation Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional About Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_about_data():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                                
                query = 'select about from professional_profile where professional_id = %s'
                values = (professional_id,)
                about_data_set = execute_query(query, values)
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': 'Professional about displayed successfully'}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional About", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional About",event_properties, temp_dict.get('Message'), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Fetching Professional About, {str(e)}")
                result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,replace_empty_values(about_data_set))
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional about.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional About Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional About Error",event_properties,temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional About Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json    
    
def update_professional_social_link():        
    result_json = {}
    try:        
        link_id = 0
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()
                if 'title' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'url' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                professional_id = user_data["user_id"]
                title = req_data["title"]
                url = req_data['url']
                if 'link_id' in req_data:
                    link_id = req_data['link_id']
                row_count = -1
                process_msg = ""
                if isUserExist("professional_social_link","id",link_id): 
                    query = 'update professional_social_link set title = %s, url = %s where id = %s and professional_id = %s'
                    values = (title, url, link_id, professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                else:
                    created_at = datetime.now()
                    query = 'insert into professional_social_link(professional_id,title,url,created_at) values(%s,%s,%s,%s)'
                    values = (professional_id, title, url, created_at,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                if row_count > 0:
                    query = "select id, title, url from professional_social_link where professional_id = %s"
                    values = (professional_id,)
                    rslt = execute_query(query, values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional social link updated successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Social Link Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Social Link Updation",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Social Link Updation, {str(e)}")
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,process_msg,0,rslt)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Unable to update professional social link."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Social Link Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Social Link Updation Error",event_properties,temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Social Link Updation Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional social link.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Social Link Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Social Link Updation Error",event_properties,temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Social Link Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def get_professional_social_link_data():
    result_json = {}
    try:                
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                if isUserExist("professional_profile","professional_id",professional_id):                
                    query = 'select title, url from professional_social_link where professional_id = %s '
                    values = (professional_id,)
                    link_data_set = execute_query(query, values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional social link displayed successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Social Link", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Social Link",event_properties,temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Social Link, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,link_data_set)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Unable to display professional social link.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Social Link Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Social Link Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Fetching Professional Social Link Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional social link.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Social Link Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Social Link Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional Social Link Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def get_selected_professional_social_link_data():
    result_json = {}
    try:                
        req_data = request.get_json()   
        if 'link_id' not in req_data:
            result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
            return result_json
        link_id = req_data["link_id"]
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                                
                query = 'select id,title, url from professional_social_link where professional_id = %s and id = %s '
                values = (professional_id,link_id,)
                link_data_set = execute_query(query, values)
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': 'Professional social link displayed successfully'}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Social Link", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Social Link",event_properties, temp_dict.get('Message'), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Fetching Professional Social Link, {str(e)}")
                result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,link_data_set) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to display professional social link.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Fetching Professional Social Link Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Fetching Professional Social Link Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Fetching Professional Social Link Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def delete_professional_social_link():
    result_json = {}
    try:        
        req_data = request.get_json()   
        if 'link_id' not in req_data:
            result_json = api_json_response_format(False,"link_id required",204,{})  
            return result_json
        link_id = req_data["link_id"]
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                                                                
                query = 'select * from professional_social_link where professional_id = %s and id = %s'
                values = (professional_id, link_id,)
                result = execute_query(query, values)
                if len(result) > 0:                
                    query = 'delete from professional_social_link where professional_id = %s and id = %s'
                    values = (professional_id,link_id,)
                    row_count = update_query(query, values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional social link deleted successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Social Link", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Social Link",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Social Link, {str(e)}")                       
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})                    
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Unable to delete professional social link.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Social Link Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Social Link Error",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Social Link Error, {str(e)}")
                        result_json = api_json_response_format(True,"Sorry! We had an issue with retrieving your profile. We request you to retry.",500,{})   
                else:             
                    result_json = api_json_response_format(False,"Professional social link not found",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to delete professional social link.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Social Link Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Social Link Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:
            print(f"Error in mixpanel event logging: Deleting Professional Social Link Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def professional_job_apply():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        user_data = get_user_data(token_result["email_id"])            
        job_id = 1  
        res = request.form
        if res['status_questions_list'] == 'null':
            result_json = api_json_response_format(True,"Questions not  posted for this job",0,{})
        else:
            converted_data = {}
            for key, value in res.items():
                if key.startswith("questions_list"):
                    index = int(key.split("[")[1].split("]")[0])
                    field_name = key.split("[")[-1].split("]")[0]
                    if index not in converted_data:
                        converted_data[index] = {}
                    converted_data[index][field_name] = value
            final_data = {"questions_list": list(converted_data.values())} 
        if token_result["status_code"] == 200:                                
            if user_data["user_role"] == "professional":
                if 'job_id' not in res:
                    result_json =  api_json_response_format(False, "Job id required", 500, {})
                    return result_json
                job_id = int(res["job_id"])
                professional_id = user_data["user_id"]
                query = 'select * from job_activity where professional_id = %s and job_id = %s'
                values = (professional_id, job_id,)
                result = execute_query(query, values)
                query = 'select required_resume, required_cover_letter, job_title from job_post where id = %s'
                values = (job_id,)
                doc_status = execute_query(query, values)
                if len(result) > 0: 
                    result_json = api_json_response_format(False,"You have already applied for this job.",201,{})
                    return result_json
                else:
                    if res['status_questions_list'] == 'null':
                        result_json = api_json_response_format(True,"Questions not posted for this job",0,{})
                    else:
                        created_at = datetime.now()
                        for i in final_data['questions_list']:
                            print(i)
                            query = 'select count(custom_pre_screen_ans) from pre_screen_ans where job_id=%s and pre_screen_ques_id=%s and professional_id=%s'
                            values = (job_id, i['question_id'], professional_id,)
                            result = execute_query(query, values)
                            if result[0]['count(custom_pre_screen_ans)'] == 0:
                                question_id = i['question_id']
                                answer = i['answer']
                                query = 'insert into pre_screen_ans(job_id,pre_screen_ques_id,professional_id,custom_pre_screen_ans,created_at) values (%s,%s,%s,%s,%s)'
                                values = (job_id ,question_id, professional_id, answer, created_at,)
                                result = update_query(query, values)
                                result_json = api_json_response_format(True,"Answers stored successfully",0,{})           
                            else:
                                result_json = api_json_response_format(False,"Answers already stored for this job",204,{})                        
                    created_at = datetime.now()
                    applied_on = created_at
                    query = 'insert into job_activity (job_id,professional_id,applied_on,application_status,created_at) values (%s,%s,%s,%s,%s)'
                    values = (job_id,professional_id,applied_on,"Not Reviewed",created_at,)
                    row_count = update_query(query, values)
                    if row_count > 0:
                        if doc_status[0]['required_resume'] == 'Y':
                            if res['resume_name'] != '':
                                resume_name = str(res['resume_name'])
                                query = "update job_activity set professional_resume = %s where professional_id = %s and job_id=%s"
                                values = (resume_name, professional_id,job_id,)
                                row_count = update_query(query, values)                            
                                if row_count > 0:                                
                                    result_json = api_json_response_format(True, "Resume updated successfully", 0,{})
                                else:
                                    result_json = api_json_response_format(False,"Could not upload your resume",500 ,{})
                                    return result_json
                            else:
                                if request.files['resume'] is not None :
                                    resume = request.files['resume']
                                    s3_pro = s3_obj.get_s3_client()   
                                    s3_pro.upload_fileobj(resume, S3_BUCKET_NAME, s3_resume_folder_name+resume.filename)
                                    # resume_text = request.files['resume'].read()
                                    # size = len(resume.read())
                                    if(resume.filename == '' or not resume):
                                        result_json = api_json_response_format(True,"This job requires a resume. Please upload a resume.",204 ,{})
                                        return result_json                               
                                    if((resume.filename.endswith(".pdf") or resume.filename.endswith(".docx"))):                       
                                        query = "update job_activity set professional_resume = %s where professional_id = %s and job_id=%s"
                                        values = (resume.filename, professional_id,job_id,)
                                        row_count = update_query(query, values)                            
                                        if row_count > 0:                                
                                            result_json = api_json_response_format(True, "Resume uploaded successfully", 0,{})
                                        else:
                                            result_json = api_json_response_format(False,"Could not upload your resume",500 ,{})
                                            return result_json
                                    else:
                                        result_json = api_json_response_format(False,"Unsupported resume format or resume is too large",204 ,{})
                                        return result_json
                                else:
                                    result_json = api_json_response_format(False,"Resume is required to apply for this job",204 ,{})
                                    return result_json                
                        if doc_status[0]['required_cover_letter'] == 'Y':
                            if request.files['cover_letter'] is not None:                        
                                cover_letter = request.files['cover_letter']
                                s3_pro = s3_obj.get_s3_client()   
                                s3_pro.upload_fileobj(cover_letter, S3_BUCKET_NAME, s3_cover_letter_folder_name+cover_letter.filename)
                                # cover_letter_text = request.files['cover_letter'].read()
                                # size = len(cover_letter.read())                        
                                if(cover_letter.filename == '' or not cover_letter):
                                    result_json = api_json_response_format(True,"This job requires a cover letter. Please upload a cover letter.",204 ,{})
                                    return result_json
                                if((cover_letter.filename.endswith(".pdf") or cover_letter.filename.endswith(".docx")) ):                            
                                    query = "update job_activity set professional_cover_letter = %s where professional_id = %s and job_id=%s"
                                    values = (cover_letter.filename, professional_id,job_id,)
                                    row_count = update_query(query, values)
                                    if row_count > 0:
                                        result_json = api_json_response_format(True, "Cover_letter uploaded successfully", 0,{})
                                    else:
                                        result_json = api_json_response_format(False,"Could not upload your cover_letter",500 ,{})
                                        return result_json
                                else:
                                    result_json = api_json_response_format(False,"Unsupported cover_letter format or cover_letter is too large",204 ,{}) 
                                    return result_json
                            else:
                                result_json = api_json_response_format(False,"Cover Letter is required to apply for this job",204 ,{})
                                return result_json
                        get_job_details = "select id, job_title, employer_id, job_status, receive_notification from job_post where id = %s"
                        title_values = (job_id,)
                        job_details = execute_query(get_job_details, title_values)
                        if len(job_details) > 0:
                            job_title = job_details[0]['job_title']
                            employer_id = job_details[0]['employer_id']
                            job_id = job_details[0]['id']
                            job_status = job_details[0]['job_status'] 
                            receive_notification = job_details[0]['receive_notification']
                        else:
                            job_title = ""
                            employer_id = 0
                            id = 0
                            receive_notification = ""
                        created_at = datetime.now()
                        base_url = "https://devapp.2ndcareers.com/get_selected_professional_detail"
                        # url = f"{base_url}?job_id={job_id}&user_id={professional_id}"
                        message = f"New Application Received: A candidate has applied for the position of {job_title}. <br/><a href=https://devapp.2ndcareers.com/employer_dashboard/candidates?job_id={job_id}&&id={professional_id}&&job_status={job_status} target='_self'>Check their details now!</a>"
                        # message = f"New Application Received: A candidate has applied for the position of {job_title}. Check their details now! - <a href='{url}'>Link to applicant details on applicant tab</a>"
                        # notification_msg = f"New Application Received: A candidate has applied for the position of {job_title}. 'Check their details now! - <Link to applicant details on applicant tab>'"
                        # if flag == 'emp':
                        #     professional_id = employer_id
                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                        values = (employer_id, message, created_at)
                        update_query(query,values) 
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Job Title' : doc_status[0]['job_title'],
                                        'Message': f"{user_data['email_id']} has successfully submitted an application for the {doc_status[0]['job_title']} position." if doc_status and 'job_title' in doc_status[0] else "Job title not found."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Apply", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Apply",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Job Apply, {str(e)}")
                        if receive_notification == 'Y':
                            query = 'select flag, created_at from job_activity where job_id = %s order by created_at'
                            values = (job_id,)
                            existing_row_count = execute_query(query, values)
                            if len(existing_row_count) > 0:
                                if existing_row_count[0]['flag'] < 6:
                                    flag_updation_query = "update job_activity set flag = flag + 1 where job_id = %s"
                                    flag_updation_values = (job_id,)
                                    update_query(flag_updation_query,flag_updation_values)
                                    background_runner.send_email_to_employer(professional_id, employer_id, job_id)
                        result_json = api_json_response_format(True,"Applied for the job successfully!",0,{})
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Job Title' : doc_status[0]['job_title'],
                                        'Message': f"{user_data['email_id']} is facing an error while submitting the application for the {doc_status[0]['job_title']} position." if doc_status and 'job_title' in doc_status[0] else "Job title not found."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Apply", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Apply",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Job Apply, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
                        return result_json
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
        # result_json = api_json_response_format(True, "Job applied successfully", 0,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in professional job apply"}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Apply Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Apply Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Job Apply Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

### search and filter , filter and search in single endpoint
# def fetch_filter_results():
#     try:
#         data = []
#         profile = {}
#         country = []
#         city = []
#         token_result = get_user_token(request)                                        
#         if token_result["status_code"] == 200:                                
#             user_data = get_user_data(token_result["email_id"])            
#             if user_data["user_role"] == "professional":
#                 professional_id = user_data['user_id']
#                 req_data = request.get_json()
#                 if 'page_number' not in req_data:
#                         result_json = api_json_response_format(False,"page number required",204,{})  
#                         return result_json
#                 page_number = req_data["page_number"]
#                 location = req_data["location"]
#                 specialisation = req_data["specialisation"]
#                 search_text = req_data.get("search_text", "").strip()
                
#                 skills_list = ["Agile Methodologies", "Algorithm Design", "Analytics", "Application Programming Interfaces (APIs)", "Budgeting", "Business Strategy", "Change Management", "Conflict Resolution", "Contract Management Skills", "Data Analysis", "Database Design", "Debugging", "Direct Sales", "Earned Value Management", "Financial Management", "Human Resource Management", "Keyword Research", "Leadership Skills", "Market Research", "Marketing Skills", "Metrics and KPIs", "Mobile Application Development", "Negotiation", "Operations Management", "Organizational Development", "Presentation", "Process Improvement", "Product Knowledge", "Project Management", "Quality Assurance (QA)", "Recruiting", "Revenue Expansion", "Risk Assessment", "SaaS Knowledge", "Sales and Budget Forecasting", "Salesforce", "Strategic Planning", "Supply Chain Management", "Talent Management", "Team Leadership", "Upselling"]
#                 specialisation_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite And Board"]
#                 sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
#                 schedule_list = ["Fixed", "Flexible", "Monday to Friday", "Weekend only"]
#                 # plan = req_data['plan']

#                 if len(location) != 0:
#                     for i in range(len(location)):
#                         res = location[i].split("&&&&&")
#                         country.append(res[1])
#                         city.append(res[0])
                
#                 offset = (page_number - 1) * 10
#                 base_query = """
#                     FROM
#                     job_post jp
#                     LEFT JOIN
#                     users u ON jp.employer_id = u.user_id
#                     LEFT JOIN
#                     job_activity ja ON jp.id = ja.job_id
#                     LEFT JOIN
#                     employer_profile ep ON jp.employer_id = ep.employer_id
#                     LEFT JOIN 
#                     sub_users su ON jp.employer_id = su.sub_user_id
#                     WHERE
#                     (jp.job_status = 'Opened') AND NOT EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s ) 
#                 """

#                 query_job_details = """
#                     SELECT DISTINCT 
#                     jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
#                     jp.additional_info, jp.skills, jp.job_status, jp.country, jp.state, jp.city, jp.work_schedule, 
#                     jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.timezone, jp.specialisation,
#                     jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
#                     jp.required_background_check, jp.required_subcontract, jp.is_application_deadline,
#                     jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at,
#                     COALESCE(u.profile_image, su.profile_image) AS profile_image, 
#                     COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
#                     COALESCE(u.payment_status, su.payment_status) AS payment_status,
#                     COALESCE(ep.sector, su.sector) AS sector,
#                     COALESCE(ep.company_description, su.company_description) AS company_description,
#                     COALESCE(ep.employer_type, su.employer_type) AS employer_type
#                     FROM 
#                         job_post jp 
#                     LEFT JOIN 
#                         users u ON jp.employer_id = u.user_id 
#                     LEFT JOIN
#                         employer_profile ep ON jp.employer_id = ep.employer_id
#                     LEFT JOIN 
#                         sub_users su ON jp.employer_id = su.sub_user_id
#                     LEFT JOIN 
#                         job_activity ja ON jp.id = ja.job_id
#                     WHERE 
#                         (jp.job_status = 'Opened') AND NOT EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s ) 
#                 """
#                 conditions = []
#                 values_job_details = [professional_id]

#                 # Adding search_text condition
#                 if search_text:
#                     search_conditions = [
#                         "jp.job_title LIKE %s",
#                         "jp.job_desc LIKE %s",
#                         "jp.country LIKE %s",
#                         "jp.city LIKE %s",
#                         "jp.skills LIKE %s",
#                         "jp.specialisation LIKE %s",
#                         "ep.company_name LIKE %s",
#                         "ep.sector LIKE %s",

#                     ]
#                     conditions.append(f"({' OR '.join(search_conditions)})")
#                     for _ in range(len(search_conditions)):
#                         values_job_details.append(f"%{search_text}%")
                
#                 # Filter by other conditions (e.g., skills, specialisation, workplace_type, etc.)
#                 if req_data.get('skills'):
#                     if "Others" in req_data.get('skills'):
#                         skill_conditions = ["jp.skills NOT LIKE %s" for _ in skills_list]
#                         conditions.append(f"({' OR '.join(skill_conditions)})")
#                         for skill in skills_list:
#                             values_job_details.append(f"%{skill}%")
#                     else:
#                         skill_conditions = ["jp.skills LIKE %s" for _ in req_data['skills']]
#                         conditions.append(f"({' OR '.join(skill_conditions)})")
#                         for skill in req_data['skills']:
#                             values_job_details.append(f"%{skill}%")
                
#                 if req_data['specialisation']:
#                     if "Others" in req_data['specialisation']:
#                         conditions.append("jp.specialisation NOT IN %s")
#                         values_job_details.append(tuple(specialisation_list),)
#                     else:
#                         conditions.append("jp.specialisation IN %s")
#                         values_job_details.append(tuple(specialisation),)
                
#                 if req_data['workplace_type']:
#                     conditions.append("jp.workplace_type IN %s")
#                     values_job_details.append(tuple(req_data['workplace_type'],))

#                 if req_data['job_type']:
#                     conditions.append("jp.job_type IN %s")
#                     values_job_details.append(tuple(req_data['job_type'],))

#                 if req_data['work_schedule']:
#                     if "Others" in req_data['work_schedule']:
#                         conditions.append("jp.work_schedule NOT IN %s")
#                         values_job_details.append(tuple(schedule_list),)
#                     else:
#                         conditions.append("jp.work_schedule IN %s")
#                         values_job_details.append(tuple(req_data['work_schedule']),)
                
#                 if req_data['sector']:
#                     if "Others" in req_data['sector']:
#                         conditions.append("ep.sector NOT IN %s")
#                         values_job_details.append(tuple(sectors_list),)
#                     else:
#                         conditions.append("ep.sector IN %s")
#                         values_job_details.append(tuple(req_data['sector']),)

#                 # Add additional filters here as needed
#                 if country:
#                     conditions.append("jp.country IN %s")
#                     values_job_details.append(tuple(country),)
                
#                 if city:
#                     conditions.append("jp.city IN %s")
#                     values_job_details.append(tuple(city),)
                
#                 # Combine conditions
#                 if conditions:
#                     query_job_details += " AND " + " AND ".join(conditions)
#                     total_count_query = "SELECT COUNT(DISTINCT jp.id) AS total_count " + base_query + " AND " + " AND ".join(conditions)
#                 else:
#                     total_count_query = "SELECT COUNT(DISTINCT jp.id) AS total_count " + base_query
                
#                 # Get total count and fetch job details
#                 total_count = execute_query(total_count_query, values_job_details)
#                 total_count = total_count[0]['total_count'] if total_count else 0
                
#                 query_job_details += " ORDER BY jp.id DESC LIMIT 10 OFFSET %s"
#                 values_job_details.append(offset,)
#                 job_details = replace_empty_values(execute_query(query_job_details, values_job_details))
                
#                 i = 0
#                 for job in job_details:
#                     if not job['job_status'] == 'closed':
#                         quest_dict = {"questions" : []}
#                         job_id = job['id']
#                         query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
#                         values = (job_id,)
#                         result = execute_query(query, values)
#                         if len(result) > 0:
#                             for r in result:
#                                 quest_dict["questions"].append(r)
#                         job.update(quest_dict)
#                         # else:
#                         #     quest_dict["questions"].append(None)
#                         #     job.update(quest_dict)
#                         job.update({'created_at': str(job['created_at'])})

#                         query = 'select count(job_id) from saved_job where job_id = %s and professional_id = %s'
#                         values = (job_id, professional_id,)
#                         rslt = execute_query(query, values)
#                         if rslt[0]['count(job_id)'] > 0:
#                             job_saved_status = "saved"
#                         else:
#                             job_saved_status = "unsaved"
#                         job.update({'saved_status': job_saved_status})

#                         query = 'select count(job_id) from job_activity where job_id = %s and professional_id = %s'
#                         values = (job_id, professional_id,)
#                         rslt = execute_query(query, values)
#                         if rslt[0]['count(job_id)'] == 0:
#                             job_applied_status = 'not_applied'
#                         else:
#                             job_applied_status = 'applied'
#                         txt = job['sector']
#                         txt = txt.replace(", ", "_")
#                         txt = txt.replace(" ", "_")
#                         sector_name = txt + ".png"
#                         job.update({'sector_image' : s3_sector_image_folder_name + sector_name})
#                         job.update({'profile_image' : s3_employer_picture_folder_name + job['profile_image']})
#                         job.update({'sector' : job['sector']})
#                         job.update({'employer_type' : job['employer_type']})
#                         job.update({'company_description' : job['company_description']})
#                         job.update({'applied_status': job_applied_status})
#                         query = 'select professional_resume from professional_profile where professional_id = %s'
#                         values = (professional_id,)
#                         rslt = execute_query(query, values)
#                         resume_name = rslt[0]['professional_resume']
#                         job.update({'user_resume' : resume_name})
#                         query = 'SELECT ij.employer_feedback, (SELECT COUNT(job_id) FROM invited_jobs WHERE professional_id = ij.professional_id AND job_id = ij.job_id) AS job_count FROM invited_jobs ij WHERE ij.professional_id = %s AND ij.job_id = %s AND ij.is_invite_sent = "Y";'
#                         values = (professional_id, job_id,)
#                         employee_feedback = execute_query(query, values)
#                         if len(employee_feedback) > 0:
#                             if employee_feedback[0]['employer_feedback'] is not None:
#                                 job.update({'invited_message' : employee_feedback[0]['employer_feedback']})
#                             else:
#                                 job.update({'invited_message' : ''})
#                             job.update({'invited_by_employer' : '1Y'})
#                         else:
#                             job.update({'invited_message' : ''})
#                             job.update({'invited_by_employer' : 'N'})
#                     else:
#                         del job_details[i]
#                     i = i + 1
#                 job_details_dict = {'job_details': job_details}
#                 profile.update(job_details_dict)               
#                 data = fetch_filter_params()
#                 percentage = show_percentage(professional_id)
#                 profile.update({'percentage':percentage})  
#                 profile.update(data)
#                 Total_job = {'total_count': total_count}
#                 profile.update(Total_job)
                
#                 # profile.update({'job_details': job_details, 'total_count': total_count})
#                 return api_json_response_format(True, "Details fetched successfully!", 0, profile)
#             else:
#                 return api_json_response_format(False, "Unauthorized user", 401, {})
#         else:
#             return api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
#     except Exception as error:
#         print(f"Error: {error}")
#         return api_json_response_format(False, str(error), 500, {})
    

def fetch_filter_results():
    try:
        data = []
        profile = {}
        country = []
        city = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                professional_id = user_data['user_id']
                req_data = request.get_json()
                if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"page number required",204,{})  
                        return result_json
                page_number = req_data["page_number"]
                location = req_data["location"]
                specialisation = req_data["specialisation"]
                # country_list = ["United States", "India", "Canada", "Europe", "United Kingdom"]
                skills_list = ["Agile Methodologies", "Algorithm Design", "Analytics", "Application Programming Interfaces (APIs)", "Budgeting", "Business Strategy", "Change Management", "Conflict Resolution", "Contract Management Skills", "Data Analysis", "Database Design", "Debugging", "Direct Sales", "Earned Value Management", "Financial Management", "Human Resource Management", "Keyword Research", "Leadership Skills", "Market Research", "Marketing Skills", "Metrics and KPIs", "Mobile Application Development", "Negotiation", "Operations Management", "Organizational Development", "Presentation", "Process Improvement", "Product Knowledge", "Project Management", "Quality Assurance (QA)", "Recruiting", "Revenue Expansion", "Risk Assessment", "SaaS Knowledge", "Sales and Budget Forecasting", "Salesforce", "Strategic Planning", "Supply Chain Management", "Talent Management", "Team Leadership", "Upselling"]
                specialisation_list = ["Sales & Marketing", "Human Resources", "Technology & Technology Management", "Finance & Accounting", "C Suite And Board"]
                sectors_list = ["Agriculture", "Arts", "Construction", "Consumer Goods", "Corporate Services", "Design", "Education", "Energy and Mining", "Entertainment", "Finance", "Hardware and Networking", "Health Care", "Legal", "Manufacturing", "Media and Communications", "Nonprofit", "Professional Services", "Public Administration", "Public Safety", "Real Estate", "Recreation and Travel", "Retail", "Software and IT Services", "Transportation and Logistics", "Wellness and Fitness"]
                schedule_list = ["Fixed", "Flexible", "Monday to Friday", "Weekend only"]
                # plan = req_data['plan']
                if len(location) != 0:
                    for i in range(len(location)):
                        res = location[i].split("&&&&&")
                        country.append(res[1])
                        city.append(res[0])
                
                offset = (page_number - 1) * 10
                base_query = """
                    FROM
                    job_post jp
                    LEFT JOIN
                    users u ON jp.employer_id = u.user_id
                    LEFT JOIN
                    job_activity ja ON jp.id = ja.job_id
                    LEFT JOIN
                    employer_profile ep ON jp.employer_id = ep.employer_id
                    LEFT JOIN 
                    sub_users su ON jp.employer_id = su.sub_user_id
                    WHERE
                    (jp.job_status = 'Opened') AND NOT EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s ) 
                """

                # Calculate total number of records
                # total_query = "SELECT COUNT(*) AS total_count FROM job_post LEFT JOIN job_activity ON job_post.id = job_activity.job_id WHERE job_status = %s AND job_activity.job_id IS NULL"
                # values = ("opened",)
                # total_count = execute_query(total_query,values)[0]['total_count']
                # Constructing the SQL query
                query_job_details = """
                                        SELECT DISTINCT 
                                        jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
                                        jp.additional_info, jp.skills, jp.job_status, jp.country, jp.state, jp.city, jp.work_schedule, 
                                        jp.workplace_type, jp.is_paid, jp.time_commitment, jp.duration, jp.timezone, jp.specialisation,
                                        jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                                        jp.required_background_check, jp.required_subcontract, jp.is_application_deadline,
                                        jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at,
                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                        COALESCE(ep.sector, su.sector) AS sector,
                                        COALESCE(ep.company_description, su.company_description) AS company_description,
                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                        FROM 
                                            job_post jp 
                                        LEFT JOIN 
                                            users u ON jp.employer_id = u.user_id 
                                        LEFT JOIN
                                            employer_profile ep ON jp.employer_id = ep.employer_id
                                        LEFT JOIN 
                                            sub_users su ON jp.employer_id = su.sub_user_id
                                        LEFT JOIN 
                                            job_activity ja ON jp.id = ja.job_id
                                        WHERE 
                                            (jp.job_status = 'Opened') AND NOT EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s ) 
                                    """
                conditions = []
                # conditions_1 = []
                values_job_details = [professional_id]

                not_active_skills_query = "select skill_name from filter_skills where is_active = %s"
                values = ('N',)
                not_active_skills = execute_query(not_active_skills_query, values)
                not_active_skills_list = [skill['skill_name'] for skill in not_active_skills]
                
                # print(no_active_skills_list)

                if req_data.get('skills'):
                    if "Others" in req_data.get('skills'):
                        skill_conditions = ["jp.skills Not Like %s" for _ in skills_list]
                        # not_active_skill_conditions = ["jp.skills Like %s" for _ in not_active_skills_list]
                        conditions.append(f"({' OR '.join(skill_conditions)})")
                        # conditions_1.append(f"({' OR '.join(not_active_skill_conditions)})")
                        # conditions = conditions + conditions_1
                        # conditions = f"(({ ' OR '.join(conditions) }) AND ({ ' OR '.join(conditions_1) }))"
                        # conditions = [conditions]
                        for skill in skills_list:
                            values_job_details.append(f"%{skill}%")
                        # for skill in not_active_skills_list:
                        #     values_job_details.append(f"%{skill}%")
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

                if req_data['work_schedule']:
                    if "Others" in req_data['work_schedule']:
                        conditions.append("jp.work_schedule NOT IN %s")
                        values_job_details.append(tuple(schedule_list),)
                    else:
                        conditions.append("jp.work_schedule IN %s")
                        values_job_details.append(tuple(req_data['work_schedule']),)

                # if req_data['plan']:
                #     conditions.append("u.pricing_category IN %s")
                #     values_job_details.append(tuple(plan,))

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
                        conditions.append("ep.sector NOT IN %s")
                        # conditions.append("su.sector NOT IN %s")
                        # values_job_details.append(tuple(sectors_list),)
                        values_job_details.append(tuple(sectors_list),)
                    else:
                        conditions.append("ep.sector IN %s")
                        # conditions.append("su.sector IN %s")
                        # values_job_details.append(tuple(req_data['sector']),)
                        values_job_details.append(tuple(req_data['sector']),)
                if conditions:
                    if len(conditions) == 1:
                        query_job_details += " AND " + conditions[0]
                    else:
                        query_job_details += " AND (" + " AND ".join(conditions) + ")"
                total_count_query = "SELECT  COUNT(DISTINCT jp.id) AS total_count " + base_query
                if conditions:
                    if len(conditions) == 1:
                        total_count_query += " AND " + conditions[0]
                    else:
                        total_count_query += " AND (" + " AND ".join(conditions) + ")"
                # total_count_query = "SELECT COUNT(*) AS total_count " + base_query
                total_count = execute_query(total_count_query, values_job_details)
                if len(total_count) > 0:
                    total_count = total_count[0]['total_count']
                else:
                    total_count = 0
                query_job_details += " ORDER by jp.id DESC LIMIT 10 OFFSET %s"
                values_job_details.append(offset,)
                # values_job_details.append(",")
                job_details = replace_empty_values(execute_query(query_job_details, values_job_details))
                
                i = 0
                for job in job_details:
                    if not job['job_status'] == 'closed':
                        quest_dict = {"questions" : []}
                        job_id = job['id']
                        query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                        values = (job_id,)
                        result = execute_query(query, values)
                        if len(result) > 0:
                            for r in result:
                                quest_dict["questions"].append(r)
                        job.update(quest_dict)
                        # else:
                        #     quest_dict["questions"].append(None)
                        #     job.update(quest_dict)
                        job.update({'created_at': str(job['created_at'])})

                        query = 'select count(job_id) from saved_job where job_id = %s and professional_id = %s'
                        values = (job_id, professional_id,)
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
                        txt = txt.replace(", ", "_")
                        txt = txt.replace(" ", "_")
                        sector_name = txt + ".png"
                        job.update({'sector_image' : s3_sector_image_folder_name + sector_name})
                        job.update({'profile_image' : s3_employer_picture_folder_name + job['profile_image']})
                        job.update({'sector' : job['sector']})
                        job.update({'employer_type' : job['employer_type']})
                        job.update({'company_description' : job['company_description']})
                        job.update({'applied_status': job_applied_status})
                        query = 'select professional_resume from professional_profile where professional_id = %s'
                        values = (professional_id,)
                        rslt = execute_query(query, values)
                        resume_name = rslt[0]['professional_resume']
                        job.update({'user_resume' : resume_name})
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
                    else:
                        del job_details[i]
                    i = i + 1
                job_details_dict = {'job_details': job_details}
                profile.update(job_details_dict)               
                data = fetch_filter_params()
                percentage = show_percentage(professional_id)
                profile.update({'percentage':percentage})  
                profile.update(data)
                Total_job = {'total_count': total_count}
                profile.update(Total_job)   
                if job_details == "" or job_details == []:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Jobs filtered successfully.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Filter", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Filter",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Job Filter, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, profile)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Jobs filtered successfully.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Filter", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Filter",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Job Filter, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in professional jobs filteration.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Filter Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Filter Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Job Filter Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def show_percentage(professional_id):              
                if isUserExist("users","user_id",professional_id):
                    query = """
                        SELECT 
                            u.email_id, u.contact_number, u.country, u.city, u.profile_image,
                            pp.professional_resume, pp.preferences, pp.about, pp.video_url,
                            COUNT(pe.id) AS education_count,
                            COUNT(pex.id) AS experience_count,
                            COUNT(ps.id) AS skill_count,
                            COUNT(pl.id) AS language_count,
                            COUNT(pai.id) AS additional_info_count,
                            COUNT(psl.id) AS social_link_count
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
                            u.user_id, u.email_id, u.contact_number, u.country, u.city, u.profile_image,
                            pp.professional_resume, pp.preferences, pp.about, pp.video_url
                    """
                    values = (professional_id,)
                    result = execute_query(query, values)
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
                    return round(value)
                else:
                    return "Something went wrong"

def professional_job_save():
    result_json = {}
    try:                  
        job_id = 0                                  
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                job_id = req_data['job_id']
                comments = ""
                professional_id = user_data["user_id"]

                query = 'select count(id) from saved_job where job_id = %s and professional_id = %s'
                values = (job_id, professional_id,)
                rslt = execute_query(query, values)
                query = 'select job_title from job_post where id = %s'
                values = (job_id,)
                job_title = execute_query(query, values)
                if rslt[0]['count(id)'] > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Job Title' : job_title[0]['job_title'],
                                        'Message': f"Job '{job_title[0]['job_title']}' has been already saved by {user_data['email_id']}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Save", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Save",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Job Save, {str(e)}")
                        result_json = api_json_response_format(False,"Job has been saved already",403,{})
                else:             
                    created_at = datetime.now()
                    query = 'insert into saved_job (job_id,professional_id,comments,created_at) values (%s,%s,%s,%s)'
                    values = (job_id,professional_id,comments,created_at,)                    
                    row_count = update_query(query, values)
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Job Title' : job_title[0]['job_title'],
                                        'Message': f"Job '{job_title[0]['job_title']}' has been saved successfully by {user_data['email_id']}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Save", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Save",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Job Save, {str(e)}")
                        result_json = api_json_response_format(True,"Job has been saved successfully!",0,{})
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Job Title' : job_title[0]['job_title'],
                                        'Message': f"An error occurred while saving the job {job_title[0]['job_title']} for {user_data['email_id']}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Save Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Save Error",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Job Save Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"An error occurred while saving the job."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Save Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Save Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Job Save Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:
        return result_json
    
def get_professional_job_link():
    job_link = ""
    try:
        req_data = request.get_json()
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"job_id required",204,{})  
            return result_json
        job_id = req_data['job_id']
        query = 'select id from job_post where id = %s'
        values = (job_id,)
        job_data_set = execute_query(query, values)
        if(len(job_data_set) > 0):
            job_link = WEB_APP_URI+"/shared_job_details?5da1760a746b0afdcac7c0976cc1dfc77711192242507a634d7489cd7d09a924b31daf879f0b9574df3e630993733c34="+str(job_id)            
            result_json = api_json_response_format(True,"Details fetched successfully!",0,{"job_link":job_link})
        else:
            result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",204,{})                    

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:
        return result_json
    
def get_shared_job_details():
    try:        
        if '5da1760a746b0afdcac7c0976cc1dfc77711192242507a634d7489cd7d09a924b31daf879f0b9574df3e630993733c34' not in request.args:
            result_json = api_json_response_format(False,"Invalid Link",401,{})  
            return result_json
        job_id = request.args.get('5da1760a746b0afdcac7c0976cc1dfc77711192242507a634d7489cd7d09a924b31daf879f0b9574df3e630993733c34')                
        query = 'select job_title, job_type, job_desc, job_status, specialisation, skills, country, state, city, work_schedule, workplace_type, number_of_openings, time_commitment, duration, calendly_link, currency, benefits, required_resume, required_cover_letter, required_background_check, required_subcontract, is_application_deadline, application_deadline_date, is_paid, is_active, created_at from job_post where id = %s'
        values = (job_id,)
        result = execute_query(query, values)
        result_json = api_json_response_format(True,"Details fetched successfully!",0,result)
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def professional_applied_jobs():
    try:                                  
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]
                query = 'select job_id, invite_to_interview,applied_on, application_status, custom_notes from job_activity where professional_id = %s'
                values = (professional_id,)                    
                data_set = execute_query(query, values)
                if len(data_set) > 0:
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,data_set)
                else:
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})

    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

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

def get_professional_profile_dashboard():
    try:
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:    
            user_data = get_user_data(token_result["email_id"])      
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]
                # req_data = request.get_json()
                # if 'from_profile_dashboard' not in req_data:
                #     result_json = api_json_response_format(False,"from_profile_dashboard key is required",204,{})  
                #     return result_json
                # from_profile_dashboard = req_data['from_profile_dashboard']
                req_data = request.form
                from_profile_dashboard = req_data.get('from_profile_dashboard')
                resume_extract_result = {"error_code" : 0}
                if from_profile_dashboard == 'true':
                    resume_extract_result = professional_details_update(request)
                profile_percentage = show_percentage(professional_id)
                # query = "SELECT u.user_id,u.first_name, u.last_name, u.email_id, u.country_code, u.contact_number, u.country, u.state, u.city, p.about,p.professional_resume, p.show_to_employer, p.upload_date,p.preferences, p.video_url, p.expert_notes, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level, pl.id AS language_id, pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s"                
                query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.country_code, u.contact_number, u.country, u.state, u.city, u.gender, u.pricing_category, p.about, p.professional_resume, p.show_to_employer, p.upload_date, p.preferences, p.video_url, p.expert_notes, p.years_of_experience, p.functional_specification, p.industry_sector, p.sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level,pl.id AS language_id,pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN   professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year = 'Present' THEN 1 ELSE 0 END DESC, pe.end_year DESC, pe.end_month DESC"
                values = (professional_id,)
                profile_result = execute_query(query, values)
                           
                # s3_pro = s3_obj.get_s3_client()                                
                
                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                intro_video_name = replace_empty_values1(profile_result[0]['video_url'])                
                s3_pic_key = s3_picture_folder_name+str(profile_image_name)
                s3_video_key = s3_intro_video_folder_name+str(intro_video_name)
                # pic_url,video_url = "",""
                # if not profile_image_name == "" :
                #     s3_pic_folder_name = s3_pic_folder_name+profile_image_name
                #     pic_url = s3_pro.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_pic_folder_name}, ExpiresIn=1, HttpMethod='GET')
                # if not intro_video_name == "" :
                #     s3_video_folder_name = s3_video_folder_name+intro_video_name
                #     video_url = s3_pro.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_video_folder_name}, ExpiresIn=1, HttpMethod='GET')                                                
                

                profile_dict = {
                    'Professional_id' :replace_empty_values1(profile_result[0]['user_id']),
                    'first_name': replace_empty_values1(profile_result[0]['first_name']),
                    'last_name': replace_empty_values1(profile_result[0]['last_name']),                                        
                    'email_id': replace_empty_values1(profile_result[0]['email_id']),
                    'pricing_category': replace_empty_values1(profile_result[0]['pricing_category']),
                    'country_code' : replace_empty_values1(profile_result[0]['country_code']),
                    'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
                    'city': replace_empty_values1(profile_result[0]['city']),
                    'state': replace_empty_values1(profile_result[0]['state']),
                    'country': replace_empty_values1(profile_result[0]['country']),
                    'gender': replace_empty_values1(profile_result[0]['gender']),
                    'years_of_experience': replace_empty_values1(profile_result[0]['years_of_experience']),
                    'functional_specification' : replace_empty_values1(profile_result[0]['functional_specification']),
                    'industry_sector': replace_empty_values1(profile_result[0]['industry_sector']),
                    'sector': replace_empty_values1(profile_result[0]['sector']),
                    'job_type': replace_empty_values1(profile_result[0]['job_type']),
                    'location_preference': replace_empty_values1(profile_result[0]['location_preference']),
                    'mode_of_communication': replace_empty_values1(profile_result[0]['mode_of_communication']),
                    'willing_to_relocate': replace_empty_values1(profile_result[0]['willing_to_relocate']),
                    'profile_image': s3_pic_key,
                    'resume_name': replace_empty_values1(profile_result[0]['professional_resume']),
                    'resume_upload_date': "",
                    'about': replace_empty_values1(profile_result[0]['about']),
                    'preferences': replace_empty_values1(profile_result[0]['preferences']),
                    'video_name': s3_video_key,
                    'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
                    'experience': {},
                    'education': {},
                    'skills': {},
                    'languages': {},
                    'additional_info': {},
                    'social_link': {},
                    'profile_percentage': profile_percentage,
                    'show_to_employer' : replace_empty_values1(profile_result[0]['show_to_employer'])
                }

                # Grouping experience data
                experience_set = set()
                experience_list = []
                for exp in profile_result:
                    if exp['experience_id'] is not None:
                        # start_date = format_date(exp['experience_start_year'], exp['experience_start_month'])
                        # if exp['experience_end_year'] == 'Present' or exp['experience_end_month'] == 'Present':
                        #     end_date = 'Present'
                        # else:
                        #     end_date = format_date(exp['experience_end_year'], exp['experience_end_month'])
                        exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], exp['experience_start_year'], exp['experience_end_year'], exp['job_description'], exp['job_location'])
                        if exp_tuple not in experience_set:
                            experience_set.add(exp_tuple)
                            experience_list.append({
                                'id': exp['experience_id'],
                                'company_name': replace_empty_values1(exp['company_name']),
                                'job_title': replace_empty_values1(exp['job_title']),
                                'start_date': replace_empty_values1(exp['experience_start_year']),                                
                                'end_date': replace_empty_values1(exp['experience_end_year']),                             
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
                        edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
                                    edu['education_start_year'], edu['education_end_year'], edu['institute_location'])
                        if edu_tuple not in education_set:
                            education_set.add(edu_tuple)
                            education_list.append({
                                'id': edu['education_id'],
                                'institute_name': replace_empty_values1(edu['institute_name']),
                                'degree_level': replace_empty_values1(edu['degree_level']),
                                'specialisation': replace_empty_values1(edu['specialisation']),
                                'start_date': replace_empty_values1(edu['education_start_year']),                                
                                'end_date': replace_empty_values1(edu['education_end_year']),
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
                profile_percentage = calculate_professional_profile_percentage(user_data['user_id'])
                query = "UPDATE users set profile_percentage = %s where user_id = %s"
                values = (profile_percentage['value'], user_data["user_id"],)
                update_query(query,values)
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']}'s profile details displayed successfully."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Professional Profile Tab", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Professional Profile Tab",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Professional Profile Tab, {str(e)}")
                if resume_extract_result['error_code'] == 0:
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
                else:
                    result_json = api_json_response_format(False, "It looks like the document you uploaded isn't a resume. Please upload a resume so we can create your 2C profile", 0, profile_dict)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing professional profile details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Profile Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Profile Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Profile Tab Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def process_cover_letter_query(openai_api_key, profile_details, job_desc ):
    global g_resume_path
    global g_openai_token_limit  
    result = {}

    try:
        if s3_exists(BUCKET_NAME,"cover_letter_prompt.json"):     
            s3 = s3_obj.get_s3_resource()   
            obj = s3.Object(BUCKET_NAME, "cover_letter_prompt.json")
            prompt_json = json.load(obj.get()['Body'])
            # print(prompt_json)
            # print(type(prompt_json))
            # prompt_json = {'level_1': {'prompt': "You are a Human Resource leader, who understands what hiring managers look for in a cover letter. You are given the resume/profile of the candidate and the Job description that they are applying for. The task given is to understand the experience of the candidate, read between the lines of who they are and what makes them unique for the specific job that they are applying for. \nSo here are the steps of what is expected: \n•\u2060 \u2060Understand the candidate using the resume, understand what’s on the resume and interpret between the lines to understand how they stand out. (eg: is this candidate who’s stayed in one company for long, grown continuously there and helped nurture talent? Have they been part of setting the culture there? Have they been in a high pace, high growth environment ? Is there signs of initiative and ability to handle ambiguity and such. As a HR expert you will understand what's valuable and interpret between the lines better) \n•\u2060 \u2060Understand and interpret the job description and what is expected out of the candidate in depth.\n•\u2060 \u2060Use this information to write a cover letter that is brief, simple and precisely explains the fit of the candidate for the role. Pick the most relevant experience to the role and write about how the experience , and skills applies to the job description in question; and touch upon any other key past roles briefly if needed. Preferred way is to talk about the most relevant experience (and most recent amongst the relevant experience) in detail and only touch other experiences to demonstrate understanding of a domain or experience in a skillset.\nGuidelines on writing style:\n•\u2060 \u2060Be brief and precise\n•\u2060 \u2060Keep the focus on experience and what they've done. Given that the candidates are experienced, writing about education would be irrelevant.\n•\u2060 \u2060Content should be two paragraphs or lesser\n•\u2060 \u2060The tone and writing style needs to be like a nice email that is specific, direct and to the point.\n•\u2060 \u2060Do not use sales/marketing words. Keep it simple and to the point.\n•\u2060 \u2060Do not use adjectives to describe the candidate. Demonstrate only through crisp and precise description of experience or skillset. \n•\u2060 Do not make up experience. When the candidate is applying for a role where the job description asks for certain information but the resume doesn’t have it; only talk about existing experience and express interest in the role in one short paragraph. Do not for any reason make up experience based on the job description.\n•\u2060 \u2060Include a subject on top and follow it up with the cover letter. Do not include any other information. Only these two elements to be included in the response.\n• Only for cases where candidate does not have relevant experience to the job description, include a few lines that shortly and politely explains that there is not a direct match of relevant experience but however a cover letter has been generated to express interest for the role, willingness to learn and highlighting any other key attributes/experiences of the candidates that demonstrates past excellence. Below that give the subject and the cover letter as usual.\n\n\n\n\n\n\n\n\nBelow is the required data:\nResume/Profile: {{profile}}\nJob description: {{jobdescription}}\n"}}
            level_1 = prompt_json["level_1"]
            level_1_prompt = level_1["prompt"]      
            level_1_prompt = level_1_prompt.replace("{{profile}}", "{{"+str(profile_details)+"}}")  
            level_1_prompt = level_1_prompt.replace("{{jobdescription}}", "{{"+str(job_desc)+"}}")  

            openai_level_1_res = get_openai_summary(openai_api_key,level_1_prompt)
            if not "error_code" in openai_level_1_res:
                chatbot_level_1_text = openai_level_1_res["summary"]
                result["is_error"] = False
                result["result"] = chatbot_level_1_text
            else:
                result["is_error"] = True
                result["result"] = str(openai_level_1_res["message"] )
    except Exception as error:
        print(f"process_quries_search in professional cover letter error : {error}")
        result["is_error"] = True
        result["result"] = str(error)   
    finally:        
        return result

def extract_and_fill_details(content, company_name, job_title, location):
    """
    Extract specific details and fill placeholders in the cover letter.
    """
    # Extracting candidate's details
    lines = content.splitlines()
    name = lines[0].strip()
    address = lines[1].strip()
    # city_state_zip = lines[2].strip()
    email = lines[2].strip()
    phone = lines[3].strip()
    print(content)

    # Current date
    today_date = datetime.now().strftime("%B %d, %Y")
    print(today_date)

    # Sample employer details (can be dynamic)
    # employer_name = "John Doe"
    # company_address = "123 Business Street"
    # company_city_state_zip = "Tech City, TC 54321"
    # Website = "2ndcareers"
    # company_name = ""
    # job_title = "Software Engineer"

    # Replacing placeholders
    filled_content = content.replace("[Your Name]", name)
    filled_content = filled_content.replace("[Your Address]", address)
    filled_content = filled_content.replace("[Location]", location)
    filled_content = filled_content.replace("[Email Address]", email)
    filled_content = filled_content.replace("[Phone Number]", phone)
    filled_content = filled_content.replace("[Job Title]", job_title)
    filled_content = filled_content.replace("[Company Name]", company_name)
    # filled_content = filled_content.replace("[Date]", today_date)
    # filled_content = filled_content.replace("[Job Board/Company Website]", Website)
    # filled_content = filled_content.replace("[Employer's Name]", employer_name)
    # filled_content = filled_content.replace("[Company Address]", company_address)
    # filled_content = filled_content.replace("[City, State, ZIP Code]", company_city_state_zip)

    # print(filled_content)
    
    return filled_content

def cover_letter():
    try:
        result_json = {}
        req = request.get_json()
        if 'job_desc' not in req:
                        result_json = api_json_response_format(False,"job_desc required",204,{})  
                        return result_json
        job_desc = req['job_desc']
        profile_data = get_professional_profile_dashboard()
        if profile_data['error_code'] == 0:
            output = process_cover_letter_query(OPENAI_API_KEY,profile_data, job_desc)

            if output["is_error"]:
                result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",500,{})
            else:
                result_data = output["result"]
                result_json = api_json_response_format(True,"Details fetched successfully!",0,result_data)
            
        else:
            result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",500,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def user_dashboard_details():
    # # Fetch notification count
    try:
        profile = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])    
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result["email_id"])
                user_id = user_data['sub_user_id']
            else:
                user_id = user_data['user_id']
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional" or user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "partner":
                    # user_id = user_data['user_id']
                    query_notification_count = 'SELECT COUNT(id) FROM user_notifications WHERE user_id = %s'
                    values_notification_count = (user_id,)
                    notification_count = execute_query(query_notification_count, values_notification_count)
                    notification_count_dict = {'notification_count': notification_count[0]['COUNT(id)']}
                    profile.update(notification_count_dict)

                    if user_data["user_role"] == "employer":
                        query_user_details = 'SELECT ep.company_name, u.profile_image, u.user_id, u.email_id, u.pricing_category, u.payment_status FROM employer_profile ep LEFT JOIN users u ON u.user_id = ep.employer_id WHERE ep.employer_id = %s'
                        values_user_details = (user_id,)
                        pic_key = s3_employer_picture_folder_name
                    elif user_data["user_role"] == "employer_sub_admin":
                        query_user_details = 'SELECT su.company_name, su.profile_image, su.sub_user_id as user_id, su.email_id, su.pricing_category, su.payment_status FROM sub_users su WHERE su.sub_user_id = %s'
                        values_user_details = (user_id,)
                        pic_key = s3_employer_picture_folder_name
                    elif user_data["user_role"] == "recruiter":
                        query_user_details = 'SELECT su.company_name, su.profile_image, su.sub_user_id as user_id, su.email_id, su.pricing_category, su.payment_status FROM sub_users su WHERE su.sub_user_id = %s'
                        values_user_details = (user_id,)
                        pic_key = s3_employer_picture_folder_name
                    elif user_data["user_role"] == "partner":
                        query_user_details = 'SELECT pp.company_name, u.profile_image, u.user_id, u.email_id, u.pricing_category, u.payment_status FROM partner_profile pp LEFT JOIN users u ON u.user_id = pp.partner_id WHERE pp.partner_id = %s'
                        values_user_details = (user_id,)                              
                        pic_key = s3_partner_picture_folder_name
                    else:
                        profile_percentage = calculate_professional_profile_percentage(user_data['user_id'])
                        query = "UPDATE users set profile_percentage = %s where user_id = %s"
                        values = (profile_percentage['value'], user_data["user_id"],)
                        update_query(query,values)
                        query_user_details = 'SELECT user_id, first_name, last_name, email_id, profile_image, pricing_category, payment_status, profile_percentage FROM users WHERE user_id = %s'
                        values_user_details = (user_id,)
                        pic_key = s3_picture_folder_name
                    
                    user_details = replace_empty_values(execute_query(query_user_details, values_user_details))
                    if len(user_details) > 0:
                        profile_image_name = replace_empty_values1(user_details[0]['profile_image'])                               
                        s3_pic_key = pic_key + str(profile_image_name)

                        user_details[0].update({'profile_image': str(s3_pic_key)})
                        user_details[0].update({'user_role': user_data['user_role']})
                        user_details[0].update({'country': user_data['country']})
                        user_details_dict = {'user_details': user_details}
                        profile.update(user_details_dict)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': f"User dashboard details fetched successfully for {user_data['email_id']}"
                                        }
                            event_properties = background_runner.process_dict(user_data["email_id"], "User Dashboard Details", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"User Dashboard Details",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: User Dashboard Details, {str(e)}")
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,profile)
                else:
                    print("1 ",str(user_data["user_role"]))
                    result_json = api_json_response_format(False, "Unauthorized user", 401, {})
            else:
                print("2 ",str(user_data["user_role"]))
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in fetching user dashboard details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "User Dashboard Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"User Dashboard Details Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: User Dashboard Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

#def get_professional_dashboard_data():
    try:
        profile = {}
        key = ''
        param = ''
        req_data = request.get_json()
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional":
                    if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"page number required",204,{})  
                        return result_json
                    if 'key' in req_data:
                        key = req_data['key']
                    if 'param' in req_data:
                        param = req_data['param']
                    update_job_status_query = 'UPDATE job_post SET is_active = %s, job_status = %s WHERE DATEDIFF(CURDATE(), created_at) >= days_left;'
                    values = ('N','paused',)
                    rs = update_query(update_job_status_query, values)
                    fetch_saved_job_status_query = 'SELECT id FROM job_post WHERE DATEDIFF(CURDATE(), created_at) >= days_left;'
                    values = ()
                    expired_jobs_id = execute_query(fetch_saved_job_status_query, values)
                    for i in expired_jobs_id:
                        query = 'UPDATE saved_job SET is_active = %s WHERE id = %s'
                        values = ('N', i['id'],)
                        rs = update_query(query, values)
                    page_number = req_data["page_number"]
                    professional_id = user_data['user_id']
                    profile_percentage = show_percentage(professional_id)                     
                    offset = (page_number - 1) * 10
                    # Calculate total number of records
                    total_query = """SELECT COUNT(*) AS total_count
                                        FROM job_post jp
                                        WHERE jp.job_status = %s 
                                        AND jp.is_active = %s
                                        AND NOT EXISTS (
                                        SELECT 1
                                        FROM job_activity ja
                                        WHERE ja.job_id = jp.id
                                        AND ja.professional_id = %s
                                        );"""
                    values = ("Opened", 'Y', professional_id,)
                    total_count = execute_query(total_query,values)[0]['total_count']
                    if key == 'sort':
                        if param == 'by_date':
                            query_job_details = "SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,u.profile_image,u.pricing_category,u.country,u.city,ep.sector,ep.company_description,ep.company_name,ep.employer_type FROM job_post jp JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id WHERE jp.job_status = %s AND jp.is_active = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY jp.created_at ASC LIMIT 10 OFFSET %s;"
                        elif param == 'asc':
                            query_job_details = "SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,u.profile_image,u.pricing_category,u.country,u.city,ep.sector,ep.company_description,ep.company_name,ep.employer_type FROM job_post jp JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id WHERE jp.job_status = %s AND jp.is_active = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY jp.job_title ASC LIMIT 10 OFFSET %s;"
                        else:
                            query_job_details = "SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,u.profile_image,u.pricing_category,u.country,u.city,ep.sector,ep.company_description,ep.company_name,ep.employer_type FROM job_post jp JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id WHERE jp.job_status = %s AND jp.is_active = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY jp.job_title DESC LIMIT 10 OFFSET %s;"
                    else:
                        query_job_details = """
                                    SELECT DISTINCT
                                        jp.id,
                                        jp.job_title,
                                        jp.job_type,
                                        jp.job_overview,
                                        jp.job_desc,
                                        jp.responsibilities,
                                        jp.additional_info,
                                        jp.skills,
                                        jp.country,
                                        jp.state,
                                        jp.city,
                                        jp.work_schedule,
                                        jp.workplace_type,
                                        jp.is_paid,
                                        jp.time_commitment,
                                        jp.timezone,
                                        jp.duration,
                                        jp.calendly_link,
                                        jp.currency,
                                        jp.benefits,
                                        jp.required_resume,
                                        jp.required_cover_letter,
                                        jp.required_background_check,
                                        jp.required_subcontract,
                                        jp.is_application_deadline,
                                        jp.application_deadline_date,
                                        jp.is_active,
                                        jp.share_url,
                                        jp.specialisation,
                                        jp.created_at,
                                        u.profile_image,
                                        u.pricing_category,
                                        u.country,
                                        u.city,
                                        ep.sector,
                                        ep.company_description,
                                        ep.company_name,
                                        ep.employer_type
                                        FROM
                                            job_post jp
                                        JOIN
                                            users u ON jp.employer_id = u.user_id
                                        LEFT JOIN
                                            employer_profile ep ON jp.employer_id = ep.employer_id
                                        WHERE
                                            jp.job_status = %s
                                            AND jp.is_active = %s
                                            AND NOT EXISTS (
                                                SELECT 1
                                                FROM job_activity ja
                                                WHERE ja.job_id = jp.id
                                                AND ja.professional_id = %s
                                            )
                                        ORDER BY
                                            jp.id DESC
                                        LIMIT 10 
                                        OFFSET %s
                                    """

                    values_job_details = ('opened', 'Y', professional_id, offset,)
                    job_details = replace_empty_values(execute_query(query_job_details, values_job_details))
                    if len(job_details) > 0:
                        id = job_details[0]["id"]
                        query = 'select * from  view_count where job_id = %s and professional_id = %s'
                        values = (id,professional_id,)
                        count = execute_query(query,values)
                        if not count:
                            current_time = datetime.now()
                            query = "INSERT INTO view_count (job_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                            values = (id, professional_id, current_time,)
                            update_query(query,values)
                    for job in job_details:
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
                        txt = txt.replace(", ", "_")
                        txt = txt.replace(" ", "_")
                        sector_name = txt + ".png"

                        query = 'select professional_resume from professional_profile where professional_id = %s'
                        values = (professional_id,)
                        rslt = execute_query(query, values)
                        resume_name = rslt[0]['professional_resume']
                        job.update({'user_resume' : resume_name})
                        job.update({'applied_status': job_applied_status})
                        job.update({'profile_image' : s3_employeer_logo_folder_name + job['profile_image']})
                        job.update({'sector_image' : s3_sector_image_folder_name + sector_name})
                        job.update({'sector' : job['sector']})
                        job.update({'employer_type' : job['employer_type']})
                        job.update({'company_description' : job['company_description']})
                        job.update({'profile_percentage' : profile_percentage})


                    job_details_dict = {'job_details': job_details}
                    profile.update(job_details_dict)    
                    Total_job = {'total_count': total_count}
                    profile.update(Total_job)                          
                    data = fetch_filter_params()
                    profile.update(data)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional dashboard details displayed successfully.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Dashboard Details, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Error in fetching professional dashboard details.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details Error",event_properties, temp_dict.get('Message'), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Dashboard Details Error, {str(e)}")
                    result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.", 401, {})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in fetching professional dashboard details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Dashboard Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def get_professional_dashboard_data():
    try:
        profile = {}
        req_data = request.get_json()
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional":
                    if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Page number required",204,{})  
                        return result_json
                    if 'key' in req_data:
                        key = req_data['key']
                    if 'param' in req_data:
                        param = req_data['param']
                    if 'country' in req_data:
                        country_filter = req_data['country']
                    # update_job_status_query = 'UPDATE job_post SET is_active = %s, job_status = %s WHERE DATEDIFF(CURDATE(), created_at) >= days_left;'
                    # values = ('N','paused',)
                    # rs = update_query(update_job_status_query, values)
                    fetch_saved_job_status_query = 'SELECT id FROM job_post WHERE DATEDIFF(CURDATE(), created_at) >= days_left;'
                    values = ()
                    expired_jobs_id = execute_query(fetch_saved_job_status_query, values)
                    # for i in expired_jobs_id:
                    #     query = 'UPDATE saved_job SET is_active = %s WHERE id = %s'
                    #     values = ('N', i['id'],)
                    #     rs = update_query(query, values)
                    # query = "UPDATE saved_job SET is_active = %s WHERE id = %s"

                    # Prepare values list for executemany
                    # values = [('N', job['id']) for job in expired_jobs_id]
                    # values = [job['id'] for job in expired_jobs_id]

                    # if values:  # Only execute if there are expired jobs
                    #     query = f"UPDATE saved_job SET is_active = 'N' WHERE id IN ({','.join(map(str, values))})"
                    #     rs = update_query(query)   # <-- use executemany inside this function

                    values = [job['id'] for job in expired_jobs_id]

                    if values:
                        placeholders = ','.join(['%s'] * len(values))
                        query = f"""
                        UPDATE saved_job
                        SET is_active = 'N'
                        WHERE id IN ({placeholders})
                        """
                        rs = update_query(query, tuple(values))

                    page_number = req_data["page_number"]
                    professional_id = user_data['user_id']
                    profile_percentage = show_percentage(professional_id)
                    # Base query for job_post
                    base_job_query = """
                        SELECT COUNT(*) AS total_count
                        FROM job_post jp
                        WHERE jp.job_status = %s 
                        AND jp.is_active = %s
                        AND NOT EXISTS (
                            SELECT 1
                            FROM job_activity ja
                            WHERE ja.job_id = jp.id
                            AND ja.professional_id = %s
                        )
                    """

                    if country_filter.lower() != "others":
                        # Add country filter for specific countries
                        job_total_query = base_job_query + " AND jp.country = %s;"
                        job_values = ("Opened", 'Y', professional_id, country_filter)

                        ext_job_total_query = """
                            SELECT COUNT(*) AS total_count
                            FROM admin_job_post
                            WHERE admin_job_status = 'opened' AND country = %s;
                        """
                        ext_values = (country_filter,)
                    else:
                        # No country filter for "others"
                        job_total_query = base_job_query + ";"
                        job_values = ("Opened", 'Y', professional_id)

                        ext_job_total_query = """
                            SELECT COUNT(*) AS total_count
                            FROM admin_job_post
                            WHERE admin_job_status = 'opened';
                        """
                        ext_values = ()

                    # Execute main job query
                    job_total_result = execute_query(job_total_query, job_values)
                    job_total_count = job_total_result[0]['total_count'] if job_total_result and job_total_result[0]['total_count'] else 0

                    # Execute external job query
                    ext_result = execute_query(ext_job_total_query, ext_values)
                    ext_job_total_count = ext_result[0]['total_count'] if ext_result and ext_result[0]['total_count'] else 0

                    total_count = job_total_count + ext_job_total_count

                    query_ext_job_details = """SELECT DISTINCT
                            ap.job_reference_id AS id,
                            ap.job_title,
                            ap.job_type,
                            ap.job_overview,
                            ap.job_description AS job_desc,
                            NULL AS responsibilities,
                            NULL AS additional_info,
                            ap.skills,
                            ap.country,
                            ap.state,
                            ap.city,
                            ap.schedule AS work_schedule,
                            ap.workplace_type,
                            NULL AS is_paid,
                            NULL AS time_commitment,
                            NULL AS timezone,
                            NULL AS duration,
                            NULL AS calendly_link,
                            NULL AS currency,
                            NULL AS benefits,
                            NULL AS required_resume,
                            NULL AS required_cover_letter,
                            NULL AS required_background_check,
                            NULL AS required_subcontract,
                            NULL AS is_application_deadline,
                            NULL AS application_deadline_date,
                            ap.is_active,
                            NULL AS share_url,
                            ap.functional_specification AS specialisation,
                            ap.functional_specification_others,
                            ap.created_at,
                            ap.admin_job_status AS job_status,   
                            COALESCE(u.profile_image, '') AS profile_image,
                            NULL AS pricing_category,
                            NULL AS payment_status,
                            u.country AS user_country,
                            u.city AS user_city,
                            ap.company_sector AS sector,
                            NULL AS company_description,
                            ap.company_name,
                            NULL AS employer_type
                        FROM admin_job_post ap
                        LEFT JOIN users u ON ap.employer_id = u.user_id
                        WHERE lower(ap.admin_job_status) = 'opened'
                        AND ap.is_active = 'Y'
                        AND ap.country = %s
                    """
                    
                    order_by_map = {'by_date': 'jp.created_at ASC','by_date_desc': 'jp.created_at DESC','asc': 'jp.job_title ASC','desc': 'jp.job_title DESC'}
                    order_by_clause = order_by_map.get(param, 'jp.created_at DESC')
                    
                    page_number = req_data["page_number"]
                    limit = 10
                    offset = (page_number - 1) * limit

                    if key == 'sort':
                        query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at, 
                                                COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                COALESCE(u.country, su.country) AS user_country, 
                                                COALESCE(u.city, su.city) AS user_city,  
                                                COALESCE(ep.sector, su.sector) AS sector, 
                                                COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                COALESCE(ep.employer_type, su.employer_type) AS employer_type FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status = %s AND jp.is_active = %s AND jp.country = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY {} LIMIT 10 OFFSET %s;"""
                    
                        query_job_details = query_job_details.format(order_by_clause)
                        values_job_details = ('opened', 'Y', country_filter, professional_id, offset,)
                        
                        final_job_details = replace_empty_values(execute_query(query_job_details, values_job_details))
                        if len(final_job_details) > 0:
                            for j in final_job_details:
                                temp_job_id = j['id']
                                query = 'select count(id) from invited_jobs where job_id = %s and professional_id = %s and is_invite_sent = "Y" '
                                values = (temp_job_id, professional_id,)
                                count_status = execute_query(query, values)
                                if len(count_status) > 0 and count_status[0]['count(id)'] > 0:
                                    j.update({"invited_by_employer" : "1Y"})
                                else:
                                    j.update({"invited_by_employer" : "N"})
                    else:
                        query = """
                            SELECT 
                                jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
                                jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, 
                                jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, 
                                jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, 
                                jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, 
                                jp.application_deadline_date, jp.is_active, jp.share_url, jp.specialisation, jp.created_at,

                                COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                COALESCE(u.country, su.country) AS user_country, 
                                COALESCE(u.city, su.city) AS user_city,  
                                COALESCE(ep.sector, su.sector) AS sector, 
                                COALESCE(ep.company_description, su.company_description) AS company_description, 
                                COALESCE(ep.company_name, su.company_name) AS company_name, 
                                COALESCE(ep.employer_type, su.employer_type) AS employer_type

                            FROM invited_jobs ij
                            JOIN job_post jp ON ij.job_id = jp.id
                            LEFT JOIN users u ON jp.employer_id = u.user_id
                            LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                            LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                            LEFT JOIN job_activity ja ON ij.job_id = ja.job_id AND ja.professional_id = %s

                            WHERE ij.professional_id = %s
                            AND ij.is_invite_sent = 'Y'
                            AND ja.job_id IS NULL
                            AND jp.job_status = 'opened'
                            AND jp.is_active = 'Y'  
                        """

                        if country_filter.lower() != "others":
                            query += """ AND jp.country = %s
                                    ORDER BY 
                                        CASE 
                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                            ELSE 4
                                        END,
                                        ij.job_id DESC"""
                            values = (professional_id, professional_id,country_filter)
                        else:
                            query += """ORDER BY 
                                    CASE 
                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                        ELSE 4
                                    END,
                                    ij.job_id DESC"""
                            values = (professional_id, professional_id)

                        invited_jobs_dict = execute_query(query, values)
                        invited_jobs_count = len(invited_jobs_dict)
                        invited_job_id_list = [job['id'] for job in invited_jobs_dict]
                        non_invited_job_id_list = [] 
                        
                        if page_number == 1 and invited_jobs_count < 11:
                            non_invited_jobs_count = 10 - invited_jobs_count
                            if non_invited_jobs_count > 0:
                                placeholders = ', '.join(['%s'] * len(invited_job_id_list))
                                if not invited_job_id_list:
                                    query = """
                                    SELECT 
                                        jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills,
                                        jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone,
                                        jp.duration, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                                        jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date,
                                        jp.is_active, jp.share_url, jp.specialisation, jp.created_at,

                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                        COALESCE(u.country, su.country) AS user_country, 
                                        COALESCE(u.city, su.city) AS user_city,  
                                        COALESCE(ep.sector, su.sector) AS sector, 
                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type

                                    FROM job_post jp
                                    LEFT JOIN users u ON jp.employer_id = u.user_id
                                    LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                    LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id

                                    WHERE jp.job_status = 'opened' 
                                    AND jp.is_active = 'Y'
                                    """
                                    if country_filter.lower() != "others":
                                        query += """AND jp.country = %s
                                                    AND NOT EXISTS (
                                                            SELECT 1 
                                                            FROM job_activity ja 
                                                            WHERE ja.job_id = jp.id AND ja.professional_id = %s
                                                    )

                                                    ORDER BY 
                                                        CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                                            ELSE 4
                                                        END ASC,
                                                        jp.created_at DESC

                                                    LIMIT %s;"""
                                        values = (country_filter, professional_id, non_invited_jobs_count,)
                                    else:
                                        query += """AND NOT EXISTS (
                                                            SELECT 1 
                                                            FROM job_activity ja 
                                                            WHERE ja.job_id = jp.id AND ja.professional_id = %s
                                                    )

                                                    ORDER BY 
                                                        CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                                            ELSE 4
                                                        END ASC,
                                                        jp.created_at DESC

                                                    LIMIT %s;"""

                                        values = (professional_id, non_invited_jobs_count,)

                                else:
                                    query = f"""
                                    SELECT 
                                        jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills,
                                        jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone,
                                        jp.duration, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                                        jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date,
                                        jp.is_active, jp.share_url, jp.specialisation, jp.created_at,

                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                        COALESCE(u.country, su.country) AS user_country, 
                                        COALESCE(u.city, su.city) AS user_city,  
                                        COALESCE(ep.sector, su.sector) AS sector, 
                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type

                                    FROM job_post jp
                                    LEFT JOIN users u ON jp.employer_id = u.user_id
                                    LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                    LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id

                                    WHERE jp.job_status = 'opened' 
                                    AND jp.is_active = 'Y'
                                    AND jp.id NOT IN ({placeholders})
                                    """
                                    if country_filter.lower() != "others":
                                        query += """AND jp.country = %s
                                                    AND NOT EXISTS (
                                                            SELECT 1 
                                                            FROM job_activity ja 
                                                            WHERE ja.job_id = jp.id AND ja.professional_id = %s
                                                    )

                                                    ORDER BY 
                                                        CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                                            ELSE 4
                                                        END ASC,
                                                        jp.created_at DESC

                                                    LIMIT %s;"""
                                        values = tuple(invited_job_id_list) + (country_filter, professional_id, non_invited_jobs_count)
                                    else:
                                        query += """AND NOT EXISTS (
                                                            SELECT 1 
                                                            FROM job_activity ja 
                                                            WHERE ja.job_id = jp.id AND ja.professional_id = %s
                                                    )

                                                    ORDER BY 
                                                        CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                                            ELSE 4
                                                        END ASC,
                                                        jp.created_at DESC

                                                    LIMIT %s;"""
                                        values = tuple(invited_job_id_list) + (professional_id, non_invited_jobs_count)
                                non_invited_jobs = execute_query(query, values)
                                # non_invited_job_id_list = [job['id'] for job in non_invited_jobs]
                            # query_job_details = """
                            #                     SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                            #                         jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                            #                         jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                            #                         jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                            #                         COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            #                         COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            #                         COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                            #                         COALESCE(u.country, su.country) AS user_country, 
                            #                         COALESCE(u.city, su.city) AS user_city,  
                            #                         COALESCE(ep.sector, su.sector) AS sector, 
                            #                         COALESCE(ep.company_description, su.company_description) AS company_description, 
                            #                         COALESCE(ep.company_name, su.company_name) AS company_name, 
                            #                         COALESCE(ep.employer_type, su.employer_type) AS employer_type
                            #                         FROM job_post jp
                            #                         LEFT JOIN users u ON jp.employer_id = u.user_id
                            #                         LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                            #                         LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                            #                         WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id IN %s
                            #                         AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                            #                         ORDER BY id DESC LIMIT 10"""
                            invited_jobs_details, non_invited_jobs_details = [], []
                            if invited_jobs_dict:
                                # invited_job_values = (tuple(invited_job_id_list),professional_id,)
                                invited_jobs_details = invited_jobs_dict
                                for ij in invited_jobs_details:
                                    ij.update({"invited_by_employer" : "1Y"})
                            if non_invited_jobs:
                                # non_invited_job_values = (tuple(non_invited_job_id_list),professional_id,)
                                non_invited_jobs_details = non_invited_jobs
                                for nij in non_invited_jobs_details:
                                    nij.update({"invited_by_employer" : "N"})
                            jobs_details = invited_jobs_details + non_invited_jobs_details
                            sorted_data = sorted(jobs_details, key=lambda x: x["invited_by_employer"])
                            final_job_details = sorted_data
                        else:
                            if invited_jobs_count < 11:
                                offset = offset - invited_jobs_count
                                if invited_jobs_count != 0:
                                    placeholders = ', '.join(['%s'] * len(invited_job_id_list))
                                    query_job_details = f"""
                                                    SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                        jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                        jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                        jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                        COALESCE(u.country, su.country) AS user_country, 
                                                        COALESCE(u.city, su.city) AS user_city,  
                                                        COALESCE(ep.sector, su.sector) AS sector, 
                                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                        FROM job_post jp
                                                        LEFT JOIN users u ON jp.employer_id = u.user_id
                                                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                        WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id NOT IN ({placeholders})"""
                                    if country_filter.lower() != "others":
                                        query_job_details += """AND jp.country = %s
                                                        AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                            ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC LIMIT 10 OFFSET %s"""
                                        invited_job_values = (tuple(invited_job_id_list) + (country_filter, professional_id,) + (offset,))
                                    else:
                                        query_job_details += """AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                            ELSE 4
                                                    END ASC,
                                                    jp.created_at DESC LIMIT 10 OFFSET %s"""
                                        invited_job_values = (tuple(invited_job_id_list) + (professional_id,) + (offset,))
                                else:
                                    query_job_details = """
                                                    SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                        jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                        jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                        jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                        COALESCE(u.country, su.country) AS user_country, 
                                                        COALESCE(u.city, su.city) AS user_city,  
                                                        COALESCE(ep.sector, su.sector) AS sector, 
                                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                        FROM job_post jp
                                                        LEFT JOIN users u ON jp.employer_id = u.user_id
                                                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                        WHERE jp.job_status = 'opened' AND jp.is_active = 'Y'"""
                                    if country_filter.lower() != "others":
                                        query_job_details += """AND jp.country = %s
                                                        AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                            ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC LIMIT 10 OFFSET %s"""
                                        invited_job_values = (country_filter, professional_id, offset,)
                                    else:
                                        query_job_details += """AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                            ELSE 4
                                                    END ASC,
                                                    jp.created_at DESC LIMIT 10 OFFSET %s"""
                                        invited_job_values = (professional_id, offset,)
                                job_details = execute_query(query_job_details, invited_job_values)
                                final_job_details = job_details
                            else:
                                query_job_details = """
                                                SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                    jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                    jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                    jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                    COALESCE(u.country, su.country) AS user_country, 
                                                    COALESCE(u.city, su.city) AS user_city,  
                                                    COALESCE(ep.sector, su.sector) AS sector, 
                                                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                    COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                    FROM job_post jp
                                                    LEFT JOIN users u ON jp.employer_id = u.user_id
                                                    LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                    LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                    WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id IN %s"""
                                if country_filter.lower() == "others":
                                    query_job_details = """AND jp.country = %s
                                                    AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                    ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                    ELSE 4
                                                END ASC, 
                                                jp.created_at DESC LIMIT 10 OFFSET %s"""
                                    invited_job_values = (tuple(invited_job_id_list),country_filter, professional_id, offset)
                                else:
                                    query_job_details = """AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                    ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                    ELSE 4
                                                END ASC,
                                                jp.created_at DESC LIMIT 10 OFFSET %s"""
                                    invited_job_values = (tuple(invited_job_id_list), professional_id, offset)
                                invited_jobs_details = execute_query(query_job_details, invited_job_values)
                                if len(invited_jobs_details) < 10:
                                    new_offset = offset - invited_jobs_count
                                    if new_offset < 0:
                                        new_offset = 0
                                    new_limit = 10 - len(invited_jobs_details)
                                    placeholders = ', '.join(['%s'] * len(invited_job_id_list))
                                    query_job_details = f"""
                                                    SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                        jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                        jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                        jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                        COALESCE(u.country, su.country) AS user_country, 
                                                        COALESCE(u.city, su.city) AS user_city,  
                                                        COALESCE(ep.sector, su.sector) AS sector, 
                                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                        FROM job_post jp
                                                        LEFT JOIN users u ON jp.employer_id = u.user_id
                                                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                        WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id NOT IN ({placeholders})"""
                                    if country_filter.lower() != "others":
                                        query_job_details += """AND jp.country = %s
                                                        AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                        ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC LIMIT %s OFFSET %s"""
                                        non_invited_job_values = (tuple(invited_job_id_list) + (country_filter, professional_id,) + (new_limit,) + (new_offset,))
                                    else:
                                        query_job_details += """AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Platinum' THEN 1
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Premium' THEN 2
                                                            WHEN COALESCE(u.pricing_category, su.pricing_category) = 'Basic' THEN 3
                                                    ELSE 4
                                                END ASC,
                                                jp.created_at DESC LIMIT %s OFFSET %s"""
                                        non_invited_job_values = (tuple(invited_job_id_list) + (professional_id,) + (new_limit,) + (new_offset,))
                                    non_invited_jobs_details = execute_query(query_job_details, non_invited_job_values)
                                    for ij in invited_jobs_details:
                                        ij.update({"invited_by_employer" : "1Y"})
                                    for nij in non_invited_jobs_details:
                                        nij.update({"invited_by_employer" : "N"})
                                    jobs_details = invited_jobs_details + non_invited_jobs_details
                                    sorted_data = sorted(jobs_details, key=lambda x: x["invited_by_employer"])
                                    invited_jobs_details = sorted_data
                                final_job_details = invited_jobs_details

                    ext_job_needed = 10 - len(final_job_details)
                    ext_job_already_shown = max(0, (page_number - 1) * 10 - job_total_count)

                    if len(final_job_details) > 0:
                        if len(final_job_details) < 10 and ext_job_needed > 0:
                            query_ext_job_details += f"LIMIT %s OFFSET %s;"
                            values = (country_filter, ext_job_needed, ext_job_already_shown)   
                            ext_job_details = execute_query(query_ext_job_details, values)
                            if len(ext_job_details) > 0:
                                final_job_details.extend(ext_job_details)
                            # job_total_count = job_total_count + len(ext_job_details)
                    else:
                        query_ext_job_details += f" LIMIT %s OFFSET %s;"
                        values = (country_filter, 10, ext_job_already_shown)
                        final_job_details = execute_query(query_ext_job_details, values)

                    if len(final_job_details) > 0:
                        id = final_job_details[0]["id"]
                        query = 'select * from  view_count where job_id = %s and professional_id = %s'
                        values = (id,professional_id,)
                        count = execute_query(query,values)
                        if not count:
                            current_time = datetime.now()
                            query = "INSERT INTO view_count (job_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                            values = (id, professional_id, current_time,)
                            update_query(query,values)
                        for job in final_job_details:
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

                        job_details_dict = {'job_details': final_job_details}
                        profile.update(job_details_dict)
                        profile.update({'total_count': total_count})                          
                        data = fetch_filter_params()
                        profile.update(data)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional dashboard details displayed successfully.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Dashboard Details, {str(e)}")
                        result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                    else:
                        profile.update({'job_details':[]})
                        profile.update({'total_count': total_count})
                        data = fetch_filter_params()
                        profile.update(data)
                        result_json = api_json_response_format(True, "No jobs found", 0, profile)

                else:
                    result_json = api_json_response_format(False, "Unauthorized user", 401, {})
            else:
                result_json = api_json_response_format(False, "User not exists", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in fetching professional dashboard details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Dashboard Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

#def get_professional_dashboard_data():
    try:
        profile = {}
        req_data = request.get_json()
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional":
                    if 'page_number' not in req_data:
                        result_json = api_json_response_format(False,"Page number required",204,{})  
                        return result_json
                    if 'key' in req_data:
                        key = req_data['key']
                    if 'param' in req_data:
                        param = req_data['param']
                    # update_job_status_query = 'UPDATE job_post SET is_active = %s, job_status = %s WHERE DATEDIFF(CURDATE(), created_at) >= days_left;'
                    # values = ('N','paused',)
                    # rs = update_query(update_job_status_query, values)
                    fetch_saved_job_status_query = 'SELECT id FROM job_post WHERE DATEDIFF(CURDATE(), created_at) >= days_left;'
                    values = ()
                    expired_jobs_id = execute_query(fetch_saved_job_status_query, values)
                    for i in expired_jobs_id:
                        query = 'UPDATE saved_job SET is_active = %s WHERE id = %s'
                        values = ('N', i['id'],)
                        rs = update_query(query, values)
                    page_number = req_data["page_number"]
                    professional_id = user_data['user_id']
                    profile_percentage = show_percentage(professional_id)
                    total_query = """SELECT COUNT(*) AS total_count
                                        FROM job_post jp
                                        WHERE jp.job_status = %s 
                                        AND jp.is_active = %s
                                        AND NOT EXISTS (
                                        SELECT 1
                                        FROM job_activity ja
                                        WHERE ja.job_id = jp.id
                                        AND ja.professional_id = %s
                                        );"""
                    values = ("Opened", 'Y', professional_id,)
                    total_count = execute_query(total_query,values)
                    if len(total_count) > 0:
                        total_count = total_count[0]['total_count']
                    page_number = req_data["page_number"]
                    limit = 10
                    offset = (page_number - 1) * limit

                    if key == 'sort':
                        if param == 'by_date':
                            query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at, 
                                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                    COALESCE(u.country, su.country) AS user_country, 
                                                    COALESCE(u.city, su.city) AS user_city,  
                                                    COALESCE(ep.sector, su.sector) AS sector, 
                                                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                    COALESCE(ep.employer_type, su.employer_type) AS employer_type FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status = %s AND jp.is_active = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY jp.created_at ASC LIMIT 10 OFFSET %s;"""
                        elif param == 'by_date_desc':
                            query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at, 
                                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                    COALESCE(u.country, su.country) AS user_country, 
                                                    COALESCE(u.city, su.city) AS user_city,  
                                                    COALESCE(ep.sector, su.sector) AS sector, 
                                                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                    COALESCE(ep.employer_type, su.employer_type) AS employer_type FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status = %s AND jp.is_active = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY jp.created_at DESC LIMIT 10 OFFSET %s;"""
                        elif param == 'asc':
                            query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                    COALESCE(u.country, su.country) AS user_country, 
                                                    COALESCE(u.city, su.city) AS user_city,  
                                                    COALESCE(ep.sector, su.sector) AS sector, 
                                                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                    COALESCE(ep.employer_type, su.employer_type) AS employer_type FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status = %s AND jp.is_active = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY jp.job_title ASC LIMIT 10 OFFSET %s;"""
                        else:
                            query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration,jp.calendly_link,jp.currency,jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                    COALESCE(u.country, su.country) AS user_country, 
                                                    COALESCE(u.city, su.city) AS user_city,  
                                                    COALESCE(ep.sector, su.sector) AS sector, 
                                                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                    COALESCE(ep.employer_type, su.employer_type) AS employer_type FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.job_status = %s AND jp.is_active = %s AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s) ORDER BY jp.job_title DESC LIMIT 10 OFFSET %s;"""
                        values_job_details = ('opened', 'Y', professional_id, offset,)
                        final_job_details = replace_empty_values(execute_query(query_job_details, values_job_details))
                        if len(final_job_details) > 0:
                            for j in final_job_details:
                                temp_job_id = j['id']
                                query = 'select count(id) from invited_jobs where job_id = %s and professional_id = %s and is_invite_sent = "Y" '
                                values = (temp_job_id, professional_id,)
                                count_status = execute_query(query, values)
                                if len(count_status) > 0 and count_status[0]['count(id)'] > 0:
                                    j.update({"invited_by_employer" : "1Y"})
                                else:
                                    j.update({"invited_by_employer" : "N"})
                    else:
                        query = "SELECT ij.job_id FROM invited_jobs ij JOIN job_post jp ON ij.job_id = jp.id LEFT JOIN job_activity ja ON ij.job_id = ja.job_id AND ja.professional_id = %s WHERE ij.professional_id = %s AND ij.is_invite_sent = 'Y' AND ja.job_id IS NULL AND jp.job_status = 'Opened' AND jp.is_active = 'Y' order by ij.job_id DESC;"
                        values = (professional_id, professional_id,)
                        invited_jobs_dict = execute_query(query, values)
                        invited_jobs_count = len(invited_jobs_dict)
                        invited_job_id_list = [job['job_id'] for job in invited_jobs_dict]
                        non_invited_job_id_list = [] 
                        
                        if page_number == 1 and invited_jobs_count < 11:
                            non_invited_jobs_count = 10 - invited_jobs_count
                            if non_invited_jobs_count > 0:
                                placeholders = ', '.join(['%s'] * len(invited_job_id_list))
                                if not invited_job_id_list:
                                    # query = """SELECT jp.id, COALESCE(u.pricing_category, su.pricing_category) AS pricing_category
                                    #             FROM job_post jp
                                    #             LEFT JOIN users u ON jp.employer_id = u.user_id
                                    #             LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                    #             WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' 
                                    #             ORDER BY CASE 
                                    #                     WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                    #                     WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                    #                     WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                    #                     ELSE 4
                                    #                 END ASC, 
                                    #                 jp.created_at DESC 
                                    #             LIMIT %s;
                                    #         """
                                    query = """SELECT jp.id, COALESCE(u.pricing_category, su.pricing_category) AS pricing_category
                                                FROM job_post jp
                                                LEFT JOIN users u ON jp.employer_id = u.user_id
                                                LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                WHERE jp.job_status = 'opened' AND jp.is_active = 'Y'
                                                AND NOT EXISTS (
                                                    SELECT 1 
                                                    FROM job_activity ja
                                                    WHERE ja.job_id = jp.id 
                                                        AND ja.professional_id = %s
                                                )
                                                ORDER BY CASE 
                                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                                        ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC 
                                                LIMIT %s;
                                            """
                                    values = (professional_id, non_invited_jobs_count,)
                                else:
                                    # query = f"""SELECT jp.id, COALESCE(u.pricing_category, su.pricing_category) AS pricing_category
                                    #             FROM job_post jp
                                    #             LEFT JOIN users u ON jp.employer_id = u.user_id
                                    #             LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                    #             WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' 
                                    #             AND jp.id NOT IN ({placeholders})
                                    #             ORDER BY CASE
                                    #                     WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                    #                     WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                    #                     WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                    #                     ELSE 4
                                    #                 END ASC, 
                                    #                 jp.created_at DESC 
                                    #             LIMIT %s;
                                    #         """
                                    # values = tuple(invited_job_id_list) + (non_invited_jobs_count,)
                                    query = f"""SELECT jp.id, COALESCE(u.pricing_category, su.pricing_category) AS pricing_category
                                                FROM job_post jp
                                                LEFT JOIN users u ON jp.employer_id = u.user_id
                                                LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' 
                                                AND jp.id NOT IN ({placeholders})
                                                AND NOT EXISTS (
                                                    SELECT 1
                                                    FROM job_activity ja
                                                    WHERE ja.job_id = jp.id 
                                                        AND ja.professional_id = %s
                                                )
                                                ORDER BY CASE
                                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'platinum' THEN 1
                                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'premium' THEN 2
                                                        WHEN COALESCE(u.pricing_category, su.pricing_category) = 'basic' THEN 3
                                                        ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC 
                                                LIMIT %s;
                                            """
                                    values = tuple(invited_job_id_list) + (professional_id, non_invited_jobs_count)
                                non_invited_jobs = execute_query(query, values)
                                non_invited_job_id_list = [job['id'] for job in non_invited_jobs]
                            query_job_details = """
                                                SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                    jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                    jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                    jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                    COALESCE(u.country, su.country) AS user_country, 
                                                    COALESCE(u.city, su.city) AS user_city,  
                                                    COALESCE(ep.sector, su.sector) AS sector, 
                                                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                    COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                    FROM job_post jp
                                                    LEFT JOIN users u ON jp.employer_id = u.user_id
                                                    LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                    LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                    WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id = %s
                                                    AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                    LIMIT 10"""         #query changed on may 22 ----> ''' AND jp.id IN %s ''' to ''' AND jp.id = %s '''
                            invited_jobs_details, non_invited_jobs_details = [], []
                            if invited_job_id_list:
                                invited_job_values = (tuple(invited_job_id_list),professional_id,)
                                invited_jobs_details = execute_query(query_job_details, invited_job_values)
                                for ij in invited_jobs_details:
                                    ij.update({"invited_by_employer" : "1Y"})
                            if non_invited_job_id_list:
                                # non_invited_job_values = (tuple(non_invited_job_id_list),professional_id,)
                                # non_invited_jobs_details = execute_query(query_job_details, non_invited_job_values)
                                # for nij in non_invited_jobs_details:
                                #     nij.update({"invited_by_employer" : "N"})
                                for nij_id in non_invited_job_id_list:
                                    non_invited_job_values = (nij_id, professional_id,)
                                    non_invited_jobs_details_dict = execute_query(query_job_details, non_invited_job_values)
                                    if non_invited_jobs_details_dict:
                                        non_invited_jobs_details_dict[0].update({"invited_by_employer" : "N"})
                                        non_invited_jobs_details.append(non_invited_jobs_details_dict[0])
                            jobs_details = invited_jobs_details + non_invited_jobs_details
                            sorted_data = sorted(jobs_details, key=lambda x: x["invited_by_employer"])
                            final_job_details = sorted_data
                        else:
                            if invited_jobs_count < 11:
                                offset = offset - invited_jobs_count
                                if invited_jobs_count != 0:
                                    placeholders = ', '.join(['%s'] * len(invited_job_id_list))
                                    query_job_details = f"""
                                                    SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                        jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                        jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                        jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                        COALESCE(u.country, su.country) AS user_country, 
                                                        COALESCE(u.city, su.city) AS user_city,  
                                                        COALESCE(ep.sector, su.sector) AS sector, 
                                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                        FROM job_post jp
                                                        LEFT JOIN users u ON jp.employer_id = u.user_id
                                                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                        WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id NOT IN ({placeholders})
                                                        AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                        WHEN u.pricing_category = 'platinum' THEN 1
                                                        WHEN u.pricing_category = 'premium' THEN 2
                                                        WHEN u.pricing_category = 'basic' THEN 3
                                                        ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC LIMIT 10 OFFSET %s"""
                                    invited_job_values = (tuple(invited_job_id_list) + (professional_id,) + (offset,))
                                else:
                                    query_job_details = """
                                                    SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                        jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                        jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                        jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                        COALESCE(u.country, su.country) AS user_country, 
                                                        COALESCE(u.city, su.city) AS user_city,  
                                                        COALESCE(ep.sector, su.sector) AS sector, 
                                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                        FROM job_post jp
                                                        LEFT JOIN users u ON jp.employer_id = u.user_id
                                                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                        WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' 
                                                        AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                        WHEN u.pricing_category = 'platinum' THEN 1
                                                        WHEN u.pricing_category = 'premium' THEN 2
                                                        WHEN u.pricing_category = 'basic' THEN 3
                                                        ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC LIMIT 10 OFFSET %s"""
                                    invited_job_values = (professional_id, offset,)
                                job_details = execute_query(query_job_details, invited_job_values)
                                final_job_details = job_details
                            else:
                                query_job_details = """
                                                SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                    jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                    jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                    jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                    COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                    COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                    COALESCE(u.country, su.country) AS user_country, 
                                                    COALESCE(u.city, su.city) AS user_city,  
                                                    COALESCE(ep.sector, su.sector) AS sector, 
                                                    COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                    COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                    COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                    FROM job_post jp
                                                    LEFT JOIN users u ON jp.employer_id = u.user_id
                                                    LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                    LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                    WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id IN %s
                                                    AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                    ORDER BY CASE
                                                    WHEN u.pricing_category = 'platinum' THEN 1
                                                    WHEN u.pricing_category = 'premium' THEN 2
                                                    WHEN u.pricing_category = 'basic' THEN 3
                                                    ELSE 4
                                                END ASC, 
                                                jp.created_at DESC LIMIT 10 OFFSET %s"""
                                invited_job_values = (tuple(invited_job_id_list),professional_id, offset)
                                invited_jobs_details = execute_query(query_job_details, invited_job_values)
                                if len(invited_jobs_details) < 10:
                                    new_offset = offset - invited_jobs_count
                                    if new_offset < 0:
                                        new_offset = 0
                                    new_limit = 10 - len(invited_jobs_details)
                                    placeholders = ', '.join(['%s'] * len(invited_job_id_list))
                                    query_job_details = f"""
                                                    SELECT jp.id, jp.job_title,jp.job_type,jp.job_overview,jp.job_desc,jp.responsibilities,jp.additional_info,jp.skills,jp.country,
                                                        jp.state,jp.city,jp.work_schedule,jp.workplace_type,jp.is_paid,jp.time_commitment,jp.timezone,jp.duration,jp.calendly_link,jp.currency,
                                                        jp.benefits,jp.required_resume,jp.required_cover_letter,jp.required_background_check,jp.required_subcontract,jp.is_application_deadline,
                                                        jp.application_deadline_date,jp.is_active,jp.share_url,jp.specialisation,jp.created_at,
                                                        COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                        COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                        COALESCE(u.payment_status, su.payment_status) AS payment_status, 
                                                        COALESCE(u.country, su.country) AS user_country, 
                                                        COALESCE(u.city, su.city) AS user_city,  
                                                        COALESCE(ep.sector, su.sector) AS sector, 
                                                        COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                        COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                        COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                        FROM job_post jp
                                                        LEFT JOIN users u ON jp.employer_id = u.user_id
                                                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                                                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                        WHERE jp.job_status = 'opened' AND jp.is_active = 'Y' AND jp.id NOT IN ({placeholders})
                                                        AND NOT EXISTS (SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id AND ja.professional_id = %s)
                                                        ORDER BY CASE
                                                        WHEN u.pricing_category = 'platinum' THEN 1
                                                        WHEN u.pricing_category = 'premium' THEN 2
                                                        WHEN u.pricing_category = 'basic' THEN 3
                                                        ELSE 4
                                                    END ASC, 
                                                    jp.created_at DESC LIMIT %s OFFSET %s"""
                                    non_invited_job_values = (tuple(invited_job_id_list) + (professional_id,) + (new_limit,) + (new_offset,))
                                    non_invited_jobs_details = execute_query(query_job_details, non_invited_job_values)
                                    for ij in invited_jobs_details:
                                        ij.update({"invited_by_employer" : "1Y"})
                                    for nij in non_invited_jobs_details:
                                        nij.update({"invited_by_employer" : "N"})
                                    jobs_details = invited_jobs_details + non_invited_jobs_details
                                    sorted_data = sorted(jobs_details, key=lambda x: x["invited_by_employer"])
                                    invited_jobs_details = sorted_data
                                final_job_details = invited_jobs_details

                    # if key == 'sort':
                    #     if param == 'by_date':
                    #         final_job_details = sorted(final_job_details, key=lambda x: x["created_at"])
                    #     elif param == 'asc':
                    #         final_job_details = sorted(final_job_details, key=lambda x: x['job_title'])
                    #     else:
                    #         final_job_details = sorted(final_job_details, key=lambda x: x['job_title'], reverse = True)
                    if len(final_job_details) > 0:
                        id = final_job_details[0]["id"]
                        query = 'select * from  view_count where job_id = %s and professional_id = %s'
                        values = (id,professional_id,)
                        count = execute_query(query,values)
                        if not count:
                            current_time = datetime.now()
                            query = "INSERT INTO view_count (job_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                            values = (id, professional_id, current_time,)
                            update_query(query,values)
                        for job in final_job_details:
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

                        job_details_dict = {'job_details': final_job_details}
                        profile.update(job_details_dict)
                        profile.update({'total_count': total_count})                          
                        data = fetch_filter_params()
                        profile.update(data)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional dashboard details displayed successfully.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details",event_properties, temp_dict.get('Message'), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Dashboard Details, {str(e)}")
                        result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                    else:
                        profile.update({'job_details':[]})
                        profile.update({'total_count': 0})
                        data = fetch_filter_params()
                        profile.update(data)
                        result_json = api_json_response_format(True, "No jobs found", 0, profile)
                        # try:
                        #     temp_dict = {'Country' : user_data['country'],
                        #                 'City' : user_data['city'],
                        #                 'Message': 'Error in fetching professional dashboard details.'}
                        #     event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details Error", temp_dict)
                        #     background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details Error",event_properties, temp_dict.get('Message'), user_data)
                        # except Exception as e:  
                        #     print(f"Error in mixpanel event logging: Professional Dashboard Details Error, {str(e)}")
                        # result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.", 401, {})
                else:
                    result_json = api_json_response_format(False, "Unauthorized user", 401, {})
            else:
                result_json = api_json_response_format(False, "User not exists", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in fetching professional dashboard details.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Dashboard Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Dashboard Details Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Dashboard Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json
    
def create_dict1(row):
    return {row['type']: row['name']}

def fetch_filter_params():
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
    """
    values = (ACTIVE_FLAG,) * 6 + (ACTIVE_FLAG,)  # Repeat ACTIVE_FLAG for each parameter
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
    'schedule': []
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

def create_dict(obj):
    temp_dict = {}
    temp = []
    a = list(obj[0].keys())[0]
    for i in obj:
      temp.append(i[a])
    temp_dict = { a : temp }
    return temp_dict

def professional_home_applied_view():
    try:
        req_data = request.get_json()
        token_result = get_user_token(request)
        key = ''
        param = ''
        if token_result["status_code"] != 200:
            return api_json_response_format(False, "Invalid Token. Please try again.", 401, {})

        user_data = get_user_data(token_result["email_id"])

        if user_data["user_role"] != "professional":
            return api_json_response_format(False, "Unauthorized user", 401, {})

        if 'page_number' not in req_data:
            return api_json_response_format(False, "Page number required", 204, {})
        if 'key' in req_data:
            key = req_data['key']
        if 'param' in req_data:
            param = req_data['param']
        page_number = 1  # Default page number
        if 'page_number' in req_data:
            page_number = int(req_data['page_number'])
        limit = 10
        offset = (page_number - 1) * limit
        professional_id = user_data["user_id"]
        profile_percentage = show_percentage(professional_id)
        order_by = ''
        query = 'SELECT job_id, application_status FROM job_activity WHERE professional_id = %s order by created_at desc'
        values = (professional_id,)
        applied_jobs_list = execute_query(query, values)
        id_list = []
        job_dict = {}
        for job in applied_jobs_list:
            id_list.append(job['job_id'])
            job_dict[job['job_id']] = job['application_status']
        job_details = []
        if len(applied_jobs_list) > 0:
            if key == 'sort' and param != '':
                if key == 'sort' and param == 'by_date_asc':
                    query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.timezone,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                            COALESCE(u.profile_image, su.profile_image) AS profile_image,
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS user_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, ja.created_at as applied_on, ja.feedback FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id LEFT JOIN job_activity ja ON jp.id = ja.job_id WHERE jp.id IN %s and ja.professional_id = %s ORDER BY jp.created_at ASC;"""
                elif key == 'sort' and param == 'by_date_desc':
                    query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.timezone,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                            COALESCE(u.profile_image, su.profile_image) AS profile_image,
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS user_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, ja.created_at as applied_on, ja.feedback FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id LEFT JOIN job_activity ja ON jp.id = ja.job_id WHERE jp.id IN %s and ja.professional_id = %s ORDER BY jp.created_at DESC;"""
                elif key == 'sort' and param == 'asc':
                    query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.timezone,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                            COALESCE(u.profile_image, su.profile_image) AS profile_image,
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS user_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, ja.created_at as applied_on, ja.feedback FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id LEFT JOIN job_activity ja ON jp.id = ja.job_id WHERE jp.id IN %s and ja.professional_id = %s ORDER BY jp.job_title ASC;"""
                elif key == 'sort' and param == 'desc':
                    query_job_details = """SELECT DISTINCT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.timezone,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS user_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status, ja.created_at as applied_on, ja.feedback FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id LEFT JOIN job_activity ja ON jp.id = ja.job_id WHERE jp.id IN %s and ja.professional_id = %s ORDER BY jp.job_title DESC;"""
                values_job_details = (tuple(id_list), professional_id,)
                job_details = replace_empty_values(execute_query(query_job_details, values_job_details))
            else:
                query_job_details = """
                        SELECT DISTINCT
                            jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, 
                            jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.timezone,jp.specialisation,
                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter,
                            jp.required_background_check, jp.required_subcontract, jp.is_application_deadline,
                            jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at, 
                            COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                            COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                            COALESCE(ep.employer_id, su.sub_user_id) AS user_id, 
                            COALESCE(ep.sector, su.sector) AS sector, 
                            COALESCE(ep.company_description, su.company_description) AS company_description, 
                            COALESCE(ep.company_name, su.company_name) AS company_name, 
                            COALESCE(ep.employer_type, su.employer_type) AS employer_type, 
                            COALESCE(u.payment_status, su.payment_status) AS payment_status,
                            ja.created_at as applied_on, ja.feedback
                        FROM 
                            job_post jp 
                        LEFT JOIN 
                            users u ON jp.employer_id = u.user_id 
                            LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                            LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                            LEFT JOIN job_activity ja ON jp.id = ja.job_id
                        WHERE jp.id IN %s and ja.professional_id = %s ORDER BY applied_on DESC
                    """
                values_job_details = (tuple(id_list), professional_id,)
                job_details = replace_empty_values(execute_query(query_job_details + order_by, values_job_details))
        else:
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Message': f"{user_data['email_id']} viewed the applied jobs section but has not applied for any jobs."}
                event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs", temp_dict)
                background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs",event_properties, temp_dict.get("Message"), user_data)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Professional Applied Jobs, {str(e)}")
            result_json = api_json_response_format(True, "You haven't applied for any jobs", 0, {})
            return result_json
        
        job_list = []
        if len(job_details) > 0:
            for job in job_details:
                status = job['job_status'].lower()
                if status == 'opened':
                    job_list.append(job)
                elif status in {'closed', 'paused'}:
                    last_updated_date = datetime.strptime(str(job['updated_at']), '%Y-%m-%d %H:%M:%S')
                    expiration_time = last_updated_date + timedelta(days=30)
                    if datetime.now() < expiration_time:
                        job_list.append(job)

            saved_status_query = 'SELECT job_id FROM saved_job WHERE professional_id = %s AND job_id IN %s'
            saved_jobs = execute_query(saved_status_query, (professional_id, tuple(id_list)))

            applied_status_query = 'SELECT job_id FROM job_activity WHERE professional_id = %s AND job_id IN %s'
            applied_jobs = execute_query(applied_status_query, (professional_id, tuple(id_list)))

            saved_jobs_set = set(job['job_id'] for job in saved_jobs)
            applied_jobs_set = set(job['job_id'] for job in applied_jobs)

            for job in job_list:
                job_id = job['id']
                job['application_status'] = job_dict[job_id]
                job['saved_status'] = "saved" if job_id in saved_jobs_set else "unsaved"
                job['applied_status'] = "applied" if job_id in applied_jobs_set else "not_applied"
                job['profile_image'] = s3_employeer_logo_folder_name + job['profile_image']
                job['sector_image'] = s3_sector_image_folder_name + job['sector'].replace(", ", "_").replace(" ", "_") + ".png"
                job['created_at'] = str(job['created_at'])
                job['profile_percentage'] = profile_percentage
                job['total_count'] = len(job_list)
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

            paginated_jobs = job_list[offset:offset + limit]
            result = replace_empty_values(paginated_jobs)
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Message': f"{user_data['email_id']} has successfully viewed the applied jobs."}
                event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs", temp_dict)
                background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs",event_properties, temp_dict.get("Message"), user_data)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Professional Applied Jobs, {str(e)}")

            result_json = api_json_response_format(True, "Details fetched successfully!", 0, {'job_details': result})
            return result_json
        else:
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Message': f"{user_data['email_id']} viewed the applied jobs section but has not applied for any jobs."}
                event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs", temp_dict)
                background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs",event_properties, temp_dict.get("Message"), user_data)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Professional Applied Jobs, {str(e)}")
            result_json = api_json_response_format(True, "You haven't applied for any jobs", 0, {})
            return result_json
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching {user_data['email_id']}'s applied jobs."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Applied Jobs Error, {str(e)}")
        print(error)
        return api_json_response_format(False, str(error), 500, {})
    
def get_professional_saved_jobs():
    try:
        result_json = {}
        req_data = request.get_json()
        token_result = get_user_token(request)
        key = ''
        param = ''
        if token_result["status_code"] != 200:
            return api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
        if 'key' in req_data:
            key = req_data['key']
        if 'param' in req_data:
            param = req_data['param']
        user_data = get_user_data(token_result["email_id"])
        if user_data["user_role"] != "professional":
            return api_json_response_format(False, "Unauthorized user", 401, {})
        
        professional_id = user_data["user_id"]
        profile_percentage = show_percentage(professional_id)

        query = 'SELECT sj.job_id FROM saved_job sj LEFT JOIN job_activity ja ON sj.job_id = ja.job_id AND sj.professional_id = ja.professional_id WHERE sj.professional_id = %s AND ja.job_id IS NULL ORDER BY `sj`.`created_at` DESC'
        values = (professional_id,)
        saved_job_id = execute_query(query, values)
        print(f"saved_job_ids: {saved_job_id}")
        saved_job_ids = [item['job_id'] for item in saved_job_id]
        placeholders = ', '.join(['%s'] * len(saved_job_ids))
        job_details_list = []
        if len(saved_job_ids) > 0:
            if key == 'sort' and param != '':
                if key == 'sort' and param == 'by_date_asc':
                    query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.timezone, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                COALESCE(u.payment_status, su.payment_status) AS payment_status,
                COALESCE(u.user_id, su.sub_user_id) AS user_id,
                sj.created_at as saved_at
                FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN saved_job sj ON jp.id = sj.job_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id IN %s and jp.is_active = %s and sj.professional_id = %s ORDER BY jp.created_at ASC;"""
                elif key == 'sort' and param == 'by_date_desc':
                    query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.timezone, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                COALESCE(u.payment_status, su.payment_status) AS payment_status,
                COALESCE(u.user_id, su.sub_user_id) AS user_id,
                sj.created_at as saved_at 
                FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN saved_job sj ON jp.id = sj.job_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id IN %s and jp.is_active = %s and sj.professional_id = %s ORDER BY jp.created_at DESC;"""
                elif key == 'sort' and param == 'asc':
                    query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.timezone, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                COALESCE(u.payment_status, su.payment_status) AS payment_status,
                COALESCE(u.user_id, su.sub_user_id) AS user_id,
                sj.created_at as saved_at
                FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN saved_job sj ON jp.id = sj.job_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id IN %s and jp.is_active = %s and sj.professional_id = %s ORDER BY jp.job_title ASC;"""
                elif key == 'sort' and param == 'desc':
                    query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.timezone, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at,
                    COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                COALESCE(u.payment_status, su.payment_status) AS payment_status,
                COALESCE(u.user_id, su.sub_user_id) AS user_id,
                sj.created_at as saved_at
                FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN saved_job sj ON jp.id = sj.job_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE jp.id IN %s and jp.is_active = %s and sj.professional_id = %s ORDER BY jp.job_title DESC;"""
                values = (tuple(saved_job_ids),'Y', professional_id,)
            else:
                # query_job_details = f"""SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, jp.timezone, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.specialisation, jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, 
                # jp.required_subcontract, jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at, 
                # COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                # COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                # COALESCE(u.payment_status, su.payment_status) AS payment_status,
                # COALESCE(u.user_id, su.sub_user_id) AS user_id, sj.created_at as saved_at 
                # FROM job_post jp 
                # LEFT JOIN users u ON jp.employer_id = u.user_id 
                # LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                # LEFT JOIN saved_job sj ON jp.id = sj.job_id WHERE jp.id IN ({placeholders}) and jp.is_active = %s and sj.professional_id = %s ORDER BY saved_at DESC;"""
                
                query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.additional_info, jp.skills, jp.country, jp.state, jp.city, 
                jp.work_schedule, jp.timezone, jp.workplace_type, jp.is_paid, jp.job_status, jp.time_commitment, jp.duration,jp.specialisation, jp.calendly_link, 
                jp.currency, jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, jp.is_application_deadline, 
                jp.application_deadline_date, jp.is_active, jp.share_url, jp.created_at, jp.updated_at, 
                COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                COALESCE(u.user_id, su.sub_user_id) AS user_id,
                sj.created_at as saved_at FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN saved_job sj ON jp.id = sj.job_id 
                LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                WHERE jp.id IN %s and jp.is_active = %s and sj.professional_id = %s ORDER BY saved_at DESC;"""
                values = (tuple(saved_job_ids),'Y', professional_id,)
            job_details = replace_empty_values(execute_query(query_job_details, values))
        else:
            try:
                temp_dict = {'Country' : user_data['country'],
		                    'City' : user_data['city'],
		                    'Message': f"{user_data['email_id']} viewed the saved jobs section but hasn't saved any jobs."}
                event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs", temp_dict)
                background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs",event_properties, temp_dict.get("Message"), user_data)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Professional Saved Jobs, {str(e)}")
            result_json = api_json_response_format(True, "You haven't saved any jobs", 0, {})
            return result_json 
        if len(job_details) > 0:
            for job in job_details:
                id = job["id"]
                query = 'select * from  view_count where job_id = %s and professional_id = %s'
                values = (id,professional_id,)
                count = execute_query(query,values)
                if not count:
                    current_time = datetime.now()
                    query = "INSERT INTO view_count (job_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                    values = (id, professional_id, current_time,)
                    update_query(query,values)
                
                quest_dict = {"questions": []}
                query = 'SELECT id, custom_pre_screen_ques FROM pre_screen_ques WHERE job_id = %s'
                values = (id,)
                result = execute_query(query, values)
                if len(result) != 0:
                    for r in result:
                        quest_dict["questions"].append(r)
                job.update(quest_dict)
                if job['user_id'] < 500000:
                    query = "select employer_type, sector, company_name from employer_profile where employer_id = %s"
                else:
                    query = "select employer_type, sector, company_name from sub_users where sub_user_id = %s"
                values = (job['user_id'],)
                emp_details = execute_query(query, values)
                if len(emp_details) > 0:
                    txt = emp_details[0]['sector']
                    txt = txt.replace(", ", "_")
                    txt = txt.replace(" ", "_")
                    sector_name = txt + ".png"
                    job.update({'sector_image' : s3_sector_image_folder_name + sector_name})
                    job.update({'profile_image' : s3_employeer_logo_folder_name + job['profile_image']})
                    job.update({'employer_type': emp_details[0]['employer_type']})
                    job.update({'sector': emp_details[0]['sector']})
                    job.update({'company_name': emp_details[0]['company_name']})
                    job.update({'created_at': str(job['created_at'])})
                    job.update({'application_deadline_date': str(job['application_deadline_date'])})
                query = 'select professional_resume from professional_profile where professional_id = %s'
                values = (professional_id,)
                rslt = execute_query(query, values)
                if len(rslt) > 0:
                    resume_name = rslt[0]['professional_resume']
                else:
                    resume_name = ''
                query = 'SELECT COUNT(job_id) FROM saved_job WHERE job_id = %s AND professional_id = %s'
                values = (id, professional_id,)
                rslt = execute_query(query, values)
                job_saved_status = "saved" if rslt[0]['COUNT(job_id)'] > 0 else "unsaved"
                job.update({'saved_status': job_saved_status})

                query = 'SELECT COUNT(job_id) FROM job_activity WHERE job_id = %s AND professional_id = %s'
                values = (id, professional_id,)
                rslt = execute_query(query, values)
                job_applied_status = 'applied' if rslt[0]['COUNT(job_id)'] > 0 else 'not_applied'
                query = 'SELECT ij.employer_feedback, (SELECT COUNT(job_id) FROM invited_jobs WHERE professional_id = ij.professional_id AND job_id = ij.job_id) AS job_count FROM invited_jobs ij WHERE ij.professional_id = %s AND ij.job_id = %s AND ij.is_invite_sent = "Y";'
                values = (professional_id, id,)
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
                job.update({'applied_status': job_applied_status})
                job.update({'user_resume' : resume_name})
                job.update({'profile_percentage' : profile_percentage})
                job_details_list.append(job)
            job_list = []
            for i in job_details_list:
                if i['job_status'] == 'Opened' or i['job_status'] == 'opened':
                    job_list.append(i)
                elif i['job_status'] == 'Closed' or i['job_status'] == 'closed' or i['job_status'] == 'Paused' or i['job_status'] == 'paused':
                    last_updated_date = str(i['updated_at'])
                    last_updated_date = datetime.strptime(last_updated_date, '%Y-%m-%d %H:%M:%S')
                    expiration_time = last_updated_date + timedelta(days=30)
                    current_date = datetime.now()
                    if current_date < expiration_time:
                        job_list.append(i)
            total_count = len(job_list)
            for k in job_list:
                k.update({'total_count' : total_count})
            # Implementing pagination
            page_number = 1  # Default page number
            if 'page_number' in req_data:
                page_number = int(req_data['page_number'])
            limit = 10
            offset = (page_number - 1) * limit
            paginated_jobs = job_list[offset:offset + limit]
            result = replace_empty_values(paginated_jobs)
            if result:
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"{user_data['email_id']} has successfully viewed the saved jobs."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Professional Saved Jobs, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, result)
            else:
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"{user_data['email_id']} viewed the saved jobs section but hasn't saved any jobs."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Professional Saved Jobs, {str(e)}")
                result_json = api_json_response_format(True, "You haven't saved any jobs", 0, result)
            return result_json
        else:
            try:
                temp_dict = {'Country' : user_data['country'],
		                    'City' : user_data['city'],
		                    'Message': f"{user_data['email_id']} viewed the saved jobs section but hasn't saved any jobs."}
                event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs", temp_dict)
                background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs",event_properties, temp_dict.get("Message"), user_data)
            except Exception as e:  
                print(f"Error in mixpanel event logging: Professional Saved Jobs, {str(e)}")
            result_json = api_json_response_format(True, "You haven't saved any jobs", 0, {})
            return result_json
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in fetching {user_data['email_id']}'s saved jobs."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Saved Jobs Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
        return result_json
    
def selected_job_details():
    try:
        req_data = request.get_json()                        
        if 'job_id' not in req_data:
            result_json = api_json_response_format(False,"job_id required",204,{})  
            return result_json
        job_id = req_data['job_id'] 
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:            
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                if job_id.startswith("Ex_"):
                    query = '''SELECT `Id` AS id, `employer_id`, `job_reference_id`, `job_title`, `job_overview`, `job_description` AS job_desc, 
                        `schedule` AS work_schedule, `skills`, `country`, `state`, `city`, `company_name`, `company_sector` AS sector, `job_type`, 
                        `workplace_type`, `Functional_Specification` AS specialisation, `functional_specification_others`, `apply_link`, 
                        `source`, `seniority`, `is_active`, `pricing_category`, `admin_job_status` AS job_status, `created_at`,
                        NULL AS applied_status, NULL AS benefits, NULL AS calendly_link, NULL AS currency, NULL AS duration, NULL AS additional_info, NULL AS application_deadline_date, NULL AS company_description,
                        NULL AS employer_type, NULL AS feedback, NULL AS invited_by_employer, NULL AS invited_message, NULL AS is_application_deadline, NULL AS is_paid, NULL AS number_of_openings, 
                        NULL AS profile_image, NULL AS profile_percentage, NULL AS questions, NULL AS recommended_by, NULL AS required_background_check, NULL AS required_cover_letter, NULL AS required_resume,
                        NULL AS required_subcontract, NULL AS responsibilities, NULL AS saved_status, NULL AS sc_recommended_notes, NULL AS sector_image, NULL AS time_commitment, NULL AS timezone, NULL AS user_resume
                        FROM admin_job_post WHERE job_reference_id = %s'''
                    values = (job_id,)
                    job_details_data_set = execute_query(query,values)

                    result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(job_details_data_set))
                    return result_json
                
                professional_id = user_data['user_id']
                profile_percentage = show_percentage(professional_id)
                query = 'select id, job_title, job_type, job_desc, job_overview, responsibilities, additional_info, job_status, specialisation, skills, country, state, city, work_schedule, workplace_type, number_of_openings, time_commitment, timezone, duration, calendly_link, currency, benefits, required_resume, required_cover_letter, required_background_check, required_subcontract, is_application_deadline, application_deadline_date, is_paid, is_active, created_at from job_post where id = %s'
                values = (job_id,)
                job_details_data_set = execute_query(query,values)
                
                query = 'select employer_id from job_post where id = %s'
                values = (job_id,)
                employer_id_dict_1 = execute_query(query, values)
                
                if employer_id_dict_1:
                    if employer_id_dict_1[0]['employer_id'] > 500000:
                        query = 'select user_id from sub_users where sub_user_id = %s'
                        values = (employer_id_dict_1[0]['employer_id'],)
                        employer_id_dict = execute_query(query, values)
                        if employer_id_dict:
                            employer_id = employer_id_dict[0]['user_id']
                    else:
                        employer_id = employer_id_dict_1[0]['employer_id']
                else:
                    result_json = api_json_response_format(False, "The job you're looking for is no longer available.", 404, {})
                    return result_json
                if employer_id < 500000:
                    query = 'select profile_image, pricing_category from users where user_id = %s'
                    values = (employer_id,)
                    profile_image = execute_query(query, values)
                    
                    query = 'select employer_type,company_name,company_description, sector from employer_profile where employer_id = %s'
                    values = (employer_id,)
                    employer_details = execute_query(query, values)
                else:
                    query = 'select profile_image, pricing_category from sub_users where sub_user_id = %s'
                    values = (employer_id,)
                    profile_image = execute_query(query, values)
                    
                    query = 'select employer_type,company_name,company_description, sector from sub_users where sub_user_id = %s'
                    values = (employer_id,)
                    employer_details = execute_query(query, values)

                query = 'select * from  view_count where job_id = %s and professional_id = %s'
                values = (job_id,professional_id,)
                count = execute_query(query,values)
                if not count:
                    current_time = datetime.now()
                    query = "INSERT INTO view_count (job_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                    values = (job_id, professional_id, current_time,)
                    update_query(query,values) 
                
                quest_dict = {"questions" : []}
                query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                values = (job_id,)
                result = execute_query(query, values)
                if len(result) != 0:
                   for r in result:
                       quest_dict["questions"].append(r)

                query = "select count(job_id) from saved_job where job_id=%s and professional_id=%s"
                values = (job_id, professional_id,)
                res = execute_query(query, values)
                if res[0]['count(job_id)'] > 0:
                    job_saved_status = 'saved'
                else:
                    job_saved_status = 'unsaved'
                
                query = 'select description from sc_recommendation where professional_id = %s and job_id = %s'
                values = (professional_id, job_id,)
                sc_recommended_notes= execute_query(query, values)
                if not sc_recommended_notes:
                    job_details_data_set[0].update({"sc_recommended_notes" : ""})
                else:
                    job_details_data_set[0].update({"sc_recommended_notes" :  sc_recommended_notes[0]['description']})
                job_details_data_set[0].update({"recommended_by" : ""})

                query = "select count(job_id) from job_activity where job_id=%s and professional_id=%s"
                values = (job_id, professional_id,)
                res = execute_query(query, values)
                if res[0]['count(job_id)'] > 0:
                    job_applied_status = 'applied'
                else:
                    job_applied_status = 'not_applied'
                query = "select feedback from job_activity where job_id=%s and professional_id=%s"
                values = (job_id, professional_id,)
                feedback_res = execute_query(query, values)
                feedback = ''
                if len(feedback_res) > 0:
                    feedback = feedback_res[0]['feedback']
                txt = employer_details[0]['sector']
                txt = txt.replace(", ", "_")
                txt = txt.replace(" ", "_")
                sector_name = txt + ".png"
                job_applied_dict = {'applied_status' : job_applied_status}
                job_saved_dict = {'saved_status' : job_saved_status}
                query = 'select professional_resume from professional_profile where professional_id = %s'
                values = (professional_id,)
                rslt = execute_query(query, values)
                resume_name = rslt[0]['professional_resume']
                query = 'SELECT ij.employer_feedback, (SELECT COUNT(job_id) FROM invited_jobs WHERE professional_id = ij.professional_id AND job_id = ij.job_id) AS job_count FROM invited_jobs ij WHERE ij.professional_id = %s AND ij.job_id = %s AND ij.is_invite_sent = "Y";'
                values = (professional_id, job_id,)
                employee_feedback = execute_query(query, values)
                if len(employee_feedback) > 0:
                    if employee_feedback[0]['employer_feedback'] is not None:
                        job_details_data_set[0].update({'invited_message' : employee_feedback[0]['employer_feedback']})
                    else:
                        job_details_data_set[0].update({'invited_message' : ''})
                    job_details_data_set[0].update({'invited_by_employer' : '1Y'})
                else:
                    job_details_data_set[0].update({'invited_message' : ''})
                    job_details_data_set[0].update({'invited_by_employer' : 'N'})
                job_details_data_set[0].update({'feedback' : feedback})
                job_details_data_set[0].update({'user_resume' : resume_name})
                job_details_data_set[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                job_details_data_set[0].update(quest_dict)
                job_details_data_set[0].update(job_applied_dict)
                job_details_data_set[0].update(job_saved_dict)
                job_details_data_set[0].update({'employer_type' : employer_details[0]['employer_type']})
                job_details_data_set[0].update({'company_description' : employer_details[0]['company_description']})
                job_details_data_set[0].update({'company_name' : employer_details[0]['company_name']})
                job_details_data_set[0].update({'sector' : employer_details[0]['sector']})
                job_details_data_set[0].update({'profile_percentage' : profile_percentage})
                job_details_data_set[0].update({'profile_image' : s3_employeer_logo_folder_name + profile_image[0]['profile_image']})
                job_details_data_set[0].update({'pricing_category' : profile_image[0]['pricing_category']})
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"{user_data['email_id']} has successfully viewed the job {job_details_data_set[0]['job_title']}."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Professional Selected Job Details", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Professional Selected Job Details",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Professional Selected Job Details, {str(e)}")
                result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(job_details_data_set))
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in professional selected job details."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Selected Job Details Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Selected Job Details Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Selected Job Details Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 
    
def get_professional_dashboard_filter_list():
    try:
        dashboard_filter_list = []
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                query = 'select skill_name from filter_skills where is_active = %s'
                values = ('Y',)
                skill = execute_query(query,values)
                dashboard_filter_list.append(create_dict(skill))                

                query = 'select sector_name from filter_sectors where is_active = %s'
                values = ('Y',)
                sectors = execute_query(query,values)
                dashboard_filter_list.append(create_dict(sectors))                
                query = 'select specialisation_name from filter_specialisation where is_active = %s'
                values = ('Y',)
                specialisation = execute_query(query,values)
                dashboard_filter_list.append(create_dict(specialisation))                
                query = 'select workplace_type from filter_workplace where is_active = %s'
                values = ('Y',)
                workplace_type = execute_query(query,values)
                dashboard_filter_list.append(create_dict(workplace_type))                
                query = 'select job_type from filter_jobtype where is_active = %s'
                values = ('Y',)
                job_type = execute_query(query,values)
                dashboard_filter_list.append(create_dict(job_type))                
                query = 'select schedule from filter_schedule where is_active = %s'
                values = ('Y',)
                schedule = execute_query(query,values)
                dashboard_filter_list.append(create_dict(schedule))                
                result_json = api_json_response_format(True,"Filtered list successfully!",0,dashboard_filter_list)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json 

def calculate_relative_time(created_at):
    created_at_dt = datetime.strptime(str(created_at), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    time_diff = now - created_at_dt
    seconds = int(time_diff.total_seconds())
    if seconds < 60:
        if seconds == 1:
            return f"{seconds} second ago"
        return f"{seconds} seconds ago"
    elif seconds < 3600:
        minutes = seconds // 60
        if minutes == 1:
            return f"{minutes} minute ago"
        return f"{minutes} minutes ago"
    elif seconds < 86400:
        hours = seconds // 3600
        if hours == 1:
            return f"{hours} hour ago"
        return f"{hours} hours ago"
    elif seconds < 604800:
        days = seconds // 86400
        if days == 1:
            return f"{days} day ago"
        return f"{days} days ago"
    elif seconds < 2419200:
        weeks = seconds // 604800
        if weeks == 1:
            return f"{weeks} week ago"
        return f"{weeks} weeks ago"
    elif seconds < 31449600:
        months = seconds // 2419200
        if months == 1:
            return f"{months} month ago"
        return f"{months} months ago"
    else:
        years = seconds // 31449600
        if years == 1:
            return f"{years} year ago"
        return f"{years} years ago"

def get_professional_notifications():
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if not user_data['is_exist']:
                user_data = get_sub_user_data(token_result["email_id"])
                professional_id = user_data['sub_user_id']
            else:
                professional_id = user_data["user_id"]
            if user_data["user_role"] == "professional" or user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "partner":
                query = 'select id, notification_msg as msg, is_viewed as view_status, created_at from user_notifications where user_id = %s order by created_at desc'
                values = (professional_id,)
                notification_list = execute_query(query,values)
                if notification_list:
                    for n in notification_list:
                        relative_time = calculate_relative_time(n['created_at'])
                        n.update({'relative_time' : relative_time})
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"Notifications fetched successfully for {user_data['email_id']}"}
                    event_properties = background_runner.process_dict(user_data["email_id"], "User Notifications", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"User Notifications",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: User Notifications, {str(e)}")
                result_json = api_json_response_format(True,"Details fetched successfully!",0,notification_list)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in fetching user notifications.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "User Notifications Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"User Notifications Error",event_properties, temp_dict.get('Message'), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: User Notifications Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def notification_status():
    try:
        token_result = get_user_token(request)
        req_data = request.get_json()                        
        if 'notification_id' not in req_data:
            result_json = api_json_response_format(False,"notification_id required",204,{})  
            return result_json
        notification_id = req_data['notification_id']                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            user_id = user_data['user_id']            
            if user_data["user_role"] == "professional" or user_data["user_role"] == "employer" or user_data["user_role"] == "partner":
                query = 'update user_notifications set is_viewed = %s where id = %s and user_id = %s'
                values = ('Y', notification_id, user_id,)
                row_count = update_query(query,values)                
                if row_count > 0:
                    result_json = api_json_response_format(True,"Notification status updated successfully",0,{})
                else:
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def delete_notifications():
    try:
        token_result = get_user_token(request)
        req_data = request.get_json()                        
        if 'notification_id' not in req_data:
            result_json = api_json_response_format(False,"notification_id required",204,{})  
            return result_json
        notification_id = req_data['notification_id']                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            user_id = user_data['user_id']            
            if user_data["user_role"] == "professional" or user_data["user_role"] == "employer" or user_data["user_role"] == "partner":
                if not notification_id == "":
                    query = 'delete from user_notifications where id = %s and user_id = %s'
                    values = (notification_id, user_id,)
                    row_count = update_query(query,values)  
                else:
                    query = 'delete from user_notifications where user_id = %s'
                    values = (user_id,)
                    row_count = update_query(query,values)              
                if row_count > 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"Notifications cleared successfully for {user_data['email_id']}"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Delete User Notifications", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Delete User Notifications",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Delete User Notifications, {str(e)}")
                    result_json = api_json_response_format(True,"Notification cleared successfully",0,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"Error in deleting notifications for {user_data['email_id']}"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Delete User Notifications Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Delete User Notifications Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Delete User Notifications Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in deleting user notifications.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Delete User Notifications Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Delete User Notifications Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Delete User Notifications Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def clear_all_notifications():
    try:
        token_result = get_user_token(request)                        
        user_data = get_user_data(token_result["email_id"])                                        
        if token_result["status_code"] == 200:                                
            user_id = user_data['user_id']            
            query = 'delete from user_notifications where user_id = %s'
            values = (user_id,)
            row_count = update_query(query,values)                
            if row_count > 0:
                result_json = api_json_response_format(True,"Notification cleared successfully",0,{})
            else:
                result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def learning_page_get_all():
    try:
        result_json = {}
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "professional":
                # Fetch event data
                query = 'select id,title, short_description, speaker_name as speaker, image, type_of_community, join_url, share_url, event_date from community where is_active = %s and type = %s order by created_at DESC LIMIT 1'
                values = ('Y', 'Event',)
                event_data_set = execute_query(query, values)
                event_list = []
                for e in event_data_set:
                    s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + e['image']
                    s3_url_key =  e['share_url']
                    e.update({"share_url" : s3_url_key})
                    e.update({"image": s3_sc_community_cover_pic_key})
                    event_list.append(e)
                
                # Fetch learning data
                query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url,l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s order by l.created_at DESC LIMIT 3'
                values = ('opened',)
                learning_data_set = execute_query(query,values)                                
                learning_list = []
                for l in learning_data_set:
                    s3_cover_pic_key = s3_partner_cover_pic_folder_name+l['image']
                    s3_attached_file_key = s3_partner_learning_folder_name+l['attached_file']
                    s3_pic_key = s3_partner_picture_folder_name+str(l['profile_image'])
                    l.update({'profile_image' : s3_pic_key})
                    l.update({"image": s3_cover_pic_key})   
                    l.update({"attached_file": s3_attached_file_key})                      
                    learning_list.append(l)

                # Fetch Careers in Impact data
                query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s ORDER BY created_at DESC LIMIT 3'
                values = ('Y', 'Careers in Impact',)
                careers_data_set = execute_query(query, values)
                careers_list = []
                for c in careers_data_set:
                    s3_sc_careers_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                    c.update({"image": s3_sc_careers_cover_pic_key})
                    careers_list.append(c)

                # Fetch community team data
                query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type = %s'
                values = ('Y', 'Community Team',)
                community_team_data_set = execute_query(query, values)
                community_team_list = []
                for c in community_team_data_set:
                    s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                    c.update({"image": s3_sc_community_cover_pic_key})
                    community_team_list.append(c)

                # Construct the result list directly with the lists
                result_list = {
                    "careers_list" : careers_list,
                    "community_team": community_team_list,
                    "event_posts": event_list,
                    "learning_posts": learning_list
                }
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']} viewed the professional learning_All page."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Learning All Tab", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Learning All Tab",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Learning All Tab, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, replace_empty_values([result_list]))
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing learning_All tab."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Learning All Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Learning All Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Learning All Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def professional_community():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False, "Page number is required", 204, {})
            return result_json
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            page_number = req_data['page_number']
            offset = (page_number - 1) * 10  # Assuming 10 items per page
            if user_data["user_role"] == "professional":
                # Fetch community data
                # query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and title = %s'
                # values = ('Y', 'Whatsapp Community',)
                # community_data_set = execute_query(query, values)
                # community_list = []
                # for c in community_data_set:
                #     s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                #     c.update({"image": s3_sc_community_cover_pic_key})
                #     community_list.append(c)
                
                # query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type = %s'
                # values = ('Y', 'Community Team',)
                # community_team_data_set = execute_query(query, values)
                # community_team_list = []
                # for c in community_team_data_set:
                #     s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                #     c.update({"image": s3_sc_community_cover_pic_key})
                #     community_team_list.append(c)

                # # Fetch event data
                # query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type = %s'
                # values = ('Y', 'Event',)
                # event_data_set = execute_query(query, values)
                # event_list = []
                # for e in event_data_set:
                #     s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + e['image']
                #     # s3_sc_community_audio_key = s3_sc_community_audio_folder_name + e['share_url']
                #     s3_url_key =  e['share_url']
                #     e.update({"share_url" : s3_url_key})
                #     e.update({"image": s3_sc_community_cover_pic_key})
                #     event_list.append(e)

                # # Fetch article data
                # query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s'
                # values = ('Y', 'Learn more',)
                # article_data_set = execute_query(query, values)
                # article_list = []
                # for a in article_data_set:
                #     s3_sc_article_cover_pic_key = s3_sc_community_cover_pic_folder_name + a['image']
                #     a.update({"image": s3_sc_article_cover_pic_key})
                #     article_list.append(a)
                get_count_query = 'select COUNT(id) as count from community where is_active = %s and type_of_community = %s'
                values = ('Y', 'Careers in Impact',)
                profile_count = execute_query(get_count_query, values)
                if profile_count and len(profile_count) > 0:
                    total_count = profile_count[0]['count']
                else:
                    total_count = 0
                query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s ORDER BY created_at DESC LIMIT 10 OFFSET %s'
                values = ('Y', 'Careers in Impact', offset,)
                careers_data_set = execute_query(query, values)
                careers_list = []
                for c in careers_data_set:
                    s3_sc_careers_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                    c.update({"image": s3_sc_careers_cover_pic_key})
                    careers_list.append(c)

                # Construct the result list directly with the lists
                # result_list = {
                #     "article_posts": article_list,
                #     "community_posts": community_list,
                #     "community_team": community_team_list,
                #     "event_posts": event_list,
                #     "careers_list" : careers_list
                # }

                result_list = {
                    "posts" : careers_list,
                    "total_count" : total_count
                }
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']} viewed the professional community page."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Community Tab", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Community Tab",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Community Tab, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, replace_empty_values([result_list]))
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing community tab."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Community Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Community Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Community Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

    
def professional_learning():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False, "Page number is required", 204, {})
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])   
            page_number = req_data['page_number']
            offset = (page_number - 1) * 10  # Assuming 10 items         
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional":
                    update_job_status_query = 'UPDATE learning SET is_active = %s, post_status = %s WHERE DATEDIFF(CURDATE(), created_at) >= days_left;'
                    values = ('N','paused',)
                    rs = update_query(update_job_status_query, values)
                    get_learning_count_query = 'SELECT count(l.id) as count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s;'
                    values = ('opened',)
                    learning_count = execute_query(get_learning_count_query, values)
                    if learning_count and len(learning_count) > 0:
                        total_count = learning_count[0]['count']
                    else:
                        total_count = 0
                    query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url,l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s order by l.created_at DESC LIMIT 10 OFFSET %s'
                    values = ('opened', offset,)
                    learning_data_set = execute_query(query,values)                                
                    new_list = []
                    result_list = []
                    for l in learning_data_set:
                        s3_cover_pic_key = s3_partner_cover_pic_folder_name+l['image']
                        s3_attached_file_key = s3_partner_learning_folder_name+l['attached_file']
                        s3_pic_key = s3_partner_picture_folder_name+str(l['profile_image'])
                        l.update({'profile_image' : s3_pic_key})
                        l.update({"image": s3_cover_pic_key})   
                        l.update({"attached_file": s3_attached_file_key})                      
                        new_list.append(l)
                    
                    result_list.append({"posts": new_list,
                                        "total_count": total_count})
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"User {user_data['email_id']} viewed the professional learning page."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Learning Tab", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Learning Tab",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Learning Tab, {str(e)}")
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(result_list))
                else:
                    result_json = api_json_response_format(False,"Unauthorized user",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing learning tab."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Learning Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Learning Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Learning Tab Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def professional_events():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])        
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional" or user_data["user_role"] == "admin":
                    query = 'select id, title, speaker_name as speaker, short_description, detailed_description, additional_notes, image, type_of_community, join_url, share_url, event_date from community where is_active = %s and type = %s ORDER BY created_at DESC LIMIT 1'
                    values = ('Y', 'Event',)
                    event_data_set = execute_query(query, values)
                    if event_data_set:
                        for e in event_data_set:
                            s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + e['image']
                            s3_url_key =  e['share_url']
                            e.update({"share_url" : s3_url_key})
                            e.update({"speaker_image": s3_sc_community_cover_pic_key})
                            e.update({"what_to_expect" : e['detailed_description']})
                            e.update({"type_of_offerings" : e['additional_notes']})
                            e.pop("additional_notes", None)
                            e.pop("detailed_description", None)
                            e.pop("image", None)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': f"User {user_data['email_id']} viewed the professional learning page."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Learning Tab", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Learning Tab",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Learning Tab, {str(e)}")
                        result_json = api_json_response_format(True,"Details fetched successfully!",0,event_data_set[0])
                    else:
                        result_json = api_json_response_format(True,"No events found",0,{})
                else:
                    result_json = api_json_response_format(False,"Unauthorized user",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing learning tab."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Learning Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Learning Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Learning Tab Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def get_detailed_description_learning():
    try:
        token_result = get_user_token(request)
        req_data = request.get_json()                        
        if 'id' not in req_data:
            result_json = api_json_response_format(False,"id required",204,{})  
            return result_json
        id = req_data['id']                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                query = 'select title, detailed_description from learning where is_active = %s and id = %s'
                values = ('Y', id,)
                detailed_description = execute_query(query,values)
                if detailed_description:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Post Title' : detailed_description[0]['title'],
                                    'Message': f"User {user_data['email_id']} viewed the detailed description of learning post '{detailed_description[0]['title']}'."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Learning Post View", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Learning Post View",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Learning Post View, {str(e)}")
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(detailed_description))
                else:
                    result_json = api_json_response_format(True,"No learning posts found",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing learning post's detailed description."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Learning Post View Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Learning Post View Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Learning Post View Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_detailed_description_community():
    try:
        token_result = get_user_token(request)
        req_data = request.get_json()                        
        if 'id' not in req_data:
            result_json = api_json_response_format(False,"id required",204,{})  
            return result_json
        id = req_data['id']                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                query = 'select title, detailed_description from community where is_active = %s and id = %s'
                values = ('Y', id,)
                detailed_description = execute_query(query,values) 
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Post Title' : detailed_description[0]['title'],
                                'Message': f"User {user_data['email_id']} viewed the detailed description of community post '{detailed_description[0]['title']}'."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Community Post View", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Community Post View",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Community Post View, {str(e)}")              
                result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(detailed_description))
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing community post's detailed description."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Community Post View Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Community Post View Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Community Post View Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def professional_get_perspectives():
    try:
        result_json = {}
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "professional":
                query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type = %s'
                values = ('Y', 'Community Team',)
                community_team_data_set = execute_query(query, values)
                community_team_list = []
                for c in community_team_data_set:
                    s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                    c.update({"image": s3_sc_community_cover_pic_key})
                    community_team_list.append(c)
                result_list = {
                    "community_team": community_team_list
                }
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']} viewed the professional_get_perspectives page."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Perspectives Tab", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Perspectives Tab",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Perspectives Tab, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, replace_empty_values([result_list]))
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing Perspectives tab."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Perspectives Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Perspectives Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Perspectives Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def learning_page_search_filter():
    try:
        result_json = {}
        req_data = request.get_json()
        if 'page_number' not in req_data:
            result_json = api_json_response_format(False, "Page number is required", 204, {})
            return result_json
        if 'key' not in req_data:
            result_json = api_json_response_format(False, "Search key is required", 204, {})
            return result_json
        if 'search_text' not in req_data:
            result_json = api_json_response_format(False, "Search text is required", 204, {})
            return result_json
        if 'page_name' not in req_data:
            result_json = api_json_response_format(False, "Page Name is required", 204, {})
            return result_json
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])   
            page_number = req_data['page_number']
            offset = (page_number - 1) * 10  # Assuming 10 items         
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional":
                    key = req_data['key']
                    search_text = req_data['search_text']
                    page_name = req_data['page_name']
                    page_number = req_data['page_number']
                    offset = (page_number - 1) * 10
                    if search_text == "":
                        result_list = []
                        if page_name == "learning":
                            get_learning_count_query = 'SELECT count(l.id) AS count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s'
                            count_values = ('opened',)
                            learning_count = execute_query(get_learning_count_query, count_values)
                            if learning_count and len(learning_count) > 0:
                                total_count = learning_count[0]['count']
                            else:
                                total_count = 0
                            if key == 'by_date':
                                get_learning_data_query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url,l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s order by l.created_at ASC LIMIT 10 OFFSET %s'
                            elif key == 'by_date_desc':
                                get_learning_data_query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url,l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s order by l.created_at DESC LIMIT 10 OFFSET %s'
                            elif key == 'asc':
                                get_learning_data_query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url,l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s order by l.title ASC LIMIT 10 OFFSET %s'
                            elif key == 'desc':
                                get_learning_data_query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url,l.created_at AS posted_on, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id) vc ON l.id = vc.partner_id WHERE l.post_status = %s order by l.title DESC LIMIT 10 OFFSET %s'
                            
                            data_values = ('opened', offset,)
                            learning_data_set = execute_query(get_learning_data_query, data_values)                             
                            learning_list = []                            
                            for l in learning_data_set:
                                s3_cover_pic_key = s3_partner_cover_pic_folder_name+l['image']
                                s3_attached_file_key = s3_partner_learning_folder_name+l['attached_file']
                                s3_pic_key = s3_partner_picture_folder_name+str(l['profile_image'])
                                l.update({'profile_image' : s3_pic_key})
                                l.update({"image": s3_cover_pic_key})   
                                l.update({"attached_file": s3_attached_file_key})                      
                                learning_list.append(l)
                            result_list.append({"posts": learning_list, "total_count": total_count})
                        elif page_name == "careers_in_impact":
                            get_community_count_query = 'SELECT COUNT(id) as count FROM community WHERE is_active = %s AND type_of_community = %s'
                            count_values = ('Y', 'Careers in Impact',)
                            community_count = execute_query(get_community_count_query, count_values)
                            if community_count and len(community_count) > 0:
                                total_count = community_count[0]['count']
                            else:
                                total_count = 0
                            if key == 'by_date':
                                get_careers_query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s ORDER BY created_at ASC LIMIT 10 OFFSET %s'
                            elif key == 'by_date_desc':
                                get_careers_query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s ORDER BY created_at DESC LIMIT 10 OFFSET %s'
                            elif key == 'asc':
                                get_careers_query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s ORDER BY title ASC LIMIT 10 OFFSET %s'
                            elif key == 'desc':
                                get_careers_query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s ORDER BY title DESC LIMIT 10 OFFSET %s'
                            
                            careers_data__values = ('Y', 'Careers in Impact', offset,)

                            careers_data_set = execute_query(get_careers_query, careers_data__values)
                            careers_list = []
                            for c in careers_data_set:
                                s3_sc_careers_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                                c.update({"image": s3_sc_careers_cover_pic_key})
                                careers_list.append(c)
                            result_list.append({"posts": careers_list, "total_count": total_count})
                    else:
                        result_list = []
                        search_text = f'%{search_text}%'
                        if page_name == 'learning':
                            learning_search_query = '''SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, 
                                                        l.detailed_description, l.image, l.attached_file, l.url,l.created_at AS posted_on, 
                                                        COALESCE(vc.view_count, 0) AS view_count 
                                                        FROM partner_profile pp 
                                                        INNER JOIN users u ON pp.partner_id = u.user_id 
                                                        INNER JOIN learning l ON pp.partner_id = l.partner_id 
                                                        LEFT JOIN (SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count 
                                                        GROUP BY partner_id) vc ON l.id = vc.partner_id 
                                                        WHERE l.post_status = %s AND (l.detailed_description LIKE %s OR l.title LIKE %s)'''
                            learning_search_count = execute_query(learning_search_query, ('opened', search_text, search_text,))
                            if learning_search_count and len(learning_search_count) > 0:
                                total_count = len(learning_search_count)
                            else:
                                total_count = 0
                            learning_data_query = learning_search_query + ' ORDER BY l.created_at DESC LIMIT 10 OFFSET %s'
                            learning_data_values = ('opened', search_text, search_text, offset,)
                            learning_data_set = execute_query(learning_data_query, learning_data_values)
                            learning_list = []
                            for l in learning_data_set:
                                s3_cover_pic_key = s3_partner_cover_pic_folder_name+l['image']
                                s3_attached_file_key = s3_partner_learning_folder_name+l['attached_file']
                                s3_pic_key = s3_partner_picture_folder_name+str(l['profile_image'])
                                l.update({'profile_image' : s3_pic_key})
                                l.update({"image": s3_cover_pic_key})   
                                l.update({"attached_file": s3_attached_file_key})                      
                                learning_list.append(l)
                            result_list.append({"posts": learning_list, "total_count": total_count})
                        elif page_name == 'careers_in_impact':
                            careers_search_query = '''SELECT id,title, short_description, image, type_of_community, join_url, share_url 
                                                    FROM community 
                                                    WHERE is_active = %s AND type_of_community = %s AND (title LIKE %s OR detailed_description LIKE %s)'''
                            careers_search_count = execute_query(careers_search_query, ('Y', 'Careers in Impact', search_text, search_text,))
                            if careers_search_count and len(careers_search_count) > 0:
                                total_count = len(careers_search_count)
                            else:
                                total_count = 0
                            careers_data_query = careers_search_query + ' ORDER BY created_at DESC LIMIT 10 OFFSET %s'
                            careers_data_values = ('Y', 'Careers in Impact', search_text, search_text, offset,)
                            careers_data_set = execute_query(careers_data_query, careers_data_values)
                            careers_list = []
                            for c in careers_data_set:
                                s3_sc_careers_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                                c.update({"image": s3_sc_careers_cover_pic_key})
                                careers_list.append(c)
                            result_list.append({"posts": careers_list, "total_count": total_count})
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"User {user_data['email_id']} viewed the learning_page_search_filter."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Learning Tab Search Filter", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Learning Tab Search Filter",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Learning Tab Search Filter, {str(e)}")
                    result_json = api_json_response_format(True,"Details fetched successfully!",0,replace_empty_values(result_list))
                else:
                    result_json = api_json_response_format(False,"Unauthorized user",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewed learning_page_search_filter."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Learning Tab Search Filter Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Learning Tab Search Filter Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Learning Tab Search Filter Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def professional_discourse_community():
    try:
        result_json = {}
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "professional":
                # Fetch community data
                query = 'select id, title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and title = %s'
                values = ('Y', 'Community',)
                community_data_set = execute_query(query, values)
                community_list = []
                for c in community_data_set:
                    s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                    c.update({"image": s3_sc_community_cover_pic_key})
                    community_list.append(c)

                result_list = {
                    "posts": community_list
                }
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']} viewed the professional Discourse community page."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Discourse Community Tab", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Discourse Community Tab",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Discourse Community Tab, {str(e)}")
                result_json = api_json_response_format(True, "Details fetched successfully!", 0, replace_empty_values([result_list]))
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing Discourse community tab."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Discourse Community Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Discourse Community Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Discourse Community Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def get_additional_info():
    result_json = {}
    try:                
        req_data = request.get_json()                        
        if 'info_id' not in req_data:
            result_json = api_json_response_format(False,"info_id required",204,{})  
            return result_json
        info_id = req_data['info_id'] 
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]                
                if isUserExist("professional_additional_info","id",info_id):
                    query = 'select id,title, description from professional_additional_info where professional_id = %s and id = %s'
                    values = (professional_id, info_id,)
                    result = execute_query(query, values)                    
                    result_json = api_json_response_format(True,"Your profile has been retrieved successfully!",0,result)
                else:             
                    result_json = api_json_response_format(False,"Sorry! We had an issue with retrieving your profile. We request you to retry.",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def update_professional_additional_info():        
    result_json = {}
    try:        
        info_id = 0                                          
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                req_data = request.get_json()
                if 'title' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                if 'description' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                professional_id = user_data["user_id"]
                title = req_data["title"]
                description = req_data['description']
                if 'info_id' in req_data:
                    info_id = req_data['info_id']
                row_count = -1
                process_msg = ""
                if isUserExist("professional_additional_info","id",info_id): 
                    query = 'update professional_additional_info set title = %s, description = %s where id = %s and professional_id=%s'
                    values = (title, description, info_id,professional_id,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                else:
                    created_at = datetime.now()
                    query = 'insert into professional_additional_info(professional_id,title,description,created_at) values(%s,%s,%s,%s)'
                    values = (professional_id, title, description, created_at,)
                    row_count = update_query(query,values)
                    process_msg = "Your profile has been updated successfully!"
                if row_count > 0:
                    query = "select id, title, description from professional_additional_info where professional_id = %s"
                    values = (professional_id,)
                    rslt = execute_query(query, values)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Professional additional info updated successfully'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Additional Info Updation", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Additional Info Updation",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Additional Info Updation, {str(e)}")
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,process_msg,0,rslt)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "Unable to update professional additional info."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Additional Info Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Additional Info Updation Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Additional Info Updation Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Unable to update professional additional info.'}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Additional Info Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Additional Info Updation Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Additional Info Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def delete_professional_additional_info():
    result_json = {}
    try:        
        req_data = request.get_json()                        
        if 'info_id' not in req_data:
            result_json = api_json_response_format(False,"info_id required",204,{})  
            return result_json
        info_id = req_data['info_id']
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                                        
                if isUserExist("professional_additional_info","id",info_id):                
                    query = 'delete from professional_additional_info where id = %s'
                    values = (info_id,)
                    row_count = update_query(query, values)                    
                    if row_count > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Professional additional info deleted successfully'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Additional Info", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Additional Info",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Additional Info, {str(e)}")
                        result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})                    
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': 'Unable to delete professional additional info.'}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Deleting Professional Additional Info Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Deleting Professional Additional Info Error",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Deleting Professional Additional Info Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry! We had an issue with updating your profile. We request you to retry.",500,{})    
                else:             
                    result_json = api_json_response_format(False,"Additional info data not found",204,{}) 
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def update_expert_notes():
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]
                query = 'update professional_profile set is_expert_notes = %s where professional_id = %s'
                value = ('Y', professional_id,)
                row_count = update_query(query, value)
                if row_count > 0:
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                else:
                    result_json = api_json_response_format(False, "Sorry! We had an issue with updating your profile. We request you to retry.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
        
def professional_job_questions():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        job_id = 1                                                     
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                query = 'select custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                values = (job_id,)
                result = execute_query(query, values)
                result_json = api_json_response_format(True,"Details fetched successfully!",0,result)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json    
    
def professional_intro_video_upload():
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]
                if 'intro_video' in request.files:
                        intro_video = request.files['intro_video']
                        intro_video_filename = intro_video.filename
                        if intro_video_filename != '':                                                                                     
                            res_data = {}
                            try:                                
                                s3_video_key = s3_intro_video_folder_name+intro_video_filename 
                                s3_pro = s3_obj.get_s3_client()
                                s3_pro.upload_fileobj(intro_video, S3_BUCKET_NAME, s3_video_key)                                                                                                               
                                res_data = {"video_name" : s3_video_key}
                            except Exception as error:
                                try:
                                    temp_dict = {'Exception' : str(error),
                                                'Message': "Error in uploading professional intro video."}
                                    event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Uploading Error", temp_dict)
                                    background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Uploading Error",event_properties, temp_dict.get("Message"), user_data)
                                except Exception as e:  
                                    print(f"Error in mixpanel event logging: Intro Video Uploading Error, {str(e)}")
                                print("upload intro video error")
                                print(error)
                                result_json = api_json_response_format(False,"could not upload intro video ",500,{})  

                            if isUserExist("professional_profile","professional_id",professional_id):  
                                query = 'update professional_profile set video_url = %s where professional_id = %s'
                                values = (intro_video_filename, professional_id,)
                                row_count = update_query(query, values)

                            else:
                                query = 'insert into professional_profile(video_url,professional_id) values(%s,%s)'
                                values = (intro_video_filename, professional_id,)
                                row_count = update_query(query, values)
                            if(row_count > 0):
                                try:
                                    temp_dict = {'Country' : user_data['country'],
                                                'City' : user_data['city'],
                                                'Message': f"User {user_data['email_id']}'s video has been uploaded successfully."}
                                    event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Uploading", temp_dict)
                                    background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Uploading",event_properties, temp_dict.get("Message"),user_data)
                                except Exception as e:  
                                    print(f"Error in mixpanel event logging: Intro Video Uploading, {str(e)}")
                                result_json = api_json_response_format(True, "Your video has been uploaded successfully",0,res_data)
                            else:
                                try:
                                    temp_dict = {'Country' : user_data['country'],
                                                'City' : user_data['city'],
                                                'Message': f"An error occurred while user {user_data['email_id']} uploading intro video."}
                                    event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Uploading Error", temp_dict)
                                    background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Uploading Error",event_properties, temp_dict.get("Message"), user_data)
                                except Exception as e:  
                                    print(f"Error in mixpanel event logging: Intro Video Uploading Error, {str(e)}")
                                result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.",500,{})                            
                        else:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'Message': f"{user_data['email_id']} facing an error while uploading intro video. File name not found"}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Uploading Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Uploading Error",event_properties, temp_dict.get("Message"), user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Intro Video Uploading Error, {str(e)}")
                            result_json = api_json_response_format(False,"File name not found",401,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"{user_data['email_id']} facing an error while uploading intro video. File not found"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Uploading Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Uploading Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Intro Video Uploading Error, {str(e)}")
                    result_json = api_json_response_format(False,"File not found! Please upload your video.",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "An error occurred while user uploading intro video."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Uploading Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Uploading Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Intro Video Uploading Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,"Error in professional_intro_video_upload() "+str(error),500,{})        
    finally:        
        return result_json
    
def delete_profile_intro_video():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
                user_data = get_user_data(token_result["email_id"])                                                    
                professional_id = user_data["user_id"]                                                
                if isUserExist("professional_profile","professional_id",professional_id):                                        
                    query = "select video_url from professional_profile where professional_id = %s;"
                    values = (professional_id,)
                    rs = execute_query(query,values)                            
                    if len(rs) > 0:                                            
                        video_name = replace_empty_values1(rs[0]["video_url"])  
                        query = 'update professional_profile set video_url = %s where professional_id = %s'
                        values = ("", professional_id,)
                        row_count = update_query(query, values)  
                        if not video_name == "":
                            try:
                                s3_pro = s3_obj.get_s3_client()                            
                                s3_video_file_key = s3_intro_video_folder_name+video_name                         
                                s3_pro.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_video_file_key)                                
                            except Exception as error:
                                try:
                                    temp_dict = {'Exception' : str(error),
                                                'Message': f"Exception in deleting {user_data['email_id']}'s intro video. s3 Bucket error"}
                                    event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Deletion Error", temp_dict)
                                    background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Deletion Error",event_properties, temp_dict.get("Message"), user_data)
                                except Exception as e:  
                                    print(f"Error in mixpanel event logging: Intro Video Deletion Error, {str(e)}")
                                result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{}) 
                                print("S3 bucket delete_profile_intro_video delete error ")
                                print(error)
                        if row_count > 0:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'Message': f"User {user_data['email_id']}'s intro video deleted successfully."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Deletion", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Deletion",event_properties, temp_dict.get("Message"),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Intro Video Deletion, {str(e)}")
                            result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})                    
                        else:
                            try:
                                temp_dict = {'Exception' : str(error),
                                            'Message': f"Error in deleting {user_data['email_id']}'s intro video."}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Deletion Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Deletion Error",event_properties, temp_dict.get("Message"),user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Intro Video Deletion Error, {str(e)}")
                            result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})  
                    else:
                        try:
                            temp_dict = {'Exception' : str(error),
                                        'Message': f"Error in deleting {user_data['email_id']}'s intro video."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Deletion Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Deletion Error",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Intro Video Deletion Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
                else:
                    result_json = api_json_response_format(False,"Unauthorized user",401,{})          
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in deleting {user_data['email_id']}'s intro video."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Intro Video Deletion Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Intro Video Deletion Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Intro Video Deletion Error, {str(e)}")
        print("Exception in S3 bucket delete_profile_intro_video()  ",error)       
        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})   
    finally:        
        return result_json

def check_file_in_s3(bucket_name, key, user_role):
    # s3_client = boto3.client('s3')
    s3_client = s3_obj.get_s3_client()
    try:
        if user_role == 'employer':
            s3_client.get_object(Bucket = bucket_name, Key = s3_employeer_logo_folder_name + key)
        elif user_role == 'partner':
            s3_client.get_object(Bucket = bucket_name, Key = s3_partner_picture_folder_name + key)
        else:
            s3_client.get_object(Bucket = bucket_name, Key = s3_picture_folder_name + key)
        print("File found.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("File not found.")
            return False
        else:
            raise e

def upload_user_profile_pic():
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])
            if user_data['is_exist']:
                user_id = user_data['user_id']
            else:
                user_data = get_sub_user_data(token_result["email_id"])
                user_id = user_data['user_id']
            if user_data["user_role"] == "professional" or user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter" or user_data["user_role"] == "partner":
                user_id = user_data["user_id"]
                if 'profile_pic' in request.files:
                        profile_pic = request.files['profile_pic']
                        profile_pic_filename = profile_pic.filename
                        if profile_pic_filename != '':                                                                                     
                            res_data = {}
                            pic_present = check_file_in_s3(S3_BUCKET_NAME, profile_pic_filename, user_data["user_role"])
                            if pic_present == True:
                                num = str(random.randint(10000, 99999))
                                txt = profile_pic_filename.split('.')
                                profile_pic_filename = txt[0] + '_' + num + '.' + txt[len(txt)-1]
                            try:
                                if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(profile_pic, S3_BUCKET_NAME, s3_employer_picture_folder_name+profile_pic_filename)                        
                                    s3_pic_file_key = s3_employer_picture_folder_name+profile_pic_filename  
                                elif user_data["user_role"] == "partner":
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(profile_pic, S3_BUCKET_NAME, s3_partner_picture_folder_name+profile_pic_filename)
                                    s3_pic_file_key = s3_partner_picture_folder_name+profile_pic_filename
                                else:
                                    s3_pro = s3_obj.get_s3_client()                            
                                    s3_pro.upload_fileobj(profile_pic, S3_BUCKET_NAME, s3_picture_folder_name+profile_pic_filename)
                                    s3_pic_file_key = s3_picture_folder_name+profile_pic_filename          
                                                    
                                # pic_url = s3_pro.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_file_key}, ExpiresIn=2, HttpMethod='GET')
                                res_data = {"picture_name" : s3_pic_file_key}
                            except Exception as error:
                                try:
                                    temp_dict = {'Exception' : str(error),
                                                'Message': f"Error in uploading user {user_data['email_id']}'s profile picture."}
                                    event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Updation Error", temp_dict)
                                    background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Updation Error",event_properties, temp_dict.get("Message"), user_data)
                                except Exception as e:  
                                    print(f"Error in mixpanel event logging: Profile Picture Updation Error, {str(e)}")
                                print("upload profile picture error ",error)                                    
                                result_json = api_json_response_format(False,"could not upload profile picture ",500,{})  
                            query = 'update users set profile_image = %s where user_id = %s'
                            values = (profile_pic_filename, user_id,)
                            row_count = update_query(query, values)
                            if user_data['user_role'] == "employer" or user_data['user_role'] == "employer_sub_admin" or user_data['user_role'] == "recruiter":
                                query = 'update sub_users set profile_image = %s where user_id = %s'
                                values = (profile_pic_filename, user_id,)
                                update_query(query, values)
                            if(row_count > 0):
                                try:
                                    temp_dict = {'Country' : user_data['country'],
                                                'City' : user_data['city'],
                                                'Message': f"User {user_data['email_id']}'s profile picture updated successfully."}
                                    event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Updation", temp_dict)
                                    background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Updation",event_properties, temp_dict.get("Message"), user_data)
                                except Exception as e:  
                                    print(f"Error in mixpanel event logging: Profile Picture Updation, {str(e)}")
                                result_json = api_json_response_format(True, "Profile picture uploaded successfully",0,res_data)
                            else:
                                try:
                                    temp_dict = {'Country' : user_data['country'],
                                                'City' : user_data['city'],
                                                'Message': f"{user_data['email_id']} facing an error while uploading profile picture."}
                                    event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Updation Error", temp_dict)
                                    background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Updation Error",event_properties, temp_dict.get("Message"), user_data)
                                except Exception as e:  
                                    print(f"Error in mixpanel event logging: Profile Picture Updation Error, {str(e)}")
                                result_json = api_json_response_format(False, "Sorry, we encountered an issue. Please try again.",500,{})                            
                        else:
                            try:
                                temp_dict = {'Country' : user_data['country'],
                                            'City' : user_data['city'],
                                            'Message': f"{user_data['email_id']} facing an error while uploading profile picture. File name not found"}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Updation Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Updation Error",event_properties, temp_dict.get("Message"), user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Profile Picture Updation Error, {str(e)}")
                            result_json = api_json_response_format(False,"File name not found",401,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"{user_data['email_id']} facing an error while uploading profile picture. File not found"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Updation Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Updation Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Profile Picture Updation Error, {str(e)}")
                    result_json = api_json_response_format(False,"File not found. Please try again.",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in uploading profile picture."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Updation Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Updation Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Profile Picture Updation Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,"Error in upload_user_profile_pic() "+str(error),500,{})        
    finally:        
        return result_json
        
def delete_user_profile_pic():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
                user_data = get_user_data(token_result["email_id"])                                                    
                user_id = user_data["user_id"]                                
                profile_pic_name = user_data["profile_image"]
                # if not profile_pic_name == "default_profile_picture.png":
                default_profile_pic_name = ''
                try:
                    if user_data["user_role"] == "employer":
                        if not profile_pic_name == "default_profile_picture_employer.png":
                            s3_pro = s3_obj.get_s3_client()                            
                            s3_pic_file_key = s3_employer_picture_folder_name+profile_pic_name                         
                            s3_pro.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_pic_file_key)
                            default_profile_pic_name = 'default_profile_picture_employer.png'
                    if user_data["user_role"] == "partner":
                        if not profile_pic_name == "default_profile_picture_partner.png":
                            s3_pro = s3_obj.get_s3_client()                            
                            s3_pic_file_key = s3_partner_picture_folder_name+profile_pic_name                         
                            s3_pro.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_pic_file_key)   
                            default_profile_pic_name = 'default_profile_picture_partner.png' 
                    else:
                        if not profile_pic_name == "default_profile_picture.png":                                
                            s3_pro = s3_obj.get_s3_client()                            
                            s3_pic_file_key = s3_picture_folder_name+profile_pic_name                         
                            s3_pro.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_pic_file_key)
                            default_profile_pic_name = 'default_profile_picture.png'
                    
                except Exception as error:
                    try:
                        temp_dict = {'Exception' : str(error),
                                    'Message': f"Exception in deleting {user_data['email_id']}'s profile picture. s3 Bucket error"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Deletion Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Deletion Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Profile Picture Deletion Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{}) 
                    print("S3 bucket profile picture delete error ")
                    print(error)

                query = 'update users set profile_image = %s where user_id = %s'
                values = (default_profile_pic_name, user_id,)
                row_count = update_query(query, values)                    
                if row_count > 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"User {user_data['email_id']}'s profile picture deleted successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Deletion", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Deletion",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Profile Picture Deletion, {str(e)}")
                    result_json = api_json_response_format(True,"Profile picture deleted successfully",0,{})                    
                else:
                    try:
                        temp_dict = {'Exception' : str(error),
                                    'Message': f"Error in deleting {user_data['email_id']}'s profile picture."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Deletion Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Deletion Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Profile Picture Deletion Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please tyr again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in deleting {user_data['email_id']}'s profile picture."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Profile Picture Deletion Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Profile Picture Deletion Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Profile Picture Deletion Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})   
    finally:        
        return result_json
    
def delete_user_resume():
    result_json = {}
    try:        
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
                user_data = get_user_data(token_result["email_id"])                                                    
                professional_id = user_data["user_id"]
                query = "select professional_resume from professional_profile where professional_id = %s"
                values = (professional_id,)
                resume_name = execute_query(query, values)
                if not resume_name[0]['professional_resume'] == "":
                    if not resume_name[0]['professional_resume'] == None:
                        try:
                            if user_data["user_role"] == "professional":
                                s3_pro = s3_obj.get_s3_client()                            
                                s3_resume_file_key = s3_resume_folder_name + resume_name[0]['professional_resume']                         
                                s3_pro.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_resume_file_key)      
                            else:                                   
                                result_json = api_json_response_format(False,"Unauthorized user",500,{})                        
                        except Exception as error:
                            try:
                                temp_dict = {'Exception' : str(error),
                                            'Message': f"Exception in deleting {user_data['email_id']}'s resume. s3 Bucket error"}
                                event_properties = background_runner.process_dict(user_data["email_id"], "Resume Deletion Error", temp_dict)
                                background_runner.mixpanel_event_async(user_data['email_id'],"Resume Deletion Error",event_properties, temp_dict.get("Message"), user_data)
                            except Exception as e:  
                                print(f"Error in mixpanel event logging: Resume Deletion Error, {str(e)}")
                            result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{}) 
                            print("S3 bucket resume delete error ")
                            print(error)

                query = 'update professional_profile set professional_resume = %s where professional_id = %s'
                values = ("", professional_id,)
                row_count = update_query(query, values)                    
                if row_count > 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"User {user_data['email_id']}'s resume deleted successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Resume Deletion", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Resume Deletion",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Resume Deletion, {str(e)}")
                    result_json = api_json_response_format(True,"Resume deleted successfully",0,{})                    
                else:
                    try:
                        temp_dict = {'Exception' : str(error),
                                    'Message': f"Error in deleting {user_data['email_id']}'s resume."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Resume Deletion Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Resume Deletion Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Resume Deletion Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})    
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please tyr again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"Error in deleting {user_data['email_id']}'s resume."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Resume Deletion Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Resume Deletion Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Resume Deletion Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})   
    finally:        
        return result_json
    
    
def professional_onClick_apply_job():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        job_id = 1                                  ######    
        # key_id = 1                            
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                query = 'select id, custom_pre_screen_ques from pre_screen_ques where job_id = %s'
                values = (job_id,)
                result = execute_query(query, values)
                result_dict = {"questions" : result}
                final_list = []
                final_list.append(result_dict)

                query = 'select required_resume, required_cover_letter from job_post where id = %s'
                values = (job_id,)
                doc_status = execute_query(query, values)
                doc_status_dict = {"docs_status" : doc_status}
                final_list.append(doc_status_dict)

                result_json = api_json_response_format(True,"Questions fetched successfully",0,final_list)
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
def store_signup_details():
    try:           
            # req_data = request.get_json()
            req_data = request.form
            if 'first_name' not in req_data:
                result_json = api_json_response_format(False,"Please enter a valid first name.",204,{})  
                return result_json
            if 'email_id' not in req_data:
                result_json = api_json_response_format(False,"Please enter a valid email ID.",204,{})  
                return result_json
            if 'last_name' not in req_data:
                result_json = api_json_response_format(False,"Please enter a valid last name.",204,{})  
                return result_json
            if 'contact_number' not in req_data:
                result_json = api_json_response_format(False,"Please enter your contact number.",204,{})  
                return result_json
            if 'country_code' not in req_data:
                result_json = api_json_response_format(False,"Please enter your location.",204,{})  
                return result_json           
            if 'country' not in req_data:
                result_json = api_json_response_format(False,"Please enter your location.",204,{})  
                return result_json
            if 'city' not in req_data:
                result_json = api_json_response_format(False,"Please enter your location.",204,{})  
                return result_json
            if 'is_age_verified' not in req_data:
                result_json = api_json_response_format(False,"Please check this box to proceed.",204,{})  
                return result_json 
            # is_age_verified = req_data['is_age_verified']
            # first_name = req_data["first_name"]
            # last_name = req_data["last_name"]
            # email_id = req_data["email_id"]
            # contact_number = req_data["contact_number"]
            # country_code = req_data["country_code"]
            # country = req_data["country"]
            # city = req_data["city"]
            if 'years_of_experience' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'gender' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'functional_specification' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'sector' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'industry_sector' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'job_type' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'location_preference' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'mode_of_communication' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json
            if 'willing_to_relocate' not in req_data:
                result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                return result_json

            is_age_verified = req_data.get('is_age_verified')
            first_name = req_data.get('first_name')
            last_name = req_data.get('last_name')
            email_id = req_data.get('email_id')
            contact_number = req_data.get('contact_number')
            country_code = req_data.get('country_code')
            country = req_data.get('country')
            city = req_data.get('city')
            file = request.files['file']
            years_of_experience = req_data.get('years_of_experience')
            gender = req_data.get('gender')
            functional_specification = req_data.get('functional_specification')
            sector = req_data.get('sector')
            industry_sector = req_data.get('industry_sector')
            job_type = req_data.get('job_type')
            location_preference = req_data.get('location_preference')
            mode_of_communication = req_data.get('mode_of_communication')
            willing_to_relocate = req_data.get('willing_to_relocate')
            login_mode = req_data.get('login_mode')
            # contact_number = None
            # if 'contact_number' in req_data:            
            #     contact_number = req_data["contact_number"]
            # country_code = None
            # if 'country_code' in req_data:
            #     country_code = req_data["country_code"]
            
            
            # user_data = get_user_data(email_id)  
            
            # if user_data["is_exist"]:
            if not is_age_verified == "Y":
                result_json = api_json_response_format(False,"Please fill in all the required fields.",409,{})  
                return result_json
            created_at = datetime.now()
            # query = 'update users set first_name = %s, last_name = %s, country_code = %s, contact_number = %s, country = %s, city = %s, pricing_category = %s, payment_status = %s, gender = %s where email_id = %s'
            query = 'insert into users (user_role_fk, first_name, last_name, gender, email_id, contact_number, country_code, city, country, pricing_category, payment_status, login_mode, login_status, login_count, email_active, profile_image, is_active, created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            values = (3, first_name, last_name, gender, email_id, contact_number, country_code, city, country, 'Basic', 'trialing', login_mode, 'IN', 1, 'Y', 'default_profile_picture.png', 'Y', created_at,)
            row_count = update_query(query,values)
            if row_count > 0:
                user_data = get_user_data(email_id)    
                user_id = user_data["user_id"]
                query = "insert into professional_profile(professional_id, created_at) values (%s,%s)"
                values = (user_id, created_at,) 
                update_query(query, values)
                s3_pro = s3_obj.get_s3_client()   
                s3_pro.upload_fileobj(file, S3_BUCKET_NAME, s3_resume_folder_name+file.filename)
                current_date = datetime.today()
                file_name = file.filename
                formatted_date = current_date.strftime("%Y/%m/%d")
                update_resume_query = 'update professional_profile set professional_resume = %s, upload_date = %s, years_of_experience = %s, functional_specification = %s, sector = %s, industry_sector = %s, job_type = %s, location_preference = %s, mode_of_communication = %s,willing_to_relocate = %s where professional_id =%s'
                update_resume_values = (file_name, formatted_date, years_of_experience,functional_specification,sector,industry_sector,job_type,location_preference,mode_of_communication,willing_to_relocate, user_data["user_id"],)
                row_count = update_query(update_resume_query, update_resume_values)
                if row_count > 0:
                    token_result = get_jwt_access_token(user_data["user_id"],email_id,) 
                    access_token =  token_result["access_token"]   
                    res_data = {"access_token" : access_token,"user_role":user_data["user_role"]}
                    background_runner.get_professional_details(user_data['user_id'])
                    result_json = api_json_response_format(True,"User account created successfully",0,res_data)
                else:
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
            # else:
            #     result_json = api_json_response_format(False,"Unauthorized user",401,{})
    except Exception as error:
        print(error)
        result_json = api_json_response_format(False,str(error),500,{})
    finally:        
        return result_json

def unsave_job():
    try:
        result_json = {}
        token_result = get_user_token(request)   
        job_id = 0                            
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                req_data = request.get_json()                        
                if 'job_id' not in req_data:
                    result_json = api_json_response_format(False,"job_id required",204,{})  
                    return result_json
                professional_id = user_data['user_id']
                job_id = req_data['job_id']
                query = 'select job_title from job_post where id = %s'
                values = (job_id,)
                job_title = execute_query(query, values)
                query = 'select count(id) from saved_job where job_id = %s and professional_id = %s'
                values = (job_id, professional_id,)
                rslt = execute_query(query, values)
                if rslt[0]['count(id)'] > 0: 
                    query = 'delete from saved_job where job_id = %s and professional_id = %s'
                    values = (job_id, professional_id,)
                    result = update_query(query, values)
                    if result > 0:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Job Title' : job_title[0]['job_title'],
                                        'Message': f"Job '{job_title[0]['job_title']}' has been unsaved successfully by {user_data['email_id']}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Unsave", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Unsave",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Job Unsave, {str(e)}")
                        result_json = api_json_response_format(True,"Job unsaved",0,{})
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Job Title' : job_title[0]['job_title'],
                                        'Message': f"An error occurred while unsaving the job {job_title[0]['job_title']} for {user_data['email_id']}."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Unsave Error", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Unsave Error",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Job Unsave Error, {str(e)}")
                        result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",0,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Job Title' : job_title[0]['job_title'],
                                    'Message': f"Job '{job_title[0]['job_title']}' was not saved by {user_data['email_id']}."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Unsave", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Unsave",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Job Unsave, {str(e)}")
                    result_json = api_json_response_format(False,"Job not saved",204,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid token. Please try again",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': f"An error occurred while unsaving the job."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Unsave Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Unsave Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Job Unsave Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def update_partner_view_count():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]
                req_data = request.get_json()
                if 'partner_id' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                partner_id =  req_data["partner_id"]                          
                query = 'select * from  view_learning_count where partner_id = %s and professional_id = %s'
                values = (partner_id,professional_id,)
                count = execute_query(query,values)
                if not count:
                    current_time = datetime.now()
                    query = "INSERT INTO view_learning_count (partner_id, professional_id,viewed_at) values (%s,%s,%s)"                 
                    values = (partner_id, professional_id, current_time,)
                    update_query(query,values) 
                    result_json = api_json_response_format(True,"success!",0,{})
                else:
                    result_json = api_json_response_format(True,"success!",0,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def update_expert_notes_status():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]
                req_data = request.get_json()
                if 'view_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                view_status =  req_data["view_status"]                          
                query = 'update professional_profile set show_to_employer = %s where professional_id = %s'
                values = (view_status, professional_id,)
                count = update_query(query,values)
                if count > 0: 
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Expert Notes View Status' : view_status,
                                    'Message': f"User {user_data['email_id']} updated the expert notes view status."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Expert Notes View Status", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Expert Notes View Status",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Expert Notes View Status, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Error in updating expert notes view status.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Expert Notes View Status Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Expert Notes View Status Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Expert Notes View Status Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in updating expert notes view status."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Expert Notes View Status Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Expert Notes View Status Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Expert Notes View Status Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_other_employer_skills():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:  
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional" or user_data["user_role"] == "admin":
                query = 'SELECT skill_name AS name FROM filter_skills WHERE is_active = %s '
                values = ('N',)
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
    
def get_other_employer_specialisation():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:  
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional" or user_data["user_role"] == "admin":
                query = 'SELECT specialisation_name AS name FROM filter_specialisation WHERE is_active = %s '
                values = ('N',)
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
    
def check_user_resume():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:  
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]
                query = 'SELECT professional_resume FROM professional_profile WHERE professional_id = %s '
                values = (professional_id,)
                result = execute_query(query, values)
                if result[0]['professional_resume'] == None or result[0]['professional_resume'] == '' or result[0]['professional_resume'] == 'NULL':
                    s3_resume_name = ''
                else:
                    s3_resume_name = s3_resume_folder_name + result[0]['professional_resume']
                result[0].update({'professional_resume' : s3_resume_name})
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

# def professional_updated_home():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]
                profile_percentage = show_percentage(professional_id)
                notification_msg = "Please take a few moments to complete your profile to increase your chances of receiving better recommendations."
                query = 'select login_count from users where user_id = %s'
                values = (professional_id,)
                rslt = execute_query(query, values)
                login_count = rslt[0]['login_count']
                if login_count <=3 and profile_percentage > 50 and profile_percentage < 80:
                    query = "SELECT notification_count FROM users WHERE user_id = %s"
                    value = (professional_id,)
                    updt_at = execute_query(query, value)
                    if updt_at[0]["notification_count"] < login_count and profile_percentage > 50 and profile_percentage < 80:
                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                        created_at = datetime.now()                    
                        values = (professional_id, notification_msg, created_at)
                        update_query(query,values)
                        query = 'update users set notification_count = %s where user_id = %s'
                        values = (int(updt_at[0]["notification_count"]) + 1, professional_id,)
                        update_query(query, values)
                if login_count <=5 and profile_percentage < 50 :
                        query = "SELECT notification_count FROM users WHERE user_id = %s"
                        value = (professional_id,)
                        updt_at = execute_query(query, value)
                        if updt_at[0]["notification_count"] < login_count and profile_percentage < 50:
                            query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                            created_at = datetime.now()                    
                            values = (professional_id, notification_msg, created_at,)
                            update_query(query,values)
                            query = 'update users set notification_count = %s where user_id = %s'
                            values = (int(updt_at[0]["notification_count"]) + 1, professional_id,)
                            update_query(query, values)

                query = 'SELECT job_id FROM `home_page_jobs` WHERE professional_id = %s AND DATEDIFF(CURDATE(), created_at) < 7;'
                values = (professional_id,)
                valid_job_id = execute_query(query,values)

                if len(valid_job_id) > 0:
                    final_job_id = valid_job_id[0]['job_id']
                else:
                    query = 'SELECT jp.id FROM `job_post` jp LEFT JOIN `job_activity` ja ON jp.id = ja.job_id AND ja.professional_id = %s WHERE ja.job_id IS NULL AND jp.job_status = %s ORDER BY jp.updated_at DESC'
                    values = (professional_id, 'opened',)
                    job_id = execute_query(query,values)
                    home_job_id_list = [item['id'] for item in job_id]
                    query = 'SELECT job_id FROM `home_page_jobs` WHERE professional_id = %s'
                    values = (professional_id,)
                    shown_job_id = execute_query(query,values)
                    shown_id_list = [item['job_id'] for item in shown_job_id]
                    id_list =  [item for item in home_job_id_list if item not in shown_id_list]
                    if len(id_list) > 0:
                        random_val = random.choice(id_list)
                        query = 'insert into home_page_jobs (professional_id, job_id, applied_status, created_at) values (%s,%s,%s,%s)'
                        created_at = datetime.now() 
                        values = (professional_id, random_val, 'N', created_at,)
                        store_home_page_job = update_query(query, values)
                        final_job_id = random_val
                    else:
                        final_job_id = 0
                query = 'select count(id) as id from job_activity where job_id = %s and professional_id = %s'
                values = (final_job_id, professional_id,)
                applied_status = execute_query(query, values)
                if len(applied_status) > 0:
                    if applied_status[0]['id'] > 0:
                        job_applied_status = 'applied'
                    else:
                        job_applied_status = 'not applied'
                else:
                    job_applied_status = ''
                job_detail_list = []
                if final_job_id != 0:
                            job_details_query = """SELECT DISTINCT
                                                jp.id,
                                                jp.job_title,
                                                jp.job_type,
                                                jp.job_overview,
                                                jp.job_desc,
                                                jp.responsibilities,
                                                jp.additional_info,
                                                jp.skills,
                                                jp.country,
                                                jp.state,
                                                jp.city,
                                                jp.work_schedule,
                                                jp.workplace_type,
                                                jp.is_paid,
                                                jp.time_commitment,
                                                jp.timezone,
                                                jp.duration,
                                                jp.calendly_link,
                                                jp.currency,
                                                jp.benefits,
                                                jp.required_resume,
                                                jp.required_cover_letter,
                                                jp.required_background_check,
                                                jp.required_subcontract,
                                                jp.is_application_deadline,
                                                jp.application_deadline_date,
                                                jp.is_active,
                                                jp.share_url,
                                                jp.specialisation,
                                                jp.created_at,
                                                COALESCE(u.profile_image, su.profile_image) AS profile_image, 
                                                COALESCE(u.pricing_category, su.pricing_category) AS pricing_category, 
                                                COALESCE(u.payment_status, su.payment_status) AS payment_status,
                                                COALESCE(ep.employer_id, su.sub_user_id) AS employer_id, 
                                                COALESCE(ep.sector, su.sector) AS sector, 
                                                COALESCE(ep.company_description, su.company_description) AS company_description, 
                                                COALESCE(ep.company_name, su.company_name) AS company_name, 
                                                COALESCE(ep.employer_type, su.employer_type) AS employer_type
                                                FROM
                                                    job_post jp
                                                LEFT JOIN
                                                    users u ON jp.employer_id = u.user_id
                                                LEFT JOIN
                                                    employer_profile ep ON jp.employer_id = ep.employer_id
                                                LEFT JOIN 
                                                    sub_users su ON jp.employer_id = su.sub_user_id
                                                WHERE
                                                    jp.job_status = %s AND jp.id = %s
                                                    
                                            """
                            values = ('opened', final_job_id,)
                            # AND NOT EXISTS (
                            #                             SELECT 1
                            #                             FROM job_activity ja
                            #                             WHERE ja.job_id = jp.id
                            #                             AND ja.professional_id = %s
                            #                         )
                            job_details = execute_query(job_details_query, values)
                            if len(job_details) > 0:
                                txt = job_details[0]['sector']
                                txt = txt.replace(", ", "_")
                                txt = txt.replace(" ", "_")
                                sector_name = txt + ".png"
                                job_details[0].update({'sector_image' : s3_sector_image_folder_name + sector_name})
                                img_key = s3_employer_picture_folder_name + job_details[0]['profile_image']
                                job_details[0].update({'profile_image' : img_key})
                                job_details[0].update({'job_applied_status' : job_applied_status})
                                job_detail_list.append(job_details[0])
                            else:
                                job_detail_list = []
                else:
                    job_detail_list = []
                
                learning_list = []
                query = 'SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id;'
                values = ()
                post_id = execute_query(query, values)

                if len(post_id) > 0:
                    for p in post_id:
                        query = 'select post_status from learning where id = %s'
                        values = (p['partner_id'],)
                        rs = execute_query(query, values)
                        if len(rs) > 0:
                            if rs[0]['post_status'] == 'opened':
                                query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.created_at, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN ( SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id ) vc ON l.id = vc.partner_id WHERE l.id = %s;' 
                                values = (p['partner_id'],)
                                post_details = execute_query(query, values)
                                if len(post_details) > 0:
                                    s3_cover_pic_key = s3_partner_cover_pic_folder_name+post_details[0]['image']
                                    s3_attached_file_key = s3_partner_learning_folder_name+post_details[0]['attached_file']
                                    s3_pic_key = s3_partner_picture_folder_name+str(post_details[0]['profile_image'])
                                    post_details[0].update({'image' : s3_cover_pic_key})
                                    post_details[0].update({'attached_file' : s3_attached_file_key})
                                    post_details[0].update({'profile_image' : s3_pic_key})
                                    learning_list.append(post_details[0])
                                    break
                else:
                    query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.created_at, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN ( SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id ) vc ON l.id = vc.partner_id where l.post_status=%s ORDER by l.id DESC LIMIT 1;'
                    values = ('opened',)
                    post_details = execute_query(query, values)
                    if len(post_details) > 0:
                                    s3_cover_pic_key = s3_partner_cover_pic_folder_name + post_details[0]['image']
                                    s3_attached_file_key = s3_partner_learning_folder_name + post_details[0]['attached_file']
                                    s3_pic_key = s3_partner_picture_folder_name + str(post_details[0]['profile_image'])
                                    post_details[0].update({'image' : s3_cover_pic_key})
                                    post_details[0].update({'attached_file' : s3_attached_file_key})
                                    post_details[0].update({'profile_image' : s3_pic_key})
                                    learning_list.append(post_details[0])
                    else:
                        learning_list = []

                # community_list = []
                # community_id_query = 'select id from community order by updated_at DESC LIMIT %s'
                # values = (1,)
                # community_ids = execute_query(community_id_query, values)
                # if len(community_ids) > 0:
                #     for id in community_ids:
                #         community_details = 'select id,title, short_description, image, join_url, share_url, type_of_community from community where is_active = %s and id = %s'
                #         values = ('Y', id['id'],)
                #         community_data = execute_query(community_details,values)
                #         if len(community_data) > 0:
                #             s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + community_data[0]['image']                    
                #             community_data[0].update({"image": s3_sc_community_cover_pic_key})
                #             community_list.append(community_data[0])
                # else:
                #     community_list = []
                community_list = []
                community_details = 'select id,title, short_description, image, join_url, share_url, type_of_community from community where is_active = %s and id = %s'
                values = ('Y', 1,)
                community_data = execute_query(community_details,values)
                if len(community_data) > 0:
                    s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + community_data[0]['image']                    
                    community_data[0].update({"image": s3_sc_community_cover_pic_key})
                    community_list.append(community_data[0])
                query = 'select * from community where type = %s'
                values = ('Article 1',)
                article_details = execute_query(query, values)
                article_image = ''
                if len(article_details) > 0:
                    article_image = s3_sc_community_cover_pic_folder_name + article_details[0]['image']
                    article_details[0].update({'image' : article_image})

                s3_home_video_url = 'home_2nd_careers.mp4'

                query = 'select count(id) as job_count from job_post where job_status = %s'
                values = ('opened',)
                job_count = execute_query(query, values)
                if len(job_count) > 0:
                    job_count = job_count[0]['job_count']
                else:
                    print("Error in job count query execution.")
                    job_count = 0
                result_list = [{"job_list" : job_detail_list,"learning_posts" : learning_list,
                                "community_posts" : community_list,"article_posts" : article_details,
                                "video_url" : s3_home_video_url, "job_count" : job_count}]
                                                          
                query = 'select welcome_count from professional_profile where professional_id = %s'
                values = (professional_id,)
                count = execute_query(query, values)
                if count[0]['welcome_count'] == 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"The home page for user {user_data['email_id']} was displayed successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Home Page",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 200, result_list)
                    query = 'update professional_profile set welcome_count = %s where professional_id = %s'
                    values = (1, professional_id,)
                    temp = update_query(query, values)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"The home page for user {user_data['email_id']} was displayed successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Home Page",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, result_list)
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in professional home page."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Home Page Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Home Page Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Home Page Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def professional_updated_home():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]
                profile_percentage = show_percentage(professional_id)
                notification_msg = "Please take a few moments to complete your profile to increase your chances of receiving better recommendations."
                query = 'select login_count from users where user_id = %s'
                values = (professional_id,)
                rslt = execute_query(query, values)
                login_count = rslt[0]['login_count']
                if login_count <=3 and profile_percentage > 50 and profile_percentage < 80:
                    query = "SELECT notification_count FROM users WHERE user_id = %s"
                    value = (professional_id,)
                    updt_at = execute_query(query, value)
                    if updt_at[0]["notification_count"] < login_count and profile_percentage > 50 and profile_percentage < 80:
                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                        created_at = datetime.now()                    
                        values = (professional_id, notification_msg, created_at)
                        update_query(query,values)
                        query = 'update users set notification_count = %s where user_id = %s'
                        values = (int(updt_at[0]["notification_count"]) + 1, professional_id,)
                        update_query(query, values)
                if login_count <=5 and profile_percentage < 50 :
                        query = "SELECT notification_count FROM users WHERE user_id = %s"
                        value = (professional_id,)
                        updt_at = execute_query(query, value)
                        if updt_at[0]["notification_count"] < login_count and profile_percentage < 50:
                            query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) values (%s,%s,%s)"
                            created_at = datetime.now()                    
                            values = (professional_id, notification_msg, created_at,)
                            update_query(query,values)
                            query = 'update users set notification_count = %s where user_id = %s'
                            values = (int(updt_at[0]["notification_count"]) + 1, professional_id,)
                            update_query(query, values)

                s3_home_video_url = 'home_2nd_careers.mp4'
                applied_jobs = []
                get_user_name_query = 'select first_name, last_name from users where user_id = %s'
                get_user_name_values = (professional_id,)
                get_user_name_result = execute_query(get_user_name_query, get_user_name_values)
                if get_user_name_result:
                    user_name = get_user_name_result[0]['first_name'] + ' ' + get_user_name_result[0]['last_name']
                else:
                    user_name = ''
                ai_recommendation = None
                if profile_percentage < 60:
                    job_details = []

                    ai_recommendation = False
                    get_platform_details_query = "SELECT COUNT(CASE WHEN user_role_fk = 3 AND email_active = 'Y' THEN user_id END) AS total_professionals, COUNT(CASE WHEN user_role_fk = 2 AND email_active = 'Y' THEN user_id END) AS total_employers, COUNT(CASE WHEN post_status = 'opened' THEN id END) AS total_learning_posts, COUNT(CASE WHEN job_status = 'opened' THEN id END) AS total_jobs FROM ( SELECT user_id, user_role_fk, email_active, NULL AS post_status, NULL AS job_status, NULL AS id FROM users UNION ALL SELECT NULL AS user_id, NULL AS user_role_fk, NULL AS email_active, post_status, NULL AS job_status, id FROM learning UNION ALL SELECT NULL AS user_id, NULL AS user_role_fk, NULL AS email_active, NULL AS post_status, job_status, id FROM job_post ) AS combined_data;"
                    platform_details_values = ()
                    platform_details_result = execute_query(get_platform_details_query, platform_details_values)
                    query = "select country from users where user_id = %s"
                    values = (professional_id,)
                    res = execute_query(query, values)
                    country = res[0]['country']
                    # query_job_details = """SELECT count(jp.id) AS total_jobs, jp.country FROM job_post jp 
                    #     LEFT JOIN users u ON jp.employer_id = u.user_id
                    #     LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id 
                    #     WHERE jp.job_status = %s and jp.is_active = %s and jp.country = %s ORDER by jp.created_at DESC;"""
                    # values = ('Opened', 'Y', country)
                    query = """SELECT COUNT(*) AS total_jobs,t.country FROM (SELECT jp.id, jp.country FROM job_post jp LEFT JOIN users u ON jp.employer_id = u.user_id LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                        WHERE jp.job_status = %s AND jp.is_active = %s AND jp.country = %s
                        UNION ALL
                        SELECT ajp.id, ajp.country FROM admin_job_post ajp LEFT JOIN users u2 ON ajp.employer_id = u2.user_id LEFT JOIN sub_users su2 ON ajp.employer_id = su2.sub_user_id
                        WHERE ajp.admin_job_status = %s AND ajp.is_active = %s AND ajp.country = %s) AS t
                    GROUP BY t.country
                    ORDER BY t.country;
                    """
                    values = ('Opened', 'Y', country,'Opened', 'Y', country)
                    country_based_total_jobs_count = execute_query(query, values)
                    if platform_details_result:
                        total_professionals = platform_details_result[0]['total_professionals']
                        total_employers = platform_details_result[0]['total_employers']
                        total_learning_posts = platform_details_result[0]['total_learning_posts']
                        # total_jobs = platform_details_result[0]['total_jobs']
                        total_jobs = country_based_total_jobs_count[0]['total_jobs']
                else:
                    job_details = []
                    ai_recommendation = True
                    total_professionals, total_employers, total_learning_posts, total_jobs = 0, 0, 0, 0
                    get_applied_jobs_query = """SELECT jp.id, jp.job_title, CONCAT(jp.city, ', ', jp.country) AS location, jp.timezone, jp.workplace_type, jp.job_type, ja.application_status, ja.created_at, (SELECT COUNT(*) FROM job_activity WHERE professional_id = %s) AS total_applied_jobs, COALESCE(u.profile_image, su.profile_image) AS profile_image 
                                                FROM job_post jp JOIN job_activity ja ON jp.id = ja.job_id 
                                                LEFT JOIN users u ON jp.employer_id = u.user_id 
                                                LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id WHERE ja.professional_id = %s 
                                                ORDER BY ja.created_at DESC LIMIT %s;"""
                    get_applied_jobs_values = (professional_id, professional_id, 4,)
                    get_applied_jobs_result = execute_query(get_applied_jobs_query, get_applied_jobs_values)
                    # s3_employer_picture_folder_name
                    if get_applied_jobs_result:
                        for j in get_applied_jobs_result:
                            j.update({'profile_image' : s3_employer_picture_folder_name + j['profile_image']})
                        applied_jobs = get_applied_jobs_result
                    
                    unapplied_ai_jobs_query = "SELECT count(ar.job_id) as count FROM ai_recommendation ar JOIN job_post jp ON ar.job_id = jp.id WHERE ar.professional_id = %s AND ar.source = 'AI' AND jp.job_status = 'opened' AND ar.job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s );"
                    values = (professional_id, professional_id,)
                    unapplied_jobs_count = execute_query(unapplied_ai_jobs_query, values)
                    if len(unapplied_jobs_count) > 0 and unapplied_jobs_count[0]['count'] == 0:
                        data = get_profile_search(professional_id)
                        out = process_quries_search(OPENAI_API_KEY,data)
                        print("recommended query :" + str(out))
                        result_json = api_json_response_format(False,out,200,{})
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
                        index_name = JOB_POST_INDEX
                        vector_store = Meilisearch(client=client, embedding=embeddings,embedders=embedders,
                            index_name=index_name)

                        if out["is_error"]:    
                            print(f"recommendation prompt query error : {out['result']}")        
                            return
                        else:
                            query = out["result"]
                        results = vector_store.similarity_search_with_score(
                            query=query,
                            embedder_name = "adra",
                            k=15
                        )
                        job_details1 = []
                        Willing_to_Relocate = data[0]['Willing_to_Relocate'].lower()
                        country = data[0]['Contact_Information']['Country'].lower()
                        city = data[0]['Contact_Information']['Address'].lower()
                        for doc, _ in results:
                            page_content = doc.page_content
                            page_content = json.loads(page_content)
                            if page_content['workplace_type'].lower() in ('hybrid', 'on-site'):
                                if Willing_to_Relocate == 'yes':
                                    if page_content['country'].lower() == country:
                                        job_details1.append(page_content)
                                else:
                                    if page_content['city'].lower() == city:
                                        job_details1.append(page_content)
                            else:
                                job_details1.append(page_content)
                        if len(job_details1) > 0:  
                            ids = list(set(data["id"] for data in job_details1))
                            admin_job_ids = [job_id for job_id in ids if str(job_id).startswith("Ex_")]
                            job_ids = [job_id for job_id in ids if not str(job_id).startswith("Ex_")]

                            ids_list_new = []
                            if job_ids:
                                query = 'select id,job_status from job_post where id IN %s'
                                values = (tuple(job_ids),)
                                id_job_status = execute_query(query, values)
                                if len(id_job_status) > 0:
                                    for i in id_job_status:
                                        if i['job_status'] == 'opened':
                                            ids_list_new.append(i['id'])

                            #admin_job_post
                            admin_ids_list_new = []
                            if admin_job_ids:
                                query = 'select job_reference_id AS id,lower(admin_job_status) AS job_status from admin_job_post where job_reference_id IN %s'
                                values = (tuple(admin_job_ids),)
                                id_admin_job_status = execute_query(query, values)
                                if len(id_admin_job_status) > 0:
                                    for i in id_admin_job_status:
                                        if i['job_status'] == 'opened':
                                            admin_ids_list_new.append(i['id'])

                            id_tuple = tuple(ids_list_new) + tuple(admin_ids_list_new)
                        else:
                            id_tuple = ()

                        query = "select job_id from ai_recommendation where professional_id = %s and job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s);"
                        values = (professional_id, professional_id,)
                        existing_ai_job_ids = execute_query(query, values)
                        
                        if len(existing_ai_job_ids) > 0:
                            existing_ai_job_ids_list = [k['job_id'] for k in existing_ai_job_ids]
                            # new_id_tuple = list(set(existing_ai_job_ids_list + id_tuple))
                            new_id_tuple = tuple(item for item in id_tuple if item not in existing_ai_job_ids_list)
                        else:
                            new_id_tuple = id_tuple
                        if len(new_id_tuple) > 0:
                            values = []
                            created_at = datetime.now()
                            for id in new_id_tuple:
                                values.append((professional_id, id, "AI", 3, created_at,))
                            query = "INSERT INTO ai_recommendation (professional_id, job_id, source, user_role_id, created_at) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE source = VALUES(source), created_at = VALUES(created_at)"
                            update_many(query, values)

                        query = "SELECT ar.job_id, jp.job_status FROM ai_recommendation ar JOIN job_post jp ON ar.job_id = jp.id WHERE ar.professional_id = %s AND ar.source = 'AI' AND jp.job_status = 'opened' AND ar.job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s ) AND ar.job_id NOT IN ( SELECT job_id FROM sc_recommendation WHERE professional_id = %s ) LIMIT 3;"
                        values = (professional_id, professional_id, professional_id,)
                        new_ai_rcmn_ids = execute_query(query, values)
                        new_ai_rcmn_ids_tuple = tuple(n['job_id'] for n in new_ai_rcmn_ids)
                        new_unique_tuple = tuple(set(new_ai_rcmn_ids_tuple))
                        if len(new_unique_tuple) > 0:
                            query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.country, jp.state, jp.city, jp.workplace_type, jp.timezone, jp.required_resume, jp.required_cover_letter, jp.created_at, COALESCE(u.profile_image, su.profile_image) AS profile_image FROM job_post jp 
                                                   LEFT JOIN users u ON jp.employer_id = u.user_id 
                                                   LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id 
                                                   WHERE jp.job_status = %s and jp.is_active = %s and jp.id IN %s ORDER by jp.id DESC LIMIT 3;
                                                """
                            values_job_details = ('Opened', 'Y', new_unique_tuple,)
                            job_details = replace_empty_values(execute_query(query_job_details, values_job_details))

                        if len(job_details)<3:
                            need_ids_count = 3 - len(job_details)

                            query = "SELECT ar.job_id, ap.admin_job_status FROM ai_recommendation ar LEFT JOIN admin_job_post ap ON ar.job_id = ap.job_reference_id WHERE ar.professional_id = %s AND ar.source = 'AI' AND ap.admin_job_status= 'opened' LIMIT 3;"
                            new_ai_rcmn_admin_ids = execute_query(query, (professional_id,))
                            new_ai_rcmn_admin_ids_tuple = tuple(n['job_id'] for n in new_ai_rcmn_admin_ids)

                            if new_ai_rcmn_admin_ids_tuple:
                                query_admin_job_details = """SELECT ap.`job_reference_id` AS id, ap.`job_title`, ap.`job_overview`, ap.`country`, ap.`state`, ap.`city`, ap.`job_type`, 
                                            ap.`workplace_type`, ap.`created_at`, u.profile_image, NULL AS required_cover_letter, NULL AS required_resume,
                                            NULL AS timezone FROM admin_job_post ap LEFT JOIN users u ON ap.employer_id=u.user_id WHERE ap.job_reference_id IN %s AND lower(ap.admin_job_status) = %s ORDER by job_reference_id DESC;"""
                                values_admin_job_details = (new_ai_rcmn_admin_ids_tuple, 'opened')
                                admin_job_details = replace_empty_values(execute_query(query_admin_job_details, values_admin_job_details))
                                
                                if admin_job_details:
                                    admin_job_details = admin_job_details[:need_ids_count]
                                    job_details.extend(admin_job_details)
                    else:
                        unapplied_ai_jobs_query = "SELECT ar.job_id, jp.job_status FROM ai_recommendation ar JOIN job_post jp ON ar.job_id = jp.id WHERE ar.professional_id = %s AND ar.source = 'AI' AND jp.job_status = 'opened' AND ar.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) AND ar.job_id NOT IN ( SELECT job_id FROM sc_recommendation WHERE professional_id = %s ) LIMIT 3;"
                        unapplied_ai_jobs = execute_query(unapplied_ai_jobs_query, (professional_id, professional_id, professional_id,))
                        ai_id_list = [i['job_id'] for i in unapplied_ai_jobs]

                        admin_rcmnd_jobs_query = "SELECT sr.job_id, jp.job_status FROM sc_recommendation sr JOIN job_post jp ON sr.job_id = jp.id WHERE sr.professional_id = %s AND sr.user_role_id = %s AND jp.job_status = 'opened' AND sr.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) LIMIT 2;"
                        values = (professional_id, 3, professional_id,)
                        admin_recmnd_jobs = execute_query(admin_rcmnd_jobs_query, values)
                        admin_id_list = []
                        if len(admin_recmnd_jobs) > 0:
                            admin_id_list = [i['job_id'] for i in admin_recmnd_jobs]
                        ai_external_job_ids = []
                        if len(ai_id_list)<3:
                            need_ids_count = 3 - len(ai_id_list)
                            query = "SELECT ar.job_id, ap.admin_job_status FROM ai_recommendation ar LEFT JOIN admin_job_post ap ON ar.job_id = ap.job_reference_id WHERE ar.professional_id = %s AND ar.source = 'AI' AND lower(ap.admin_job_status)= 'opened' LIMIT %s;"
                            values = (professional_id,need_ids_count)
                            ai_external_recmnd_job = execute_query(query, values)
                            ai_external_job_ids = [i['job_id'] for i in ai_external_recmnd_job]
                        recommended_id_list = admin_id_list + ai_id_list
                        if len(recommended_id_list) != 0:
                            query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.country, jp.state, jp.city, jp.workplace_type, jp.timezone, jp.required_resume, jp.required_cover_letter, jp.created_at, COALESCE(u.profile_image, su.profile_image) AS profile_image FROM job_post jp 
                                                   LEFT JOIN users u ON jp.employer_id = u.user_id 
                                                   LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                                                   WHERE jp.job_status = %s and jp.is_active = %s and jp.id IN %s ORDER by jp.id DESC LIMIT 3;
                                                """
                            values_job_details = ('Opened', 'Y', recommended_id_list,)
                            job_details = replace_empty_values(execute_query(query_job_details, values_job_details))

                        if ai_external_job_ids:
                            ext_query_job_details = """SELECT ap.`job_reference_id` AS id, ap.`job_title`, ap.`job_overview`, ap.`country`, ap.`state`, ap.`city`, ap.`job_type`, 
                                        ap.`workplace_type`, ap.`created_at`, u.profile_image, NULL AS required_cover_letter, NULL AS required_resume,
                                        NULL AS timezone FROM admin_job_post ap LEFT JOIN users u ON ap.employer_id=u.user_id WHERE ap.job_reference_id IN %s AND lower(ap.admin_job_status) = %s ORDER by job_reference_id DESC;"""
                            values_ext_query_job_details = (tuple(ai_external_job_ids), 'opened',)
                            ext_job_details = replace_empty_values(execute_query(ext_query_job_details, values_ext_query_job_details))
                            if ext_job_details:
                                job_details.extend(ext_job_details)

                    if len(job_details) == 0:
                        ai_recommendation = False
                        query_job_details = """SELECT jp.id, jp.job_title, jp.job_type, jp.country, jp.state, jp.city, jp.workplace_type, jp.timezone, jp.required_resume, jp.required_cover_letter, jp.created_at, COALESCE(u.profile_image, su.profile_image) AS profile_image FROM job_post jp 
                        LEFT JOIN users u ON jp.employer_id = u.user_id 
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id 
                        WHERE jp.job_status = %s and jp.is_active = %s and jp.country = %s ORDER by jp.created_at DESC LIMIT 4;
                    """
                        values_job_details = ('Opened', 'Y', user_data['country'])
                        job_details = replace_empty_values(execute_query(query_job_details, values_job_details))


                    for temp_job in job_details:
                        temp_job.update({'profile_image' : s3_employer_picture_folder_name + temp_job['profile_image']})

                community_list = []
                community_details = 'select id,title, short_description, image, join_url, share_url, type_of_community from community where is_active = %s and id = %s'
                values = ('Y', 1,)
                community_data = execute_query(community_details,values)
                if len(community_data) > 0:
                    s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + community_data[0]['image']                    
                    community_data[0].update({"image": s3_sc_community_cover_pic_key})
                    community_list.append(community_data[0])
                
                get_learning_post_query = "SELECT v.partner_id, COUNT(*) AS view_count FROM view_learning_count v JOIN learning l ON v.partner_id = l.id WHERE l.post_status = 'opened' GROUP BY v.partner_id ORDER BY view_count DESC LIMIT 3;"
                learning_post_values = ()
                learning_post_result = execute_query(get_learning_post_query, learning_post_values)
                learning_list = []
                if learning_post_result:
                    for l in learning_post_result:
                        query = 'SELECT pp.company_name, u.profile_image, l.id, l.partner_id, l.title, l.detailed_description, l.image, l.attached_file, l.url, l.created_at as posted_on, COALESCE(vc.view_count, 0) AS view_count FROM partner_profile pp INNER JOIN users u ON pp.partner_id = u.user_id INNER JOIN learning l ON pp.partner_id = l.partner_id LEFT JOIN ( SELECT partner_id, COUNT(*) AS view_count FROM view_learning_count GROUP BY partner_id ) vc ON l.id = vc.partner_id WHERE l.id = %s;' 
                        values = (l['partner_id'],)
                        learning_post_details = execute_query(query, values)
                        if len(learning_post_details) > 0:
                            s3_cover_pic_key = s3_partner_cover_pic_folder_name + learning_post_details[0]['image']
                            s3_attached_file_key = s3_partner_learning_folder_name + learning_post_details[0]['attached_file']
                            s3_pic_key = s3_partner_picture_folder_name + str(learning_post_details[0]['profile_image'])
                            learning_post_details[0].update({'image' : s3_cover_pic_key})
                            learning_post_details[0].update({'attached_file' : s3_attached_file_key})
                            learning_post_details[0].update({'profile_image' : s3_pic_key})
                            learning_list.append(learning_post_details[0])

                # query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and type_of_community = %s ORDER BY created_at DESC LIMIT 3'
                # values = ('Y', 'Careers in Impact',)
                # careers_data_set = execute_query(query, values)
                # careers_list = []
                # for c in careers_data_set:
                #     s3_sc_careers_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                #     c.update({"image": s3_sc_careers_cover_pic_key})
                #     careers_list.append(c)
                
                get_show_video_status = "select show_video_status from professional_profile where professional_id = %s"
                show_video_status_dict = execute_query(get_show_video_status, (professional_id,))
                if show_video_status_dict:
                    show_video_status = show_video_status_dict[0]['show_video_status']
                else:
                    show_video_status = ''
                #learning_posts key holds the careers_list data
                result_list = [{"total_professionals" : total_professionals,"learning_posts" : learning_list,
                            "total_employers" : total_employers,"total_learning_posts" : total_learning_posts,
                            "video_url" : s3_home_video_url, "total_jobs" : total_jobs, "profile_percentage" : profile_percentage, "community_list" : community_list,
                            "user_name" : user_name, "applied_jobs" : applied_jobs, "ai_job_details" : job_details, "show_video_status" : show_video_status, "is_recommended": ai_recommendation}]
                                                          
                query = 'select welcome_count from professional_profile where professional_id = %s'
                values = (professional_id,)
                count = execute_query(query, values)
                if count[0]['welcome_count'] == 0:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"The home page for user {user_data['email_id']} was displayed successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Home Page",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 200, result_list)
                    query = 'update professional_profile set welcome_count = %s where professional_id = %s'
                    values = (1, professional_id,)
                    temp = update_query(query, values)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': f"The home page for user {user_data['email_id']} was displayed successfully."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Home Page", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Home Page",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Home Page, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, result_list)
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in professional home page."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Home Page Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Home Page Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Home Page Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def update_home_page_video():
    result_json = {}
    try:
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":                                        
                professional_id = user_data["user_id"]
                req_data = request.get_json()
                if 'show_video_status' not in req_data:
                    result_json = api_json_response_format(False,"Please fill in all the required fields.",204,{})  
                    return result_json
                show_video_status =  req_data["show_video_status"]                          
                query = 'update professional_profile set show_video_status = %s where professional_id = %s'
                values = (show_video_status, professional_id,)
                count = update_query(query,values)
                if count > 0: 
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Show Home Page Video Status' : show_video_status,
                                    'Message': f"User {user_data['email_id']} updated the home page video display status."}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Home Page Video Status", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Home Page Video Status",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Home Page Video Status, {str(e)}")
                    result_json = api_json_response_format(True,"Your profile has been updated successfully!",0,{})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'Error in updating home page video display status.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Home Page Video Status Error", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Home Page Video Status Error",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Home Page Video Status Error, {str(e)}")
                    result_json = api_json_response_format(False,"Sorry, we encountered an issue. Please try again.",500,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized User",401,{})
        else:
            result_json = api_json_response_format(False,"Invalid Token. Please try again.",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in updating home page video display status."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Home Page Video Status Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Home Page Video Status Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Home Page Video Status Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get1_ip():
    ip_address = request.remote_addr
    return api_json_response_format(True,str(ip_address),500,{}) 

def search_result():
    result_json = {}
    try:
        profile = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional" or user_data["user_role"] == "admin":
                req_data = request.get_json()                        
                if 'search_text' not in req_data:
                    result_json = api_json_response_format(False, "search text required", 204, {})  
                    return result_json
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False, "page number required", 204, {})  
                    return result_json

                professional_id = user_data['user_id']
                profile_percentage = show_percentage(professional_id)
                page_number = req_data['page_number']
                search_text = req_data['search_text']
                offset = (page_number - 1) * 10

                    # Preprocess the search text for flexible matching
                search_text_hyphen = search_text.replace(" ", "-")
                search_text_space = search_text.replace("-", " ")  #'specialisation','skills', 'sector'

                column_query = """
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE (TABLE_NAME = 'job_post' OR (TABLE_NAME = 'employer_profile' AND COLUMN_NAME = 'company_name') OR (TABLE_NAME = 'employer_profile' AND COLUMN_NAME = 'sector'))
                    AND TABLE_SCHEMA = DATABASE()
                    AND COLUMN_NAME NOT IN ('job_overview', 'id', 'employer_id', 'responsibilities', 'additional_info', 'job_status', 'state', 'work_schedule', 'calendly_link', 'share_url', 'salary', 'custom_notes', 'currency', 'benefits', 'required_resume', 'required_cover_letter', 'required_background_check', 'required_subcontract', 'is_application_deadline', 'application_deadline_date', 'is_paid', 'days_left', 'calc_day', 'is_active', 'is_role_filled', 'hired_candidate_id', 'feedback', 'created_at', 'updated_at', 'job_type', 'workplace_type', 'number_of_openings', 'time_commitment', 'timezone', 'duration')
                """
                columns = execute_query(column_query)
                column_names = [col['COLUMN_NAME'] for col in columns]

                print(column_names)

                # Step 2: Construct LIKE clauses for job_post columns and also check employer_profile's company_name
                # like_clauses = " OR ".join([
                #     f"(jp.`job_title` LIKE %s OR jp.`{col}` LIKE %s OR jp.`{col}` LIKE %s)" if col != 'company_name' and col != 'sector'
                #     else "(ep.`company_name` LIKE %s OR ep.`company_name` LIKE %s OR ep.`company_name` LIKE %s)" 
                #     for col in column_names
                # ])
                like_clauses = " OR ".join([
                    f"(jp.`job_title` LIKE %s OR jp.`{col}` LIKE %s OR jp.`{col}` LIKE %s)" if col not in ['company_name', 'sector']
                    else "(ep.`company_name` LIKE %s OR ep.`company_name` LIKE %s OR ep.`company_name` LIKE %s)" 
                    if col == 'company_name'
                    else "(ep.`sector` LIKE %s OR ep.`sector` LIKE %s OR ep.`sector` LIKE %s)" 
                    for col in column_names
                ])
                if user_data["user_role"] == "admin":
                    count_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.responsibilities, 
                            jp.additional_info, jp.skills, jp.country AS job_country, jp.state, jp.city AS job_city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.calendly_link, jp.currency, 
                            jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.custom_notes, jp.share_url, 
                            jp.specialisation, jp.created_at,
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
                        FROM 
                            job_post jp
                        LEFT JOIN users u ON jp.employer_id = u.user_id
                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                        WHERE jp.job_status != 'drafted'
                            AND jp.is_active = 'Y'
                            AND ({like_clauses})
                            AND NOT EXISTS (
                                SELECT 1
                                FROM job_activity ja
                                WHERE ja.job_id = jp.id 
                                AND ja.professional_id = {professional_id}
                            )
                        ORDER BY jp.id DESC;"""
                    sql_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.responsibilities, 
                            jp.additional_info, jp.skills, jp.country AS job_country, jp.state, jp.city AS job_city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.calendly_link, jp.currency, 
                            jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.custom_notes, jp.share_url, 
                            jp.specialisation, jp.created_at,
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
                        FROM 
                            job_post jp
                        LEFT JOIN users u ON jp.employer_id = u.user_id
                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                        WHERE jp.job_status != 'drafted'
                            AND jp.is_active = 'Y'
                            AND ({like_clauses})
                            AND NOT EXISTS (
                                SELECT 1
                                FROM job_activity ja
                                WHERE ja.job_id = jp.id 
                                AND ja.professional_id = {professional_id}
                            )
                        ORDER BY jp.id DESC LIMIT 10 OFFSET {offset};"""
                else:
                    count_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.responsibilities, 
                            jp.additional_info, jp.skills, jp.country AS job_country, jp.state, jp.city AS job_city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.calendly_link, jp.currency, 
                            jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.custom_notes, jp.share_url, 
                            jp.specialisation, jp.created_at,
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
                        FROM 
                            job_post jp
                        LEFT JOIN users u ON jp.employer_id = u.user_id
                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                        WHERE jp.job_status = 'opened'
                            AND jp.is_active = 'Y'
                            AND ({like_clauses})
                            AND NOT EXISTS (
                                SELECT 1
                                FROM job_activity ja
                                WHERE ja.job_id = jp.id 
                                AND ja.professional_id = {professional_id}
                            )
                        ORDER BY jp.id DESC"""

                    sql_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_status, jp.job_overview, jp.job_desc, jp.responsibilities, 
                            jp.additional_info, jp.skills, jp.country AS job_country, jp.state, jp.city AS job_city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, jp.calendly_link, jp.currency, 
                            jp.benefits, jp.required_resume, jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, jp.custom_notes, jp.share_url, 
                            jp.specialisation, jp.created_at,
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
                        FROM 
                            job_post jp
                        LEFT JOIN users u ON jp.employer_id = u.user_id
                        LEFT JOIN employer_profile ep ON jp.employer_id = ep.employer_id
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                        WHERE jp.job_status = 'opened'
                            AND jp.is_active = 'Y'
                            AND ({like_clauses})
                            AND NOT EXISTS (
                                SELECT 1
                                FROM job_activity ja
                                WHERE ja.job_id = jp.id 
                                AND ja.professional_id = {professional_id}
                            )
                        ORDER BY jp.id DESC LIMIT 10 OFFSET {offset};"""

                search_params = []
                for col in column_names:
                    # For company_name and other columns, add all 3 variations of the search text
                    search_params.extend([f"%{search_text}%", f"%{search_text_hyphen}%", f"%{search_text_space}%"])


                # Ensure enough parameters are passed to match the placeholders
                job_count = execute_query(count_query, search_params)
                job_details = execute_query(sql_query, search_params)



                if len(job_details) > 0:
                    id = job_details[0]["id"]
                    query = 'SELECT * FROM view_count WHERE job_id = %s AND professional_id = %s'
                    values = (id, professional_id)
                    count = execute_query(query, values)

                    if not count:
                        current_time = datetime.now()
                        query = "INSERT INTO view_count (job_id, professional_id, viewed_at) VALUES (%s, %s, %s)"                 
                        values = (id, professional_id, current_time)
                        update_query(query, values)

                    for job in job_details:
                        quest_dict = {"questions": []}
                        job_id = job['id']
                        query = 'SELECT id, custom_pre_screen_ques FROM pre_screen_ques WHERE job_id = %s'
                        values = (job_id,)
                        result = execute_query(query, values)
                        if len(result) != 0:
                            quest_dict["questions"].extend(result)
                        job.update(quest_dict)
                        job.update({'created_at': str(job['created_at'])})

                        query = 'select professional_id from sc_recommendation where job_id = %s and user_role_id = %s'
                        values = (job['id'], 2,)
                        prof_id_list = execute_query(query, values)
                        temp_list = []
                        for j in prof_id_list:
                            query = "WITH LatestExperience AS ( SELECT pe.professional_id, pe.id AS experience_id, pe.start_month, pe.start_year, pe.job_title, pe.created_at, ROW_NUMBER() OVER ( PARTITION BY pe.professional_id ORDER BY CASE WHEN (pe.start_year IS NULL OR pe.start_year = '') THEN 0 ELSE pe.start_year END DESC, CASE WHEN (pe.start_month IS NULL OR pe.start_month = '') THEN 0 ELSE pe.start_month END DESC, pe.created_at DESC ) AS rn FROM professional_experience AS pe ) SELECT u.first_name, u.last_name, u.country, u.state, u.city, u.pricing_category, u.is_active, u.payment_status, p.professional_id,le.job_title, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN LatestExperience AS le ON p.professional_id = le.professional_id AND le.rn = 1 LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE p.professional_id = %s"
                            values = (j['professional_id'],)
                            info = execute_query(query, values)
                            if len(info) > 0:
                                if info[0]['profile_image'] != '':
                                    s3_pic_key = s3_picture_folder_name + str(info[0]['profile_image'])
                                else:
                                    s3_pic_key = ''
                                if user_data['user_role'] == 'admin':
                                    mod_id = "2C-PR-" + str(info[0]['professional_id'])
                                    info[0].update({'professional_id' : mod_id})
                                info[0].update({'profile_image' : s3_pic_key})
                                temp_list.append(info[0])
                        job.update({"recommended_professional" : temp_list})

                        query = 'SELECT COUNT(job_id) AS count FROM saved_job WHERE job_id = %s AND professional_id = %s'
                        values = (job_id, professional_id)
                        rslt = execute_query(query, values)
                        job_saved_status = "saved" if rslt[0]['count'] > 0 else "unsaved"
                        job.update({'saved_status': job_saved_status})

                        query = 'SELECT COUNT(job_id) AS count FROM job_activity WHERE job_id = %s AND professional_id = %s'
                        values = (job_id, professional_id)
                        rslt = execute_query(query, values)
                        job_applied_status = 'applied' if rslt[0]['count'] > 0 else 'not_applied'

                        if job['sector'] != '':
                            txt = job['sector'].replace(", ", "_").replace(" ", "_")
                            sector_name = f"{txt}.png"
                        else:
                            sector_name = ''

                        query = 'SELECT professional_resume FROM professional_profile WHERE professional_id = %s'
                        values = (professional_id,)
                        rslt = execute_query(query, values)
                        if len(rslt) > 0:
                            resume_name = rslt[0]['professional_resume']
                        else:
                            resume_name = ''

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
                        job.update({'user_resume': resume_name})
                        job.update({'applied_status': job_applied_status})
                        job.update({'profile_image': s3_employeer_logo_folder_name + job['profile_image']})
                        job.update({'sector_image': s3_sector_image_folder_name + sector_name})
                        job.update({'sector': job['sector']})
                        job.update({'employer_type': job['employer_type']})
                        job.update({'company_description': job['company_description']})
                        job.update({'profile_percentage': profile_percentage})

                    job_details_dict = {'job_details': job_details}
                    profile.update(job_details_dict)    
                    Total_job = {'total_count': len(job_count)}
                    profile.update(Total_job)                          
                    data = fetch_filter_params()
                    profile.update(data)
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': 'The search results have been displayed successfully.'}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Search", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Search",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Job Search, {str(e)}")
                    result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "The search results were displayed successfully, but no records matched the search text"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Search", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Search",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Job Search, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, {})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid token. Please try again", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "An error occurred while fetching professional job search results."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Job Search Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Job Search Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Job Search Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def search_result_applied():
    result_json = {}
    try:
        profile = {}
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "professional":
                req_data = request.get_json()
                if 'search_text' not in req_data:
                    result_json = api_json_response_format(False, "search text required", 204, {})
                    return result_json
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False, "page number required", 204, {})
                    return result_json

                professional_id = user_data['user_id']
                profile_percentage = show_percentage(professional_id)
                page_number = req_data['page_number']
                search_text = req_data['search_text'].lower()
                offset = (page_number - 1) * 10

                # Step 1: Retrieve the applied jobs for the professional
                query = 'SELECT job_id, application_status, feedback FROM job_activity WHERE professional_id = %s ORDER BY job_id DESC'
                values = (professional_id,)
                applied_jobs_list = execute_query(query, values)
                if applied_jobs_list:
                    # Step 2: Filter applied jobs based on search_text
                    job_ids = [job['job_id'] for job in applied_jobs_list]
                    job_statuses = {job['job_id']: job['application_status'] for job in applied_jobs_list}
                    feedbacks = {job['job_id']: job['feedback'] for job in applied_jobs_list}
                    placeholders = (','.join(['%s'] * len(job_ids)))

                    # Step 3: Fetch the job details where the job IDs match and search_text is found
                    column_query = """
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'job_post' 
                    AND TABLE_SCHEMA = DATABASE()
                    AND COLUMN_NAME NOT IN ('job_overview', 'job_desc')
                    """
                    columns = execute_query(column_query)
                    column_names = [col['COLUMN_NAME'] for col in columns if col['COLUMN_NAME'] != 'job_title']

                    # Step 4: Construct the dynamic SQL query with job_title prioritized
                    like_clauses = f"jp.`job_title` LIKE %s OR " + ' OR '.join([f"jp.`{col}` LIKE %s" for col in column_names])
                    count_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.job_status,
                            jp.additional_info, jp.skills, jp.country, jp.state, jp.city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, 
                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, 
                            jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, 
                            jp.share_url, jp.specialisation, jp.created_at, 
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
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                        WHERE jp.is_active = 'Y' 
                          AND jp.id IN ({placeholders})
                          AND ({like_clauses})
                        ORDER BY jp.id DESC"""
                    sql_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.job_status,
                            jp.additional_info, jp.skills, jp.country as job_country, jp.state as job_state, jp.city as job_state, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, 
                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, 
                            jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, 
                            jp.share_url, jp.specialisation, jp.created_at, 
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
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id
                        WHERE jp.is_active = 'Y' 
                          AND jp.id IN ({placeholders})
                          AND ({like_clauses})
                        ORDER BY jp.id DESC LIMIT 10 OFFSET {offset}
                    """

                    search_term = f"%{search_text}%"
                    params = job_ids + [search_term] * len(column_names) + [offset]
                    job_count = execute_query(count_query, params)
                    # job_count = execute_query(count_query, job_ids + [search_term] + [search_term] * len(column_names))   AND jp.id IN ()
                    job_details = execute_query(sql_query, params) #job_ids + [search_term] + [search_term] * len(column_names)

                    if job_details:
                        # Process the job details as in the original code
                        for job in job_details:
                            quest_dict = {"questions": []}
                            job_id = job['id']
                            query = 'SELECT id, custom_pre_screen_ques FROM pre_screen_ques WHERE job_id = %s'
                            values = (job_id,)
                            result = execute_query(query, values)
                            if result:
                                quest_dict["questions"].extend(result)
                            job.update(quest_dict)
                            job.update({'created_at': str(job['created_at'])})

                            query = 'SELECT COUNT(job_id) AS count FROM saved_job WHERE job_id = %s AND professional_id = %s'
                            values = (job_id, professional_id)
                            rslt = execute_query(query, values)
                            job_saved_status = "saved" if rslt[0]['count'] > 0 else "unsaved"
                            job.update({'saved_status': job_saved_status})

                            query = 'SELECT COUNT(job_id) AS count FROM job_activity WHERE job_id = %s AND professional_id = %s'
                            values = (job_id, professional_id)
                            rslt = execute_query(query, values)
                            job_applied_status = 'applied' if rslt[0]['count'] > 0 else 'not_applied'

                            txt = job['sector'].replace(", ", "_").replace(" ", "_")
                            sector_name = f"{txt}.png"

                            query = 'SELECT professional_resume FROM professional_profile WHERE professional_id = %s'
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
                            job.update({'user_resume': resume_name})
                            job.update({'applied_status': job_applied_status})
                            job.update({'application_status': job_statuses[job_id]})  # Add the application_status
                            job.update({'feedback': feedbacks[job_id]})  # Add the feedback
                            job.update({'profile_image': s3_employeer_logo_folder_name + job['profile_image']})
                            job.update({'sector_image': s3_sector_image_folder_name + sector_name})
                            job.update({'sector': job['sector']})
                            job.update({'employer_type': job['employer_type']})
                            job.update({'company_description': job['company_description']})
                            job.update({'profile_percentage': profile_percentage})

                        job_details_dict = {'job_details': job_details}
                        profile.update(job_details_dict)
                        Total_job = {'total_count': len(job_count)}
                        profile.update(Total_job)
                        data = fetch_filter_params()
                        profile.update(data)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': "The search results for applied jobs have been displayed successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs Search", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs Search",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Applied Jobs Search, {str(e)}")
                        result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': "The search results for applied jobs were displayed successfully, but no records matched the search text"}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs Search", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs Search",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Applied Jobs Search, {str(e)}")
                        result_json = api_json_response_format(True, "No records found", 0, {})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "The search results for applied jobs were displayed successfully, but no records matched the search text"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs Search", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs Search",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Applied Jobs Search, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, {})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid token. Please try again", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "An error occurred while fetching professional applied jobs search results."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Applied Jobs Search Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Applied Jobs Search Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Applied Jobs Search Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

def search_result_saved():
    result_json = {}
    try:
        profile = {}
        token_result = get_user_token(request)   
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":
                req_data = request.get_json()                        
                if 'search_text' not in req_data:
                    result_json = api_json_response_format(False, "search text required", 204, {})  
                    return result_json
                if 'page_number' not in req_data:
                    result_json = api_json_response_format(False, "page number required", 204, {})  
                    return result_json

                professional_id = user_data['user_id']
                profile_percentage = show_percentage(professional_id)
                page_number = req_data['page_number']
                search_text = req_data['search_text'].lower()
                offset = (page_number - 1) * 10

                # Step 1: Retrieve the saved jobs for the professional that haven't been applied to
                query = """
                SELECT sj.job_id 
                FROM saved_job sj 
                LEFT JOIN job_activity ja 
                ON sj.job_id = ja.job_id 
                AND sj.professional_id = ja.professional_id 
                WHERE sj.professional_id = %s 
                AND ja.job_id IS NULL 
                ORDER BY sj.job_id ASC
                """
                values = (professional_id,)
                saved_job_id = execute_query(query, values)

                if saved_job_id:
                    job_ids = [job['job_id'] for job in saved_job_id]

                    # Step 2: Fetch the column names to search within the job_post table
                    column_query = """
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'job_post' 
                    AND TABLE_SCHEMA = DATABASE()
                    AND COLUMN_NAME NOT IN ('job_overview', 'job_desc')
                    """
                    columns = execute_query(column_query)
                    column_names = [col['COLUMN_NAME'] for col in columns if col['COLUMN_NAME'] != 'job_title']

                    # Step 3: Construct the dynamic SQL query with job_title prioritized
                    like_clauses = f"jp.`job_title` LIKE %s OR " + ' OR '.join([f"jp.`{col}` LIKE %s" for col in column_names])
                    
                    count_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.job_status,
                            jp.additional_info, jp.skills, jp.country as job_country, jp.state as job_job_state, jp.city as job_city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, 
                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, 
                            jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, 
                            jp.share_url, jp.specialisation, jp.created_at,
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
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id 
                        WHERE jp.is_active = 'Y' 
                          AND jp.id IN ({','.join(['%s'] * len(job_ids))})
                          AND ({like_clauses})
                        ORDER BY jp.id DESC
                    """
                    
                    sql_query = f"""
                        SELECT DISTINCT 
                            jp.id, jp.job_title, jp.job_type, jp.job_overview, jp.job_desc, jp.responsibilities, jp.job_status,
                            jp.additional_info, jp.skills, jp.country as job_country, jp.state as job_job_state, jp.city as job_city, jp.work_schedule, 
                            jp.workplace_type, jp.is_paid, jp.time_commitment, jp.timezone, jp.duration, 
                            jp.calendly_link, jp.currency, jp.benefits, jp.required_resume, 
                            jp.required_cover_letter, jp.required_background_check, jp.required_subcontract, 
                            jp.is_application_deadline, jp.application_deadline_date, jp.is_active, 
                            jp.share_url, jp.specialisation, jp.created_at,
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
                        LEFT JOIN sub_users su ON jp.employer_id = su.sub_user_id 
                        WHERE jp.is_active = 'Y' 
                          AND jp.id IN ({','.join(['%s'] * len(job_ids))})
                          AND ({like_clauses})
                        ORDER BY jp.id DESC LIMIT 10 OFFSET {offset}
                    """

                    search_term = f"%{search_text}%"
                    like_values = [search_term] * (len(column_names) + 1)  # +1 for job_title
                    job_count = execute_query(count_query, job_ids + like_values)
                    job_details = execute_query(sql_query, job_ids + like_values)

                    if job_details:
                        # Process the job details
                        for job in job_details:
                            quest_dict = {"questions": []}
                            job_id = job['id']
                            query = 'SELECT id, custom_pre_screen_ques FROM pre_screen_ques WHERE job_id = %s'
                            values = (job_id,)
                            result = execute_query(query, values)
                            if result:
                                quest_dict["questions"].extend(result)
                            job.update(quest_dict)
                            job.update({'created_at': str(job['created_at'])})

                            query = 'SELECT COUNT(job_id) AS count FROM saved_job WHERE job_id = %s AND professional_id = %s'
                            values = (job_id, professional_id)
                            rslt = execute_query(query, values)
                            job_saved_status = "saved" if rslt[0]['count'] > 0 else "unsaved"
                            job.update({'saved_status': job_saved_status})

                            query = 'SELECT COUNT(job_id) AS count FROM job_activity WHERE job_id = %s AND professional_id = %s'
                            values = (job_id, professional_id)
                            rslt = execute_query(query, values)
                            job_applied_status = 'applied' if rslt[0]['count'] > 0 else 'not_applied'

                            txt = job['sector'].replace(", ", "_").replace(" ", "_")
                            sector_name = f"{txt}.png"

                            query = 'SELECT professional_resume FROM professional_profile WHERE professional_id = %s'
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
                            job.update({'user_resume': resume_name})
                            job.update({'applied_status': job_applied_status})
                            job.update({'profile_image': s3_employeer_logo_folder_name + job['profile_image']})
                            job.update({'sector_image': s3_sector_image_folder_name + sector_name})
                            job.update({'sector': job['sector']})
                            job.update({'employer_type': job['employer_type']})
                            job.update({'company_description': job['company_description']})
                            job.update({'profile_percentage': profile_percentage})

                        job_details_dict = {'job_details': job_details}
                        profile.update(job_details_dict)    
                        Total_job = {'total_count': len(job_count)}
                        profile.update(Total_job)                          
                        data = fetch_filter_params()
                        profile.update(data)
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': "The search results for saved jobs have been displayed successfully."}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs Search", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs Search",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Saved Jobs Search, {str(e)}")
                        result_json = api_json_response_format(True, "Details fetched successfully!", 0, profile)
                    else:
                        try:
                            temp_dict = {'Country' : user_data['country'],
                                        'City' : user_data['city'],
                                        'Message': "The search results for saved jobs were displayed successfully, but no records matched the search text"}
                            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs Search", temp_dict)
                            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs Search",event_properties, temp_dict.get("Message"), user_data)
                        except Exception as e:  
                            print(f"Error in mixpanel event logging: Professional Saved Jobs Search, {str(e)}")
                        result_json = api_json_response_format(True, "No records found", 0, {})
                else:
                    try:
                        temp_dict = {'Country' : user_data['country'],
                                    'City' : user_data['city'],
                                    'Message': "The search results for saved jobs were displayed successfully, but no records matched the search text"}
                        event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs Search", temp_dict)
                        background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs Search",event_properties, temp_dict.get("Message"), user_data)
                    except Exception as e:  
                        print(f"Error in mixpanel event logging: Professional Saved Jobs Search, {str(e)}")
                    result_json = api_json_response_format(True, "No records found", 0, {})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid token. Please try again", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "An error occurred while fetching professional saved jobs search results."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Professional Saved Jobs Search Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Professional Saved Jobs Search Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Saved Jobs Search Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False, str(error), 500, {})        
    finally:        
        return result_json

def get_payment_status_professional():
    result_json = {}
    try:                             
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])            
            if user_data["user_role"] == "professional":  
                professional_id = user_data["user_id"]
                email_id = user_data["email_id"]
                query = 'select payment_status from users where user_id = %s'
                values = (professional_id,)
                ps = execute_query(query,values)
                if len(ps) >0:
                    payment_status = ps[0]["payment_status"]
                    if payment_status =="canceled" or payment_status =="incomplete":
                        price_id = PROFESSIONAL_BASIC_PLAN_ID  # Replace with your actual price ID
                        price = stripe.Price.retrieve(price_id)
                        if price["unit_amount"] == 0:
                            create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
                            result_json = api_json_response_format(False,f"Trial period",0,{})
                        else:
                            result_json = api_json_response_format(False,f"Trial period of {email_id} has ended. please Subcribe to continue.",300,{})                     
                    else:
                        result_json = api_json_response_format(False,f"Trial period",0,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json  

def get_job_post_detail():
    def convert_datetime(obj):
        """Helper function to convert datetime objects to strings."""
        if isinstance(obj, datetime):
            return obj.isoformat()  # Converts to 'YYYY-MM-DDTHH:MM:SS' format
        raise TypeError("Type not serializable")
    result_json = {}
    try:                             
        query = 'select id from job_post order by id'
        values = ()
        job_id_list = execute_query(query,values)
        rslt_arr = []
       
        for i in job_id_list:
            query = 'select * from job_post where id = %s'
            values = (i["id"],)
            job_post_details = execute_query(query,values)
            if job_post_details:
                rslt_arr.append(job_post_details[0]) 
        with open("job_details.json", "a") as f:
            # json.dump(rslt_arr, f, indent=4)
            json.dump(rslt_arr, f, indent=4, default=convert_datetime)
            f.write("\n")
        # if len(ps) >0:
        #     payment_status = ps[0]["payment_status"]
        #     if payment_status =="canceled" or payment_status =="incomplete":
        #         price_id = PROFESSIONAL_BASIC_PLAN_ID  # Replace with your actual price ID
        #         price = stripe.Price.retrieve(price_id)
        #         if price["unit_amount"] == 0:
        #             create_trial_session(email_id,PROFESSIONAL_TRIAL_PERIOD,PROFESSIONAL_BASIC_PLAN_ID,"Basic")
        #             result_json = api_json_response_format(False,f"Trial period",0,{})
        #         else:
        #             result_json = api_json_response_format(False,f"Trial period of {email_id} has ended. please Subcribe to continue.",300,{})                     
        #     else:
        #         result_json = api_json_response_format(False,f"Trial period",0,{})
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json  
    
def get_help_videos_details():
    try:
        token_result = get_user_token(request)
        if token_result["status_code"] == 200:
            user_data = get_user_data(token_result["email_id"])
            if user_data["user_role"] == "professional" or user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                query = 'select id, title, url, description from help_videos where user_role = %s'
                if user_data['user_role'] == 'professional':
                    values = ('professional',)
                elif user_data['user_role'] == 'employer' or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
                    values = ('employer',)
                help_video_data_set = execute_query(query, values)
                if help_video_data_set:
                    result_json = api_json_response_format(True, "Details fetched successfully!", 200, help_video_data_set)
                else:
                    result_json = api_json_response_format(False, "No help videos found", 404, {})
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']} viewed the help videos."}
                    event_properties = background_runner.process_dict(user_data["email_id"], "Help Videos Tab", temp_dict)
                    background_runner.mixpanel_event_async(user_data['email_id'],"Help Videos Tab",event_properties, temp_dict.get("Message"), user_data)
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Help Videos Tab, {str(e)}")
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        else:
            result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing community tab."}
            event_properties = background_runner.process_dict(user_data["email_id"], "Community Tab Error", temp_dict)
            background_runner.mixpanel_event_async(user_data['email_id'],"Community Tab Error",event_properties, temp_dict.get("Message"), user_data)
        except Exception as e:  
            print(f"Error in mixpanel event logging: Community Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json


# def get_professional_id():
#     result_json = {}
#     try:                             
#         query = 'select user_id from users where email_active = %s and user_role_fk = %s and created_at > %s'
#         value = ('Y', 3, '2024-09-13 00:00:00')
#         professional_ids = execute_query(query, value)
#         professional_id = [id['user_id'] for id in professional_ids]
#         # print(professional_id)
#         f = open("update_prof_percent.txt", "a")
#         for id in professional_id:
#                 percentage = calculate_professional_profile_percentage(id)
#                 query = "update users set profile_percentage = %s where user_id = %s"
#                 values = (int(percentage['value']), id,)
#                 execute_query(query, values)
#                 text = f"update users set profile_percentage = {int(percentage['value'])} where user_id = {id};"
#             # if percentage['value'] > 60:
#             #     json_string = json.dumps(percentage)
#                 f.write(text)
#                 f.write("\n")
            
#     except Exception as error:
#         print(error)        
#         result_json = api_json_response_format(False,str(error),500,{})        
#     finally:        
#         return result_json  

# import uuid
# from mixpanel import Mixpanel
# import platform

# MIXPANEL_PROJECT_TOKEN ="08cb6e1e4bfa54d9288408e774d90765"
# mp = Mixpanel(MIXPANEL_PROJECT_TOKEN)
# def mixpanel_store_events(distinct_id,event_name,event_properties):
#         try:           
#             mp.track(distinct_id,event_name, event_properties)
#             print(f"Event {event_name} successfully stored into mixpanel for {distinct_id}.")
#         except Exception as error:
#             print("Exception in mixpanel_store_events() ",error) 

# def mixpanel_store_user_profile(user_email_id,user_properties):
#     try:
#         mp.people_set(user_email_id, user_properties, meta = {'$ignore_time' : False, '$ip' : 0})
#         print(f"User {user_email_id} successfully stored into mixpanel.")      
#     except Exception as error:
#         print("Exception in mixpanel_store_user_profile() ",error)

# def track_user_event():
#     rslt_arr = []
#     # query = "select user_id, first_name, last_name, email_id, city, country, pricing_category, login_mode, created_at, user_role_fk from users where email_active='Y' and user_role_fk != 1;"
#     query = "select user_id, first_name, last_name, email_id, city, country, pricing_category, login_mode, created_at, user_role_fk from users where user_role_fk != 1;"
#     values = ()
#     rslt = execute_query(query, values)
#     for rs in rslt:
#         user_id = rs['user_id']
#         first_name = rs['first_name']
#         last_name = rs['last_name']
#         email_id = rs['email_id']
#         city = rs['city']
#         country = rs['country']
#         pricing_category = rs['pricing_category']
#         login_mode = rs['login_mode']
#         created_at = str(rs['created_at'])
#         date_format = "%Y-%m-%d %H:%M:%S"
#         date_object = datetime.strptime(created_at, date_format)
#         timestamp = int(time.mktime(date_object.timetuple()))
#         random_id = uuid.uuid4().hex
#         if rs['user_role_fk'] == 2 : 
#             query = "select designation, company_name, employer_type, sector, website_url from employer_profile where employer_id = %s"
#             values = (user_id,)
#             emp_details = execute_query(query, values)
#             organization_name = emp_details[0]['company_name']
#             organization_type = emp_details[0]['employer_type']
#             sector = emp_details[0]['sector']
#             title = emp_details[0]['designation']
#             website = emp_details[0]['website_url']
#             event_name = "Employer Profile Creation"
#             details_dict = {
#                         "event": event_name,
#                         "properties": {
#                             "time": timestamp,
#                             "distinct_id": email_id,
#                             "$distinct_id": email_id,
#                             "$insert_id": random_id,
#                             "$os": "Linux",
#                             "$time": timestamp,
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
#                             'Signup Mode' : login_mode,
#                             'User plan' :  pricing_category
#                             }
#                         }
#         elif rs['user_role_fk'] == 3 :
#             event_name = "Professional Profile Creation"
#             details_dict = {
#                         "event": event_name,
#                         "properties": {
#                             "time": timestamp,
#                             "distinct_id": email_id,
#                             "$distinct_id": email_id,
#                             "$insert_id": random_id,
#                             "$os": "Linux",
#                             "$time": timestamp,
#                             "City": city,
#                             "Country": country,
#                             "Email": email_id,
#                             "First Name": first_name,
#                             "Last Name": last_name,
#                             "Signup Mode": login_mode,
#                             "User plan": pricing_category
#                             }
#                         }
#         else:
#             query = "select company_name, partner_type, sector, website_url from partner_profile where partner_id = %s"
#             values = (user_id,)
#             partner_details = execute_query(query, values)
#             partner_type = partner_details[0]['partner_type']
#             sector = partner_details[0]['sector']
#             company_name = partner_details[0]['company_name']
#             event_name = "Partner Profile Creation"
#             details_dict = {
#                         "event": event_name,
#                         "properties": {
#                             "time": timestamp,
#                             "distinct_id": email_id,
#                             "$distinct_id": email_id,
#                             "$insert_id": random_id,
#                             "$os": "Linux",
#                             "$time": timestamp,
#                             'First Name'    : first_name,
#                             'Last Name'     : last_name,
#                             'Email'         : email_id,
#                             'Company Name' : company_name,
#                             'Sector' : sector,
#                             'Partner Type' : partner_type,
#                             'Website' : website, 
#                             'City': city,
#                             'Country': country,
#                             'Signup Mode' : login_mode,
#                             'User plan' :  pricing_category
#                             }
#                         }
#         rslt_arr.append(details_dict)
#     with open("mixpanel_event_prop.json", "a") as f:
#         json.dump(rslt_arr, f, indent=4)  # Adding indent=4 for pretty print
#         f.write("\n")
    
#     return "True"

# def track_partner_post():
#     # query = "select u.email_id, u.city, u.country, u.first_name, u.last_name, u.login_mode, u.pricing_category, u.user_role_fk, u.created_at, pp.company_name, pp.sector, pp.partner_type, pp.website_url from partner_profile pp left join users u on u.user_id = pp.partner_id;"
#     query = "select u.email_id, u.city, u.country, u.first_name, u.last_name, u.login_mode, u.pricing_category, u.user_role_fk, l.created_at, l.title, l.url from learning l left join users u on u.user_id = l.partner_id where id in (12,13,14);"
#     values = ()
#     rslt = execute_query(query, values)
#     rslt_arr = []
#     for rs in rslt:
#         email_id = rs['email_id']
#         # first_name = rs['first_name']
#         # last_name = rs['last_name']
#         # city = rs['city']
#         # country = rs['country']
#         # login_mode = rs['login_mode']
#         # pricing_category = rs['pricing_category']
#         # user_role_fk = rs['user_role_fk']
#         # company_name = rs['company_name']
#         # sector = rs['sector']
#         # partner_type = rs['partner_type']
#         # website_url = rs['website_url']
#         title = rs['title']
#         url = rs['url']
#         created_at = str(rs['created_at'])
#         date_format = "%Y-%m-%d %H:%M:%S"
#         date_object = datetime.strptime(created_at, date_format)
#         timestamp = int(time.mktime(date_object.timetuple()))
#         random_id = uuid.uuid4().hex
#         try:
#             event_name = "Create Partner Post"
#             event_properties = {   
#                 "event": event_name,
#                 "properties": {
#                     "time": timestamp,
#                     'distinct_id' : email_id, 
#                     "$distinct_id": email_id,
#                     "$insert_id": random_id,
#                     "$os": "Linux",
#                     '$time': timestamp,          
#                     'Email' : email_id,
#                     'Post Title' : title,
#                     'URL' : url,
#                     'Post Created Status' : "Success"
#                 }
#             }
            
#             # event_name = "Partner Post Creation"
#             # mixpanel_store_events(email_id,event_name,event_properties)
#             rslt_arr.append(event_properties)
#             # user_properties = {
#             #                 '$distinct_id' : email_id,
#             #                 '$distinct_id' : email_id,
#             #                 '$last_seen': timestamp,
#             #                 '$city' : city,
#             #                 '$country' : country,
#             #                 '$email'   : email_id,
#             #                 '$first_name'    : first_name,
#             #                 '$last_name' : last_name,
#             #                 'Company Name' : company_name,
#             #                 'Sector' : sector,
#             #                 'Partner Type' : partner_type,
#             #                 'Website' : website_url,
#             #                 'Signup Mode' : login_mode,
#             #                 'User plan' :  pricing_category
#             #             }
#             # rslt_arr.append(user_properties)
#         except Exception as e:  
#             print("Error in partner create post mixpanel_event_log : %s",str(e))
#     with open("mixpanel_partner_post.json", "a") as f:
#             json.dump(rslt_arr, f, indent=4)  # Adding indent=4 for pretty print
#             f.write("\n")

#     return "True"

# def track_job_post():
    # query = "select jp.employer_id, jp.job_title, jp.job_type, jp.workplace_type, jp.work_schedule, jp.specialisation, jp.city, jp.country, jp.timezone, jp.created_at, u.email_id from job_post jp left join users u on u.user_id = jp.employer_id where id in (200047, 200048, 200049, 200050, 200051, 200052, 200053, 200054);"
#     # query = "select u.email_id, u.city, u.country, u.first_name, u.last_name, u.login_mode, u.pricing_category, u.user_role_fk, u.created_at, ep.designation, ep.company_name, ep.sector, ep.employer_type, ep.website_url from employer_profile ep left join users u on u.user_id = ep.employer_id;"
#     # query = "select jp.job_title, jp.job_status, jp.is_role_filled, jp.feedback, jp.hired_candidate_id, jp.created_at, u.email_id, u.city, u.country from job_post jp left join users u on u.user_id = jp.employer_id where jp.job_status = %s"
    # values = ()
    # rslt = execute_query(query, values)
    # rslt_arr = []
    # for rs in rslt:
    #     email_id = rs['email_id']
    #     job_title = rs['job_title']
    #     job_type = rs['job_type']
    #     workplace_type = rs['workplace_type']
    #     work_schedule = rs['work_schedule']
    #     specialisation = rs['specialisation']
    #     timezone = rs['timezone']
#         # first_name = rs['first_name']
#         # last_name = rs['last_name']
        # city = rs['city']
        # country = rs['country']
#         # pricing_category = rs['pricing_category']
#         # login_mode = rs['login_mode']
#         # user_role_fk = rs['user_role_fk']
#         # designation = rs['designation']
#         # company_name = rs['company_name']
#         # sector = rs['sector']
#         # employer_type = rs['employer_type']
#         # website_url = rs['website_url']
#         # is_role_filled = rs["is_role_filled"]
#         # candidate_id = rs["hired_candidate_id"]
#         # job_status = "Closed"
#         # feedback = rs["feedback"]
        # created_at = str(rs['created_at'])
        # date_format = "%Y-%m-%d %H:%M:%S"
        # date_object = datetime.strptime(created_at, date_format)
        # timestamp = int(time.mktime(date_object.timetuple()))
        # random_id = uuid.uuid4().hex
#         # try:
#         #     event_name = "Close Job Post"
#         #     event_properties = {   
#         #         "event": event_name,
#         #         "properties": { 
#         #             "time": timestamp,
#         #             'distinct_id' : email_id, 
#         #             "$distinct_id": email_id,
#         #             "$insert_id": random_id,
#         #             "$os": "Linux",
#         #             '$time': timestamp,         
#         #             'Email' : email_id,
#         #             'Job Title' : job_title,
#         #             'Job Status' : job_status,
#         #             'City' : city,
#         #             'Country' : country,
#         #             'Is Role Filled' : is_role_filled,
#         #             'Candidate ID' : candidate_id,
#         #             'Feedback' : feedback,
#         #             'Message' : f"Status of '{job_title}' updated to {job_status}."
#         #         }
#         #     }
        # try:
        #     event_name = "Create Job Post"
        #     event_properties = {   
        #         "event": event_name,
        #         "properties": { 
        #             "time": timestamp,
        #             'distinct_id' : email_id, 
        #             "$distinct_id": email_id,
        #             "$insert_id": random_id,
        #             "$os": "Linux",
        #             '$time': timestamp,         
        #             'Email' : email_id,
        #             'Job Title' : job_title,
        #             'Job Type' : job_type,
        #             'Workplace Type' : workplace_type,
        #             'Work Schedule' : work_schedule,
        #             'Specialisation' : specialisation,
        #             'Job City' : city,
        #             'Job Country' : country,
        #             'Job Timezone' : timezone,
        #             'Post Created Status' : "Success"
        #         }
        #     }
        #     rslt_arr.append(event_properties)
#             # event_name = "Job Post Creation"
#             # mixpanel_store_events(email_id,event_name,event_properties)
#             # user_properties = {
#             #                 '$distinct_id' : email_id,
#             #                 '$distinct_id' : email_id,
#             #                 '$last_seen': timestamp,
#             #                 '$city' : city,
#             #                 '$country' : country,
#             #                 '$email'   : email_id,
#             #                 '$first_name'    : first_name,
#             #                 '$last_name' : last_name,
#             #                 'Organization Name' : company_name,
#             #                 "Employer's Title" : designation,
#             #                 'Sector' : sector,
#             #                 'Organization Type' : employer_type,
#             #                 'Website' : website_url,
#             #                 'Signup Mode' : login_mode,
#             #                 'User plan' :  pricing_category
#             #             }
#             # rslt_arr.append(user_properties)
    #     except Exception as e:  
    #         print("Error in employer create post mixpanel_event_log : %s",str(e))
    # with open("mixpanel_job_prop.json", "a") as f:
    #         json.dump(rslt_arr, f, indent=4)  # Adding indent=4 for pretty print
    #         f.write("\n")

    # return "true"

# def track_prof_users():
#     query = "select u.email_id, u.city, u.country, u.first_name, u.last_name, u.login_mode, u.pricing_category, u.user_role_fk, u.created_at from users u where user_role_fk = 3"
#     values = ()
#     rslt = execute_query(query, values)
#     rslt_arr = []
#     for rs in rslt:
#         email_id = rs['email_id']
#         first_name = rs['first_name']
#         last_name = rs['last_name']
#         city = rs['city']
#         country = rs['country']
#         login_mode = rs['login_mode']
#         pricing_category = rs['pricing_category']
#         user_role_fk = rs['user_role_fk']
#         created_at = str(rs['created_at'])
#         date_format = "%Y-%m-%d %H:%M:%S"
#         date_object = datetime.strptime(created_at, date_format)
#         timestamp = int(time.mktime(date_object.timetuple()))
#         try:
#             # event_properties = {    
#             #         '$distinct_id' : email_id, 
#             #         '$time': int(time.mktime(datetime.now().timetuple())),
#             #         '$os' : platform.system(),          
#             #         'Email' : email_id,
#             #         'Post Title' : title,
#             #         'URL' : url,
#             #         'Post Created Status' : "Success"
#             #     }
            
#             # event_name = "Partner Post Creation"
#             # mixpanel_store_events(email_id,event_name,event_properties)
#             user_properties = {
#                             '$distinct_id' : email_id,
#                             '$distinct_id' : email_id,
#                             '$last_seen': timestamp,
#                             '$city' : city,
#                             '$country' : country,
#                             '$email'   : email_id,
#                             '$first_name'    : first_name,
#                             '$last_name' : last_name,
#                             'Signup Mode' : login_mode,
#                             'User plan' :  pricing_category
#                         }
#             rslt_arr.append(user_properties)
#         except Exception as e:  
#             print("Error in professional mixpanel_event_log : %s",str(e))
#     with open("mixpanel_professional_prop.json", "a") as f:
#             json.dump(rslt_arr, f, indent=4)  # Adding indent=4 for pretty print
#             f.write("\n")

#     return "True"
# import csv

# def get_professional_profile_detail():
#     try:       
#         query = "select user_id from users where email_active = 'Y' and user_role_fk=3;"
#         values = ()
#         professional_id_list = execute_query(query, values)
#         # professional_id_list = [100003,100007]
#         with open("Live_Professional_Data.csv", mode="w", newline="") as file:
#             writer = csv.writer(file)
#             for id in professional_id_list:
#                 professional_id = id['user_id']
#                 profile_percentage = show_percentage(professional_id)
#                 query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.country_code, u.contact_number, u.country, u.state, u.city, u.pricing_category, p.about, p.professional_resume, p.show_to_employer, p.upload_date, p.preferences, p.video_url, p.expert_notes, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level,pl.id AS language_id,pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN   professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year = 'Present' THEN 1 ELSE 0 END DESC, pe.end_year DESC, pe.end_month DESC"
#                 values = (professional_id,)
#                 profile_result = execute_query(query, values)
#                 profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
#                 intro_video_name = replace_empty_values1(profile_result[0]['video_url'])                
#                 s3_pic_key = s3_picture_folder_name+str(profile_image_name)
#                 s3_video_key = s3_intro_video_folder_name+str(intro_video_name)
            
#                 profile_dict = {
#                     'Professional_id' :replace_empty_values1(profile_result[0]['user_id']),
#                     'first_name': replace_empty_values1(profile_result[0]['first_name']),
#                     'last_name': replace_empty_values1(profile_result[0]['last_name']),                                        
#                     'email_id': replace_empty_values1(profile_result[0]['email_id']),
#                     'country_code' : replace_empty_values1(profile_result[0]['country_code']),
#                     'contact_number': replace_empty_values1(profile_result[0]['contact_number']),
#                     'city': replace_empty_values1(profile_result[0]['city']),
#                     'country': replace_empty_values1(profile_result[0]['country']),
#                     'profile_image': s3_pic_key,
#                     'resume_name': replace_empty_values1(profile_result[0]['professional_resume']),
#                     'about': replace_empty_values1(profile_result[0]['about']),
#                     'preferences': replace_empty_values1(profile_result[0]['preferences']),
#                     'video_name': s3_video_key,
#                     'pricing_category': replace_empty_values1(profile_result[0]['pricing_category']),
#                     'expert_notes': replace_empty_values1(profile_result[0]['expert_notes']),
#                     'profile_percentage': profile_percentage
#                 }

#                 # Grouping experience data
#                 experience_set = set()
#                 experience_list = []
#                 for exp in profile_result:
#                     if exp['experience_id'] is not None:
#                         exp_tuple = (exp['experience_id'], exp['company_name'], exp['job_title'], exp['experience_start_year'], exp['experience_end_year'], exp['job_description'], exp['job_location'])
#                         if exp_tuple not in experience_set:
#                             experience_set.add(exp_tuple)
#                             experience_list.append({
#                                 'id': exp['experience_id'],
#                                 'company_name': replace_empty_values1(exp['company_name']),
#                                 'job_title': replace_empty_values1(exp['job_title']),
#                                 'start_date': replace_empty_values1(exp['experience_start_year']),                                
#                                 'end_date': replace_empty_values1(exp['experience_end_year']),                             
#                                 'job_description': replace_empty_values1(exp['job_description']),
#                                 'job_location': replace_empty_values1(exp['job_location'])
#                             })

#                 # profile_dict['experience'] = experience_list

#                 # Grouping education data
#                 education_set = set()
#                 education_list = []
#                 for edu in profile_result:
#                     if edu['education_id'] is not None:
#                         edu_tuple = (edu['education_id'], edu['institute_name'], edu['degree_level'], edu['specialisation'],
#                                     edu['education_start_year'], edu['education_end_year'], edu['institute_location'])
#                         if edu_tuple not in education_set:
#                             education_set.add(edu_tuple)
#                             education_list.append({
#                                 'id': edu['education_id'],
#                                 'institute_name': replace_empty_values1(edu['institute_name']),
#                                 'degree_level': replace_empty_values1(edu['degree_level']),
#                                 'specialisation': replace_empty_values1(edu['specialisation']),
#                                 'start_date': replace_empty_values1(edu['education_start_year']),                                
#                                 'end_date': replace_empty_values1(edu['education_end_year']),
#                                 'institute_location': replace_empty_values1(edu['institute_location'])
#                             })

#                 # profile_dict['education'] = education_list

#                 # Grouping skills data
#                 skills_set = set()
#                 skills_list = []
#                 for skill in profile_result:
#                     if skill['skill_id'] is not None:
#                         skill_tuple = (skill['skill_id'], skill['skill_name'], skill['skill_level'])
#                         if skill_tuple not in skills_set:
#                             skills_set.add(skill_tuple)
#                             skills_list.append({
#                                 'id': skill['skill_id'],
#                                 'skill_name': replace_empty_values1(skill['skill_name']),
#                                 'skill_level': replace_empty_values1(skill['skill_level'])
#                             })

#                 # profile_dict['skills'] = skills_list

#                 # Grouping languages data
#                 languages_set = set()
#                 languages_list = []
#                 for lang in profile_result:
#                     if lang['language_id'] is not None:
#                         lang_tuple = (lang['language_id'], lang['language_known'], lang['language_level'])
#                         if lang_tuple not in languages_set:
#                             languages_set.add(lang_tuple)
#                             languages_list.append({
#                                 'language_known': replace_empty_values1(lang['language_known']),
#                                 'id':  lang['language_id'],
#                                 'language_level': replace_empty_values1(lang['language_level'])
#                         })

#                 # profile_dict['languages'] = languages_list
#                 # Grouping additional info data
#                 additional_info_set = set()
#                 additional_info_list = []
#                 for info in profile_result:
#                     if info['additional_info_id'] is not None:
#                         info_tuple = (info['additional_info_id'], info['additional_info_title'], info['additional_info_description'])
#                         if info_tuple not in additional_info_set:
#                             additional_info_set.add(info_tuple)
#                             additional_info_list.append({
#                             'id': info['additional_info_id'],
#                             'title': replace_empty_values1(info['additional_info_title']),
#                             'description': replace_empty_values1(info['additional_info_description'])
#                         })

#                 # profile_dict['additional_info'] = additional_info_list
#                 # Grouping social link data
#                 social_link_set = set()
#                 social_link_list = []
#                 for link in profile_result:
#                     if link['social_link_id'] is not None:
#                         link_tuple = (link['social_link_id'], link['social_link_title'], link['social_link_url'])
#                         if link_tuple not in social_link_set:
#                             social_link_set.add(link_tuple)
#                             social_link_list.append({
#                             'title': replace_empty_values1(link['social_link_title']),
#                             'id':  link['social_link_id'],
#                             'url': replace_empty_values1(link['social_link_url'])
#                         })

#                 # profile_dict['social_link'] = social_link_list

#                 experience_list1 = [
#                 {
#                     'company_name': replace_empty_values1(exp['company_name']),
#                     'job_title': replace_empty_values1(exp['job_title']),
#                     'start_date': replace_empty_values1(exp['experience_start_year']),
#                     'end_date': replace_empty_values1(exp['experience_end_year']),
#                     'job_description': replace_empty_values1(exp['job_description']),
#                     'job_location': replace_empty_values1(exp['job_location'])
#                 }
#                 for exp in profile_result if exp['experience_id'] is not None
#                 ]
                
#                 education_list1 = [
#                     {
#                         'institute_name': replace_empty_values1(edu['institute_name']),
#                         'degree_level': replace_empty_values1(edu['degree_level']),
#                         'specialisation': replace_empty_values1(edu['specialisation']),
#                         'start_date': replace_empty_values1(edu['education_start_year']),
#                         'end_date': replace_empty_values1(edu['education_end_year']),
#                         'institute_location': replace_empty_values1(edu['institute_location'])
#                     }
#                     for edu in profile_result if edu['education_id'] is not None
#                 ]

#                 skills_list1 = [
#                     {
#                         'skills_name': replace_empty_values1(sk['skill_name']),
#                         'skill_level': replace_empty_values1(sk['skill_level'])
#                     }
#                     for sk in profile_result if skill['skill_id'] is not None
#                 ]

#                 languages_list1 = [
#                     {
#                         'language_known': replace_empty_values1(lang['language_known']),
#                         'language_level': replace_empty_values1(lang['language_level'])
#                     }
#                     for lang in profile_result if lang['language_id'] is not None
#                 ]

#                 additional_info_list1 = [
#                     {
#                         'title': replace_empty_values1(i['additional_info_title']),
#                         'description': replace_empty_values1(i['additional_info_description'])
#                     }
#                     for i in profile_result if info['additional_info_id'] is not None
#                 ]

#                 social_link_list1 = [
#                     {
#                         'social_link_title': replace_empty_values1(sclink['social_link_title']),
#                         'social_link_url': replace_empty_values1(sclink['social_link_url'])
#                     }
#                     for sclink in profile_result if link['social_link_id'] is not None
#                 ]

#                 writer.writerow([f"Professional ID: {professional_id}"])
#                 writer.writerow(profile_dict.keys())  # Column headers
#                 writer.writerow(profile_dict.values())

#                 writer.writerow(["Experience:"])
#                 for experience in experience_list:
#                     writer.writerow(experience.values())

#                 writer.writerow(["Education:"])
#                 for education in education_list:
#                     writer.writerow(education.values())

#                 writer.writerow(["Skills:"])
#                 for skill in skills_list:
#                     writer.writerow(skill.values())

#                 # Languages section
#                 writer.writerow(["Languages:"])
#                 for language in languages_list:
#                     writer.writerow(language.values())

#                 # Additional Info section
#                 writer.writerow(["Additional Information:"])
#                 for info in additional_info_list:
#                     writer.writerow(info.values())

#                 # Social Links section
#                 writer.writerow(["Social Links:"])
#                 for link in social_link_list:
#                     writer.writerow(link.values())
#                 writer.writerow([])
#                 writer.writerow([])

#         result_json = api_json_response_format(True,"Details fetched successfully!",0,profile_dict)
#     except Exception as error:
#         print(error)        
#         result_json = api_json_response_format(False,str(error),500,{})        
#     finally:        
#         return result_json

def get_training_data():
    try:
        result_json = {}
        token_result = get_user_token(request)                                        
        if token_result["status_code"] == 200:                                
            user_data = get_user_data(token_result["email_id"])        
            if user_data["is_exist"]:
                if user_data["user_role"] == "professional" or user_data["user_role"] == "admin":
                    # user_id = user_data['user_id']
                    query = "SELECT `id`,`user_id`,`image`, `title`, `title_description`, `type_of_offering`, `program_level`, `about_speaker`, `about_program`, `what_to_expect`, `speaker_name`, `registration_link`, `certificate_program`,`event_date_time`, `created_at`, `certification_name` FROM `training_table`"
                    values = ()
                    event_data_set = execute_query(query, values)
                    if event_data_set:
                        for e in event_data_set:
                            s3_sc_trailing_cover_pic_key = s3_trailing_cover_pic_folder_name + e['image']
                            e.update({"image": s3_sc_trailing_cover_pic_key})
                    result_json = api_json_response_format(True, "details fetched successfully", 200, {"data":event_data_set})

                else:
                    result_json = api_json_response_format(False,"Unauthorized user",401,{})
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
        else:
                result_json = api_json_response_format(False,"Invalid Token. Please try again",401,{})

                
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

# def mixpanel_professional_weekly_signup():
#     try:
#         result_json = {}
#         token_result = get_user_token(request)                                        
#         if token_result["status_code"] == 200:                                
#             user_data = get_user_data(token_result["email_id"])        
#             if user_data["is_exist"]:
#                 if user_data["user_role"] == "professional":
                    
#                     query = "SELECT COUNT(*) AS weekly_professional_signups FROM users WHERE user_role_fk = %s AND YEARWEEK(created_at, 1) = YEARWEEK(CURDATE(), 1);"
#                     values = (3,)
#                     res = execute_query(query, values)
#                     result_json = api_json_response_format(True, "Details fetched successfully!", 200, res)

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

    