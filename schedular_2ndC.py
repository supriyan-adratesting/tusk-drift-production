from datetime import timedelta, timezone
from datetime import time as dt_time
from threading import *
import threading
import logging
import os
import json
from datetime import datetime,date
import mysql.connector
from mysql.connector import Error
import boto3
from dotenv import load_dotenv
load_dotenv()
import schedule
from  openai import OpenAI
import meilisearch
from langchain_community.vectorstores import Meilisearch
from langchain_openai import OpenAIEmbeddings
from time import sleep
from meilisearch import Client
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail,Personalization,To,Cc
import razorpay
import sys

#need to check
from datetime import timedelta, timezone
from datetime import time as dt_time
from threading import *
import threading
import logging
import os
import json
from datetime import datetime,date
import mysql.connector
from mysql.connector import Error
import boto3
from dotenv import load_dotenv
load_dotenv()
import schedule
from  openai import OpenAI
import meilisearch
from langchain_community.vectorstores import Meilisearch
from langchain_openai import OpenAIEmbeddings
from time import sleep
from meilisearch import Client
from sendgrid import SendGridAPIClient
from flask import Flask, request, jsonify
from sendgrid.helpers.mail import Mail,Personalization,To,Cc
from src.controllers.professional.professional_process import professional_details_update, show_percentage, vector_search_init, process_quries
from src.models.aws_resources import S3_Client
from src.models.background_task import BackgroundTask
from flask_executor import Executor
from src import app
from PyPDF2 import PdfReader
from io import BytesIO
import docx2txt
from botocore.exceptions import ClientError
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import logging
from tqdm import tqdm
from botocore.config import Config
from mysql.connector import Error  # or pymysql.err.Error — adjust import
import time
import threading
import signal
import sys
import atexit


s3_obj = S3_Client()
executor = Executor(app)
background_runner = BackgroundTask(executor)
S3_BUCKET_NAME = os.environ.get('CDN_BUCKET')
s3_resume_folder_name = "professional/resume/"
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
PROFILE_INDEX = os.environ.get("PROFILE_INDEX")
MEILISEARCH_PROFESSIONAL_INDEX = os.environ.get("MEILISEARCH_PROFESSIONAL_INDEX")
MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
BUCKET_NAME = os.environ.get('PROMPT_BUCKET')

# Configure logging properly (instead of print)
logger = logging.getLogger(__name__)
class DocumentWrapper:
    def __init__(self, document):
        self.document = document
        self.page_content = json.dumps(document)  # Serialize the document to a JSON string
        self.metadata = document  # Use the document itself as metadata
        self.id = str(uuid.uuid4())  # Generate a unique ID for each document

    def to_dict(self):
        return self.document


is_shutting_down = False
class SchedulerThread(Thread):
    def __init__(self):
        # super().__init__()
        # self.db_con = None  
        # self.event = Event()
        # self.logger = logging.getLogger('CallSchedulerThread')
        # self.logger.setLevel(logging.DEBUG)
        # self.ch = logging.StreamHandler()
        # self.ch.setLevel(logging.DEBUG)
        # self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # self.ch.setFormatter(self.formatter)
        # self.logger.addHandler(self.ch)
        # self.max_retries = 2
        # self.retry_delay = 10
        super().__init__(daemon=True)
        self._stop_event = threading.Event()
        self.is_shutting_down = False   # ← instance variable
        self.logger = logging.getLogger('CallSchedulerThread')
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            self.ch = logging.StreamHandler()
            self.ch.setLevel(logging.DEBUG)
            self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            self.ch.setFormatter(self.formatter)
            self.logger.addHandler(self.ch)
        self.shutdown_event = threading.Event()
        # self.executor = ThreadPoolExecutor(max_workers=1)
        # self.event = threading.Event()
        self.db_con = None  
        self.max_retries = 2
        self.retry_delay = 10
        # Initialize database connection
        self._init_db_connection() 
        
        # self.outbound_user_checking_interval = int(os.environ.get('OUTBOUND_USER_CHECK_INTERVAL'))
        self.g_openai_completion_token_limit =int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))
        self.BUCKET_NAME = os.environ.get('PROMPT_BUCKET')
        self.S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
        self.S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
        self.AWS_REGION = os.environ.get('AWS_REGION')
        self.OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
        self.JOB_POST_INDEX = os.environ.get("JOB_POST_INDEX")
        self.g_resume_path = os.getcwd()
        self.g_prompt_file_path = os.getcwd()
        self.g_summary_model_name = os.environ.get('SUMMARY_MODEL_NAME')
        self.g_token_encoding_txt = os.environ.get('TOKEN_ENCODING_TEXT')
        self.g_openai_token_limit =int(os.environ.get('OPENAI_TOKEN_LIMIT'))
        self.s3_client_obj = self.get_s3_client()    
        self.sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
        self.mail_redirect_url = os.environ.get('MAIL_REDIRECT_URL')
        self.delay = 10
        self.razorpay_client_id = os.environ.get('RAZORPAY_KEY_ID')
        self.razorpay_client_secret = os.environ.get('RAZORPAY_KEY_SECRET')
        os.environ["OPENAI_API_KEY"] = self.OPENAI_API_KEY
        
    def _init_db_connection(self):
        retries = 0
        while retries < self.max_retries:
            try:
                self.DB_HOST = os.environ.get('MYSQL_HOST')
                self.DB_USERNAME = os.environ.get('MYSQL_USER')
                self.DB_PASSWD = os.environ.get('MYSQL_PASSWORD')
                self.DB_NAME = os.environ.get('MYSQL_DB')
                self.db_con = mysql.connector.connect(
                    host=self.DB_HOST,
                    user=self.DB_USERNAME,
                    password=self.DB_PASSWD,
                    database=self.DB_NAME,
                    autocommit=True
                )

                if self.db_con.is_connected():
                    self.logger.info("Connected to the database")
                    return
                else:
                    self.logger.info("Failed to connect to the database")

            except Error as e:
                retries += 1
                self.logger.error(f"Exception in DB connection: {e}")
                sleep(self.retry_delay)
    
    # def run(self):
    #     self.logger.info("Scheduler thread started")
    #     schedule.every(7).days.do(self.ai_recommended_jobs)
    #     schedule.every(5).minutes.do(self.update_razorpay_details)
    #     schedule.every(1).day.do(self.send_stripe_renewal_email)
    #     # schedule.every(1).minutes.do(self.process_refunds)
    #     schedule.every(1).day.do(self.update_job_status)
    #     schedule.every(1).day.do(self.update_partner_post_status)
    #     schedule.every(1).hour.do(self.update_sub_users)
    #     while True:
    #         try:
    #             self.event.wait(60)
    #             schedule.run_pending()                
    #         except Exception as error:
    #             self.logger.error(f"Scheduler thread error: {error}")


    def stop(self):
        self.logger.info("Stopping SchedulerThread...")
        # self.logger.info("Stopping SchedulerThread...")
        # self._stop_event.set()
        # if self.executor:
        #     self.executor.shutdown(wait=False)
        #     self.executor = None
        return self._stop_event.is_set()

    def stopped(self):
        return self._stop_event.is_set()
    
    def create_new_connection(self):
        return mysql.connector.connect(
            host=os.environ.get('MYSQL_HOST'),
            user=os.environ.get('MYSQL_USER'),
            password=os.environ.get('MYSQL_PASSWORD'),
            database=os.environ.get('MYSQL_DB'),
            autocommit=False
        )

    def run(self):
        self.logger.info("Scheduler thread started")
        current_time = datetime.now() 
        # print current time with timezone
        # print("Current Time: ", current_time.astimezone(timezone(timedelta(hours=5, minutes=30))))
        current_time = datetime.now(timezone.utc)
        ist_time = current_time.astimezone(
            timezone(timedelta(hours=5, minutes=30))
        )

        self.logger.info(f"Current UTC Time: {current_time}")
        self.logger.info(f"Current IST Time: {ist_time}")
        schedule.every(7).days.do(self.ai_recommended_jobs)
        schedule.every(5).minutes.do(self.update_razorpay_details)
        schedule.every(1).day.do(self.send_stripe_renewal_email)
        # schedule.every(1).minutes.do(self.process_refunds)
        schedule.every(1).day.do(self.update_job_status)
        schedule.every(1).day.do(self.update_partner_post_status)
        schedule.every(1).hour.do(self.update_sub_users)
        # schedule.every().day.at("04:40").do(self.resume_available_professional)
        # schedule.every().wednesday.at("15:35").do(self.resume_available_professional)
        global is_shutting_down   # ← Add this line
        # Create pool only when the thread actually starts working
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        self.logger.info("Thread pool created inside scheduler thread")
        self.logger.info("Thread pool created successfully inside scheduler thread")
        self.logger.info(f"Initial state - shutdown? {self.thread_pool._shutdown}")
        # Example: run resume_available_professional every 30 seconds
        while not self.stopped():
            try:
                if is_shutting_down or sys.is_finalizing():
                    self.logger.info("Shutting down — skipping scheduled jobs")
                    break
                # self.resume_available_professional()
                schedule.run_pending()
                # Wait before next run
                self._stop_event.wait(timeout=30)
                # time.sleep(60)

            except Exception:
                self.logger.exception("Scheduler thread error")

        self.logger.info("Scheduler thread stopped cleanly")
        # Cleanup inside the thread
        if self.thread_pool:
            try:
                self.thread_pool.shutdown(wait=True, cancel_futures=True)
                self.logger.info("Thread pool shut down cleanly from inside thread")
            except Exception:
                self.logger.exception("Thread pool shutdown failed")

    def api_json_response_format(status,message,error_code,data):
        result_json = {"success" : status,"message" : message,"error_code" : error_code,"data": data}
        return result_json
    
    def execute_query(self,query, values):
        retries = 0
        while retries < self.max_retries:
            try:
                if not self.db_con.is_connected():
                    self.logger.warning("Database connection lost. Reinitializing...")
                    self._init_db_connection()
                cursor = self.db_con.cursor(dictionary=True)
                cursor.execute(query, values)
                data = cursor.fetchall()
                cursor.close()
                return data  # Return data if query is successful
            except Error as e:
                retries += 1
                self.logger.warning(f"Query execution failed: {e}")
                if retries < self.max_retries:
                    sleep(self.retry_delay)
                else:
                    self.logger.error(f"Max retries reached: {query}")
            finally:
                if 'cursor' in locals():
                    cursor.close()
        
    def update_query(self, query, values):
        retries = 0
        while retries < self.max_retries:
            try:
                if not self.db_con.is_connected():
                    self.logger.warning("Database connection lost. Reinitializing...")
                    self._init_db_connection()

                cursor = self.db_con.cursor()
                cursor.execute(query, values)
                self.db_con.commit()

                row_count = cursor.rowcount
                cursor.close()
                return row_count if row_count > 0 else 1

            except Error as e:
                retries += 1
                self.logger.warning(f"Update query failed: {e}")
                if retries >= self.max_retries:
                    self.logger.error("Max retries reached for update query")
                    return -1
            finally:
                if 'cursor' in locals():
                    cursor.close()
    def resume_update_query(self, db_conn, query: str, values: tuple = None, many: bool = False) -> int:
        if values is None:
            values = ()

        retries = 0
        last_exception = None

        while retries <= self.max_retries:
            cursor = None
            try:
                if not db_conn.is_connected():
                    raise RuntimeError("DB connection lost")

                cursor = db_conn.cursor()

                if many:
                    cursor.executemany(query, values)
                else:
                    cursor.execute(query, values)
                db_conn.commit()

                return cursor.rowcount

            except Error as e:
                if db_conn:
                    db_conn.rollback() 
                last_exception = e
                retries += 1
                if retries > self.max_retries:
                    raise last_exception
                time.sleep(0.5 * retries)

            finally:
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass


    def get_s3_client(self):
        try:           
            s3_client__obj = boto3.client('s3', aws_access_key_id=self.S3_ACCESS_KEY, aws_secret_access_key=self.S3_SECRET_KEY,region_name=self.AWS_REGION)             
            # s3_client__obj = boto3.client('s3')         
            return s3_client__obj
        except Exception as error:
            print("error in get_s3_client(): ",str(error))

    def get_s3_resource(self):
        try:                        
            # Use the new temporary credentials
            s3_resource_obj = boto3.resource('s3', aws_access_key_id=self.S3_ACCESS_KEY, aws_secret_access_key=self.S3_SECRET_KEY,region_name=self.AWS_REGION)    
            # s3_resource_obj = boto3.resource('s3')         
            return s3_resource_obj
        except Exception as error:
            print("error in get_s3_resource(): ",str(error))

    def s3_exists(self,s3_bucket, s3_key):
        try:
            s3_cient = self.get_s3_client()
            s3_cient.head_object(Bucket=s3_bucket,Key=s3_key)
            return True
        except Exception as e:
            print("s3_exists error : "+str(e))
            return False

    def get_openai_summary(self,l_openai_api_key,req_prompt): 
        result = {}    
        global openai_api_key
                
        openai_api_key = l_openai_api_key
        OpenAI.api_key = openai_api_key    

        try:                 
            req_messages = [{"role": "user", "content": req_prompt}]
            response = self.process_openai_completion(req_messages,OpenAI.api_key)
            print(f"process_openai_completion response {response}")
            result["data_id"] = str(response.id)            
            result["summary"] = str(response.choices[0].message.content)
        except Exception as error:       
            print("Error in get_openai_summary(): "+str(error))
            result = self.api_json_response_format(False,str(error),500,{}) 
        finally:        
            return result

    def process_openai_completion(self,req_messages,openai_api_key):
        try:
            print(f"g_summary_model_name : {self.g_summary_model_name}, g_openai_completion_token_limit : {self.g_openai_completion_token_limit}, req_messages: {req_messages}")
            self.g_summary_model_name
            client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            return client.chat.completions.create(
                                        model=self.g_summary_model_name,
                                        messages=req_messages,
                                        temperature=0,
                                        max_tokens=self.g_openai_completion_token_limit,
                                        top_p=1,
                                        frequency_penalty=0,
                                        presence_penalty=0
                                    )
        except Exception as error:       
            print("Error in process_openai_completion(): "+str(error)) 

    def process_quries_search(self,openai_api_key,l_query_txt):
        self.g_resume_path
        self.g_openai_token_limit          
        result = {}
        try:
            if self.s3_exists(self.BUCKET_NAME,"job_recommend_prompt.json"):     
                s3_resource = self.get_s3_resource()
                obj = s3_resource.Bucket(self.BUCKET_NAME).Object("job_recommend_prompt.json")
                json_file_content = obj.get()['Body'].read().decode('utf-8')        
                prompt_json = json.loads(json_file_content)
                level_1 = prompt_json["level_1"]
                level_1_prompt = level_1["prompt"]      
                level_1_prompt = level_1_prompt.replace("{{data}}", "{{"+str(l_query_txt)+"}}")  
                openai_level_1_res = self.get_openai_summary(openai_api_key,level_1_prompt) 
                if not "error_code" in openai_level_1_res:
                    chatbot_level_1_text = openai_level_1_res["summary"]                     
                    del openai_level_1_res                          
                    result["is_error"] = False
                    result["result"] = chatbot_level_1_text
                else:
                    result["is_error"] = True
                    result["result"] = str(openai_level_1_res["message"] )                    
        except Exception as error:
            print(f"process_quries_search error : {error}")
            result["is_error"] = True
            result["result"] = str(error)  
        finally:        
            return result

    def format_profile(self,profile_data):
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
    
    def replace_empty_values1(self,data):
        if data == 'N/A' or data == None:
            data = ''
        return data

    def isUserExist(self,table,column,user_id):
        try:
            query = "select * from "+str(table)+"  where "+str(column)+" = %s"                
            values = (user_id,)        
            rs = self.execute_query(query,values)            
            if len(rs) > 0:                    
                return True       
            else:
                return False
        except Exception as error:
            print("Exception in isUserExist() ",error)
            return False    
    
    def get_profile_search(self,professional_id):
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
            profile_result = self.execute_query(query, values)

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
                profile_dict['first_name'] = self.replace_empty_values1(profile_result[0]['first_name'])
                profile_dict['last_name'] = self.replace_empty_values1(profile_result[0]['last_name'])
                profile_dict['email_id'] = self.replace_empty_values1(profile_result[0]['email_id'])
                profile_dict['contact_number'] = self.replace_empty_values1(profile_result[0]['contact_number'])
                profile_dict['city'] = self.replace_empty_values1(profile_result[0]['city'])
                profile_dict['about'] = self.replace_empty_values1(profile_result[0]['about'])

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
                            'institute_name': self.replace_empty_values1(row['institute_name']),
                            'degree_level': self.replace_empty_values1(row['degree_level']),
                            'specialisation': self.replace_empty_values1(row['specialisation']),
                            'education_start_month': self.replace_empty_values1(row['education_start_month']),
                            'education_start_year': self.replace_empty_values1(row['education_start_year']),
                            'education_end_month': self.replace_empty_values1(row['education_end_month']),
                            'education_end_year': self.replace_empty_values1(row['education_end_year']),
                            'institute_location': self.replace_empty_values1(row['institute_location'])
                        }
                        profile_dict['education'].append(education_data)

                    # Skills
                    if len(profile_dict['skills']) < MAX_SKILLS_ENTRIES and row['skill_id'] not in {skill['id'] for skill in profile_dict['skills']}:
                        skills_data = {
                            'id': row['skill_id'],
                            'skill_name': self.replace_empty_values1(row['skill_name']),
                            'skill_level': self.replace_empty_values1(row['skill_level'])
                        }
                        profile_dict['skills'].append(skills_data)

                    # Experience
                    if len(profile_dict['experience']) < MAX_EXPERIENCE_ENTRIES and row['experience_id'] not in {exp['id'] for exp in profile_dict['experience']}:
                        experience_data = {
                            'id': row['experience_id'],
                            'company_name': row['company_name'],
                            'job_title': row['job_title'],
                            'experience_start_month': self.replace_empty_values1(row['experience_start_month']),
                            'experience_start_year': self.replace_empty_values1(row['experience_start_year']),
                            'experience_end_month': self.replace_empty_values1(row['experience_end_month']),
                            'experience_end_year': self.replace_empty_values1(row['experience_end_year']),
                            'job_description': self.replace_empty_values1(row['job_description']),
                            'job_location': self.replace_empty_values1(row['job_location'])
                        }
                        profile_dict['experience'].append(experience_data)

                    # Languages
                    if len(profile_dict['languages']) < MAX_LANGUAGES_ENTRIES and row['language_id'] not in {lang['id'] for lang in profile_dict['languages']}:
                        languages_data = {
                            'id': row['language_id'],
                            'language_known': self.replace_empty_values1(row['language_known']),
                            'language_level': self.replace_empty_values1(row['language_level'])
                        }
                        profile_dict['languages'].append(languages_data)

                    # Social Links
                    if len(profile_dict['social_links']) < MAX_SOCIAL_LINKS_ENTRIES and row['social_link_id'] not in {link['id'] for link in profile_dict['social_links']}:
                        social_links_data = {
                            'id': self.replace_empty_values1(row['social_link_id']),
                            'title': self.replace_empty_values1(row['social_link_title']),
                            'url': self.replace_empty_values1(row['social_link_url'])
                        }
                        profile_dict['social_links'].append(social_links_data)

                    # Additional Info
                    if len(profile_dict['additional_info']) < MAX_ADDITIONAL_INFO_ENTRIES and row['additional_info_id'] not in {info['id'] for info in profile_dict['additional_info']}:
                        additional_info_data = {
                            'id': self.replace_empty_values1(row['additional_info_id']),
                            'title': self.replace_empty_values1(row['additional_info_title']),
                            'description': self.replace_empty_values1(row['additional_info_description'])
                        }
                        profile_dict['additional_info'].append(additional_info_data)

                profiles.append(self.format_profile(profile_dict))
            return profiles
        except Exception as error:
            print("Error:", error)
            return (False, str(error), 500, {})
        
    def get_user_data(self, email_id):    
        user_data = {}
        try:  
            query = """SELECT users.user_id, users.is_active, users.payment_status, users.login_status, users.login_mode, users.user_pwd, users.email_id,
                        users.profile_image, user_role.user_role, users.city, users.country, users.pricing_category, users.first_name, users.last_name, 
                        users.email_active, users.login_count, users.country_code, users.contact_number, users.existing_pricing_key
                        FROM users
                        INNER JOIN user_role ON user_role.role_id = users.user_role_fk
                        WHERE users.email_id = %s;"""
            values = (email_id,)
            rs = self.execute_query(query,values)                     
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

    def get_sub_user_data(self, email_id):    
        user_data = {}
        try:  
            query = """SELECT su.sub_user_id, su.user_id, su.is_active, su.profile_image, su.payment_status, su.login_status, su.login_mode, su.user_pwd, 
                        su.email_id, su.city, su.country, ur.user_role, su.pricing_category, su.first_name, su.last_name, su.email_active, su.login_count, 
                        su.country_code, su.phone_number, su.existing_pricing_key 
                        FROM sub_users su INNER JOIN user_role ur ON ur.role_id = su.role_id 
                        WHERE su.email_id = %s;"""
            values = (email_id,)
            rs = self.execute_query(query,values)                     
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
    
    def add_ai_recommendations(self, id_list, professional_id):
                cursor = self.db_con.cursor()
                flag = 0
                current_time = datetime.now()
                for id in id_list:
                    # job_id = job.get('id')
                    if id:
                        check_query = """
                            SELECT COUNT(job_id) as count
                            FROM job_activity
                            WHERE professional_id = %s AND job_id = %s
                        """
                        rs = self.execute_query(check_query, (professional_id, id))
                        if rs and rs[0]['count'] == 0:
                            insert_query = """
                                INSERT INTO ai_recommendation (professional_id, job_id, source, user_role_id, created_at)
                                VALUES (%s, %s, 'AI', 3, %s) ON DUPLICATE KEY UPDATE source = VALUES(source), created_at = VALUES(created_at)
                            """
                            cursor.execute(insert_query, (professional_id, id, current_time))
                            row_count = cursor.rowcount    
                            if row_count > 0:
                                flag = flag + 1
                return flag
    
    def send_sc_job_recmnd_email(self, email_id, body, redirect_url):
        try:
            from_addr = os.environ.get('SENDER_EMAIL')
            from_email = "2nd Careers <" + from_addr + ">"
            to_addr = email_id
            subject = "Job Recommendation from 2nd Careers"
            event_name = "2ndC AI Job recommendation"
            message = Mail(
                from_email=from_email,
                to_emails= to_addr,
                subject= subject,
                html_content=body)
            try:
                sg = SendGridAPIClient(self.sendgrid_api_key)
                sg.send(message)
            except Exception as error:
                print(error.message)
        except Exception as e:
            print("Error in send_job_recommended_email",str(e))

    def schedule_tasks(self, professional_id):
        try:
            self.logger.info("Scheduler tasks started")
            cursor = self.db_con.cursor()
            delete_query = "delete from ai_recommendation where professional_id = %s"
            values = (professional_id,)
            cursor.execute(delete_query, values)
            # count_ai_job_id_query = "SELECT DISTINCT ai_recommendation.job_id, job_post.job_status FROM ai_recommendation JOIN job_post ON ai_recommendation.job_id = job_post.id WHERE ai_recommendation.professional_id = %s AND ai_recommendation.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) AND ai_recommendation.job_id NOT IN ( SELECT job_id FROM sc_recommendation WHERE professional_id = %s ) GROUP BY ai_recommendation.job_id ORDER BY MAX(ai_recommendation.id) LIMIT 3;"
            # count_ai_job_id_values = (professional_id, professional_id, professional_id,)
            # count_ai_job_ids = self.execute_query(count_ai_job_id_query, count_ai_job_id_values)
            # final_job_id_list = []
            # if len(count_ai_job_ids) == 3:
            #     for i in count_ai_job_ids:
            #         if i['job_status'] == 'opened':
            #             final_job_id_list.append(i['job_id'])
            # else:
            id = professional_id
            data = self.get_profile_search(id)
            query = self.process_quries_search(self.OPENAI_API_KEY, data)
            vector_store = Meilisearch(
                client=meilisearch.Client(url=os.environ.get("MEILI_HTTP_ADDR")),
                embedding=OpenAIEmbeddings(deployment="text-embedding-ada-002"),
                embedders={"adra": {"source": "userProvided", "dimensions": 1536}},
                index_name=self.JOB_POST_INDEX,
            )
            if query["is_error"]:
                print(f"recommendation prompt query error : {query['result']}")
                return
            else:
                query_res = query["result"]
                print("job recommendation query : " + query_res)
            results = vector_store.similarity_search_with_score(query=query_res, embedder_name="adra")

            job_details1 = [json.loads(doc.page_content) for doc, _ in results]

            unique_dict = {item["id"]: item for item in job_details1}
            job_details1 = list(unique_dict.values())
            ai_recmnd_id_dict = {item["id"]: item['id'] for item in job_details1}
            ai_recmnd_id_list = list(ai_recmnd_id_dict.values())
            new_ai_rcmnd_id_list = []
            if ai_recmnd_id_list:
                for id in ai_recmnd_id_list:
                    query = 'select job_status from job_post where id = %s'
                    values = (id,)
                    job_status_details = self.execute_query(query, values)
                    if len(job_status_details) and job_status_details[0]['job_status'] == 'opened':
                        new_ai_rcmnd_id_list.append(id)
            flag = self.add_ai_recommendations(new_ai_rcmnd_id_list, professional_id)

            ai_job_id_query = "select job_id from ai_recommendation where professional_id = %s and job_id NOT IN (select job_id from job_activity where professional_id = %s) and job_id NOT IN (select job_id from sc_recommendation where professional_id = %s) ORDER BY id LIMIT 3"
            ai_job_id_values = (professional_id, professional_id, professional_id,)
            ai_job_ids = self.execute_query(ai_job_id_query, ai_job_id_values)

            final_job_id_list = [ i['job_id'] for i in ai_job_ids]

            email_query = "select email_id from users where user_id = %s"
            values = (professional_id,)
            email_id_dict = self.execute_query(email_query, values)
            email_id = email_id_dict[0]['email_id'] if email_id_dict else ''
            result_list = []
            if final_job_id_list:
                for id in final_job_id_list:
                    job_details_query = "SELECT jp.job_title, jp.employer_id, CONCAT(SUBSTRING(jp.job_overview, 1, 100), '...') AS job_overview, CONCAT(COALESCE(jp.city, ''), ', ', COALESCE(jp.country, '')) AS job_location, COALESCE(ep.company_name, 'N/A') AS company_name, u.email_id FROM job_post jp LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id LEFT JOIN users u on u.user_id = jp.employer_id WHERE jp.id = %s"
                    values = (id,)
                    job_details = self.execute_query(job_details_query, values)
                    if job_details:
                        result_list.append(job_details[0])
                if len(result_list) == 3:
                    index = open(os.getcwd() +"/schedular_templates/3_ai_job_recommendation.html",'r').read()
                elif len(result_list) == 2:
                    index = open(os.getcwd() +"/schedular_templates/2_ai_job_recommendation.html",'r').read()
                else:
                    index = open(os.getcwd() +"/schedular_templates/1_ai_job_recommendation.html",'r').read()
                body, redirect_url = '', ''
                if len(result_list) > 0:
                    temp_var = 0
                    for job in result_list:
                        temp_var = str(int(temp_var) + 1)
                        job_title = job['job_title']
                        company_name = job['company_name']
                        job_overview = job['job_overview']
                        job_location = job['job_location']
                        title_placeholder = f"{{title{temp_var}}}"
                        location_placeholder = f"{{location{temp_var}}}"
                        desc_placeholder = f"{{job_desc{temp_var}}}"
                        index = index.replace(title_placeholder, job_title if job_title else "")
                        index = index.replace(location_placeholder, job_overview if job_overview else "")
                        index = index.replace(desc_placeholder, job_location if job_location else "")
                    index = index.replace("{recommended_tab_link}",f"{self.mail_redirect_url}/professional/recommended_jobs?prof_id={professional_id}")
                    body = index
                    self.send_sc_job_recmnd_email(email_id, body, redirect_url)
            return True
        except Exception as error:
            self.logger.error(f"Scheduler tasks error: {error}")

    def ai_recommended_jobs(self):
        try:
            self.logger.info("ai recommended called")
            query = "select user_id from users where user_role_fk = 3 and profile_percentage > 60 and email_active = 'Y'"
            values = ()
            professional_id_list = self.execute_query(query, values)
            for id in professional_id_list:
                if id['user_id'] == 100018: #or id['user_id'] == 100475:# or id['user_id'] == 100356 or id['user_id'] == 100523 or id['user_id'] == 100524:
                    # threading.Timer(self.delay, self.schedule_tasks, args=(id['user_id'],)).start()
                    self.schedule_tasks(id['user_id'])
                    sleep(5)
        except Exception as error:
            self.logger.error(f"ai_recommended_jobs error: {error}")

    def store_job_details_in_meilisearch_cloud(self, documet):
        MEILISEARCH_JOB_INDEX = os.environ.get("MEILISEARCH_JOB_INDEX")
        MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
        MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
        try:
            # Store documents in Meilisearch
            index_name = MEILISEARCH_JOB_INDEX
            client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
            index = client.index(index_name)
            index.add_documents(documet)
            print("Job details successfully stored in Meilisearch Cloud.")
        except ValueError as ve:
            print(f"ValueError: {ve}")
        except Exception as error:
            print(f"An error occurred while storing job details in Meilisearch Cloud: {error}")

    def get_job_details(self, job_id):
        try:
            query = f"""SELECT jp.`id` AS job_id, ep.`employer_id`, ep.company_name, ep.designation, ep.company_description, ep.sector, ep.employer_type, ep.website_url, u.city, u.country,
                    u.pricing_category, jp.`job_title`, jp.`job_type`, jp.`job_overview`, jp.`job_desc`, jp.`responsibilities`, jp.`additional_info`, jp.`job_status`,
                    jp.`specialisation`, jp.`skills`, jp.`country` as job_country, jp.`state`, jp.`city` as job_city, jp.`work_schedule`, jp.`workplace_type`,  jp.`sector` as job_sector, jp.`number_of_openings`,
                    jp.`time_commitment`, jp.`timezone`, jp.`duration`, jp.`calendly_link`, jp.`share_url`, jp.`salary`, jp.`custom_notes`, jp.`currency`, jp.`benefits`,
                    jp.`required_resume`, jp.`required_cover_letter`, jp.`required_background_check`, jp.`required_subcontract`, jp.`receive_notification`, jp.`is_application_deadline`,
                    jp.`application_deadline_date`, jp.`is_paid`, jp.`days_left`,    jp.`calc_day`, jp.`is_active`, jp.`is_role_filled`, jp.`hired_candidate_id`, 
                    jp.`feedback`, 
                    DATE_FORMAT(jp.`created_at`, '%%Y-%%m-%%d %%H:%%i:%%s') AS created_at, 
                    DATE_FORMAT(jp.`updated_at`, '%%Y-%%m-%%d %%H:%%i:%%s') AS updated_at, 
                    DATE_FORMAT(jp.`closed_on`, '%%Y-%%m-%%d %%H:%%i:%%s') AS closed_on,
                    COALESCE(psq.questions, '[]') AS questions
                    FROM `job_post` jp
                    LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id
                    LEFT JOIN users u ON u.user_id = jp.employer_id
                    LEFT JOIN (SELECT sq.job_id, JSON_ARRAYAGG(sq.`custom_pre_screen_ques`) AS questions FROM pre_screen_ques sq GROUP BY sq.job_id) AS psq ON psq.job_id = jp.id where jp.id = {job_id}
                    GROUP BY jp.`id`, ep.`employer_id`, ep.company_name, ep.designation, ep.company_description, ep.sector, ep.employer_type, ep.website_url, u.pricing_category;"""
            # values = (job_id,)
            job_details_dict = self.execute_query(query, None)
            if job_details_dict:
                job_skills = job_details_dict[0]["skills"]
                if job_skills:
                    skills_list = [item.strip() for item in job_skills.split(",")]
                    job_details_dict[0].update({'skills':skills_list})
                else:
                    skills_list = []
            self.store_job_details_in_meilisearch_cloud(job_details_dict)
        except Exception as error:
            print(f"An error occurred while fetching job details from DB to store in Meilisearch: {error}")

    def update_job_status(self):
        try:
            self.logger.info("update_job_status called")
            cursor = self.db_con.cursor()
            fetch_query = "SELECT id, employer_id, job_title, job_status, created_at, DATEDIFF(CURDATE(), DATE(created_at)) AS days_since_posted FROM job_post where created_at > '2025-01-01 00:00:00' and job_status IN ('opened','paused', 'closed');"
            values = ()
            job_status_list = self.execute_query(fetch_query, values)
            for job in job_status_list:
                if job['days_since_posted'] == 41:
                    insert_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                    created_at = datetime.now()                    
                    values = (job['employer_id'],f"Your job post {job['job_title']} will expire soon.",created_at,)
                    cursor.execute(insert_query,values)
                if job['days_since_posted'] == 46:
                    update_query = "UPDATE job_post SET job_status = 'closed' WHERE id = %s"
                    values = (job['id'],)
                    try:
                        cursor.execute(update_query, values)
                        self.get_job_details(job['id'])
                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                        created_at = datetime.now()                    
                        values = (job['employer_id'],f"Your job post {job['job_title']} has been closed.",created_at,)
                        cursor.execute(query,values)
                    except Exception as e:
                        self.logger.error(f"Error updating job status: {e}")

            fetch_query = "SELECT id, employer_id, job_title, job_status, created_at, DATEDIFF(CURDATE(), DATE(created_at)) AS days_since_posted FROM job_post where created_at < '2025-01-01 00:00:00' and job_status IN ('opened','paused', 'closed');"
            values = ()
            job_status_list = self.execute_query(fetch_query, values)
            for job in job_status_list:
                if job['days_since_posted'] == 85:
                    try:
                        insert_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                        created_at = datetime.now()                    
                        values = (job['employer_id'],f"Your job post {job['job_title']} will expire soon.",created_at,)
                        cursor.execute(insert_query,values)
                    except Exception as e:
                        self.logger.error(f"Error in sending close job notification: {e}")
                if job['days_since_posted'] > 91:
                    try:
                        update_query = "UPDATE job_post SET job_status = 'closed' WHERE id = %s"
                        values = (job['id'],)
                        cursor.execute(update_query, values)
                        self.get_job_details(job['id'])
                    except Exception as e:
                        self.logger.error(f"Error in closing job status: {e}")
                if job['days_since_posted'] == 91:
                    update_query = "UPDATE job_post SET job_status = 'closed' WHERE id = %s"
                    values = (job['id'],)
                    try:
                        cursor.execute(update_query, values)
                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                        created_at = datetime.now()                    
                        values = (job['employer_id'],f"Your job post {job['job_title']} has been closed.",created_at,)
                        cursor.execute(query,values)
                    except Exception as e:
                        self.logger.error(f"Error closing job status at 90th day: {e}")
                payment_status_query = 'select payment_status from users where user_id = %s'
                values = (job['employer_id'],)
                payment_status_dict = self.execute_query(payment_status_query, values)
                if payment_status_dict:
                    if payment_status_dict[0]['payment_status'] != 'active':
                        get_job_count = "select count(id) from job_post where employer_id = %s and job_status = 'opened'"
                        values = (job['employer_id'],)
                        job_count_dict = self.execute_query(get_job_count, values)
                        if job_count_dict:
                            if job_count_dict[0]['count(id)'] == 0:
                                update_query = "UPDATE users SET pricing_category = %s, payment_status = %s, is_trial_started = %s WHERE user_id = %s"
                                values = ('Basic', 'canceled', 'Y', job['employer_id'],)
                                cursor.execute(update_query, values)
        except Exception as e:
            self.logger.error(f"Error updating job status: {e}")

    def update_partner_post_status(self):
        try:
            self.logger.info("update_partner_post_status called")
            cursor = self.db_con.cursor()
            fetch_query = "SELECT id, partner_id, title, post_status, created_at, DATEDIFF(CURDATE(), DATE(created_at)) AS days_since_posted FROM learning where created_at > '2025-01-01 00:00:00' and job_status IN ('opened','paused');"
            values = ()
            post_status_list = self.execute_query(fetch_query, values)
            for post in post_status_list:
                if post['days_since_posted'] == 86:
                    insert_query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                    created_at = datetime.now()                    
                    values = (post['partner_id'],f"Your ad {post['title']} will expire soon.",created_at,)
                    cursor.execute(insert_query,values)
                if post['days_since_posted'] == 91:
                    update_query = "UPDATE learning SET post_status = 'closed' WHERE id = %s"
                    values = (post['id'],)
                    try:
                        cursor.execute(update_query, values)
                        query = "INSERT INTO user_notifications (user_id, notification_msg, created_at) VALUES ( %s, %s, %s);"
                        created_at = datetime.now()                    
                        values = (post['partner_id'],f"Your ad {post['title']} has been closed.",created_at,)
                        cursor.execute(query,values)
                    except Exception as e:
                        self.logger.error(f"Error updating partner post status: {e}")
        except Exception as e:
            self.logger.error(f"Error updating partner post status: {e}")

    def update_sub_users(self):
        try:
            self.logger.info("update_sub_users called")
            cursor = self.db_con.cursor()
            get_employer_id = "select user_id from users where user_role_fk = %s and email_active = %s"
            employer_id_values = (2, 'Y',)
            employer_id_dict = self.execute_query(get_employer_id, employer_id_values)
            if employer_id_dict:
                for id in employer_id_dict:
                    employer_id = id['user_id']
                    query = 'select pricing_category as current_plan, old_plan from users where user_id = %s'
                    values = (employer_id,)
                    plan_details = self.execute_query(query, values)
                    if plan_details:
                        current_plan = plan_details[0]['current_plan']
                        old_plan = plan_details[0]['old_plan']
                        if (current_plan == 'Basic' and old_plan == 'Premium') or (current_plan == 'Basic' and old_plan == 'Platinum'):
                            update_assign_jobs = 'update assigned_jobs set user_id = %s where employer_id = %s'
                            update_assign_jobs_values = (employer_id, employer_id,)
                            cursor.execute(update_assign_jobs, update_assign_jobs_values)
                        elif ((current_plan == 'Premium' and old_plan == 'Platinum')):
                            # get_sub_users_count_query = "select ur.user_role, count(su.role_id) from sub_users su left join user_role ur on ur.role_id = su.role_id where su.user_id = %s group by su.role_id;"
                            # get_sub_users_count_values = (employer_id,)
                            # sub_users_count_dict = self.execute_query(get_sub_users_count_query, get_sub_users_count_values)
                            # sub_users_list = []
                            # if sub_users_count_dict:
                            sub_admin_count = "select ur.user_role, count(su.role_id) from sub_users su left join user_role ur on ur.role_id = su.role_id where su.user_id = %s and su.role_id = 9;"
                            recruiter_count = "select ur.user_role, count(su.role_id) from sub_users su left join user_role ur on ur.role_id = su.role_id where su.user_id = %s and su.role_id = 10;"
                            values = (employer_id,)
                            sub_admin_count_dict = self.execute_query(sub_admin_count, values)
                            recruiter_count_dict = self.execute_query(recruiter_count, values)
                            if sub_admin_count_dict:
                                sub_admin_count = sub_admin_count_dict[0]['count(su.role_id)']
                            else:
                                sub_admin_count = 0
                            if recruiter_count_dict:
                                recruiter_count = recruiter_count_dict[0]['count(su.role_id)']
                            else:
                                recruiter_count = 0
                            
                            if sub_admin_count > 1 or (sub_admin_count == 0 and recruiter_count > 1):
                                get_sub_user_id = 'select sub_user_id from sub_users where user_id = %s and role_id = %s order by created_at asc limit 1'
                                if (sub_admin_count == 0 and recruiter_count > 1):
                                    get_sub_user_values = (employer_id, 10,)
                                else:
                                    get_sub_user_values = (employer_id, 9,)
                                sub_user_id_dict = self.execute_query(get_sub_user_id, get_sub_user_values)
                                if sub_user_id_dict:
                                    sub_user_id = sub_user_id_dict[0]['sub_user_id']
                                    update_assign_jobs = 'update assigned_jobs set user_id = %s where employer_id = %s'
                                    update_assign_jobs_values = (employer_id, employer_id,)
                                    cursor.execute(update_assign_jobs, update_assign_jobs_values)

                                    delete_sub_users_query = 'delete from sub_users where user_id = %s and sub_user_id NOT IN (%s)'
                                    delete_sub_users_values = (employer_id, sub_user_id,)
                                    cursor.execute(delete_sub_users_query, delete_sub_users_values)
                    else:
                        print("Error in getting plan details")
            else:
                print("Employer id dict is empty.")
        except Exception as error:
            self.logger.error(f"Error in removing sub_users: {error}")
    
    def notify_plan_end(self,to_address, full_name, url):
        from_addr = os.environ.get('SENDER_EMAIL')
        from_email = "2nd Careers <" + from_addr + ">"
        to_addr = to_address
        query = "select current_period_start, current_period_end, pricing_category from users where email_id = %s"
        values = (to_address,)
        res = self.execute_query(query, values)
        index = open(os.getcwd()+"/templates/plan_upgrade.html",'r').read()
        # index = open("/home/applied-sw02/Documents/1_SC_SRC/Nov_29_Dev/2ndcareers-back-end/second_careers_project/templates/plan_upgrade.html", 'r').read()
        # index = index.replace("{plan}", plan if plan is not None else "")
        index = index.replace("{full_name}", full_name if full_name is not None else "")
        index = index.replace("{redirect_url}", url if url is not None else "")

        body = index
        subject = "Your Free Trial Ends Today!"
        message = Mail(
            from_email=from_email,
            to_emails= to_addr,
            subject=subject,
            html_content=body)
        try:
            sg = SendGridAPIClient(self.sendgrid_api_key)
            sg.send(message)
        except Exception as error:
            print("Error in notify_plan_end_email", str(error))
            print(error.message)

    def send_plan_end_email(self, email_id):
        try:
            cursor = self.db_con.cursor()
            query = 'select first_name, last_name from users where email_id = %s'
            values = (email_id,)
            data = cursor.execute(query, values)
            if len(data) > 0:
                first_name = data[0]['first_name']
                last_name = data[0]['last_name']
                full_name = first_name + " " + last_name
            else:
                full_name = ''
            redirect_url = f"https://devapp.2ndcareers.com/employer_dashboard/pricing-plan"
            self.notify_plan_end(email_id, full_name, redirect_url, "Trial End Notification")  
        except Exception as e:
            print(f"Exception in send_plan_end_email()",e)
        return "Email sent"

    def store_employer_details_in_meilisearch_cloud(self, documet):
        MEILISEARCH_EMPLOYER_INDEX = os.environ.get("MEILISEARCH_EMPLOYER_INDEX")
        MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
        MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
        try:
            # Store documents in Meilisearch
            index_name = MEILISEARCH_EMPLOYER_INDEX
            client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
            index = client.index(index_name)
            index.add_documents(documet)
            print("Employer details successfully stored in Meilisearch Cloud.")
        except ValueError as ve:
            print(f"ValueError: {ve}")
        except Exception as error:
            print(f"An error occurred while storing employer details in Meilisearch Cloud: {error}")

    def store_partner_details_in_meilisearch_cloud(self, documet):
        MEILISEARCH_PARTNER_INDEX = os.environ.get("MEILISEARCH_PARTNER_INDEX")
        MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
        MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
        try:
            # Store documents in Meilisearch
            index_name = MEILISEARCH_PARTNER_INDEX
            client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
            index = client.index(index_name)
            index.add_documents(documet)
            print("Partner details successfully stored in Meilisearch Cloud.")
        except ValueError as ve:
            print(f"ValueError: {ve}")
        except Exception as error:
            print(f"An error occurred while storing partner details in Meilisearch Cloud: {error}")

    def get_employer_details(self, employer_id):
        try:
            query = "SELECT u.user_id, u.user_role_fk, u.email_active, u.first_name, u.last_name, u.email_id, u.dob, u.country_code, u.contact_number, u.country, u.state, u.gender, u.company_code, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.subscription_id, u.is_trial_started, u.old_plan, u.existing_pricing_key, ep.company_name, ep.designation, ep.company_description, ep.sector, ep.employer_type, ep.website_url FROM users u LEFT JOIN employer_profile ep ON ep.employer_id = u.user_id WHERE u.user_role_fk = 2 and u.user_id = %s ORDER BY u.user_id;"
            values = (employer_id,)
            emp_details_dict = self.execute_query(query, values)
            self.store_employer_details_in_meilisearch_cloud(emp_details_dict)
        except Exception as error:
            print(f"An error occurred while fetching employer details from DB to store in Meilisearch: {error}")
    
    def get_partner_details(self, partner_id):
        try:
            query = """SELECT u.user_id, u.user_role_fk, u.email_active, u.first_name, u.last_name, u.email_id, u.dob, u.country_code, u.contact_number, u.country, u.state, u.gender, u.company_code, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.subscription_id, u.is_trial_started, u.old_plan, u.existing_pricing_key, 
                        pp.designation, pp.company_name, pp.company_description, pp.partner_type, pp.sector, pp.website_url, 
                        COALESCE(learn_json.learning_id, '[]') AS learning_id, COALESCE(learn_json.learning_title, '[]') AS learning_title, 
                        COALESCE(learn_json.learning_post_status, '[]') AS learning_post_status, 
                        COALESCE(learn_json.learning_description, '[]') AS learning_description 
                        FROM users u 
                        LEFT JOIN partner_profile pp ON pp.partner_id = u.user_id 
                        LEFT JOIN (SELECT partner_id, JSON_ARRAYAGG(id) AS learning_id,JSON_ARRAYAGG(title) AS learning_title, JSON_ARRAYAGG(post_status) AS learning_post_status, JSON_ARRAYAGG(detailed_description) AS learning_description FROM learning GROUP BY partner_id) AS learn_json ON learn_json.partner_id = u.user_id 
                        WHERE u.user_role_fk = 6 and u.user_id = %s;"""
            values = (partner_id,)
            partner_details_dict = self.execute_query(query, values)
            self.store_partner_details_in_meilisearch_cloud(partner_details_dict)
        except Exception as error:
            print(f"An error occurred while fetching partner details from DB to store in Meilisearch: {error}")

    def notify_renewal_will_end(self,to_address, full_name, plan_name, validity_year,validity_end, url, custom_subject):
        from_addr = os.environ.get('SENDER_EMAIL')
        from_email = "2nd Careers <" + from_addr + ">"
        to_addr = to_address

        if plan_name == 'Premium' and validity_year == 1:
            # template_path = "/templates/plan_will_renewal_premium_annual.html"
            # index = open(os.getcwd()+"/templates/plan_will_renewal.html",'r').read()
            # template_path = "/home/adrasw-sam/Documents/yesterday/dec25dev/2ndcareers-back-end/second_careers_project/templates/plan_will_renewal_premium_annual.html"
            # index = open("/home/adrasw-sam/Documents/yesterday/dec25dev/2ndcareers-back-end/second_careers_project/templates/plan_will_renewal_premium_annual.html",'r').read()
            index = open(os.getcwd()+"/templates/plan_will_renewal_premium_annual.html", 'r').read()
        elif plan_name == 'Premium' and validity_year == 2:
            # template_path = "/templates/plan_will_renewal_premium_biennial.html"
            # index = open("/home/adrasw-sam/Documents/yesterday/dec25dev/2ndcareers-back-end/second_careers_project/templates/plan_will_renewal_premium_biennial.html", 'r').read()
            index = open(os.getcwd()+"/templates/plan_will_renewal_premium_biennial.html", 'r').read()
        elif plan_name == 'Platinum' and validity_year == 1:
            # template_path = "/templates/plan_will_renewal_platinum_annual.html"
            index = open(os.getcwd()+"/templates/plan_will_renewal_platinum_annual.html", 'r').read()
        elif plan_name == 'Platinum' and validity_year == 2:
            # template_path = "/templates/plan_will_renewal_platinum_biennial.html"
            index = open(os.getcwd()+"/templates/plan_will_renewal_platinum_biennial.html", 'r').read()
        else:
            # template_path = "/templates/plan_will_renewal.html"
            # template_path = "/home/adrasw-sam/Documents/yesterday/dec25dev/2ndcareers-back-end/second_careers_project/templates/plan_will_renewal.html"
            # index = open(os.getcwd()+"/templates/plan_will_renewal.html",'r').read()
            # index = open("/home/adrasw-sam/Documents/yesterday/dec25dev/2ndcareers-back-end/second_careers_project/templates/plan_will_renewal.html", 'r').read()
        # index = open(os.getcwd()+template_path,'r').read()
        
            index = open(os.getcwd()+"/templates/plan_will_renewal.html",'r').read()
        # index = open("/home/applied-sw02/Documents/1_SC_SRC/Nov_29_Dev/2ndcareers-back-end/second_careers_project/templates/plan_upgrade.html", 'r').read()
        # index = index.replace("{plan}", plan if plan is not None else "")
        index = index.replace("{full_name}", full_name if full_name is not None else "")
        # index = index.replace("{redirect_url}", url if url is not None else "")
        # index = index.replace("{date}", validity_end)
        index = index.replace("{date}",validity_end.strftime("%d %b %Y") if validity_end else "")

        body = index
        subject = custom_subject
        message = Mail(
            from_email=from_email,
            to_emails= to_addr,
            subject=subject,
            html_content=body)
        try:
            sg = SendGridAPIClient(self.sendgrid_api_key)
            sg.send(message)
        except Exception as error:
            print("Error in notify_renewal_will_end", str(error))
            print(error.message)

    def send_renewal_email(self, email_id: str):
        try:
            cursor = self.db_con.cursor(dictionary=True)
            query = 'SELECT first_name, last_name, pricing_category, current_period_start, current_period_end FROM users WHERE email_id = %s'
            cursor.execute(query, (email_id,))
            data = cursor.fetchall()

            if data:
                full_name = f"{data[0]['first_name']} {data[0]['last_name']}"
            else:
                full_name = ''

            pricing_category = data[0]['pricing_category'] if data else ''
            current_period_end = data[0]['current_period_end'] if data else None
            current_period_start = data[0]['current_period_start'] if data else None

            validity_start_ts = current_period_start
            validity_end_ts = current_period_end

            validity_start = datetime.fromtimestamp(validity_start_ts)
            validity_end = datetime.fromtimestamp(validity_end_ts)

            validity_years = int(
                    round((validity_end - validity_start).days / 365.25)
                )

            validity_years = int(validity_years)

           
            if current_period_start and current_period_end:
                redirect_url = "https://devapp.2ndcareers.com/employer_dashboard/pricing-plan"
                self.notify_renewal_will_end(
                    email_id,
                    full_name,
                    pricing_category,
                    validity_years,
                    validity_end,
                    redirect_url,
                    "Your Subscription Is Renewing Soon"
                )

        except Exception as e:
            print("Exception in send_renewal_email()", e)

        return "Email sent"
    
    def process_refunds(self):
        try:
            razorpay_client = razorpay.Client(auth=(self.razorpay_key_id, self.razorpay_key_secret))

            self.logger.into("process_refunds called")
            query = "SELECT * FROM refund_requests WHERE status = %s"
            values = ('pending',)
            refund_requests = self.execute_query(query, values)
            for request in refund_requests:
                subscription_id = request['subscription_id']
                refundable_amount = request['refundable_amount']
                reason = request['reason']
                created_at = request['created_at']

                seven_days = 7 * 24 * 60 * 60
                # create refund only if request is after 7 days of subscription creation
                if (datetime.now() - created_at).total_seconds() == seven_days:
                    if subscription_id and refundable_amount > 0:
                        try:
                            subscription_data = razorpay_client.invoice.all({'subscription_id': subscription_id})

                            payment_id = subscription_data['items'][0]['payment_id']

                            refund_result = razorpay_client.refund.create({
                                    "payment_id": payment_id,
                                    "amount": refundable_amount * 100,  # Amount in paise
                                    "speed": "normal",
                                    "notes": {
                                        "reason": reason
                                    }
                                })
                            if refund_result and refund_result.get('status') == 'processed':
                                update_query = "UPDATE refund_requests SET status = %s, refund_id = %s, payment_id = %s, updated_at = %s WHERE id = %s"
                                update_values = ('processed', refund_result['id'], refund_result['payment_id'], datetime.now(), request['id'])
                                self.execute_query(update_query, update_values)
                        except Exception as e:
                            self.logger.error(f"Error processing refund for request ID {request['id']}: {e}")
        except Exception as error:
            print(f"An error occurred while processing refunds: {error}")

    # def update_stripe_details(self):
    #     try:
    #         query = "SELECT * FROM stripe_customers"
    #         values = ()
    #         user_details = self.execute_query(query, values)
    #         if user_details:
    #             email_list = [user['email'] for user in user_details]
    #             for email in email_list:
    #                 user_data = self.get_user_data(email)
    #                 if not user_data['is_exist']:
    #                     user_data = self.get_sub_user_data(email)
    #                     user_id = user_id_data['user_id']

    #                 subscription_id = user_data.get('subscription_id')
    #                 if subscription_id:
    #                     subscription = self.stripe_client.Subscription.retrieve(subscription_id)
    #                     status = active

    #                         current_period_end = datetime.fromtimestamp('current_period_end'], tz=timezone.utc)
    #                         current_time = datetime.now(timezone.utc)
    #                         pricing_category = user_data.get('pricing_category')
    #                     # Calculate the difference between current time and period end
    #                     THIRTY_DAYS = 30 * 24 * 60 * 60
    #                     if (current_time >= current_period_end - THIRTY_DAYS) and pricing_category != 'Basic':
    #                         self.notify_renewal_will_end(email, full_name, redirect_url, "Your Subscription Is Renewing Soon")
    #     except Exception as error:
    #         print(f"An error occurred while updating user's stripe details in DB: {error}")

    def send_stripe_renewal_email(self):
        try:
            self.logger.info("update stripe details")
            # print("update stripe details")
            query = "select user_id, email_id, payment_status, is_cancelled,current_period_end, current_period_start from users where payment_status = 'active'"
            values = ()
            user_details = self.execute_query(query, values)
            if not user_details:
                return
            if user_details:
                query = "select email_id from razorpay_customers"
                values = ()
                razorpay_details = self.execute_query(query, values)
                if razorpay_details:
                    # user_email = {u["email_id"] for u in user_details}
                    razorpay_email = {r["email_id"] for r in razorpay_details}

                    # Users NOT present in razorpay_customer
                    # not_in_razorpay = list(user_ids - razorpay_user_ids)
                    non_razorpay_users = [
                        u for u in user_details
                        if u["email_id"] not in razorpay_email
                        and u["is_cancelled"] == "N"
                    ]
                    if not non_razorpay_users:
                        return

                    # query = "select email_id, payment_status, current_period_start, current_period_end from users where email = %s limit 2"
                    # values = (non_razorpay_users,)
                    # res = self.execute_query(query, values)

                    today = datetime.utcnow().date()
                    MAIL_BEFORE_DAYS = 30

                    for user in non_razorpay_users:
                        # print(non_razorpay_users)
                        if not user["current_period_end"]:
                            continue
                        period_end = datetime.fromtimestamp(
                            user["current_period_end"],
                            tz=timezone.utc
                        ).date()

                        days_left = (period_end - today).days

                        if 0 < days_left == MAIL_BEFORE_DAYS:
                            print(f"Sending renewal email to {user['email_id']}")
                            self.send_renewal_email(
                                email_id=user["email_id"]
                            )
                    
                    
                print("updated successfully")

        except Exception as error:
            print(f"An error occurred while updating user's stripe details in DB: {error}")

            

                    # Further processing can be done here as needed
    def update_razorpay_details(self):
        try:
            self.logger.info("update_razorpay_details called")
            cursor = self.db_con.cursor()
            query = """select * from razorpay_customers where subscription_status IN (%s, %s, %s, %s)"""
            values = ('subscription.authenticated', 'subscription.activated', 'order.paid', 'trialing',)
            user_details = self.execute_query(query, values)

            if user_details:
                for user in user_details:
                    # Get is_renewal_email_sent for each individual user
                    is_renewal = user.get('is_renewal_email_sent')  # 'Y' or 'N'
                    user_data = self.get_user_data(user['email_id'])
                    if user_data:
                        user_role = user_data['user_role']
                    else:
                        user_data = self.get_sub_user_data(user['email_id'])
                        if user_data:
                            user_role = user_data['user_role']
                        else:
                            user_role = ''
                    email_id = user['email_id']
                    if user['current_period_end'] is not None:
                        current_time = datetime.now(timezone.utc)
                        period_end = datetime.fromtimestamp(user['current_period_end'], tz=timezone.utc)

                        pricing_category = user_data.get('pricing_category')
                        # Calculate the difference between current time and period end
                        THIRTY_DAYS = timedelta(days=30)

                        # if (current_time >= period_end - THIRTY_DAYS) and pricing_category != 'Basic':
                        #     print(f"Sending renewal email to {user['email_id']}")
                        #     self.notify_renewal_will_end(user['email_id'])
                        # print(f"Current Time: {current_time},email_id:{email_id} Period End: {period_end}, Pricing Category: {pricing_category}, Is Renewal Email Sent: {is_renewal}")
                        days_left = (period_end.date() - current_time.date()).days

                        # if (
                        #     period_end - THIRTY_DAYS <= current_time < period_end
                        #     and pricing_category != 'Basic' and is_renewal == 'N'
                        # ):
                        if (
                            days_left == 30
                            and pricing_category != 'Basic'
                            and is_renewal == 'N'
                        ):
                            print(f"Sending renewal email to {user['email_id']}")
                            self.send_renewal_email(user['email_id'])
                            
                            update_query = "UPDATE razorpay_customers SET is_renewal_email_sent = %s WHERE email_id = %s"
                            update_values = ('Y', user['email_id'],)
                            self.execute_query(update_query, update_values)

                        if current_time > period_end:
                            update_rp_query = "UPDATE razorpay_customers SET subscription_status = %s, payment_status = %s WHERE email_id = %s"
                            update_users_table = "UPDATE users SET payment_status = %s WHERE email_id = %s"
                            update_sub_users_table = "UPDATE sub_users SET payment_status = %s WHERE user_id = %s"
                            
                            get_user_id_query = "SELECT user_id FROM users WHERE email_id = %s"
                            user_id_list = self.execute_query(get_user_id_query, (user['email_id'],))
                            if user_id_list:
                                user_id = user_id_list[0]['user_id']
                            else:
                                user_id = None

                            if user['subscription_status'] == 'subscription.authenticated' or user['subscription_status'] == 'trialing':
                                values = ('subscription.trial_ended', 'trial_expired', user['email_id'])
                                user_table_values = ('trial_expired', user['email_id'])
                                sub_user_table_values = ('trial_expired', user_id)
                            elif user['subscription_status'] == 'subscription.activated' or user['subscription_status'] == 'order.paid':
                                values = ('subscription.ended', 'unpaid', user['email_id'])
                                user_table_values = ('unpaid', user['email_id'])
                                sub_user_table_values = ('unpaid', user_id)
                            
                            if user_role == 'employer':
                                update_user_plan = 'update user_plan_details set no_of_jobs = %s, total_jobs = %s where user_id = %s'
                                update_user_plan_values = (0, 0, user_id,)
                                cursor.execute(update_user_plan, update_user_plan_values)
                                self.get_employer_details(user_id)
                            elif user_role == 'partner':
                                update_user_plan = 'update user_plan_details set no_of_jobs = %s, total_jobs = %s, additional_jobs_count = %s where user_id = %s'
                                update_user_plan_values = (0, 0, 0, user_id,)
                                cursor.execute(update_user_plan, update_user_plan_values)
                                self.get_partner_details(user_id)
                            
                            cursor.execute(update_rp_query, values)
                            cursor.execute(update_users_table, user_table_values)
                            cursor.execute(update_sub_users_table, sub_user_table_values)
                    if user['subscription_status'] == 'trialing':
                        if period_end - current_time == timedelta(days=3):
                            self.send_plan_end_email(user['email_id'])
                            # self.send_renewal_email(user['email_id'])
        except Exception as error:
            print(f"An error occurred while updating user's razorpay details in DB: {error}")

    def fetch_users_with_resume(self,limit=None):
        """
        Fetch all users who have a professional resume and profile percentage <= 30.
        Shutdown-aware: returns early if the scheduler is stopping.
        """
        self.logger.info("Fetching users with resumes...")

        # Early exit if stopping
        if self.stopped() or is_shutting_down or sys.is_finalizing():
            self.logger.warning("Skipping fetch — scheduler is shutting down")
            return []

        try:
            query = """
            SELECT u.email_id, p.professional_id, p.professional_resume
            FROM users u
            JOIN professional_profile p ON p.professional_id = u.user_id
            WHERE p.professional_resume IS NOT NULL
            AND TRIM(p.professional_resume) <> ''
            AND p.resume_status = 'PENDING'
            AND u.profile_percentage <= 30
            AND u.user_role_fk = %s
            AND u.email_active = 'Y'
            ORDER BY p.professional_id
            LIMIT %s
            """
            values = (3, limit)

            # Execute the query
            res = self.execute_query(query, values)

            if not res:
                self.logger.info("No users found with professional resume and profile < 30%")
                return []

            # Check shutdown again after query
            if self.stopped():
                self.logger.warning("Shutdown detected after query — skipping batch")
                return []

            return res

        except Exception as e:
            if self.stopped():
                self.logger.info("fetch_users_with_resume interrupted due to shutdown")
            else:
                self.logger.error("Error fetching users with resumes", exc_info=True)
            return []


    def mark_processing(self, professional_ids):
        if not professional_ids:
            return

        placeholders = ",".join(["%s"] * len(professional_ids))

        query = f"""
        UPDATE professional_profile
        SET resume_status = 'PROCESSING'
        WHERE professional_id IN ({placeholders})
        """

        self.execute_query(query, tuple(professional_ids))

    def resume_available_professional(self):
        print("[*] Resume available profession function started")
        global is_shutting_down
        BATCH_SIZE = 60
        # if is_shutting_down or sys.is_finalizing():
        #     # return jsonify({"error": "Server shutting down"}), 503
        #     self.logger.warning("Server shutting down — skipping resume update")
        #     return
        # offset = 0
        total_processed = 0
        total_success = 0
        total_failed = 0
        total_s3_missing = 0
        batch_no = 1
        # s3_client = s3_obj.get_s3_client() 
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('S3_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('S3_SECRET_KEY'),
            region_name=os.environ.get('AWS_REGION')
        )
        while True:
            if is_shutting_down or sys.is_finalizing():
                self.logger.warning("Shutdown detected — stopping resume processing")
                break

            users = self.fetch_users_with_resume(limit=BATCH_SIZE)
            
            if not users:
                self.logger.info("No more users to process — finishing job")
                break
            
            self.logger.info(f"Batch {batch_no} started — {len(users)} users")

            # 🔹 Mark users as PROCESSING immediately (VERY IMPORTANT)
            professional_ids = [u["professional_id"] for u in users]
            self.mark_processing(professional_ids)

            # batch_number = (offset // BATCH_SIZE) + 1
            # self.logger.info(
            #     f"Batch {batch_number} started — {len(users)} users (total processed so far: {total_processed})"
            # )
            success_users = []
            failed_users = []
            s3_file_missing = []
            future_to_user = {}
            # users = request.json.get("users", [])
            # if not users:
            #     # return jsonify({"error": "No users provided"}), 400
            #     self.logger.warning("No users provided for resume processing")
            #     return
            try:
                for user in users:
                    if is_shutting_down or sys.is_finalizing():
                        self.logger.warning("Shutting down — skipping remaining tasks")
                        break
                    if self.thread_pool._shutdown or not self.thread_pool:
                        self.logger.warning("Threadexecutor already shutdown — skipping batch")
                        break
                    try:
                        self.logger.debug(f"Before submitting tasks - pool alive? {not self.thread_pool._shutdown}")
                        future = self.thread_pool.submit(self.process_one_resume, user, s3_client)
                        print("[*] Thread pool submitted")
                        future_to_user[future] = user
                    except RuntimeError as exc:
                        if "shutdown" in str(exc).lower():
                            self.logger.warning("Executor already shut down — stopping batch")
                            break
                        raise
                if not future_to_user:
                    self.logger.info("No tasks submitted this batch")
                    # return
                else:

                    
                    for future in as_completed(future_to_user):
                        print("[*] As completed called")
                        user = future_to_user[future]
                        prof_id = user.get("professional_id", "unknown")

                        try:
                            result = future.result()

                            if result.get("success", False):
                                success_users.append(prof_id)
                                print(f"[*] Resume processed successfully for professional_id: {prof_id}")
                            else:
                                failed_users.append(prof_id)
                                reason = result.get("reason", "").lower()
                                if "missing" in reason or "not found" in reason:
                                    s3_file_missing.append(prof_id)
                                print(f"[*] Resume processing failed for professional_id: {prof_id}, reason: {reason}")

                        except Exception as e:
                            self.logger.exception(f"Failed to process {prof_id}")
                            failed_users.append(prof_id)

                    self.logger.info(
                        f"Batch finished → "
                        f"One Batch success: {len(success_users):3d} | "
                        f"One Batch failed:  {len(failed_users):3d} | "
                        f"One Batch S3 missing: {len(s3_file_missing):3d} "
                    )
                total_processed += len(users)
                total_success += len(success_users)
                total_failed += len(failed_users)
                total_s3_missing += len(s3_file_missing)

            except Exception as e:
                self.logger.exception(f"Unexpected error in batch {batch_number}")

            batch_no += 1

        self.logger.info(
            f"Weekly resume processing completed → "
            f"Total users processed: {total_processed} | "
            f"Success: {total_success} | Failed: {total_failed} | S3 missing: {total_s3_missing}"
        )
 
    def _update_experience(self, db_conn,professional_id, experiences):
        if not experiences:
            return

        try:
            # self.begin_transaction()   # ← per section transaction

            if self.isUserExist("professional_experience", "professional_id", professional_id):
                self.resume_update_query(db_conn, "DELETE FROM professional_experience WHERE professional_id = %s", (professional_id,))

            now = datetime.now(timezone.utc)

            for exp in experiences:
                start_year = exp.get("Start_Year") or None
                if start_year == "": start_year = None
                end_year   = exp.get("End_Year")   or None
                if end_year == "":   end_year = None

                values = (
                    professional_id,
                    exp.get("Organization_Name", ""),
                    exp.get("Job_Title", ""),
                    start_year,
                    end_year,
                    exp.get("Start_Month", ""),
                    exp.get("End_Month", ""),
                    "Y" if exp.get("Currently_Working", False) else "N",
                    exp.get("Job_Description", ""),
                    exp.get("Work_Location", ""),
                    now
                )

                insert_query = """
                    INSERT INTO professional_experience 
                    (professional_id, company_name, job_title, start_year, end_year,
                    start_month, end_month, is_currently_working, job_description,
                    job_location, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                row_count = self.resume_update_query(db_conn, insert_query, values)
                if row_count != 1:
                    raise ValueError(f"Experience insert failed for {professional_id}")

        except Exception as e:
            logger.error(f"Experience update failed for {professional_id}", exc_info=True)
            raise   # let outer transaction know it failed

    def begin_transaction(self, db_conn):
        # self.conn.begin() or self.cursor.execute("START TRANSACTION")
        db_conn.start_transaction()

    def commit(self, db_conn):
        # self.conn.commit()
        db_conn.commit()

    def rollback(self, db_conn):
        # self.conn.rollback()
        db_conn.rollback()


    def _update_education(self,db_conn, professional_id, educations):
        if not educations:
            return

        # Delete old records
        if self.isUserExist("professional_education", "professional_id", professional_id):
            delete_query = "DELETE FROM professional_education WHERE professional_id = %s"
            self.resume_update_query(db_conn,delete_query, (professional_id,))

        from datetime import datetime

        for edu in educations:
            start_year = edu.get("Start_Year")
            end_year   = edu.get("End_Year") or edu.get("year_of_passing")

            if start_year == "":
                start_year = None
            if end_year == "":
                end_year = None

            values = (
                edu.get("Institute_Name", ""),
                edu.get("Location", ""),
                edu.get("Degree", ""),
                edu.get("Major", ""),
                start_year,
                edu.get("Start_Month", ""),
                edu.get("End_Month", ""),
                end_year,
                "Y" if edu.get("Is_Pursuing", False) else "N",
                datetime.now(timezone.utc),
                professional_id
            )

            insert_query = """
                INSERT INTO professional_education 
                (institute_name, institute_location, degree_level, specialisation,
                start_year, start_month, end_month, end_year, is_pursuing, created_at, professional_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            row_count = self.resume_update_query(db_conn, insert_query, values)
            # handle error if needed
            if row_count != 1:
                raise ValueError(f"Failed to insert education record for professional_id={professional_id}")

    def _update_skills(self, db_conn,professional_id, skills):
        if not skills:
            return

        # Delete old records
        if self.isUserExist("professional_skill", "professional_id", professional_id):
            delete_query = "DELETE FROM professional_skill WHERE professional_id = %s"
            self.resume_update_query(db_conn,delete_query, (professional_id,))

        # from datetime import datetime

        for skill_item in skills:
            values = (
                skill_item.get("Skill", ""),
                skill_item.get("Skill_Level", ""),
                datetime.now(timezone.utc),
                professional_id
            )

            insert_query = """
                INSERT INTO professional_skill 
                (skill_name, skill_level, created_at, professional_id)
                VALUES (%s, %s, %s, %s)
            """

            row_count = self.resume_update_query(db_conn,insert_query, values)
            if row_count != 1:
                raise ValueError(f"Failed to insert skills record for professional_id={professional_id}")

    def _update_languages(self, db_conn, professional_id, languages):
        """
        Update professional languages.
        Deletes old entries and inserts new valid ones.
        """
        if not languages:
            return

        # Delete existing records
        if self.isUserExist("professional_language", "professional_id", professional_id):
            delete_query = "DELETE FROM professional_language WHERE professional_id = %s"
            self.resume_update_query(db_conn, delete_query, (professional_id,))

        # from datetime import datetime

        for lang in languages:
            language_known = lang.get("Language", "").strip()
            language_level = lang.get("Language_Level", "").strip()

            # Skip empty language names
            if not language_known:
                continue

            values = (
                professional_id,
                language_known,
                language_level,
                datetime.now(timezone.utc)
            )

            insert_query = """
                INSERT INTO professional_language 
                (professional_id, language_known, language_level, created_at)
                VALUES (%s, %s, %s, %s)
            """

            row_count = self.resume_update_query(db_conn,insert_query, values)

            if row_count != 1:
                raise ValueError(f"Failed to insert language record for professional_id={professional_id}")
            # You may want to collect success/failure instead of overwriting result each time

        # Final response can be set outside if you want one message for all sections


    def _update_about(self, db_conn, professional_id, about_text):
        """
        Update the 'about' section in professional_profile.
        """
        if not about_text:  # or about_text.strip() == "" depending on your needs
            return

        # from datetime import datetime

        # Optional: clear first (but usually just update)
        # If you want to be sure it's reset when empty, but here we skip when empty
        update_query = """
            UPDATE professional_profile 
            SET about = %s, updated_at = %s 
            WHERE professional_id = %s
        """
        values = (about_text.strip(), datetime.now(timezone.utc), professional_id)

        row_count = self.resume_update_query(db_conn,update_query, values)

        if row_count != 1:
                raise ValueError(f"Failed to update about in professional_profile for professional_id={professional_id}")

        # Optional: check if row was affected
        # if row_count == 0:
        #     # maybe insert if no row exists, depending on your schema


    def _update_social_links(self, db_conn, professional_id, social_links):
        """
        Update social media links.
        Deletes old ones and inserts new non-empty URLs.
        Expects dict like {"LinkedIn": "https://...", "GitHub": "..."}
        """
        if not social_links or not isinstance(social_links, dict):
            return

        # Delete existing records
        if self.isUserExist("professional_social_link", "professional_id", professional_id):
            delete_query = "DELETE FROM professional_social_link WHERE professional_id = %s"
            self.resume_update_query(db_conn, delete_query, (professional_id,))

        # from datetime import datetime

        for title, url in social_links.items():
            url = (url or "").strip()
            if not url:
                continue

            title = (title or "").strip()
            if not title:
                continue

            values = (
                professional_id,
                title,
                url,
                datetime.now(timezone.utc)
            )

            insert_query = """
                INSERT INTO professional_social_link 
                (professional_id, title, url, created_at)
                VALUES (%s, %s, %s, %s)
            """

            row_count = self.resume_update_query(db_conn, insert_query, values)
            if row_count != 1:
                raise ValueError(f"Failed to update social in professional_social_link for professional_id={professional_id}")


    def _update_additional_info(self, db_conn, professional_id, additional_info):

        if not additional_info or not isinstance(additional_info, dict):
            return

        # ---- Delete old data ----
        delete_query = """
            DELETE FROM professional_additional_info
            WHERE professional_id = %s
        """
        self.resume_update_query(db_conn, delete_query, (professional_id,))

        insert_query = """
            INSERT INTO professional_additional_info
            (professional_id, title, description, created_at)
            VALUES (%s, %s, %s, %s)
        """

        now_utc = datetime.now(timezone.utc)

        for title, raw_value in additional_info.items():

            if not raw_value:
                continue

            descriptions = []

            # ---- CASE 1: Dict (YOUR CURRENT CASE) ----
            if isinstance(raw_value, dict):
                for k, v in raw_value.items():
                    if v and str(v).strip():
                        descriptions.append(f"{k}: {v}")

            # ---- CASE 2: List/Tuple ----
            elif isinstance(raw_value, (list, tuple)):
                for item in raw_value:
                    if isinstance(item, dict):
                        for k, v in item.items():
                            if v and str(v).strip():
                                descriptions.append(f"{k}: {v}")
                    else:
                        if str(item).strip():
                            descriptions.append(str(item).strip())

            # ---- CASE 3: String ----
            elif isinstance(raw_value, str):
                if raw_value.strip():
                    descriptions.append(raw_value.strip())

            # ---- Insert if anything valid exists ----
            if not descriptions:
                continue

            final_description = " | ".join(descriptions)

            values = (
                professional_id,
                title,
                final_description,
                now_utc
            )


            row_count = self.resume_update_query(db_conn, insert_query, values)
            if row_count != 1:
                raise ValueError(f"Failed to update professional_additional_info in professional_additional_info for professional_id={professional_id}")

    def show_percentage(self,professional_id):              
            if self.isUserExist("users","user_id",professional_id):
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
                result = self.execute_query(query, values)
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
            
    def vector_search_init(self,professional_id):
        try:
            profile = self.get_profile_search(professional_id)
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

    def get_professional_details(self, professional_id):
        try:
            query = """SELECT 
                        u.user_id,
                        u.user_role_fk,
                        u.email_active,
                        u.first_name,
                        u.last_name,
                        u.email_id,
                        u.dob,
                        u.country_code,
                        u.contact_number,
                        u.country,
                        u.state,
                        u.gender,
                        u.company_code,
                        u.city,
                        u.profile_percentage,
                        u.pricing_category,
                        u.is_active,
                        u.payment_status,
                        u.subscription_id,
                        u.is_trial_started,
                        u.old_plan,
                        u.existing_pricing_key,
                        p.professional_resume,
                        p.expert_notes,
                        p.about,
                        p.preferences,
                        p.video_url,
                        p.years_of_experience, 
                        p.functional_specification, 
                        p.industry_sector, 
                        p.sector, 
                        p.job_type, 
                        p.location_preference, 
                        p.mode_of_communication, 
                        p.willing_to_relocate,
                        u2.profile_image,
                        COALESCE(skill_json.skill_id, '[]') AS skill_id,
                        COALESCE(skill_json.skill_name, '[]') AS skill_name,
                        COALESCE(skill_json.skill_level, '[]') AS skill_level,
                        COALESCE(edu_json.education_id, '[]') AS education_id,
                        COALESCE(edu_json.institute_name, '[]') AS institute_name,
                        COALESCE(edu_json.degree_level, '[]') AS degree_level,
                        COALESCE(edu_json.specialisation, '[]') AS specialisation,
                        COALESCE(edu_json.education_start_month, '[]') AS education_start_month,
                        COALESCE(edu_json.education_start_year, '[]') AS education_start_year,
                        COALESCE(edu_json.education_end_month, '[]') AS education_end_month,
                        COALESCE(edu_json.education_end_year, '[]') AS education_end_year,
                        COALESCE(edu_json.institute_location, '[]') AS institute_location,
                        COALESCE(add_json.additional_info_id, '[]') AS additional_info_id,
                        COALESCE(add_json.additional_info_title, '[]') AS additional_info_title,
                        COALESCE(add_json.additional_info_description, '[]') AS additional_info_description,
                        COALESCE(social_json.social_link_id, '[]') AS social_link_id,
                        COALESCE(social_json.social_link_title, '[]') AS social_link_title,
                        COALESCE(social_json.social_link_url, '[]') AS social_link_url,
                        COALESCE(lang_json.language_id, '[]') AS language_id,
                        COALESCE(lang_json.language_known, '[]') AS language_known,
                        COALESCE(lang_json.language_level, '[]') AS language_level,
                        COALESCE(exp_json.experience_id, '[]') AS experience_id,
                        COALESCE(exp_json.company_name, '[]') AS company_name,
                        COALESCE(exp_json.job_title, '[]') AS job_title,
                        COALESCE(exp_json.job_description, '[]') AS job_description,
                        COALESCE(exp_json.job_location, '[]') AS job_location,
                        COALESCE(exp_json.experience_start_month, '[]') AS experience_start_month,
                        COALESCE(exp_json.experience_start_year, '[]') AS experience_start_year,
                        COALESCE(exp_json.experience_end_month, '[]') AS experience_end_month,
                        COALESCE(exp_json.experience_end_year, '[]') AS experience_end_year,
                        p.mode_of_communication AS raw_mode_of_communication
                    FROM users u
                    LEFT JOIN professional_profile p ON p.professional_id = u.user_id
                    LEFT JOIN sub_users u2 ON u2.user_id= u.user_id
                    LEFT JOIN (
                        SELECT ps.professional_id, JSON_ARRAYAGG(ps.id) AS skill_id, JSON_ARRAYAGG(ps.skill_name) AS skill_name, JSON_ARRAYAGG(ps.skill_level) AS skill_level
                        FROM professional_skill ps
                        GROUP BY ps.professional_id
                    ) AS skill_json ON skill_json.professional_id=u.user_id
                    LEFT JOIN (
                        SELECT ed.professional_id, JSON_ARRAYAGG(ed.id) AS education_id, JSON_ARRAYAGG(ed.institute_name) AS institute_name, JSON_ARRAYAGG(ed.degree_level) AS degree_level,JSON_ARRAYAGG(ed.start_month) AS education_start_month, 
                        JSON_ARRAYAGG(ed.start_year) AS education_start_year, JSON_ARRAYAGG(ed.end_month) AS education_end_month, JSON_ARRAYAGG(ed.end_year) AS education_end_year, JSON_ARRAYAGG(ed.specialisation) AS specialisation,
                        JSON_ARRAYAGG(ed.institute_location) AS institute_location
                        FROM professional_education ed
                        GROUP BY ed.professional_id
                    ) AS edu_json ON edu_json.professional_id = u.user_id
                    LEFT JOIN (
                        SELECT pe.professional_id, JSON_ARRAYAGG(pe.id) AS experience_id, JSON_ARRAYAGG(pe.company_name) AS company_name, JSON_ARRAYAGG(pe.job_title) AS job_title, JSON_ARRAYAGG(pe.job_description) AS job_description,
                        JSON_ARRAYAGG(pe.job_location) AS job_location, JSON_ARRAYAGG(pe.start_month) AS experience_start_month, JSON_ARRAYAGG(pe.start_year) AS experience_start_year,
                        JSON_ARRAYAGG(pe.end_month) AS experience_end_month, JSON_ARRAYAGG(pe.end_year) AS experience_end_year
                        FROM professional_experience pe 
                        GROUP BY pe.professional_id
                    ) AS exp_json ON exp_json.professional_id = u.user_id
                    LEFT JOIN (
                        SELECT pai.professional_id, JSON_ARRAYAGG(pai.id) AS additional_info_id, JSON_ARRAYAGG(pai.title) AS additional_info_title, JSON_ARRAYAGG(pai.description) AS additional_info_description
                        FROM professional_additional_info pai
                        GROUP BY pai.professional_id
                    ) AS add_json ON add_json.professional_id = u.user_id
                    LEFT JOIN(
                        SELECT psl.professional_id, JSON_ARRAYAGG(psl.id) AS social_link_id, JSON_ARRAYAGG(psl.title) AS social_link_title, JSON_ARRAYAGG(psl.url) AS social_link_url
                        FROM professional_social_link psl
                        GROUP BY psl.professional_id
                    ) AS social_json ON social_json.professional_id =  u.user_id
                    LEFT JOIN(
                        SELECT pl.professional_id, JSON_ARRAYAGG(pl.id) AS language_id, JSON_ARRAYAGG(pl.language_known) AS language_known, JSON_ARRAYAGG(pl.language_level) AS language_level
                        FROM professional_language pl
                        GROUP BY pl.professional_id
                    ) AS lang_json ON lang_json.professional_id = u.user_id
                    WHERE u.user_role_fk = 3 and u.user_id = %s;"""
            values = (professional_id,)
            professional_details_dict = self.execute_query(query, values)
            if professional_details_dict:
                mode_of_communication = professional_details_dict[0]["raw_mode_of_communication"]
                functional_specification = professional_details_dict[0]["functional_specification"]
                sector = professional_details_dict[0]["sector"]
                industry_sector = professional_details_dict[0]["industry_sector"]
                job_type = professional_details_dict[0]["job_type"]
                if mode_of_communication:
                    mode_of_communication_list = [item.strip() for item in mode_of_communication.split(",")]
                    professional_details_dict[0].update({'mode_of_communication':mode_of_communication_list})
                else:
                    mode_of_communication_list = []

                if functional_specification:
                    functional_specification_list = [item.strip() for item in functional_specification.split(",")]
                    professional_details_dict[0].update({'functional_specification':functional_specification_list})
                else:
                    functional_specification_list = []

                if industry_sector:
                    industry_sector_list = [item.strip() for item in industry_sector.split(",")]
                    professional_details_dict[0].update({'industry_sector':industry_sector_list})
                else:
                    industry_sector_list = []
                if sector:
                    sector_list = [item.strip() for item in sector.split(",")]
                    professional_details_dict[0].update({'sector':sector_list})
                else:
                    sector_list = []
                if job_type:
                    job_type_list = [item.strip() for item in job_type.split(",")]
                    professional_details_dict[0].update({'job_type':job_type_list})
                else:
                    job_type_list = []

            self.store_professional_details_in_meilisearch_cloud(professional_details_dict)
        except Exception as error:
            print(f"An error occurred while fetching partner details from DB to store in Meilisearch: {error}")


    def store_professional_details_in_meilisearch_cloud(self, documet):
        try:
            # Store documents in Meilisearch
            index_name = MEILISEARCH_PROFESSIONAL_INDEX
            client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
            index = client.index(index_name)
            index.add_documents(documet)
            print("Professional details successfully stored in Meilisearch Cloud.")
        except ValueError as ve:
            print(f"ValueError: {ve}")
        except Exception as error:
            print(f"An error occurred while storing professional details in Meilisearch Cloud: {error}")

    def s3_exists(self,s3_bucket, s3_key):
    
        try:
            s3_cient = s3_obj.get_s3_client()
            s3_cient.head_object(Bucket=s3_bucket,Key=s3_key)
            return True
        except Exception as e:
            print("s3_exists error : "+str(e))
            return False

    
    def process_quries(self,openai_api_key,l_query_txt):
        global g_resume_path
        global g_openai_token_limit  
        res_json = {}
        result = ""

        try:
            if self.s3_exists(BUCKET_NAME,"extraction_prompts.json"):     
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
                MAX_CHARS = 12000
                level_1_prompt = level_1_prompt.replace("{{resume_content}}", "{{"+str(l_query_txt)+"}}")  
                clean_text = l_query_txt.replace("\x00", "")
                clean_text = clean_text[:MAX_CHARS]
                # 🔹 Inject resume text safely
                level_1_prompt = level_1_prompt.replace("{{resume_content}}",clean_text)
                openai_level_1_res = self.get_openai_summary(openai_api_key,level_1_prompt) 
                if not "error_code" in openai_level_1_res:
                    print("no error in process_queries")
                    chatbot_level_1_text = openai_level_1_res["summary"].strip()
                    chatbot_level_1_text = chatbot_level_1_text.strip('```json').strip('```')
                    # chatbot_level_1_text = chatbot_level_1_text.strip('```')
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
    
    def parse_openai_output(self, out):
        print("Parse openai output")
        if isinstance(out, dict):
            return out

        if not isinstance(out, str):
            return {
                "is_error": True,
                "data": None,
                "message": f"Invalid OpenAI output type: {type(out)}"
            }

        try:
            if "#####" in out:
                json_part, flag_part = out.rsplit("#####", 1)
                flag = flag_part.strip()
                is_error = flag.lower() == "true"
            else:
                json_part = out
                is_error = False

            # Clean up possible markdown or extra spaces
            json_clean = json_part.strip().strip('```json').strip('```').strip()

            data = json.loads(json_clean)
            
            return {
                "is_error": is_error,
                "data": data,
                "message": None
            }

        except json.JSONDecodeError as jde:
            return {
                "is_error": True,
                "data": None,
                "message": f"JSON parse error: {str(jde)}. Raw: {out[:300]}..."
            }
        except Exception as e:
            return {
                "is_error": True,
                "data": None,
                "message": f"Failed to parse OpenAI response: {str(e)}. Raw: {out[:300]}..."
            }
        
    def update_resume_status(self, db_conn,professional_id, status):
        query = """
        UPDATE professional_profile
        SET resume_status = %s
        WHERE professional_id = %s
        """
        self.resume_update_query(db_conn, query, (status, professional_id))


    def process_one_resume(self, user: Dict[str, Any], s3_client) -> Dict[str, Any]:
        """
        Process a single user's resume in a thread-safe, atomic way.
        
        Important changes:
        - Uses shared s3_client passed from batch processor
        - Wraps all DB writes in a transaction → rollback on any failure
        - Uses UTC timestamps
        - Better empty/missing value handling
        - Logs failed inserts for debugging
        """
        db_conn = None
        professional_id = user.get("professional_id")
        resume_name     = user.get("professional_resume", "").strip()
        email_id        = user.get("email_id")

        result = {
            "professional_id": professional_id,
            "email_id": email_id,
            "status": "failed",
            "reason": None,
            "success": False
        }

         # ── Helper to safely update resume status if db_conn is None ──
        def safe_update_status(status):
            temp_conn = None
            try:
                if db_conn and db_conn.is_connected():
                    self.update_resume_status(db_conn, professional_id, status)
                else:
                    temp_conn = self.create_new_connection()
                    self.update_resume_status(temp_conn, professional_id, status)
                    self.commit(temp_conn)
            except Exception as e:
                logger.warning(f"Failed to update resume_status={status} for {professional_id}: {e}")
            finally:
                if temp_conn and temp_conn.is_connected():
                    temp_conn.close()

        if not professional_id or not resume_name:
            result["reason"] = "Missing professional_id or resume name"
            logger.warning(f"Skipping {professional_id}: {result['reason']}")
            # self.update_resume_status(db_conn,professional_id, "FAILED")
            safe_update_status("FAILED")
            return result

        try:
            s3_key = f"{s3_resume_folder_name}{resume_name}"

            # ── Download from S3 ───────────────────────────────────────
            try:
                response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                pdf_bytes = response["Body"].read()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    result["reason"] = "S3 file missing"
                    # self.update_resume_status(db_conn,professional_id, "S3_MISSING")
                    safe_update_status("S3_MISSING")
                    return result
                raise

            # ── Extract text ───────────────────────────────────────────
            text = self._extract_text_from_bytes(pdf_bytes, resume_name)
            if not text.strip():
                result["reason"] = "Extracted text is empty"
                safe_update_status("FAILED")   # 👈 IMPORTANT
                return result

            # ── Parse with OpenAI ──────────────────────────────────────
            raw_out = self.process_quries(OPENAI_API_KEY, text)
            out = self.parse_openai_output(raw_out)

            if out.get("is_error", True):
                result["reason"] = out.get("message", "OpenAI processing failed")
                safe_update_status("FAILED")   # 👈 IMPORTANT
                return result

            data = out.get("data", {})
            if not data:
                result["reason"] = "No structured data returned from OpenAI"
                safe_update_status("FAILED")   # 👈 IMPORTANT
                return result

            # ── Validate user role ─────────────────────────────────────
            user_data = self.get_user_data(email_id)
            if user_data.get("user_role") != "professional":
                result["reason"] = "User is not registered as professional"
                safe_update_status("FAILED")   # 👈 IMPORTANT
                return result

            # ── Start database transaction ─────────────────────────────
            try:
                db_conn = self.create_new_connection()  # THREAD SAFE

                self.begin_transaction(db_conn)   # ← Adjust method name if different (e.g. self.conn.begin())

                # Core profile updates
                self._update_experience(db_conn,professional_id, data.get("Work_Experience", []))
                self._update_education(db_conn,professional_id, data.get("Education", []))
                self._update_skills(db_conn,professional_id, data.get("Skills", []))
                self._update_languages(db_conn,professional_id, data.get("Languages", []))
                self._update_about(db_conn,professional_id, data.get("About", ""))
                self._update_social_links(db_conn,professional_id, data.get("Social_Links", {}))
                self._update_additional_info(db_conn,professional_id, data.get("Additional_Information", {}))

                # Final profile metadata
                now_utc = datetime.now(timezone.utc)
                # upload_date_str = now_utc.strftime("%Y/%m/%d")

                self.resume_update_query(
                    db_conn,
                    """
                    UPDATE professional_profile 
                    SET professional_resume = %s, 
                        updated_at = %s 
                    WHERE professional_id = %s
                    """,
                    (resume_name, now_utc, professional_id)
                )

                profile_percentage = self.show_percentage(professional_id)
                self.resume_update_query(
                    db_conn,
                    """
                    UPDATE users 
                    SET profile_percentage = %s,
                        updated_at = %s 
                    WHERE user_id = %s
                    """,
                    (profile_percentage, now_utc, professional_id)
                )

                # Optional post-processing steps
                try:
                    self.vector_search_init(professional_id)
                    self.get_professional_details(professional_id)
                except Exception as post_err:
                    logger.warning(f"Post-processing failed for {professional_id} but DB changes are safe", exc_info=True)

                self.commit(db_conn)   # ← Only reaches here if everything succeeded
                # self.update_resume_status(db_conn,professional_id, "SUCCESS")\
                safe_update_status("SUCCESS")
                result["status"] = "success"
                result["success"] = True
                result["reason"] = "Resume processed successfully"

            except Exception as db_err:
                self.rollback(db_conn)
                logger.exception(f"Database transaction failed and rolled back for professional_id={professional_id}")
                result["reason"] = f"Database update failed: {str(db_err)}"
                return result

        finally:
            if 'db_conn' in locals() and db_conn is not None:
                try:
                    if db_conn.is_connected():
                        db_conn.close()
                except Exception as close_err:
                    logger.warning(f"Failed to close db_conn: {close_err}")
        return result

    # ── Helper method (add this to your class if not already present) ──
    def _extract_text_from_bytes(self, file_bytes: bytes, filename: str) -> str:
        """Extract text from PDF or DOCX bytes."""
        filename_lower = filename.lower()
        try:
            if filename_lower.endswith(".pdf"):
                reader = PdfReader(BytesIO(file_bytes))
                pages_text = [page.extract_text() or "" for page in reader.pages]
                return "\n".join(pages_text)
            else:
                # assuming docx
                return docx2txt.process(BytesIO(file_bytes))
        except Exception as e:
            logger.warning(f"Text extraction failed for {filename}: {e}")
            return ""
        

ai_call_thread = SchedulerThread()
ai_call_thread.start()

try:
    # Keep the main thread alive forever
    while ai_call_thread.is_alive():
        time.sleep(60)  # or any large number
except KeyboardInterrupt:
    logger.info("Shutting down scheduler...")
    is_shutting_down = True
    ai_call_thread.stop()
    ai_call_thread.join(timeout=10)
    logger.info("Scheduler stopped")