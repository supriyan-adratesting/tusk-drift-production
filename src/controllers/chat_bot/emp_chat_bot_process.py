import os, json, time
from dotenv import load_dotenv
import meilisearch
from meilisearch import Client
from meilisearch.index import Index
from  openai import OpenAI
from langchain_community.vectorstores import Meilisearch
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from src.models.aws_resources import S3_Client
from datetime import datetime, timezone

from src.models.mysql_connector import execute_query,update_query,update_query_last_index, chat_bot_execute_query, chat_bot_update_query_last_index, run_query
from src.models.user_authentication import get_user_data,isUserExist,api_json_response_format, get_sub_user_data
# from src.controllers.chat_bot.chat_bot_process import s3_exists,get_openai_summary



s3_obj = S3_Client()

load_dotenv()


PROFILE_INDEX = os.environ.get("PROFILE_INDEX")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
BUCKET_NAME = os.environ.get('PROMPT_BUCKET')

g_resume_path = os.getcwd()
g_prompt_file_path = os.getcwd()
g_summary_model_name = os.environ.get('SUMMARY_MODEL_NAME')
g_openai_completion_token_limit =int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))

g_token_encoding_txt = os.environ.get('TOKEN_ENCODING_TEXT')
g_openai_token_limit = int(os.environ.get('OPENAI_MAX_TOKEN_LIMIT')) #15,000
s3_picture_folder_name = "professional/profile-pic/"
s3_intro_video_folder_name = "professional/profile-video/"
s3_sc_community_cover_pic_folder_name = "2ndcareers/cover-pic/"

emp_functions_def = [
    {
        "type": "function",
        "function":{
            "name": "schedulemeeting",
            "description": """This function extracts a client name, email and a meeting date and time from a transcript. Below is the script for your call. Follow it and in the same order:
    1. Extract the client name.
    2. Extract and format the email:
    - Convert the email to a standard format, e.g., 
    "kian at g mail dot com" -> "kian@gmail.com"
    "engineering at adra product studio dot com" -> "engineering@adraproductstudio.com"
    3. Extract and format the date: 
    "June 29th" ->  29.06.2024
    "July 2nd" -> 02.07.2024
    4. Extract and format the time (UTC):   
    "at 12 pm" ->  12:00:00
    "at 5 pm" ->  17:00:00
    "at 6 30 am" ->  06:30:00
    Ensure the function includes extra validation.""",
            "parameters": {
                "type": "object",
                "properties": {
                    "formatted_value": {
                        "type": "string",
                        "description": "The estimated FORMATTED value of the email.",
                    },
                    "formatted_date": {
                        "type": "string",
                        "description": """The estimated FORMATTED value of the date in %Y-%m-%d.""",
                    },
                    "formatted_time": {
                        "type": "string",
                        "description": "The estimated FORMATTED value of the time in %H:%M:%S.",
                    },
                    "name": {
                        "type": "string",
                        "description": "The name of the client.",
                    }
                },
                "required": ["formatted_value","formatted_date","formatted_time","name"],
            },
        }
    },
    {
        "type": "function",
        "function":{
            "name": "sendemail",
            "description": "It will give the product category.",
            "parameters": {
                "type": "object",
                "properties": {                                    
                    "name": {
                        "type": "string",
                        "description": "The name of the recipient.",
                    },
                    "recipient_email": {
                        "type": "string",
                        "description": "The email address of the recipient.",
                    }
                },
                "required": ["name","recipient_email"],
            },
        }
    },
    {
    "type": "function",
    "function": {
        "name": "GetQAWithRAG",
        "description": "Retrieve relevant career platform guidance using vector search and RAG. Convert user inquiries into precise queries based on their role, plan, and engagement status. Ensure responses align with the user's onboarding stage, career objectives, and platform features. For any 'How to' questions related to job applications, upskilling, networking, or premium features, include links to guides, FAQs, and tutorial videos where applicable.",
        "parameters": {
            "type": "object",
            "properties": {
                "user_query": {
                    "type": "string",
                    "description": "This is the query for vector search/retrieval, used in RAG. Convert the user's question into a meaningful query by understanding the context, role, and plan. If the user asks a 'How to?' question, include an ask for links to relevant guides, FAQs, and video tutorials specific to their plan and role."
                }
            },
            "required": ["user_query"]
            }
        }
    },
    {
        "type": "function",
        "function":{
            "name": "composio_functions",
            "description": "This to integrate with third party tools and execute the actions, based on the input task prompt",
            "parameters": {
                "type": "object",
                "properties": {
                    "input_task": {
                        "type": "string",
                        "description": "The input task prompt for the composio agent.",
                    }
                },
                "required": ["input_task"],
            }
        }
    },
    {
    "type": "function",
    "function": {
        "name": "get_employer_posted_jobs",
        "description": "fetch the jobs posted by the employer"
        }
    },
    {
        "type": "function",
        "function": {
        "name": "job_post_draft",
        "description": "create a job for the employer",
        "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "integer",
                        "description": "Job id of the drafted job",
                    },
                    "job_title": {
                        "type": "string",
                        "description": "Name of the job",
                    },
                    "job_type": {
                        "type": "string",
                        "description": "Type of the job eg. Full time or contract.",
                    },
                    "job_overview": {
                        "type": "string",
                        "description": "Overview of the job",
                    },
                    "job_desc": {
                        "type": "string",
                        "description": "Description of the job",
                    },
                    "responsibilities": {
                        "type": "string",
                        "description": "Responsibilities of the job role",
                    },
                    "skills": {
                        "type": "string",
                        "description": "Skills required for this job, including technical and soft skills (e.g., Python, SQL, Communication). Provide skills as a comma-separated string."
                    },
                    "specialisation": {
                        "type": "string",
                        "description": "Specialisation of the job for eg.Sale & Marketing",
                    },
                    "country": {
                        "type": "string",
                        "description": "Name of the country the job opening",
                    },
                    "city": {
                        "type": "string",
                        "description": "Name of the city the job opening",
                    },
                    "work_schedule": {
                        "type": "string",
                        "description": "Work schedule eg. monday to friday",
                    },
                    "workplace_type": {
                        "type": "string",
                        "description": "Work place type eg. Remote or On-site",
                    },
                    "sector": {
                        "type": "string",
                        "description": "Sector of the company like Agriculture, Arts, Education, Health care, etc...",
                    },
                    "number_of_openings": {
                        "type": "integer",
                        "description": "Number of openings for the job"
                    },
                    "time_commitment": {
                        "type": "string",
                        "description": "Time commitment format (e.g., '8 Hrs/Week', '6 Hrs/Day')",
                        "pattern": "^[0-9]+\\sHrs/(Week|Day)$"
                    },
                    "timezone": {
                        "type": "string",
                        "description": "Timezone eg. UTC, GMT",
                    },
                    "salary": {
                        "type": "string",
                        "description": "Salary for the job",
                    },
                    "currency": {
                        "type": "string",
                        "description": "currency type eg.INR or USD",
                    },
                    "benefits": {
                        "type": "string",
                        "description": "Benefite of applying this job eg.PF, Insurance",
                    },
                    "required_resume": {
                        "type": "string",
                        "description": "Is resume required for this job or not",
                    },
                    "required_cover_letter": {
                        "type": "string",
                        "description": "Is cover letter required for this job or not",
                    },
                    "receive_notification": {
                        "type": "string",
                        "description": "Is receive notification required or not",  
                    },
                    "application_deadline_date": {
                        "type": "string",
                        "description": "End of the job applying date",
                    },
                    "pre_screen_ques": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "Additional question related to the jobs like Ready to relocate"
                    }
                },
                # "required": ["job_title","job_type","job_overview","job_desc","country","city","workplace_type","work_schedule","sector","time_commitment","timezone","required_resume","required_cover_letter","receive_notification"], # "skills","specialisation","salary","currency","benefits","number_of_openings","application_deadline_date"
            }
        }
    },
    {
        "type": "function",
        "function": {
        "name": "get_opened_jobs_count",
        "description": "Get the number of opened or paused jobs",
        "parameters": {
                "type": "object",
                "properties": {
                    "job_status": {
                        "type": "string",
                        "description": "Status of the job eg. opened or paused",
                    }
                },
                "required": ["job_status"],
            }
        }
    }, 
    {
        "type": "function",
        "function": {
        "name": "get_drafted_job_post",
        "description": "Retrieve jobs that are in drafted status."
        }
    },   
    {
        "type": "function",
        "function": {
        "name": "get_not_reviewed_applicants_count",
        "description": "Get the count of applicants whose applications have not been reviewed."
        }
    },
    {
        "type": "function",
        "function": {
        "name": "get_latest_three_not_reviewed_applicants",
        "description": "Get latest three applicants whose application is in not reviewed status."
        }
    },
    {
    "type": "function",
    "function": {
        "name": "get_latest_job_applicant",
        "description": "Retrieve the most recent job applicant for each jobs posted by that user."
        }
    },
    {
    "type": "function",
    "function": {
        "name": "get_job_applied_applicants_count",
        "description": "Get the count of applicants who applied for a job within a given time range.",
        "parameters": {
            "type": "object",
            "properties": {
                "number_of_days": {
                    "type": "integer",
                    "description": "Time range for job applications e.g., 'latest 3 days', 'last 24 hours'."
                }
            },
            }
        }
    },
    {
        "type": "function",
        "function": {
        "name": "get_shortlisted_applicants_count",
        "description": "Get the count of applicants who have been shortlisted."
        }
    },
    {
        "type": "function",
        "function": {
        "name": "get_invited_applicants_count",
        "description": "Get the count of applicants who have been invited."
        }
    },
    {
    "type": "function",
    "function": {
        "name": "get_interview_invited_applicants_name",
        "description": "Retrieve the names of applicants who have been invited for an interview."
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_recommended_and_applied_applicants",
            "description": "Get the names of applicants who have applied and been recommended for a job."
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_recommended_and_not_applied_applicants",
            "description": "Get the names of applicants who have not applied but have been recommended for a job.",
            "parameters": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "integer",
                    "description": "Get job id of the selected job"
                }
            },
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_remaining_days_to_close",
            "description": "Retrieve the number of days remaining before a job posting or application closes."
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_company_about",
            "description": "Retrieve the company's about"
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_latest_not_reviewed_applicants",
            "description": "Get the lastest applicant whose application is not reviewed"
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_company_about",
            "description": "Update the company's about",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_description": {
                        "type": "string",
                        "description": "Description of the company"
                    }
                },
                "required": ['company_description']
            }
        }
    }
]

def s3_exists(s3_bucket, s3_key):
    try:
        s3_cient = s3_obj.get_s3_client()
        s3_cient.head_object(Bucket=s3_bucket,Key=s3_key)
        return True
    except Exception as e:
        print("s3_exists error : "+str(e))
        return False

def get_openai_summary(l_openai_api_key,req_prompt): 
    result = {}    
    global openai_api_key
            
    openai_api_key = l_openai_api_key
    OpenAI.api_key = openai_api_key    

    try:                 
        req_messages = [{"role": "user", "content": req_prompt}]
        response = process_openai_completion_for_job(req_messages,OpenAI.api_key)
        print(f"process_openai_completion response {response}")
        result["data_id"] = str(response.id)            
        result["summary"] = str(response.choices[0].message.content)
    except Exception as error:       
        print("Error in get_openai_summary(): "+str(error))
        result = api_json_response_format(False,str(error),500,{}) 
    finally:        
        return result

def process_openai_completion_for_job(req_messages,openai_api_key):
    try:
        
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
        print("Error in process_openai_completion_for_job(): "+str(error))

async def get_latest_job_applicant(arguments):
    try:
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['sub_user_id']
        query = "SELECT id, job_title FROM job_post WHERE employer_id=%s ORDER BY id DESC"
        value = (employer_id,)
        result = execute_query(query,value)
        
        for r in result:

            query = "SELECT CONCAT(u.first_name, ' ' , u.last_name) as full_name FROM `job_activity` ja LEFT JOIN users u ON ja.professional_id = u.user_id WHERE ja.job_id = %s ORDER BY ja.created_at DESC LIMIT 1"
            value = (r['id'],)
            prof_details = execute_query(query,value)
            if prof_details:
                r.update({"professional_name": f"{prof_details[0]['full_name']}" })
            else: 
                r.update({"professional_name": "" })

        if not result:
            return "No users applied for this job"
        return result
    except Exception as e:  
        print("Error in get_latest_job_applicant : %s",str(e))


async def get_employer_posted_jobs(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        query = "select count(id) from job_post where employer_id IN (%s) and job_status = 'opened'"
        values = (combined_employer_ids_list,)
        job_count = execute_query(query, values)
        if len(job_count) > 0 and job_count[0]['count(id)'] > 0:
            query = "SELECT `id`, `job_title` FROM `job_post` WHERE employer_id = %s and job_status = 'opened' ORDER by created_at DESC LIMIT 1"
            values = (employer_id,)
            result = execute_query(query,values)
        elif job_count[0]['count(id)'] == 0:
            result = {"message": "You haven't posted any jobs yet."}
    except Exception as e:
        print("Error in get_employer_posted_jobs: %s",str(e))
    finally:
        return result

async def get_not_reviewed_applicants_count(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        posted_jobs_query = "select id from job_post where employer_id IN %s"
        posted_jobs_values = (tuple(combined_employer_ids_list),)
        posted_jobs = execute_query(posted_jobs_query, posted_jobs_values)
        if len(posted_jobs) > 0:
            id_list = [i['id'] for i in posted_jobs]
        else:
            id_list = []

        query = """select count(ja.id) as not_reviewed_count, ja.job_id, jp.job_title from job_activity ja 
                    LEFT JOIN job_post jp on ja.job_id = jp.id where ja.job_id IN %s and ja.application_status = %s GROUP BY ja.job_id;"""
        values = (tuple(id_list), 'Not Reviewed',)
        not_reviewed_count_dict = execute_query(query, values)
        if len(not_reviewed_count_dict) > 0 and not_reviewed_count_dict[0]['not_reviewed_count'] > 0:
            result = not_reviewed_count_dict
        else:
            result = {"not_reviewed_count" : 0}
    except Exception as e:
        print("Error in get_not_reviewed_applicants_count: %s",str(e))
    finally:
        return result

async def get_invited_applicants_count(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        posted_jobs_query = "select id from job_post where employer_id IN %s"
        posted_jobs_values = (tuple(combined_employer_ids_list),)
        posted_jobs = execute_query(posted_jobs_query, posted_jobs_values)
        if len(posted_jobs) > 0:
            id_list = [i['id'] for i in posted_jobs]
        else:
            id_list = []

        query = """select count(ja.id) as invited_count, ja.job_id, jp.job_title from job_activity ja 
                    LEFT JOIN job_post jp on ja.job_id = jp.id where ja.job_id IN %s and ja.application_status = %s GROUP BY ja.job_id;"""
        values = (tuple(id_list), 'Contacted',)
        invited_count_dict = execute_query(query, values)
        if len(invited_count_dict) > 0 and invited_count_dict[0]['invited_count'] > 0:
            result = invited_count_dict
        else:
            result = {"invited_count" : 0}
    except Exception as e:
        print("Error in get_invited_applicants_count: %s",str(e))
    finally:
        return result
    
async def get_shortlisted_applicants_count(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        posted_jobs_query = "select id from job_post where employer_id IN %s"
        posted_jobs_values = (tuple(combined_employer_ids_list),)
        posted_jobs = execute_query(posted_jobs_query, posted_jobs_values)
        if len(posted_jobs) > 0:
            id_list = [i['id'] for i in posted_jobs]
        else:
            id_list = []

        query = """select count(ja.id) as shortlisted_count, ja.job_id, jp.job_title from job_activity ja 
                    LEFT JOIN job_post jp on ja.job_id = jp.id where ja.job_id IN %s and ja.application_status = %s GROUP BY ja.job_id;"""
        values = (tuple(id_list), 'Shortlisted',)
        shortlisted_count_dict = execute_query(query, values)
        if len(shortlisted_count_dict) > 0 and shortlisted_count_dict[0]['shortlisted_count'] > 0:
            result = shortlisted_count_dict
        else:
            result = {"shortlisted_applicants_count" : 0}
    except Exception as e:
        print("Error in get_shortlisted_applicants_count: %s",str(e))
    finally:
        return result
    
async def get_latest_three_not_reviewed_applicants(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        posted_jobs_query = "select id from job_post where employer_id IN %s and job_status = 'opened' order by created_at DESC limit 1"
        posted_jobs_values = (tuple(combined_employer_ids_list),)
        posted_jobs = execute_query(posted_jobs_query, posted_jobs_values)
        if len(posted_jobs) > 0:
            recent_job_id = posted_jobs[0]['id'] 
        else:
            result = "No jobs has been posted recently"
            return result

        query = """select professional_id from job_activity where job_id = %s and application_status = %s LIMIT 3;"""
        values = (recent_job_id, 'Not Reviewed',)
        not_reviewed_professional_dict = execute_query(query, values)
        professional_details_list = []
        if len(not_reviewed_professional_dict) > 0 and not_reviewed_professional_dict[0]['professional_id'] > 0:
            for i in not_reviewed_professional_dict:
                query = 'select user_id, CONCAT(first_name,' ', last_name) as full_name from users where user_id = %s;'
                values = i['professional_id']
                professional_details = execute_query(query, values)
                if professional_details:
                    professional_details_list.append(professional_details[0])
        else:
            result = "No applicants has been applied recently"
            return result
    except Exception as e:
        print("Error in get_latest_three_not_reviewed_applicants: %s",str(e))
    finally:
        return result

async def get_latest_not_reviewed_applicants(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)


        posted_jobs_query = "select id from job_post where employer_id IN (%s) and job_status = 'opened' order by created_at DESC limit 1"
        posted_jobs_values = (combined_employer_ids_list,)
        posted_jobs = execute_query(posted_jobs_query, posted_jobs_values)
        if len(posted_jobs) > 0:
            recent_job_id = posted_jobs[0]['id'] 
        else:
            result = "No jobs has been posted recently"
            return result

        query = """select professional_id from job_activity where job_id = %s and application_status = %s LIMIT 1;"""
        values = (recent_job_id, 'Not Reviewed',)
        not_reviewed_professional_dict = execute_query(query, values)
        professional_details_list = []
        if len(not_reviewed_professional_dict) > 0 and not_reviewed_professional_dict[0]['professional_id'] > 0:
            for i in not_reviewed_professional_dict:
                query = 'select user_id, CONCAT(first_name,' ', last_name) as full_name from users where user_id = %s;'
                values = i['professional_id']
                professional_details = execute_query(query, values)
                if professional_details:
                    professional_details_list.append(professional_details[0])
        else:
            result = "No applicants has been applied recently"
            return result
    except Exception as e:
        print("Error in get_latest_not_reviewed_applicants: %s",str(e))
    finally:
        return result

async def get_job_applied_applicants_count(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)


        posted_jobs_query = "select id from job_post where employer_id IN %s and job_status = 'opened';"
        posted_jobs_values = (tuple(combined_employer_ids_list),)
        posted_jobs = execute_query(posted_jobs_query, posted_jobs_values)
        if len(posted_jobs) > 0:
            id_list = [i['id'] for i in posted_jobs] 
        else:
            result = "No jobs has been posted recently"
            return result

        if 'number_of_days' not in arguments:
            query = """select ja.job_id, jp.job_title, count(ja.professional_id) as applied_count from job_activity ja LEFT join job_post jp on jp.id = ja.job_id where ja.job_id IN %s GROUP by ja.job_id;"""
            values = (tuple(id_list),)
        else:
            query = """select ja.job_id, jp.job_title, count(ja.professional_id) as applied_count from job_activity ja LEFT join job_post jp on jp.id = ja.job_id where ja.job_id IN %s and ja.created_at > NOW() - INTERVAL %s DAY GROUP by ja.job_id;"""
            values = (tuple(id_list), arguments['number_of_days'],)
        applied_count_dict = execute_query(query, values)
        if len(applied_count_dict) > 0:
            result = applied_count_dict
            return result
        else:
            result = "No applicants has been applied."
            return result
    except Exception as e:
        print("Error in get_job_applied_applicants_count: %s",str(e))
    finally:
        return result
    
async def get_interview_invited_applicants_name(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_emp_id = user_data['user_id']
        else:
            user_data = get_sub_user_data(user_email)
            owner_emp_id = user_data['user_id']
            employer_id = user_data['sub_user_id']

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_emp_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_emp_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)


        posted_jobs_query = "select id from job_post where employer_id IN %s"
        posted_jobs_values = (tuple(combined_employer_ids_list),)
        posted_jobs = execute_query(posted_jobs_query, posted_jobs_values)
        if len(posted_jobs) > 0:
            id_list = [i['id'] for i in posted_jobs]
        else:
            id_list = []
        result = []
        for i in id_list:
            query = "select ja.professional_id, CONCAT(u.first_name,' ',u.last_name) as full_name from job_activity ja LEFT JOIN users u on u.user_id = ja.professional_id where ja.job_id = %s and ja.application_status = %s;"
            values = (i, 'contacted',)
            invited_applicants = execute_query(query, values)
            result.append({"job_id" : i, "invited_applicants" : invited_applicants})
    except Exception as e:
        print("Error in get_interview_invited_applicants_name: %s",str(e))
        result = "No applicants has been invited."
        return result
    finally:
        return result
    

def replace_empty_values(data):
    for item in data:
        for key, value in item.items():
            if value == 'N/A' or value == None:
                item[key] = ''
    return data
def format_profile(profile_data):

    profile = {
    "user_id": profile_data['professional_id'],
    "email_active" : 'Y',
    "user_role_fk" : 3,
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
def format_job_details(profile_data):
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
        JOIN 
            users u ON jp.employer_id = u.user_id WHERE jp.job_status = %s and jp.id = %s
        """

        values_job_details = ('opened', job_id,)
        job_details = execute_query(query_job_details, values_job_details)
        cleaned_job_details = replace_empty_values(job_details)
        
        profiles = [format_job_details(profile) for profile in cleaned_job_details]

        # with open('job_data.json', "w") as outfile:
        #     json.dump(profiles, outfile, indent=4, cls=CustomEncoder)  # Apply the custom encoder
        
        return profiles
    except Exception as error:
        print("Error:", error)
        return (False, str(error), 500, {})
    
def employer_process_quries_search(openai_api_key,l_query_txt):
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

def employer_candidate_recommended(job_id):
    try:
        data = get_job_detail(job_id)
        # f = open('job_data.json')
        # data = json.load(f)

        out = employer_process_quries_search(OPENAI_API_KEY,data)
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

        results = vector_store.similarity_search_with_score(
            query=query,
            embedder_name = "adra",
        )
        professional_details = []
        for doc, _ in results:
            page_content = doc.page_content
            professional_details.append(json.loads(page_content))        
        return professional_details
    except Exception as error:
        print(error)        
        return (False,str(error),500,{})   

async def get_opened_jobs_count(arguments):
    try:
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        status = arguments['job_status']

        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['sub_user_id']
            owner_id = user_data["user_id"]

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        query = 'SELECT COUNT(id) FROM `job_post` WHERE employer_id IN %s AND job_status = %s'
        value = (tuple(combined_employer_ids_list),status,)

        result = execute_query(query,value)
        if result == -1:
            return f"something went worng while fetching number of {status} jobs"
        if result and result[0]['COUNT(id)'] == 0:
            return f"There is no {status} jobs"
        return result[0]['COUNT(id)']
    except Exception as e:  
        print("Error in get_opened_jobs_count : %s",str(e))

async def get_drafted_job_post(arguments):
    try:
        #need to change
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['sub_user_id']
            owner_id = user_data["user_id"]

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            if user_data['user_role'] == 'employer':
                combined_employer_ids_list.append(owner_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        query = 'SELECT `id`,`job_title` FROM `job_post` WHERE employer_id IN %s AND job_status = %s'
        value = (tuple(combined_employer_ids_list),'drafted',)
        result = chat_bot_execute_query(query,value)        

        if result == -1:
            return "something went worng while fetching drafted jobs"
        
        if not result:
            return "There is no drafted jobs"
        else:
            return result
    except Exception as e:
        print("Error in get_drafted_job_post : %s",str(e))

async def get_recommended_and_applied_applicants(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['sub_user_id']
            owner_id = user_data["user_id"]

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        query = "select id, job_title from job_post where employer_id IN (%s) and job_status = %s order by id desc limit 1;"
        values = (combined_employer_ids_list, 'opened',)
        last_posted_job = execute_query(query, values)
        if len(last_posted_job) > 0:
            last_posted_job_id = last_posted_job[0]['id']
            recomended = employer_candidate_recommended(last_posted_job_id)
            ids = list(set(data["id"] for data in recomended))
            query = "select professional_id from job_activity where job_id = %s"
            values = (last_posted_job_id,)
            professional_ids = execute_query(query, values)
            if professional_ids:
                professional_ids = [data['professional_id'] for data in professional_ids]    
            ids = [x for x in ids if x in professional_ids]        
            # for x in ids:
            #     if x not in professional_ids:
            #         ids.remove(x)

            new_ids_list = []
            if len(ids) > 3:
                new_ids_list.append(ids[0])
                new_ids_list.append(ids[1])
                new_ids_list.append(ids[2])
            else:
                new_ids_list = ids
            result = []
            for prof_id in new_ids_list:  
                query = "select user_id, CONCAT(first_name,' ',last_name) as full_name from users where user_id = %s"
                values = (prof_id,)
                user_data = execute_query(query, values)
                if len(user_data) > 0:
                    redirect_url = f'https://devapp.2ndcareers.com/employer_dashboard/candidates?job_id={last_posted_job[0]["id"]}&&id={user_data[0]["user_id"]}'
                    user_data[0].update({"redirect_url" : redirect_url})
                    result.append(user_data)
            return result                
    except Exception as e:
        print("Error in get_recommended_and_applied_applicants: %s",str(e))
        result = "No recommended profile found."
        return result
    finally:
        return result

async def get_recommended_and_not_applied_applicants(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        job_id = arguments['job_id']
        # user_data = get_user_data(user_email)
        # if user_data["is_exist"]:
        #     employer_id = user_data["user_id"]
        # else:
        #     user_data = get_sub_user_data(user_email)
        #     employer_id = user_data['sub_user_id']

        # if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
        #     query = "select sub_user_id from sub_users where user_id = %s"
        #     values = (employer_id,)
        #     sub_user_id = execute_query(query, values)
        #     combined_employer_ids_list = []
        #     if sub_user_id:
        #         combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
        # elif user_data['user_role'] == 'recruiter':
        #     combined_employer_ids_list = employer_id
        # combined_employer_ids_list.append(employer_id)

        # query = "select id, job_title from job_post where employer_id IN %s and job_status = %s order by id desc limit 1;"
        # values = (tuple(combined_employer_ids_list), 'opened',)
        # last_posted_job = execute_query(query, values)
        # if len(last_posted_job) > 0:
        #     last_posted_job_id = last_posted_job[0]['id']
        recomended = employer_candidate_recommended(job_id)
        ids = list(set(data["id"] for data in recomended))

        query = "select professional_id from job_activity where job_id = %s"
        values = (job_id,)
        professional_ids = execute_query(query, values)
        if professional_ids:
            professional_ids = [data['professional_id'] for data in professional_ids]            
        for x in ids:
            if x in professional_ids:
                ids.remove(x)

        new_ids_list = []
        if len(ids) > 3:
            new_ids_list.append(ids[0])
            new_ids_list.append(ids[1])
            new_ids_list.append(ids[2])
        else:
            new_ids_list = ids
        result = []
        for prof_id in new_ids_list:  
            query = "select user_id from users where user_id = %s"
            values = (prof_id,)
            user_data = execute_query(query, values)
            if len(user_data) > 0:
                user_id = user_data[0]['user_id']
                redirect_url = f'https://devapp.2ndcareers.com/employer_dashboard/pool?prof_id="2C-PR-{user_id}"' #&&id={user_data['user_id']}
                user_data[0].update({"redirect_url" : redirect_url})
                result.append(user_data)
        return result                
    except Exception as e:
        print("Error in get_recommended_and_not_applied_applicants: %s",str(e))
        result = "No recommended profile found."
        return result
    finally:
        return result

async def get_remaining_days_to_close(arguments):
    try:
        result = {}
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
            owner_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['sub_user_id']
            owner_id = user_data["user_id"]

        combined_employer_ids_list = []
        if user_data['user_role'] == 'employer' or user_data['user_role'] == 'employer_sub_admin':
            query = "select sub_user_id from sub_users where user_id = %s"
            values = (owner_id,)
            sub_user_id = execute_query(query, values)
            if sub_user_id:
                combined_employer_ids_list = [i['sub_user_id'] for i in sub_user_id]
            combined_employer_ids_list.append(owner_id)
        elif user_data['user_role'] == 'recruiter':
            combined_employer_ids_list.append(employer_id)

        query = "SELECT id, job_title, DATEDIFF(NOW(), created_at) AS days_posted_since, job_status FROM `job_post` where employer_id IN (%s) ORDER BY `job_post`.`id` DESC;"
        values = (combined_employer_ids_list,)
        days_posted_since = execute_query(query, values)
        result = days_posted_since
        return result
    except Exception as e:
        print("Error in get_remaining_days_to_close: %s",str(e))
        result = "No recommended profile found."
        return result
    finally:
        return result
    
async def job_post_draft(arguments):
    try:
        email_id = arguments.get("user_email", "")  # Assuming email_id is passed
        user_data = get_user_data(email_id)
        if not user_data['is_exist']:
            user_data = get_sub_user_data(email_id)
            employer_id = user_data['sub_user_id']
        else:
            employer_id = user_data['user_id']

        job_status = "drafted"
        key_id = arguments.get('job_id', 0)  # 0 means new job post

        # Extract skills from request
        new_skills = arguments.get('skills', "")  # Default to empty string
        new_skills_list = [skill.strip() for skill in new_skills.split(",") if skill.strip()]  # Convert to list
        allowed_fields = [
                "job_title", "job_type", "work_schedule", "job_overview", "workplace_type",
                "country", "city", "timezone", "specialisation", "required_subcontract",
                "job_desc", "required_resume", "required_cover_letter",
                "required_background_check", "time_commitment", "receive_notification",
                "duration", "is_paid", "is_active"
            ]
        created_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        
        if key_id == 0:
            # **INSERT NEW JOB POST**
            query_fields = ["employer_id", "job_status", "created_at"]
            query_values = [employer_id, job_status, created_at]
            placeholders = ["%s", "%s", "%s"]

            for field in allowed_fields:
                if field in arguments:
                    query_fields.append(field)
                    query_values.append(arguments[field])
                    placeholders.append("%s")

            # Store skills as a comma-separated string
            query_fields.append("skills")
            query_values.append(", ".join(new_skills_list))  # Convert list to string
            placeholders.append("%s")

            insert_query = f"INSERT INTO job_post ({', '.join(query_fields)}) VALUES ({', '.join(placeholders)})"
            print(insert_query)
            print(query_values)
            result = update_query_last_index(insert_query, tuple(query_values))

            if result['row_count'] > 0:
                key_id = result['last_index']
                if 'pre_screen_ques' in arguments:
                    query = "INSERT INTO `pre_screen_ques`(`job_id`, `custom_pre_screen_ques`,created_at) VALUES (%s,%s,%s)"
                    values = (key_id,arguments['pre_screen_ques'],created_at,)
                    result =  update_query_last_index(query,values)
                    if result['row_count']>0:
                        print("additional questions added")
                return api_json_response_format(True, "Job post draft saved successfully", 0, {"job_id": result["last_index"]})
            else:
                return api_json_response_format(False, "Failed to save job post", 500, {})

        else:
            # **UPDATE EXISTING JOB POST**
            msg = ""
            update_fields = []
            update_values = []
            if 'pre_screen_ques' in arguments:
                query = "INSERT INTO `pre_screen_ques`(`job_id`, `custom_pre_screen_ques`,created_at) VALUES (%s,%s,%s)"
                values = (key_id,arguments['pre_screen_ques'],created_at,)
                result =  update_query_last_index(query,values)
                if result['row_count']>0:
                    msg += "Additonal questions updated successfully "

            # Fetch existing skills if updating skills
            if any(key in allowed_fields for key in arguments.keys()):
                if 'skills' in arguments:
                    existing_skills_query = "SELECT skills FROM job_post WHERE id=%s"
                    existing_skills_result = chat_bot_execute_query(existing_skills_query, (key_id,))
                    if existing_skills_result:
                        existing_skills = existing_skills_result[0]['skills']
                        existing_skills_list = [skill.strip() for skill in existing_skills.split(",") if skill.strip()]
                        
                        # Append new skills and remove duplicates
                        updated_skills = list(set(existing_skills_list + new_skills_list))
                        updated_skills_str = ", ".join(updated_skills)  # Convert list to string

                        update_fields.append("skills=%s")
                        update_values.append(updated_skills_str)

                for field in allowed_fields:
                    if field in arguments:
                        update_fields.append(f"{field}=%s")
                        update_values.append(arguments[field])

                if not update_fields:
                    return api_json_response_format(False, "No fields to update", 400, {})

                update_values.append(key_id)
                update_query = f"UPDATE job_post SET {', '.join(update_fields)} WHERE id=%s"

                result = update_query_last_index(update_query, tuple(update_values))

                if result['row_count'] > 0:
                    msg += "Jobs updated successfully"
                else:
                    return api_json_response_format(False, "No changes made or invalid job ID", 400, {})
            return api_json_response_format(True, msg, 200, {})
    except Exception as error:
        print(error)
        return api_json_response_format(False, f"Error in job_post_draft: {str(error)}", 500, {})
    
async def get_company_about(arguments):
    try:
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['user_id']
        query = 'select company_description from employer_profile where employer_id = %s'
        values = (employer_id,)
        result_dict = execute_query(query, values)
        if result_dict:
            result = result_dict[0]
        else:
            result = {}
        return result
    except Exception as error:
        print(f'Error in get_company_about: Message: {error}')
        result = f'Error in get_company_about: Message: {error}'
        return result
    finally:
        return result
    
async def get_employer_posted_jobs(arguments):
    try:
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if not user_data['is_exist']:
            user_data = get_sub_user_data(user_email)
            employer_id =  user_data['sub_user_id']
            owner_emp_id = user_data['user_id']
        else:
            employer_id =  user_data['user_id']
            owner_emp_id = employer_id
        sub_users_list = []
        if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin" or user_data["user_role"] == "recruiter":
            if user_data["user_role"] == "employer" or user_data["user_role"] == "employer_sub_admin":
                get_sub_users_query = "select sub_user_id from sub_users where user_id = %s"
                get_sub_users_values = (owner_emp_id,)
                sub_users_dict = execute_query(get_sub_users_query, get_sub_users_values)
                if sub_users_dict:
                    sub_users_list = [i['sub_user_id'] for i in sub_users_dict]
                sub_users_list.append(owner_emp_id)

            if user_data["user_role"] == "recruiter":
                sub_users_list.append(user_data['sub_user_id'])
        if sub_users_list:
            query = 'select * from job_post where employer_id IN %s'
            values = (tuple(sub_users_list),)
            result_dict = execute_query(query, values)
            if result_dict:
                result = result_dict
            else:
                result = {}
        else:
            result = {}
        return result
    except Exception as error:
        print(f'Error in get_company_about: Message: {error}')
        result = f'Error in get_company_about: Message: {error}'
        return result
    finally:
        return result
    
async def update_company_about(arguments):
    try:
        user_email = arguments['user_email']
        company_description = arguments['company_description']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['user_id']
        query = 'update employer_profile SET company_description = %s where employer_id = %s'
        values = (company_description,employer_id,)
        result_dict = update_query(query, values)
        if result_dict > 0:
            result = result_dict
        else:
            result = {}
        return result
    except Exception as error:
        print(f'Error in update_company_about: Message: {error}')      
        result =  f'Error in update_company_about: Message: {error}'
        return result        
    finally:        
        return result
    
async def get_employer_plan_details(arguments):
    try:
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        if user_data["is_exist"]:
            employer_id = user_data["user_id"]
        else:
            user_data = get_sub_user_data(user_email)
            employer_id = user_data['user_id']
        query = 'select pricing_category, payment_currency, current_period_end from users where user_id = %s'
        values = (employer_id,)
        result_dict = execute_query(query, values)
        if result_dict:
            current_period_end = result_dict[0]['current_period_end']
            formatted_date = datetime.fromtimestamp(current_period_end, tz=timezone.utc)
            result_dict[0].update({'current_period_end' : formatted_date})
            result_dict[0].update({'user_role' : user_data['user_role']})
            result = result_dict
        else:
            result = []
        return result
    except Exception as error:
        print(f'Error in get_company_about: Message: {error}')
        result = f'Error in get_company_about: Message: {error}'
        return result
    finally:
        return result
