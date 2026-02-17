import uuid
from src import app
from src.controllers.jwt_tokens.jwt_token_required import get_user_token,get_jwt_access_token
from src.models.user_authentication import get_user_data,isUserExist,api_json_response_format, get_sub_user_data
from src.models.mysql_connector import execute_query,update_query,update_query_last_index, chat_bot_execute_query, chat_bot_update_query_last_index, run_query
from src.models.llama_index import LLAMA_INDEX
from src.models.aws_resources import S3_Client

import secrets
import os, json, time, ast, boto3
from flask import request
from dotenv import load_dotenv
from  openai import OpenAI
from flask_executor import Executor
from datetime import datetime, date
import meilisearch
from meilisearch import Client
from meilisearch.index import Index
from langchain_community.vectorstores import Meilisearch
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from src.controllers.chat_bot.emp_chat_bot_process import emp_functions_def, get_opened_jobs_count, get_not_reviewed_applicants_count, get_latest_three_not_reviewed_applicants, get_latest_job_applicant, get_job_applied_applicants_count, get_shortlisted_applicants_count, get_invited_applicants_count, get_interview_invited_applicants_name, get_drafted_job_post, job_post_draft,get_remaining_days_to_close,get_company_about,get_recommended_and_not_applied_applicants,get_recommended_and_applied_applicants,get_latest_not_reviewed_applicants,get_employer_posted_jobs, update_company_about, get_employer_plan_details

load_dotenv()

DEFAULT_CHAT_INIT_MSG = os.environ.get('DEFAULT_CHAT_INIT_MSG')
DEFAULT_CHAT_PROMPT = os.environ.get('DEFAULT_CHAT_PROMPT')
DEFAULT_MODEL = os.environ.get('DEFAULT_MODEL')
HUBSPOT_ACCESS_TOKEN = str(os.environ.get('HUBSPOT_ACCESS_TOKEN'))
DEFAULT_MODEL_API_KEY = os.environ.get("OPENAI_API_KEY")
DEFAULT_MODEL_BASE_URL = "https://api.openai.com/v1"
DEFAULT_CHAT_IDLE_TIME = os.environ.get('DEFAULT_CHAT_IDLE_TIME')
DEFAULT_PAUSE_MSG = os.environ.get('DEFAULT_PAUSE_MSG')
DEFAULT_CLOSE_MSG = os.environ.get('DEFAULT_CLOSE_MSG')
OPENAI_COMPLETION_TOKEN_LIMIT = int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))
JOB_POST_INDEX = os.environ.get("JOB_POST_INDEX")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
BUCKET_NAME = os.environ.get('PROMPT_BUCKET')

s3_obj = S3_Client()
g_resume_path = os.getcwd()
g_prompt_file_path = os.getcwd()
g_summary_model_name = os.environ.get('SUMMARY_MODEL_NAME')
g_openai_completion_token_limit =int(os.environ.get('OPENAI_COMPLETION_TOKEN_LIMIT'))

g_token_encoding_txt = os.environ.get('TOKEN_ENCODING_TEXT')
g_openai_token_limit = int(os.environ.get('OPENAI_MAX_TOKEN_LIMIT')) #15,000
s3_picture_folder_name = "professional/profile-pic/"
s3_intro_video_folder_name = "professional/profile-video/"
s3_sc_community_cover_pic_folder_name = "2ndcareers/cover-pic/"
s3_partner_cover_pic_folder_name = "partner/cover-pic/"
s3_partner_learning_folder_name = "partner/learning-doc/"
s3_partner_picture_folder_name = "partner/profile-pic/"
s3_trailing_cover_pic_folder_name = "2ndcareers/trailing-pic/"
rag_folder_name = "2ndcareers/RAG"


llama_index = LLAMA_INDEX()
executor = Executor(app)

prof_functions_def = [
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
            },
        }
  },
  {
    "type": "function",
    "function": {
        "name": "GetAIRecommendedJobs",
        "description": "Get AI-recommended job postings for a professional.",
        "parameters": {
            "type": "object",
            "properties": {
                "about": {
                    "type": "string",
                    "description": "The 'about' section in a resume is a brief professional summary highlighting your experience, key skills, and career goals"
                },
                "skills": {
                    "type": "string",
                    "description": "A list of users expertise and technical or soft skills relevant to users profession."
                },
                "preferences": {
                    "type": "string",
                    "description": "This is users work preferences, such as preferred job roles, work environment, or industries of interest."
                },
                "additional_info": {
                    "type": "string",
                    "description": "Extra details like certifications, languages, achievements, or personal interests that add value to users profile."
                }
            }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "GetProfileAndUpdate",
            "description": "Get profile percentage and update or Update the user profile for about, skills, or additional_info. Any of these fields may be provided or omitted.",
            "parameters": {
                "type": "object",
                "properties": {
                    "about": {
                        "type": "string",
                        "description": "User's brief professional summary highlighting experience, key skills, and career goals."
                    },
                    "skills": {
                        "type": "array",
                        "description": "A list of expertise of upskilling, including technical and soft skills relevant to the user's profession.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "skill_name": {
                                    "type": "string",
                                    "description": "The name of the skill, e.g., Python, SQL, Communication, Team management"
                                },
                                "skill_level": {
                                    "type": "string",
                                    "description": "The proficiency level of the skill, such as Beginner, Intermediate, or Expert."
                                }
                            }
                        }
                    },
                    "preference": {
                        "type": "string",
                        "description": "Work preferences such as job roles, "
                    },
                    "specialisation": {
                        "type": "string",
                        "description": "Specialisation of the user e.g: Software developer, "
                    },
                    "additional_info": {
                        "type": "array",
                        "description": "A list of additional details such as certifications, board positions, or achievements that add value to the profile.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "description": "Category of additional info, such as 'Certifications', 'Certificates Earned', or 'Board Positions Held'."
                                },
                                "description": {
                                    "type": "string",
                                    "description": "The specific name of the certification, certificate, or board position (e.g., 'AWS Certified Solutions Architect', 'Google Data Analytics Certificate')."
                                }
                            }
                        }
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "GetEventsAndCommunity",
            "description": "Fetches past events, community, and upcoming event data for a professional user.",
            }
    },
    {
        "type": "function",
        "function": {
            "name": "getprofessionalcommunity",
            "description": "Retrieves professional community information including groups, networking spaces, member discussions, and community activities for the user.",
            }
    },
    {
        "type": "function",
        "function": {
            "name": "gettrainingdata",
            "description": "Retrieves structured training courses, guided programs, and skill-building sessions including past, current, and upcoming trainings.",
            }
    },
    {
        "type": "function",
        "function": {
            "name": "getprofessionallearning",
            "description": "Retrieves on-demand learning content such as recorded videos, podcasts, and archived sessions.",
            }
    },
    {
        "type": "function",
        "function": {
            "name": "getprofessionalperspectives",
            "description": "Retrieves blog articles, expert insights, and thought leadership content (Perspectives section).",
            }
    },
    {
        "type": "function",
        "function": {
            "name": "get_professional_saved_jobs",
            "description": "Get the saved jobs.",
            }
    },
    {
    "type": "function",
    "function": {
        "name": "GetAdminJobs",
        "description": "Get Super admin recommended job postings for a professional.",
        }
    },
    {
    "type": "function",
    "function": {
        "name": "check_applied_status",
        "description": "Check whether the user applied the job or not",
        "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Fetch job id from the given link.",
                    }
                },
            }
        }
    },
    {
    "type": "function",
    "function": {
        "name": "get_latest_jobs",
        "description": "Retrieve the three most recent job postings available on the platform.",
        }
    },
    {
    "type": "function",
    "function": {
        "name": "get_latest_learnings",
        "description": "Retrieve the three most recent learning opportunities available on the platform.",
        }
    },
    {
    "type": "function",
    "function": {
        "name": "GetQueryRelatedJobs",
        "description": "Retrieve the jobs that matches the user entered search query or text available on the platform.",
        "parameters": {
                "type": "object",
                "properties": {
                    "search_text": {
                        "type": "string",
                        "description": "Search the available jobs in the platform based on the entered text. The search text can be a job title, skill, or any relevant keyword related to the job.",
                    }
                },
                "required": ["search_text"],
            }
        }
    }
]

async def get_latest_jobs(arguments):

    try:
        # job_link = "https://devapp.2ndcareers.com/professional/saved_jobs?job_id="
        user_email = arguments['user_email']
        user_data = get_user_data(user_email)
        professional_id = user_data['user_id']
        query = "SELECT jp.id, jp.job_title, SUBSTRING_INDEX(jp.job_desc, ' ', 100) AS short_job_desc, jp.created_at FROM job_post jp WHERE jp.id NOT IN ( SELECT ja.job_id FROM job_activity ja WHERE ja.professional_id = %s ) AND jp.job_status = 'opened' ORDER BY jp.created_at DESC LIMIT 3;"
        values = (professional_id,)
        latest_jobs = chat_bot_execute_query(query, values)

        if not latest_jobs:
            return "No latest jobs on the platform" 
        for job in latest_jobs:
            job.update({'job_url': f"https://devapp.2ndcareers.com/professional/all_jobs?job_id={job['id']}"})
        
        return latest_jobs
    except Exception as e:  
        print("Error in get_latest_jobs : %s",str(e))
    


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
                    result = chat_bot_execute_query(query, values)
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

async def GetQAWithRAG(arguments):
    global llama_index
    try:
        print("GetQAWithRAG")      
        user_query = arguments['user_query']        
        conversation_id = arguments['conversation_id']
        query_engine_object = arguments['query_engine_object']
        print(user_query)    

        #LLama index Q&A
        answer = llama_index.askQuestions(query_engine_object,user_query,conversation_id)       
        return str(answer)
    except Exception as e:
        return f"Unable to get answer from RAG. {e}"

async def get_professional_saved_jobs(arguments):
    try:
        # job_link = os.environ.get("JOB_LINK")
        job_link = "https://devapp.2ndcareers.com/professional/saved_jobs?job_id="
        user_data = get_user_data(arguments['user_email'])
        professional_id = user_data["user_id"]
        result_json=[]
        profile_percentage = show_percentage(professional_id)

        query = '''SELECT sj.job_id, jp.job_title, jp.job_status FROM saved_job sj LEFT JOIN job_activity ja ON sj.job_id = ja.job_id AND sj.professional_id = ja.professional_id 
                    JOIN job_post jp ON jp.id = sj.job_id WHERE sj.professional_id = %s AND ja.job_id IS NULL ORDER BY `sj`.`created_at` DESC;'''
        values = (professional_id,)
        saved_job = execute_query(query, values)
        # job_list = [job['job_title'] for job in saved_job]
        for job in saved_job:
                        if job['job_status'] == 'opened':
                            result_json.append({'job_title': job['job_title'],
                                                'job_url': f"{job_link}{job['job_id']}"})
                    # result_json = [job['job_title'],  for job in id_job_status if job['job_status'] == 'opened']
        if not result_json:
            return "no saved or viewed jobs"
        return result_json
        # return job_list

    except Exception as e:
            print(f'Failed to execute saved_jobs functions. Exception :  {e}')
            return "Failed to execute functions. Exception occured. Please try again."

async def getprofessionalperspectives(arguments):
    try:
        result_json = {}
        email_id = arguments['user_email']
        user_data = get_user_data(email_id)
        if user_data["user_role"] == "professional":
            # if user_data["user_role"] == "professional":
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
            except Exception as e:  
                print(f"Error in mixpanel event logging: Perspectives Tab, {str(e)}")
            result_json = api_json_response_format(True, "Details fetched successfully!", 0, replace_empty_values([result_list]))
        else:
            result_json = api_json_response_format(False, "Unauthorized user.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing Perspectives tab."}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Perspectives Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

async def getprofessionallearning(arguments):
    try:
        result_json = {}
        # req_data = request.get_json()
        page_number = int(arguments.get("page_number", 1))
        # if 'page_number' not in req_data:
        #     result_json = api_json_response_format(False, "Page number is required", 204, {})
        #     return result_json
        email_id = arguments['user_email']
        user_data = get_user_data(email_id)
        if user_data["user_role"] == "professional":
            # page_number = req_data['page_number']
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
        except Exception as e:  
            print(f"Error in mixpanel event logging: Learning Tab Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json


async def gettrainingdata(arguments):
    try:
        result_json = {}
        email_id = arguments['user_email']
        user_data = get_user_data(email_id)
        # token_result = get_user_token(request)                                        
        # if user_data["user_role"] == "professional":
            # user_data = get_user_data(user_data["email_id"])        
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
                
    except Exception as error:
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

async def getprofessionalcommunity(arguments):
    try:
        email_id = arguments['user_email']
        user_data = get_user_data(email_id)
        if user_data["user_role"] == "professional":
            # Fetch community data
            query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and title = %s'
            values = ('Y', 'Community',)
            community_data_set = chat_bot_execute_query(query, values)
            community_list = []
            for c in community_data_set:
                s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                c.update({"image": s3_sc_community_cover_pic_key})
                community_list.append(c)

            # Fetch event data
            query = 'select id,title, short_description, image, type_of_community, join_url, event_date, share_url from community where is_active = %s and type = %s'
            values = ('Y', 'Event',)
            event_data_set = chat_bot_execute_query(query, values)
            event_list = []
            for e in event_data_set:
                s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + e['image']
                # s3_sc_community_audio_key = s3_sc_community_audio_folder_name + e['share_url']
                s3_url_key =  e['share_url']
                e.update({"share_url" : s3_url_key})
                e.update({"image": s3_sc_community_cover_pic_key})
                current_date = date.today()
                event_date = datetime.strptime(str(e['event_date']), '%Y-%m-%d %H:%M:%S')
                if event_date.date() > current_date:
                    event_list.append(e)
            
            query = 'select id,title, short_description, image, type_of_community, join_url, event_date, share_url from community where is_active = %s and type_of_community = %s'
            values = ('Y', 'Careers in Impact',)
            careers_data_set = chat_bot_execute_query(query, values)
            careers_list = []
            for c in careers_data_set:
                s3_sc_careers_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                c.update({"image": s3_sc_careers_cover_pic_key})
                careers_list.append(c)

            # Construct the result list directly with the lists
            result_list = {
                "community_posts": community_list,
                "event_posts": event_list,
                "careers_list" : careers_list
            }
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Message': f"User {user_data['email_id']} viewed the professional community page."}
            except Exception as e:  
                print(f"Error in mixpanel event logging: Community Tab, {str(e)}")
            result_json = api_json_response_format(True, "Details fetched successfully!", 0, replace_empty_values([result_list]))
        else:
            result_json = api_json_response_format(False, "Unauthorized user", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing community tab."}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Community Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

async def GetEventsAndCommunity(arguments):
    try:
        email_id = arguments['user_email']
        user_data = get_user_data(email_id)
        if user_data["user_role"] == "professional":
            # Fetch community data
            query = 'select id,title, short_description, image, type_of_community, join_url, share_url from community where is_active = %s and title = %s'
            values = ('Y', 'Community',)
            community_data_set = chat_bot_execute_query(query, values)
            community_list = []
            for c in community_data_set:
                s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                c.update({"image": s3_sc_community_cover_pic_key})
                community_list.append(c)

            # Fetch event data
            query = 'select id,title, short_description, image, type_of_community, join_url, event_date, share_url from community where is_active = %s and type = %s'
            values = ('Y', 'Event',)
            event_data_set = chat_bot_execute_query(query, values)
            event_list = []
            for e in event_data_set:
                s3_sc_community_cover_pic_key = s3_sc_community_cover_pic_folder_name + e['image']
                # s3_sc_community_audio_key = s3_sc_community_audio_folder_name + e['share_url']
                s3_url_key =  e['share_url']
                e.update({"share_url" : s3_url_key})
                e.update({"image": s3_sc_community_cover_pic_key})
                current_date = date.today()
                event_date = datetime.strptime(str(e['event_date']), '%Y-%m-%d %H:%M:%S')
                if event_date.date() > current_date:
                    event_list.append(e)
            
            query = 'select id,title, short_description, image, type_of_community, join_url, event_date, share_url from community where is_active = %s and type_of_community = %s'
            values = ('Y', 'Careers in Impact',)
            careers_data_set = chat_bot_execute_query(query, values)
            careers_list = []
            for c in careers_data_set:
                s3_sc_careers_cover_pic_key = s3_sc_community_cover_pic_folder_name + c['image']
                c.update({"image": s3_sc_careers_cover_pic_key})
                careers_list.append(c)

            # Construct the result list directly with the lists
            result_list = {
                "community_posts": community_list,
                "event_posts": event_list,
                "careers_list" : careers_list
            }
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Message': f"User {user_data['email_id']} viewed the professional community page."}
            except Exception as e:  
                print(f"Error in mixpanel event logging: Community Tab, {str(e)}")
            result_json = api_json_response_format(True, "Details fetched successfully!", 0, replace_empty_values([result_list]))
        else:
            result_json = api_json_response_format(False, "Unauthorized user", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing community tab."}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Community Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

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
        profile_result = chat_bot_execute_query(query, values)

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
        # Log the error for debugging purposes
        print("Error:", error)
        return (False, str(error), 500, {})

def s3_exists(s3_bucket, s3_key):
    try:
        s3_cient = s3_obj.get_s3_client()
        s3_cient.head_object(Bucket=s3_bucket,Key=s3_key)
        return True
    except Exception as e:
        print("s3_exists error : "+str(e))
        return False

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

def get_openai_summary(l_openai_api_key,req_prompt): 
    result = {}    
    global openai_api_key
            
    openai_api_key = l_openai_api_key
    OpenAI.api_key = openai_api_key    

    try:                 
        req_messages = [{"role": "user", "content": req_prompt}]
        response = process_openai_completion_for_job(req_messages,OpenAI.api_key)
        result["data_id"] = str(response.id)            
        result["summary"] = str(response.choices[0].message.content)
    except Exception as error:       
        print("Error in get_openai_summary(): "+str(error))
        result = api_json_response_format(False,str(error),500,{}) 
    finally:        
        return result

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

async def GetAIRecommendedJobs(arguments):
    user_email = arguments['user_email']
    result_json = {}
    # job_link = os.environ.get("JOB_LINK")
    job_link =  "https://devapp.2ndcareers.com/professional/recommended_jobs?job_id="
    try:
        profile = {}
        user_data = get_user_data(user_email)
        professional_id = user_data["user_id"]
        profile_percentage = show_percentage(professional_id)
        if profile_percentage > 50:
            # unapplied_ai_jobs_query = "SELECT count(job_id) as count FROM ai_recommendation WHERE professional_id = %s AND source = 'AI' AND job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s);"
            # values = (professional_id, professional_id,)
            # unapplied_jobs_count = chat_bot_execute_query(unapplied_ai_jobs_query, values)
            # job_details = []
            # if len(unapplied_jobs_count) > 0:
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
                # Make similarity search
                #query = out

                results = vector_store.similarity_search_with_score(
                    query=query,
                    embedder_name = "adra",
                )
                job_details1 = []
                for doc, _ in results:
                    page_content = doc.page_content
                    job_details1.append(json.loads(page_content))
                if len(job_details1) > 0:
                    ids = list(set(data["id"] for data in job_details1))
                    query = 'select job_id from job_activity where professional_id = %s'
                    values = (professional_id,)
                    applied_job_ids_dict = execute_query(query, values)
                    applied_job_ids = [i['job_id'] for i in applied_job_ids_dict]
                    new_id_list = []
                    if len(applied_job_ids) > 0:
                        for z in ids:
                            if z not in applied_job_ids:
                                new_id_list.append(z)
                    else:
                        new_id_list = ids
                    if new_id_list:
                        query = 'select id, job_title, job_status from job_post where id IN %s'
                        values = (tuple(new_id_list),)
                        id_job_status = chat_bot_execute_query(query, values)
                        print(id_job_status)

                        result = []
                        for job in id_job_status:
                            if job['job_status'] == 'opened':
                                result.append({'job_title': job['job_title'],
                                                    'job_url': f"{job_link}{job['id']}"})
                    else:
                        result = []
                    
                    if not result:
                        # return "No jobs available"
                        result_json = api_json_response_format(False,"No jobs available",0,{})
                    # result_json = [job['job_title'],  for job in id_job_status if job['job_status'] == 'opened']
                    else:
                        result_json['jobs'] = result
                        result_json['profile_details'] = data
                    
                else:
                    result_json = api_json_response_format(True,"No new recommendations found",0,data)           
        else:
            profile_percentage = {'profile_percentage' : profile_percentage} 
            profile.update(profile_percentage) 
            notification_msg = "Please take a few moments to complete your profile to increase your chances of receiving better recommendations."
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Message': 'Jobs not recommended, User profile percentage is less than 50.'}
            except Exception as e:  
                print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
            result_json = api_json_response_format(True, notification_msg, 0, profile)
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in recommended jobs view.'}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
async def GetAdminJobs(arguments):
    user_email = arguments['user_email']
    # job_link = os.environ.get("JOB_LINK")
    job_link =  "https://devapp.2ndcareers.com/professional/recommended_jobs?job_id="
    result_json = {}
    try:
        profile = {}
        user_data = get_user_data(user_email)
        professional_id = user_data["user_id"]
        profile_percentage = show_percentage(professional_id)
        if profile_percentage > 50:
            data = get_profile_search(professional_id)
            unapplied_ai_jobs_query = "SELECT count(job_id) as count FROM sc_recommendation WHERE professional_id = %s AND job_id NOT IN (SELECT job_id FROM job_activity WHERE professional_id = %s);"
            values = (professional_id, professional_id,)
            unapplied_jobs_count = chat_bot_execute_query(unapplied_ai_jobs_query, values)
            if len(unapplied_jobs_count) > 0:
                admin_rcmnd_jobs_query = "SELECT sr.job_id, jp.job_status, jp.job_title FROM sc_recommendation sr JOIN job_post jp ON sr.job_id = jp.id WHERE sr.professional_id = %s AND jp.job_status = 'opened' AND sr.job_id NOT IN ( SELECT job_id FROM job_activity WHERE professional_id = %s ) LIMIT 2;"
                values = (professional_id, professional_id,)
                admin_recmnd_jobs = execute_query(admin_rcmnd_jobs_query, values)
                result = []
                for job in admin_recmnd_jobs:
                    if job['job_status'] == 'opened':
                        result.append({'job_title': job['job_title'],
                                            'job_url': f"{job_link}{job['job_id']}"})
                    
                if not result:
                    result_json = api_json_response_format(False,"No jobs available",0,{})
                else:
                    result_json['jobs'] = result
                    result_json['profile_details'] = data                     
        else:
            profile_percentage = {'profile_percentage' : profile_percentage} 
            profile.update(profile_percentage) 
            notification_msg = "Please take a few moments to complete your profile to increase your chances of receiving better recommendations."
            try:
                temp_dict = {'Country' : user_data['country'],
                            'City' : user_data['city'],
                            'Message': 'Jobs not recommended, User profile percentage is less than 50.'}
            except Exception as e:  
                print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
            result_json = api_json_response_format(True, notification_msg, 0, profile)
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in recommended jobs view.'}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json
    
async def get_recent_jobs(arguments):
    user_email = arguments['user_email']
    result_json = {}
    try:
        profile = {}
        user_data = get_user_data(user_email)
        professional_id = user_data["user_id"]
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': 'Error in recommended jobs view.'}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Recommended Jobs View Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def get_professional_profile_dashboard(email_id: str):
    result_json = {}
    try:
            user_data = get_user_data(email_id)      
            if user_data["user_role"] == "professional":
                professional_id = user_data["user_id"]
                profile_percentage = show_percentage(professional_id)
                query = "SELECT u.user_id, u.first_name, u.last_name, u.email_id, u.country_code, u.contact_number, u.country, u.state, u.city, u.gender, u.pricing_category, p.about, p.professional_resume, p.show_to_employer, p.upload_date, p.preferences, p.video_url, p.expert_notes, p.years_of_experience, p.functional_specification, p.industry_sector, p.sector, p.job_type, p.location_preference, p.mode_of_communication, p.willing_to_relocate, pe.id AS experience_id, pe.company_name, pe.job_title, pe.start_month AS experience_start_month, pe.start_year AS experience_start_year, pe.end_month AS experience_end_month, pe.end_year AS experience_end_year, pe.job_description, pe.job_location, ed.id AS education_id, ed.institute_name, ed.degree_level, ed.specialisation, ed.start_month AS education_start_month, ed.start_year AS education_start_year, ed.end_month AS education_end_month, ed.end_year AS education_end_year, ed.institute_location, ps.id AS skill_id, ps.skill_name, ps.skill_level,pl.id AS language_id,pl.language_known, pl.language_level, pai.id AS additional_info_id, pai.title AS additional_info_title, pai.description AS additional_info_description, psl.id AS social_link_id, psl.title AS social_link_title, psl.url AS social_link_url, u2.profile_image FROM users AS u LEFT JOIN professional_profile AS p ON u.user_id = p.professional_id LEFT JOIN professional_experience AS pe ON u.user_id = pe.professional_id LEFT JOIN   professional_education AS ed ON u.user_id = ed.professional_id LEFT JOIN professional_skill AS ps ON u.user_id = ps.professional_id LEFT JOIN professional_language AS pl ON u.user_id = pl.professional_id LEFT JOIN professional_additional_info AS pai ON u.user_id = pai.professional_id LEFT JOIN professional_social_link AS psl ON u.user_id = psl.professional_id LEFT JOIN users AS u2 ON u.user_id = u2.user_id WHERE u.user_id = %s ORDER BY CASE WHEN pe.end_year = 'Present' THEN 1 ELSE 0 END DESC, pe.end_year DESC, pe.end_month DESC"
                values = (professional_id,)
                profile_result = chat_bot_execute_query(query, values)
                                           
                profile_image_name = replace_empty_values1(profile_result[0]['profile_image'])
                intro_video_name = replace_empty_values1(profile_result[0]['video_url'])                
                s3_pic_key = s3_picture_folder_name+str(profile_image_name)
                s3_video_key = s3_intro_video_folder_name+str(intro_video_name)

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
                result_json = api_json_response_format(True,"Details fetched successfuly",0,profile_dict)
                try:
                    temp_dict = {'Country' : user_data['country'],
                                'City' : user_data['city'],
                                'Message': f"User {user_data['email_id']}'s profile details displayed successfully."}
                except Exception as e:  
                    print(f"Error in mixpanel event logging: Professional Profile Tab, {str(e)}")
            else:
                result_json = api_json_response_format(False,"Unauthorized user",401,{})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing professional profile details."}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Professional Profile Tab Error, {str(e)}")
        print(error)        
        result_json = api_json_response_format(False,str(error),500,{})        
    finally:        
        return result_json

def update_profile_details(arguments):
    try:
        # token_result = get_user_token(request)
        # if token_result["status_code"] == 200:
            user_data = get_user_data(arguments['user_email'])
            req_data = {}                 
            if 'about' in arguments:
                req_data['about'] = arguments['about']
            if 'skills' in arguments:
                req_data['skills'] = arguments['skills']
            if 'preference' in arguments:
                req_data['preference'] = arguments['preference']
            if 'additional_info' in arguments:
                req_data['additional_info'] = arguments['additional_info']
            if 'specialisation' in arguments:
                req_data['specialisation'] = arguments['specialisation']

            if user_data["user_role"] == "professional":
                # Fetch community data
                for key,value in req_data.items():
                    table_name = ''
                    created_at = datetime.now()
                    if key == 'about':
                        query = 'update professional_profile set about = %s where professional_id = %s'
                        values = [(value, user_data['user_id'],)]
                    if key == 'preference':
                        query = 'update professional_profile set preferences = %s where professional_id = %s'
                        values = [(value, user_data['user_id'],)]
                    if key == 'skills':
                        query = 'insert into professional_skill (professional_id, skill_name, skill_level, created_at) values (%s, %s, %s, %s)'
                        values = [(user_data['user_id'], v['skill_name'], v['skill_level'], created_at,) for v in value] 
                    if key == 'additional_info':
                        query = 'insert into professional_additional_info (professional_id, title, description, created_at) values (%s, %s, %s, %s)'
                        # values = [(user_data['user_id'], value[0]['title'], value[0]['description'], created_at,)]
                        values = [(user_data['user_id'], v['title'], v['description'], created_at) for v in value]  
                    
                    if key == 'specialisation':
                        query = 'UPDATE `professional_profile` SET `functional_specification`= %s WHERE professional_id = %s'
                        # values = [(user_data['user_id'], value[0]['title'], value[0]['description'], created_at,)]
                        values = [(value,user_data['user_id'])]

                    res = chat_bot_update_query_last_index(query, values)
                    if res['row_count'] > 0:
                        print(key,"Update")
                result_json = api_json_response_format(True, "Details updated successfully!", 0, {})
            else:
                result_json = api_json_response_format(False, "Unauthorized user", 401, {})
        # else:
        #     result_json = api_json_response_format(False, "Invalid Token. Please try again.", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in viewing community tab."}
        except Exception as e:  
            print(f"Error in mixpanel event logging: Community Tab Error, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json

async def GetProfileAndUpdate(arguments):
    user_email = arguments['user_email']
    try:
        profile = {}
        user_data = get_user_data(user_email)
        professional_id = user_data["user_id"]
        profile_percentage = show_percentage(professional_id)
        # profile_percentage = 61
        # If profile is below 60%, return profile completion link
        if profile_percentage < 60:
            return "Profile not filled. Go to this link: https://devapp.2ndcareers.com/professional/profile"

        # If profile is above 60%, check unfilled fields
        user_data = get_professional_profile_dashboard(user_email)
        if user_data:
            unfilled_keys = {key for key, value in user_data.get('data', {}).items() if not value}
            if unfilled_keys:
                important_keys = {'about', 'skills', 'additional_info', 'preferences'}
                missing_important_keys = important_keys & unfilled_keys  # Find missing important fields
                missing_important_keys -= arguments.keys()  # Remove already provided fields
                res = update_profile_details(arguments)
                if missing_important_keys:
                    if not any(key in important_keys for key in arguments.keys()):
                        return f"Profile percentage is {profile_percentage}. These profile fields are missing: {', '.join(missing_important_keys)}."
                    
                    if res.get('success'):
                        return (
                            f"Successfully updated these profile fields: {', '.join(arguments.keys())}. "
                            f"Kindly update these profile fields: {', '.join(missing_important_keys)}."
                            if missing_important_keys else
                            f"Successfully updated all required profile fields."
                        )
                
                # If missing keys are not 'about', 'skills', etc., just send a profile update link
                return f"Profile percentage is {profile_percentage}.There are some unfilled fields in user profile kindly go to this link: https://devapp.2ndcareers.com/professional/profile"

        return "Your profile is good."
    except Exception as e:
        print(f"Error in GetProfileAndUpdate: {str(e)}")
        return "An error occurred while fetching profile details."
    
async def check_applied_status(arguments):
    user_email = arguments['user_email']
    job_id = arguments['job_id']
    try:
        profile = {}
        user_data = get_user_data(user_email)
        professional_id = user_data["user_id"]
        profile_percentage = show_percentage(professional_id)
        query = "SELECT count(`id`) FROM `job_activity` WHERE professional_id = %s AND job_id = %s"
        values = (professional_id,job_id,)
        res = execute_query(query,values)
        if len(res)>0 and res[0]['count(`id`)']==0:
            return "Job not applied."
        elif len(res)>0 and res[0]['count(`id`)']>0:
            return "Job applied."
        else:
            return f"Error while executing query in check applied job status"
    except Exception as e:
        print(f"Error in GetProfileAndUpdate: {str(e)}")
        return "An error occurred while fetching profile details."

def get_user_role_plan_from_emailid(email_id):
    try:
        if email_id:
            user_data = get_user_data(email_id)
            if user_data['is_exist']:
                user_role = user_data['user_role']
                user_plan = user_data['pricing_category']
                user_id = user_data['user_id']
                login_count = user_data['login_count']
                user_first_name = user_data['first_name']
                user_last_name = user_data['last_name']
                user_name = str(user_first_name) + str(user_last_name)
            else:
                user_data = get_sub_user_data(email_id)
                user_role = user_data['user_role']
                user_plan = user_data['pricing_category']
                user_id = user_data['sub_user_id']
                login_count = user_data['login_count']
                user_first_name = user_data['first_name']
                user_last_name = user_data['last_name']
                user_name = str(user_first_name) + " " + str(user_last_name)
            # query = "SELECT u.user_id, CONCAT(u.first_name, ' ', u.last_name) AS full_name, COALESCE(u.email_id, su.email_id) AS email_id, COALESCE(ur_main.user_role, ur_sub.user_role) AS user_role, COALESCE(u.pricing_category, su.pricing_category) AS user_plan FROM users u LEFT JOIN user_role ur_main ON ur_main.role_id = u.user_role_fk LEFT JOIN sub_users su ON su.user_id = u.user_id LEFT JOIN user_role ur_sub ON ur_sub.role_id = su.role_id WHERE %s IN (u.email_id, su.email_id)"
            # values = (email_id,)
            # user_config = chat_bot_execute_query(query, values)
            # user_role = user_config[0]['user_role']
            # user_plan = user_config[0]['user_plan']
            # user_id = user_config[0]['user_id']
            # user_name = user_config[0]['full_name']
    except Exception as e:
        print("Error in user role and plan: %s",str(e))
    finally:
        return user_role, user_plan, user_id, user_name, login_count
    
def get_agent_config(user_role,user_plan):
    agent_config_data = {}
    try:
        if user_role == 'employer_sub_admin' or user_role == 'recruiter':
            user_role = 'employer'
        query = "SELECT attribute_name, attribute_value FROM agent_attributes where user_role=%s AND user_plan=%s"
        value = (user_role,user_plan,)
        agent_config = chat_bot_execute_query(query,value)
        for config in agent_config:
                agent_config_data[str(config["attribute_name"])] = str(config["attribute_value"]) 
    except Exception as e:
        print("Error in agent config : %s",str(e))
    finally:
        return agent_config_data

def get_chat_summary(email_id): 
      
    try: 
        if email_id:       
            query = "SELECT transcript_summary FROM chat_history WHERE email_id=%s ORDER BY created_at DESC"
            values = (email_id,)         
            chat_summary = chat_bot_execute_query(query, values)
            if chat_summary != -1:
                chat_summary = next((row["transcript_summary"] for row in chat_summary if row["transcript_summary"]), None)
    except Exception as e:  
        print("Error in chat config in get_chat_summary : %s",str(e))
    finally:
        return chat_summary
    
def get_user_config(email_id,conversation_id="",alias_name=""): 
    user_config_data = {}   
    try:
        user_data = get_user_data(email_id)
        if user_data['is_exist']:
            query = "SELECT login_count FROM users WHERE email_id=%s"
        else:
            query = "SELECT login_count FROM sub_users WHERE email_id=%s"
        values = (email_id,)
        user_config = chat_bot_execute_query(query, values)
    except Exception as e:  
        print("Error in user config : %s",str(e))
    finally:
        return user_config

def add_conversation(email_id,usr_phoneno,conversation_id,user_role,user_plan,user_id,user_name,chat_type):
  
    try:
        if email_id and usr_phoneno:
            query = "INSERT INTO chat_history (user_id, user_name, email_id,usr_phoneno,conversation_id,user_role,user_plan) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            values = [(user_id,user_name,email_id,usr_phoneno,conversation_id,user_role,user_plan,)]
            conv_obj = chat_bot_update_query_last_index(query,values)
            if conv_obj['row_count'] > 0:
                conv_obj_id = conv_obj['last_index']
                print("Conversation added.")
        else:
            print("Email id or User phoneno not found.")
    except Exception as e:  
        print("Error in add_conversation : %s",str(e))

def add_chat_history(email_id,usr_phoneno,conversation_id,chat_history,transcript,pause_time=0,pause_count = 0):
    chat_id = None
    try:  
        query = "SELECT chat_id FROM chat_history WHERE email_id = %s AND usr_phoneno = %s"
        values = (email_id,usr_phoneno,)
        res_obj = chat_bot_execute_query(query,values)            
        for res in res_obj:
            chat_id = res['chat_id']

        if chat_id:
            query = "UPDATE chat_history SET chat_conversation = %s,transcript = %s,conversation_id = %s,pause_time = %s,pause_count = %s WHERE chat_id = %s"
            values = [(chat_history,transcript,conversation_id,pause_time,pause_count,chat_id,)]
            chat_bot_update_query_last_index(query, values) 
            print("User chat conversation updated.")    
        else:        
            query = "INSERT INTO chat_history (email_id,usr_phoneno,conversation_id,chat_conversation,transcript,pause_time) VALUES (%s,%s,%s,%s,%s,%s)"
            values = [(email_id,usr_phoneno,conversation_id,chat_history,transcript,pause_time)]
            chat_obj = chat_bot_update_query_last_index(query,values)
            if chat_obj['row_count'] > 0:
                chat_obj_id = chat_obj['last_index']
    except Exception as e:  
        print("Error in add chat history function : %s",str(e))

def get_conversationid_from_user_phoneno(email_id,usr_phoneno):
    conversation_id = None
    try:
        query = "SELECT conversation_id FROM chat_history WHERE email_id = %s AND usr_phoneno = %s"
        values = (email_id,usr_phoneno,)
        res_obj = chat_bot_execute_query(query,values)           
        for res in res_obj:
            conversation_id = res['conversation_id']
    except Exception as e:  
        print("Error in get_conversationid_from_user_phoneno : %s",str(e))
    finally:
        return conversation_id
    
def get_chat_history(conversation_id):
    chat_json = {}
    try:
        if conversation_id:
            query = "SELECT email_id,chat_conversation,transcript,pause_count,pause_time,disp_count,user_role,user_plan FROM chat_history WHERE conversation_id = %s"
            values = (conversation_id,)
            chat_obj = chat_bot_execute_query(query,values)            
            for chat in chat_obj:                
                chat_json["chat_conversation"] = chat['chat_conversation']
                chat_json["transcript"] = chat['transcript']   
                chat_json["pause_count"] = chat['pause_count']
                chat_json["pause_time"] = chat['pause_time'] 
                chat_json["disp_count"] = chat['disp_count']        
                chat_json["user_role"] = chat['user_role']
                chat_json["user_plan"] = chat['user_plan']
                chat_json["email_id"] = chat['email_id']
                
        
    except Exception as e:  
        print("Error in update_chat_history : %s",str(e))
    finally:
        return chat_json
    
def update_chat_history(conversation_id,chat_history,transcript,pause_count = 0,pause_time = 0,disp_count = 0,feedback = "",rating=0):
    try:
        if conversation_id:
            query = "UPDATE chat_history SET chat_conversation = %s,transcript = %s,feedback = %s,rating=%s,pause_count = %s,pause_time = %s,disp_count = %s WHERE conversation_id = %s"
            values = [(chat_history,transcript,feedback,rating,pause_count,pause_time,disp_count,conversation_id)]
            chat_bot_update_query_last_index(query, values)
            print("Chat conversation updated.")
        else:
            print("Conversation id not found.")
    except Exception as e:  
        print("Error in update_chat_history : %s",str(e))

def process_openai_completion_new(conversation_history,modelName,chat_type,user_language,functions_def,model_api_key=DEFAULT_MODEL_API_KEY,model_base_url=DEFAULT_MODEL_BASE_URL):
        
        if chat_type == "widget" or chat_type == "config-widget":
            # global functions_def    
            # if hubspot_properties:
            #     hubspot_properties_json = json.loads(hubspot_properties)
            #     for fun_ef in functions_def:
            #         if fun_ef["function"]["name"] == "CreateHubspotContact":
            #             fun_ef["function"]["parameters"]["properties"] = hubspot_properties_json
            #             if hubspot_properties_required:
            #                 fun_ef["function"]["parameters"]["required"] = hubspot_properties_required.split(",")
            #             else:
            #                 fun_ef["function"]["parameters"]["required"] = []
            if functions_def:
                functions_def_str = json.dumps(functions_def)
                functions_def_str = functions_def_str.replace("{{LANG}}", user_language)
                functions_def = json.loads(functions_def_str)  
            else:
                functions_def = None         
        
            client = OpenAI(api_key=model_api_key,base_url=model_base_url)
            response = client.chat.completions.create(
                model=modelName,
                messages=conversation_history,
                temperature=0,
                tools=functions_def,
                tool_choice="auto" if functions_def else None,
                max_tokens=OPENAI_COMPLETION_TOKEN_LIMIT,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )

        else:
            client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            response = client.chat.completions.create(
                model=modelName,
                messages=conversation_history,
                temperature=0,
                functions=functions_def,
                function_call="auto",
                max_tokens=OPENAI_COMPLETION_TOKEN_LIMIT,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )
        return response

def process_openai_completion(req_messages,modelName):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    return client.chat.completions.create(
                                model=modelName,
                                messages=req_messages,
                                temperature=0,
                                max_tokens=g_openai_completion_token_limit,
                                top_p=1,
                                frequency_penalty=0,
                                presence_penalty=0
                            )

def get_transcript_summary(transcript,email_id,transcript_summary_prompt,conversation_id="",alias_name=""):
    transcript_summary = None
    try:
        # agent_config = get_user_config(email_id)
        # if agent_config:   
        #     if 'transcript_summary_prompt' in agent_config:
        #         transcript_summary_prompt = agent_config['transcript_summary_prompt']
        #     else:
        # transcript_summary_prompt = os.environ.get('TRANSCRIPT_SUMMARY_PROMPT')    
        
        req_messages =  [{"role": "system", "content": transcript_summary_prompt}]
        req_messages.append({"role": "user", "content": f"Transcript : {transcript}"})
        response = process_openai_completion(req_messages,os.environ.get('OPENAI_MODEL_NAME'))                   
        transcript_summary = str(response.choices[0].message.content)        
    except Exception as e:  
        print("Error in transcript summary : %s",str(e))
        # background_runner.update_log_async(MODULE_NAME,"get_transcript_summary",alias_name,"Exception occured in get transcript summary. Error : "+str(e),500,client_name,service_name,conversation_id)
    finally:
        return transcript_summary
    
def event_end_session(conversation_id,agent_config):
    try:
        transcript_summary_prompt = agent_config['transcript_summary_prompt']
        query = "SELECT transcript, email_id FROM chat_history WHERE conversation_id = %s"
        values = (conversation_id,)
        res_obj = chat_bot_execute_query(query, values, conversation_id=conversation_id)
        for res in res_obj:
            try:
                transcript_summary = get_transcript_summary(res['transcript'],res['email_id'],transcript_summary_prompt)
                query = "UPDATE chat_history SET transcript_summary = %s WHERE conversation_id = %s"
                values = [(str(transcript_summary),conversation_id,)]
                chat_bot_update_query_last_index(query, values)
                print(f"Chat transcript summary updated for {conversation_id}.")         
            except Exception as e:  
                print("Transcript summary update error : %s",str(e))     
    except Exception as e:  
        print("Error in mixpanel_event_log : %s",str(e))


def update_transcript(transcaript_data,flag):
    try:
        if "conversation_id" in transcaript_data:         
            conversation_id = transcaript_data['conversation_id']

        if "transcript_data_str" in transcaript_data:     
            transcript_data_str = transcaript_data['transcript_data_str']

        if conversation_id:
            query = "UPDATE chat_history SET transcript = %s WHERE conversation_id = %s"
            values = [(str(transcript_data_str),conversation_id,)]
            chat_bot_update_query_last_index(query, values)
            if flag == "end":
                event_end_session(conversation_id,)
            return api_json_response_format(True,str(f"Transcript updated for {conversation_id}."),200,{})            

    except Exception as e:  
        print("Error in update_transcript : %s",str(e))
        return api_json_response_format(False,str(f"Transcript not updated for {conversation_id}. Exception : {e}"),500,{})
    
def conversation_interpreter_async(chat_conversation_req_json):
    try:
        flag = "default"
        res = update_transcript(chat_conversation_req_json,flag)
        print(res) 
    except Exception as e:
        print(f"Exception in chat_conversation_interpreter : {e}") 
def reset_chat(conversation_id):
    res = False
    try:
        query = "UPDATE conversation_history SET product_offer = '',ahp_object = '',transcript = '',is_qualified = 'N',product_id = 0 WHERE conversation_id = %s"
        values = [(conversation_id,)]
        chat_bot_update_query_last_index(query, values) 
        print("Client conversation reset done.")

        query = "UPDATE chat_history SET chat_conversation = '',transcript = '',pause_count = 0,pause_time = 0,disp_count = 0 WHERE conversation_id = %s"
        values = [(conversation_id,)]
        chat_bot_update_query_last_index(query, values) 
        print("Client chat conversation reset done.")
        res = True   
    except Exception as e:  
        print("Error in reset chat history : %s",str(e))
    finally:
        return res

def get_phoneno_from_client_service_name(email_id,language=""):
    res = {}
    try:       
        query = "SELECT email_id FROM users WHERE email_id = %s"
        values = (email_id,)
        user_obj = chat_bot_execute_query(query,values)        
    except Exception as e:  
        print("Error in get_phoneno_from_client_service_name : %s",str(e))
    finally:
        return user_obj
    
async def get_latest_learnings(arguments):
    try:
        query = "SELECT `title`, `detailed_description`,`url` FROM `learning` where post_status = 'opened' ORDER BY id DESC LIMIT %s;"
        value = (3,)
        learning_obj = chat_bot_execute_query(query,value)

        if not learning_obj:
            return "No latest learnings on the platform"
        return learning_obj
    except Exception as e:  
        print("Error in get_latest_learnings : %s",str(e))
    
async def chatbot_fn(flag,email_id,session_id,msg,chat_type,feedback,rating,lang_id=None,user_language="English"):
    global llama_index
    global HUBSPOT_ACCESS_TOKEN
    chat_conversation = []
    transcript = []
    user_role, user_plan, user_id, user_name, login_count = get_user_role_plan_from_emailid(email_id) 
    agent_config = get_agent_config(user_role,user_plan)
    if flag == "init":
        conversation_id = secrets.token_urlsafe(16)
        
        # user_init_prompt = get_user_config("init") 
        chat_summary = get_chat_summary(email_id)
        if not chat_summary:
            chat_summary = ""
        user_config = get_user_config(email_id)
        # learning_post = get_learning_post()
        login_streak = user_config[0]['login_count'] 
        user_data = get_user_data(email_id)
        if not user_data['is_exist']:
            user_data = get_sub_user_data(email_id)
            
        if agent_config:
            prompt_key = None
            if login_count == 1:
                prompt_key = 'welcome_prompt'
            elif login_count > 1:
                prompt_key = 'return_visit_prompt'

            if prompt_key and prompt_key in agent_config:
                data = ast.literal_eval(agent_config[prompt_key])
                data['response'] = data['response'].replace('{{summary}}', chat_summary)
                return_prompt = data['prompt']
                chat_init_msg_str = f"{return_prompt}\n\n{data['response']}"
            else:
                result_init_prompt = list_transcript_summary(email_id,agent_config)
                chat_init_msg_str = result_init_prompt
                return_prompt = chat_init_msg_str
        else:
            chat_init_msg_str = DEFAULT_CHAT_INIT_MSG.replace("{{USERNAME}}", str(user_data['first_name']))
            return_prompt = chat_init_msg_str

        add_conversation(email_id,session_id,conversation_id,user_role,user_plan,user_id,user_name,"chat")      

        if agent_config:   
            if 'chat_prompt' in agent_config:
                chat_prompt = agent_config['chat_prompt']
                chat_prompt = chat_prompt.replace("{{SUMMARY}}", str(chat_summary))
            else:
                chat_prompt = DEFAULT_CHAT_PROMPT
            
            if 'chat_init_prompt' in agent_config:
                chat_init_prompt = agent_config['chat_init_prompt']

            if 'ci_model_name' in agent_config:
                modelName = agent_config['ci_model_name']
        else:            
            chat_prompt = DEFAULT_CHAT_PROMPT
            modelName = DEFAULT_MODEL   

        chat_conversation = []
        transcript = []
        chat_conversation.append({"role": "system", "content": chat_prompt})
        chat_conversation.append({"role": "assistant", "content": chat_init_msg_str})
        transcript.append("sender" +":BOT"+ ":" + chat_init_msg_str)
        chat_history_str = json.dumps(chat_conversation)
        transcript_str = json.dumps(transcript)
        cur_milliseconds = int(round(time.time() * 1000))
        add_chat_history(email_id,session_id,conversation_id,chat_history_str,transcript_str,cur_milliseconds)
        res_json = {}
        res_json["message"] = return_prompt
        res_json["conversation_id"] = conversation_id
        return api_json_response_format(True, "success", 200, res_json)
    elif flag == "step":
        chat_conversation = []
        transcript = []
        hubspot_properties = None
        hubspot_properties_required = None
        function_name = None
        
        # agent_config = get_agent_config(user_role,user_plan)
        conversation_id = get_conversationid_from_user_phoneno(email_id,session_id)
        # chat_summary = get_chat_summary(email_id, conversation_id) 
        # crm_config = get_crm_config(client_id)  
        if agent_config:
            if 'ci_model_name' in agent_config:
                modelName = agent_config["ci_model_name"]
            else:
                modelName = DEFAULT_MODEL

            if 'ci_model_api_key' in agent_config:
                ci_model_api_key = agent_config["ci_model_api_key"]
            else:
                ci_model_api_key = DEFAULT_MODEL_API_KEY


            if 'ci_model_base_url' in agent_config:
                ci_model_base_url = agent_config["ci_model_base_url"]
            else:
                ci_model_base_url = DEFAULT_MODEL_BASE_URL
            
            if 'chat_idle_time_sec' in  agent_config:
                chat_idle_time_sec = int(agent_config["chat_idle_time_sec"]) 
            else:
                 chat_idle_time_sec = DEFAULT_CHAT_IDLE_TIME
            if 'qa_prompt' in agent_config:
                qa_prompt = str(agent_config["qa_prompt"])       
            
            if 'similarity_top_k' in agent_config:
                similarity_top_k = str(agent_config["similarity_top_k"])    

        else:
            modelName = DEFAULT_MODEL
            chat_idle_time_sec = DEFAULT_CHAT_IDLE_TIME
            qa_prompt = ""
            ci_model_api_key = DEFAULT_MODEL_API_KEY
            ci_model_base_url = DEFAULT_MODEL_BASE_URL

        if conversation_id:
            chat_history_json = get_chat_history(conversation_id)
            if chat_history_json:
                chat_conversation_str = chat_history_json['chat_conversation']
                transcript_str = chat_history_json['transcript']
                pause_count = int(chat_history_json['pause_count'])
                disp_count = int(chat_history_json['disp_count'])
                pause_time = int(chat_history_json['pause_time'])
                # email_id = chat_history_json['email_id']
                # search_limit = int(chat_history_json['search_limit'])     
                user_role = chat_history_json['user_role']
                user_plan = chat_history_json['user_plan']
                # client_service_name = client_name+"_"+service_name
                if user_role == 'professional':
                    functions_def = prof_functions_def
                elif user_role == 'employer' or user_role == 'employer_sub_admin' or user_role == 'recruiter':
                    functions_def = emp_functions_def
                
                if chat_conversation_str:
                    chat_conversation = json.loads(chat_conversation_str)                
                if transcript_str:
                    transcript = json.loads(transcript_str)

                if msg == "":
                    cur_milliseconds = int(round(time.time() * 1000))
                    pause_milliseconds = cur_milliseconds - pause_time
                    pause_seconds = round(pause_milliseconds/1000)
                    if pause_seconds >= chat_idle_time_sec:
                        pause_count = pause_count + 1
                        if pause_count > 1:                            
                            chat_conversation.append({"role": "assistant", "content": DEFAULT_CLOSE_MSG})
                            transcript.append("sender" +":BOT"+ ":" + DEFAULT_CLOSE_MSG)
                            chat_history_str = json.dumps(chat_conversation)
                            transcript_str = json.dumps(transcript)                        
                            update_chat_history(conversation_id,chat_history_str,transcript_str,pause_count,pause_time)
                            res_json = {}
                            res_json["message"] = DEFAULT_CLOSE_MSG
                            res_json["conversation_id"] = conversation_id
                            # event_end_session(conversation_id)
                            return api_json_response_format(True, "success", 201, res_json)
                        else:  
                            if disp_count == 0:                          
                                chat_conversation.append({"role": "assistant", "content": DEFAULT_PAUSE_MSG})
                                transcript.append("sender" +":BOT"+ ":" + DEFAULT_PAUSE_MSG)
                                chat_history_str = json.dumps(chat_conversation)
                                transcript_str = json.dumps(transcript)         
                                cur_milliseconds = int(round(time.time() * 1000)) 
                                disp_count = disp_count + 1              
                                update_chat_history(conversation_id,chat_history_str,transcript_str,pause_count,cur_milliseconds,disp_count)
                                res_json = {}
                                res_json["message"] = DEFAULT_PAUSE_MSG
                                res_json["conversation_id"] = conversation_id
                                return api_json_response_format(True, "success", 200, res_json)
                            else:
                                return api_json_response_format(True, "success", 200, {})
                    else:
                        return api_json_response_format(True, "success", 200, {})                    
                

                chat_conversation.append({"role": "user", "content": msg})
                transcript.append("sender" +":HUMAN"+ ":" + msg)
                # openai_res = process_openai_completion_new(chat_conversation,modelName,chat_type,user_language,hubspot_properties,hubspot_properties_required,ci_model_api_key,ci_model_base_url)
                openai_res = process_openai_completion_new(chat_conversation,modelName,chat_type,user_language,functions_def,ci_model_api_key,ci_model_base_url)
                openai_res = openai_res.choices[0].message
                if openai_res:
                    if openai_res.tool_calls:
                        for tool_call in openai_res.tool_calls:
                            function_name = str(tool_call.function.name)
                            arguments = tool_call.function.arguments
                            function_params = json.loads(arguments)
                            function_params["conversation_id"] = conversation_id
                            function_params["user_email"] = chat_history_json['email_id']
                            if function_name == "GetQAWithRAG":
                                if user_role == 'employer_sub_admin' or user_role == 'recruiter':
                                    user_role = 'employer'
                                collection_name = f"{user_role}_{user_plan}"
                                vector_index = llama_index.getQdrantVectorIndex(collection_name)
                                query_engine_object = llama_index.getQueryEngine(vector_index,5,collection_name,qa_prompt,transcript)
                                function_params["query_engine_object"] = query_engine_object                  

                            # elif function_name == "GetAIRecommendedJobs":
                            #     function_params["user_email"] = chat_history_json['email_id']
                            # elif function_name == "GetProfileAndUpdate":
                            #     function_params["user_email"] = chat_history_json['email_id']
                            # elif function_name == "update_profile_details":
                            #     function_params["user_email"] = chat_history_json['email_id']
                            # elif function_name == "GetEventsAndCommunity":
                            #     function_params["user_email"] = chat_history_json['email_id']
                            # elif function_name == "get_professional_saved_jobs":
                            #     function_params["user_email"] = chat_history_json['email_id']
                            # elif function_name == "GetAdminJobs":
                            #     function_params["user_email"] = chat_history_json['email_id']
                            # elif function_name == "check_applied_status":
                            #     function_params["user_email"] = chat_history_json['email_id']
                            # elif function_name == "display_print":
                            #     print("employer function def")
                            # elif function_name == "get_latest_jobs":
                            #     function_params["user_email"] = chat_history_json['email_id']

                            fn_call = [{"id": tool_call.id,"type": "function","function": {"name":function_name,"arguments":arguments}}]
                            #chat_conversation.append({"role": "assistant", "content": None,"function_call":fn_call})         
                            chat_conversation.append({"role": "assistant", "content": None,"tool_calls":fn_call})                            
                            function = globals()[function_name]
                            result = await function(function_params)
                            chat_conversation.append({"tool_call_id": tool_call.id,"role": "tool", "content": str(result)})
                            #chat_conversation.append({"role": "assistant", "content": result})      
                            if function_name == "GetQAWithRAG":
                                print(f"GetQAWithRAG response : {result}")
                                ai_res_text = result
                            # elif function_name == "GenerateFlowChart": 
                            #     ai_res_text = result   
                            else:
                                openai_res = process_openai_completion_new(chat_conversation,modelName,chat_type,user_language,"",ci_model_api_key,ci_model_base_url)
                                openai_res = openai_res.choices[0].message
                                ai_res_text = openai_res.content
                    else:
                        ai_res_text = openai_res.content

                    pause_count = 0
                    disp_count = 0
                    
                    if ai_res_text is None:
                        ai_res_text = ''
                    chat_conversation.append({"role": "assistant", "content": ai_res_text})
                    transcript.append("sender" +":BOT"+ ":" + ai_res_text)
                    chat_history_str = json.dumps(chat_conversation)
                    transcript_str = json.dumps(transcript)
                    pause_time_millisec = int(round(time.time() * 1000))
                    update_chat_history(conversation_id,chat_history_str,transcript_str,pause_count,pause_time_millisec,disp_count)
                    chat_conversation_req_json = {}
                    chat_conversation_req_json["transcript_data_str"] = str(transcript_str) 
                    chat_conversation_req_json["conversation_id"] = str(conversation_id)
                    # task_id = uuid.uuid4().hex  
                    # executor.submit_stored(task_id, conversation_interpreter_async, chat_conversation_req_json)                    
                    res_json = {}
                    res_json["message"] = ai_res_text
                    res_json["conversationchatbot_fn_id"] = conversation_id
                    return api_json_response_format(True, "success", 200, res_json)
                else:
                    # background_runner.update_log_async(MODULE_NAME,"chatbot_fn","Chatbot","Chatbot error : "+str(openai_res.content),404)
                    return api_json_response_format(False, "Openai error : "+str(openai_res.content), 404, {})
            else:
                # background_runner.update_log_async(MODULE_NAME,"chatbot_fn","Chatbot","Conversation id not found.",404)
                return api_json_response_format(False, "Conversation not found. Please reset and try again.", 404, {})   
        else:
            # background_runner.update_log_async(MODULE_NAME,"chatbot_fn","Chatbot","Conversation id not found.",404)
            return api_json_response_format(False, "Conversation id not found.", 404, {})   

    elif flag == "reset":
        conversation_id = get_conversationid_from_user_phoneno(email_id,session_id)
        res = reset_chat(conversation_id)
        if res:
            res_json = {}
            res_json["message"] = ""
            res_json["conversation_id"] = conversation_id
            return api_json_response_format(True, "success!", 200, res_json)
        else:
            # background_runner.update_log_async(MODULE_NAME,"chatbot_fn","Chatbot","Chat reset not successfull",404)
            return api_json_response_format(False, "Chat reset not successfull. Please try again.", 404, {})
    elif flag == "close":        
        conversation_id = get_conversationid_from_user_phoneno(email_id,session_id)
        if conversation_id:
            chat_history_json = get_chat_history(conversation_id)
            # lang_config = get_lang_config(lang_id,service_id)
            if chat_history_json:
                chat_conversation = []
                transcript = []
                chat_conversation_str = chat_history_json['chat_conversation']
                transcript_str = chat_history_json['transcript']
                pause_count = int(chat_history_json['pause_count'])
                pause_time = int(chat_history_json['pause_time'])

                if chat_conversation_str:
                    chat_conversation = json.loads(chat_conversation_str)                
                if transcript_str:
                    transcript = json.loads(transcript_str)
                lang_config = ""
                if lang_config: 
                    if 'chat_close_msg' in lang_config:
                        chat_close_msg = lang_config['chat_close_msg'] 
                    else:
                        chat_close_msg = DEFAULT_CLOSE_MSG 
                else:                    
                    chat_close_msg = DEFAULT_CLOSE_MSG
                
                chat_conversation.append({"role": "assistant", "content": chat_close_msg})
                transcript.append("sender" +":BOT"+ ":" + chat_close_msg)
                chat_history_str = json.dumps(chat_conversation)
                transcript_str = json.dumps(transcript)                        
                update_chat_history(conversation_id,chat_history_str,transcript_str,pause_count,pause_time,feedback=feedback,rating=rating)
                res_json = {}
                res_json["message"] = chat_close_msg
                res_json["conversation_id"] = conversation_id
                event_end_session(conversation_id,agent_config)
                return api_json_response_format(True, "success", 200, res_json)
            else:
                return api_json_response_format(False, "Conversation not found. Please reset and try again.", 404, {})   
        else:
            return api_json_response_format(False, "Conversation id not found.", 404, {})
        

async def chatbot_widget(request):
    try:
        chatbot_type = "widget"
        data = request.json        

        if not data:
            return api_json_response_format(False, "Invalid input.", 400, {})
        
        if "email_id" not in data:
            return api_json_response_format(False, "Please provide email id", 404, {})
        
        if "session_id" not in data:
            return api_json_response_format(False, "Please provide session id", 404, {})

        if "msg" not in data:
            return api_json_response_format(False, "Please provide msg", 404, {})
        
        if "flag" not in data:
            return api_json_response_format(False, "Please provide flag", 404, {})

        if "language" not in data:
            return api_json_response_format(False, "Please provide language", 404, {}) 

        if "chatbot_type" in data:
            chatbot_type = data.get('chatbot_type')  
         
        
        email_id = data.get('email_id')
        session_id = data.get('session_id')       
        msg = data.get('msg')
        flag = data.get('flag') 
        language = data.get('language')
        feedback = ""
        rating = 0

        if flag == 'close':
            if 'feedback' in data:
                feedback = data.get('feedback')
            
            if 'rating' in data:
                rating = data.get('rating')
            else:
                return api_json_response_format(False, "Please provide rating", 404, {})
        
        if "email_id" not in data:
            return api_json_response_format(False, "Please provide email id", 404, {})
        
        
        if not session_id:
            return api_json_response_format(False, "User session id not found.", 404, {})
        
        if not language:
            return api_json_response_format(False, "User language not found.", 404, {})
        
        user_data = get_user_data(email_id)
        if user_data['is_exist']:
            query = "select email_id from users where email_id = %s"
        else:
            query = "select email_id from sub_users where email_id = %s"
        values = (email_id,)
        email_id_dict = execute_query(query, values)

        if email_id_dict:
            email_id = email_id_dict[0]['email_id']
        else:
            return api_json_response_format(False,"User not exist",404,{})

        
        res =  await chatbot_fn(flag,email_id,session_id,msg,chatbot_type,feedback,rating,language)
        return res
        
    except Exception as e:
        error = f"Could not initiate chatbot. Error: {str(e)}"
        print(error)
        return api_json_response_format(False, error, 500, {}) 

async def create_vector(request): 
    try:
        data = request.json        

        if not data:
            return api_json_response_format(False, "Invalid input.", 400, {})
        
        if "user_role" not in data:
            return api_json_response_format(False, "Please provide user_role", 404, {})    

        if "user_plan" not in data:
            return api_json_response_format(False, "Please provide user_plan", 404, {})     
        
        # collection_name = data.get('collection_name')  
        user_role = data.get('user_role')
        user_plan = data.get('user_plan')

        if not user_plan:
            return api_json_response_format(False, "Document name not found.", 404, {})
        
        if not user_role:
            return api_json_response_format(False, "user_role not found.", 404, {})
        
        collection_name = f"{user_role}_{user_plan}"
        folder_name = f"{user_role}/{user_plan}"
        query = 'INSERT INTO `qdrant_collections` (`role`, user_plan, `collection_name`, `folder_name`) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE `collection_name` = VALUES(`collection_name`), `folder_name` = VALUES(`folder_name`);'
        value = [(user_role,user_plan,collection_name,folder_name)]
        res = chat_bot_update_query_last_index(query,value)
        if res['row_count'] > 0:
            print("updated successfully")
        index = llama_index.createQdrantVectorIndex(collection_name,rag_folder_name,folder_name)
        if index:
            print("Vector index created successfully.")
    except Exception as e:
        print(f"Error occured in update vector index manual process. Error : {e}")
    finally:
        return "success"

async def get_ragData(request):
    data = request.json        

    if not data:
        return api_json_response_format(False, "Invalid input.", 400, {})
    
    if "user_role" not in data:
        return api_json_response_format(False, "Please provide user role", 404, {})
    
    if "user_plan" not in data:
        return api_json_response_format(False, "Please provide user plan", 404, {})
    
    if "conversation_id" not in data:
        return api_json_response_format(False, "Please provide conversation_id", 404, {})
    
    if "query" not in data:
        return api_json_response_format(False, "Please provide transcript", 404, {})
    
    user_role = data.get('user_role')
    user_plan = data.get('user_plan')
    conversation_id = data.get('conversation_id')
    query = data.get('query')
    transcript = ""
    try:
        arguments = {}
        arguments = {'user_query' : query,
                'conversation_id' :conversation_id}

        query = "SELECT `attribute_value` FROM `agent_attributes` WHERE attribute_name = 'qa_prompt'"
        res = execute_query(query)
        if res:
            qa_prompt = res[0]['attribute_value']
        query = 'SELECT `transcript` FROM `chat_history` WHERE conversation_id = %s'
        res = execute_query(query,(conversation_id,))
        if res:
            transcript = res[0]['transcript']
        collection_name = f"{user_role}_{user_plan}"
        vector_index = llama_index.getQdrantVectorIndex(collection_name)
        query_engine_object = llama_index.getQueryEngine(vector_index,5,collection_name,qa_prompt,transcript)
        arguments['query_engine_object'] = query_engine_object
        result = await GetQAWithRAG(arguments)

    except Exception as e:
        print(f"Error occured in update vector index manual process. Error : {e}")
        result = str(e)
    finally:
        return result

def get_transcript(email_id):
    transcript_summary = None
    try:
        if email_id:
            query = "SELECT transcript,conversation_id FROM `chat_history` WHERE email_id = %s ORDER BY created_at DESC limit 1"
            values = (email_id,)
            transcript_data = chat_bot_execute_query(query, values)

            if transcript_data and len(transcript_data) > 0:
                transcript_result = transcript_data[0]['transcript']
                conversation_id = transcript_data[0]['conversation_id']
                if transcript_result:
                    transcript_summary_prompt = os.environ.get('TRANSCRIPT_SUMMARY_PROMPT')    
        
                    req_messages =  [{"role": "system", "content": f"You are an assistant that summarizes text. {transcript_summary_prompt}"},]
                    req_messages.append({"role": "user", "content": f"Transcript : {transcript_result}"})
                    response = process_openai_completion(req_messages,os.environ.get('OPENAI_MODEL_NAME')) 
                    if response:                  
                        transcript_summary = str(response.choices[0].message.content)
                        query = "UPDATE chat_history SET transcript_summary = %s WHERE email_id = %s AND conversation_id = %s;"
                        values = (transcript_summary,email_id,conversation_id)
                        execute_summary = chat_bot_execute_query(query, values) 
                    else:
                        print("there is response from process openai completion")
                else:
                    print("there is no transcript data")
            else:
                transcript_summary = None

    except Exception as e:  
        print("Error in transcript : %s",str(e))
    finally:
        return transcript_summary
    
def list_transcript_summary(email_id,agent_config):
    init_prompt = None
    transcript_summary = None
    user_data = get_user_data(email_id)
    if not user_data['is_exist']:
        user_data = get_sub_user_data(email_id)
    try: 
        if email_id:       
            query = "SELECT transcript_summary FROM `chat_history` WHERE email_id = %s ORDER BY created_at DESC limit 1"
            values = (email_id,)
            database_summary = chat_bot_execute_query(query, values)
            chat_summary = database_summary[0].get('transcript_summary')
            if not chat_summary:
                transcript_data = get_transcript(email_id)
                if transcript_data:
                    transcript_summary = transcript_data
            else:
                transcript_summary = chat_summary

            if transcript_summary:
                chat_bot_init_prompt = agent_config['generate_init_prompt']
                req_messages =  [{"role": "system", "content": f"You are an assistant that summarizes text. {chat_bot_init_prompt}"},]
                req_messages.append({"role": "user", "content": f"Transcript : {transcript_summary}"})
                response = process_openai_completion(req_messages,os.environ.get('OPENAI_MODEL_NAME'))
                if response:                  
                    init_prompt = str(response.choices[0].message.content)
                else:
                    init_prompt = agent_config['chat_init_msg']
                    init_prompt = init_prompt.replace("{{USERNAME}}", str(user_data['first_name']))
            else:
                init_prompt = agent_config['chat_init_msg']
                init_prompt = init_prompt.replace("{{USERNAME}}", str(user_data['first_name']))

    except Exception as e:  
        print("Error in chat config in list_transcript_summary: %s",str(e))
    finally:
        return init_prompt
    

def uploadDocument(request):
    
    if not request.form:
        return api_json_response_format(False,str("Please fill out the form"),202,{})
    
    if "file_name" in request.form:
        file_name = request.form.get("file_name")
    
    else:
        return api_json_response_format(False,str("File name is required"),202,{})
    
    if "user_role" in request.form:
        user_role = request.form.get("user_role")
    else:
        return api_json_response_format(False,str("File category is required"),202,{})
    
    if "file" in request.files:
        file = request.files.get('file')
    else:
        return api_json_response_format(False,str("File is required"),202,{})
    
    if "user_plan" in request.form:
        user_plan = request.form.get('user_plan')
    else:
        return api_json_response_format(False,str("User id is required"),202,{})

    
    try:
        bucket_name = os.environ.get('CDN_BUCKET')
        S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
        S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
        AWS_REGION = os.environ.get('AWS_REGION')
        s3_client_obj = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,region_name=AWS_REGION)
               
        s3_client_obj.put_object(Bucket=bucket_name,Body='', Key=f"{rag_folder_name}/{user_role}/")
        s3_client_obj.put_object(Bucket=bucket_name, Body='', Key=f"{rag_folder_name}/{user_role}/{user_plan}/")

        # for file in files:
        if file and file_name != '':
            try:
               
                key=f"{rag_folder_name}/{user_role}/{user_plan}/{file_name}"
                file.seek(0)
                s3_client_obj.upload_fileobj(file, bucket_name, key)
        
            except Exception as e:
                print(f"Error while upload catalog docs into s3 bucket.")
                return api_json_response_format(False, f"Error while upload catalog docs into s3 bucket. {e}", 201, {})
        return api_json_response_format(True, "Files uploaded successfully.", 200, {})

    except Exception as e:
        return api_json_response_format(False, f"Invalid file. Exception: {e}", 500, {})

def match_profiles(search_text, job_details):
    batch_size = 30
    filtered_profiles = []
    # if s3_exists(BUCKET_NAME,"admin_search_prompt.json"):     
    #         s3_resource = s3_obj.get_s3_resource()
    #         obj = s3_resource.Bucket(BUCKET_NAME).Object("admin_search_prompt.json")
    #         json_file_content = obj.get()['Body'].read().decode('utf-8')        
    #         prompt_json = json.loads(json_file_content)
    #         level_1 = prompt_json["level_1"]
    #         level_1_prompt = level_1["prompt"]      
    #         level_1_prompt = level_1_prompt.replace("{{search_text}}", search_text)

    for i in range(0, len(job_details), batch_size):
        batch = job_details[i:i+batch_size]
        # prompt = level_1_prompt.replace("{{batch}}", str(batch))
        prompt = f"""Match user-provided search text with the most relevant and potentially relevant job details from the provided list.

                    Guidelines to filter jobs:
                    - Search query will be matched against fields like job title, job_overview, job_desc, specialisation and skills in each job.
                    - Include jobs where the search text matches fully or partially with the fields.
                    - For potential matches, consider jobs where the fields suggest relevance to the search query, even if there's no direct match (e.g., related skills, similar job titles).
                    - Ensure the matching is case-insensitive and prioritize relevance when multiple matches are found.

                    Output Requirements:
                    - The output must strictly be in valid JSON format.
                    - Escape all necessary characters to ensure JSON validity.
                    - Return only the filtered and potentially relevant jobs as a JSON array containing the complete details of matched jobs.
                    - Store the output in a variable called `job_details`. The variable should contain an array of objects.
                    - Do not include any additional text, comments, or explanation after the JSON output.

                    Inputs:
                    1. Search Text: {search_text}
                    2. Profiles: {batch}

                    Example output:
                    {{
                    "job_details": [{{"job_id" : <job_id>}}] 
                    ]
                    }}"""
        prompt = prompt.replace("{{batch}}", str(batch))
        prompt = prompt.replace("{{search_text}}", search_text)
    
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
            batch_results = json.loads(clean_code)["job_details"]
            filtered_profiles.extend(batch_results)
        except Exception as e:
            print(f"Error in match_profiles(): {e}")
            return {"error": str(e)}
    return {"job_details": filtered_profiles}

async def GetQueryRelatedJobs(arguments):
    try:
        email_id = arguments['user_email']
        search_text = arguments['search_text']
        user_data = get_user_data(email_id)
        result_json = []
        if user_data["user_role"] == "professional":
            user_id = user_data["user_id"]
            get_opened_jobs = 'SELECT id, job_title, job_overview, job_desc, specialisation, skills FROM job_post jp WHERE job_status = %s AND NOT EXISTS ( SELECT 1 FROM job_activity ja WHERE ja.job_id = jp.id and ja.professional_id = %s )'
            values = ("opened", user_id,)
            opened_jobs = execute_query(get_opened_jobs, values)
            job_link = "https://devapp.2ndcareers.com/professional/all_jobs?job_id="
            open_ai_result = match_profiles(search_text, opened_jobs)
            final_data = open_ai_result['job_details']
            resultant_search_ids = [id['id'] for id in final_data]

            for id in resultant_search_ids:
                get_job_title = 'SELECT job_title FROM job_post WHERE id = %s'
                values = (id,)
                job_title = execute_query(get_job_title, values)
                if job_title:
                    job_title = job_title[0]['job_title']
                else:
                    job_title = ""
                result_json.append({'job_title': job_title,
                                    'job_url': f"{job_link}{id}"})
            return result_json
        else:
            result_json = api_json_response_format(False, "Unauthorized user", 401, {})
    except Exception as error:
        try:
            temp_dict = {'Exception' : str(error),
                        'Message': "Error in GetQueryRelatedJobs."}
        except Exception as e:  
            print(f"Error in mixpanel event logging: GetQueryRelatedJobs, {str(e)}")
        print(error)
        result_json = api_json_response_format(False, str(error), 500, {})
    finally:
        return result_json
