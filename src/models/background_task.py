import uuid
from mixpanel import Mixpanel
import os
from datetime import datetime,date, timedelta, timezone
import platform
import time
from src.models.mysql_connector import execute_query,update_query,update_query_last_index
from meilisearch import Client

MEILISEARCH_PROFESSIONAL_INDEX = os.environ.get("MEILISEARCH_PROFESSIONAL_INDEX")
MEILISEARCH_EMPLOYER_INDEX = os.environ.get("MEILISEARCH_EMPLOYER_INDEX")
MEILISEARCH_PARTNER_INDEX = os.environ.get("MEILISEARCH_PARTNER_INDEX")
MEILISEARCH_JOB_INDEX = os.environ.get("MEILISEARCH_JOB_INDEX")
MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
MEILISEARCH_MASTER_KEY = os.environ.get("MEILISEARCH_MASTER_KEY")
MEILISEARCH_ADMIN_JOB_INDEX =  os.environ.get("MEILISEARCH_ADMIN_JOB_INDEX")

class BackgroundTask:
    def __init__(self, executor):        
        self.executor = executor
        self.mp = Mixpanel(os.environ.get('MIXPANEL_PROJECT_TOKEN'))

    def mixpanel_store_events(self,distinct_id,event_name,event_properties):
        try:           
            self.mp.track(distinct_id,event_name, event_properties)
            print(f"Event {event_name} successfully stored into mixpanel for {distinct_id}.")
        except Exception as error:
            print("Exception in mixpanel_store_events() ",error) 

    def mixpanel_store_user_profile(self,user_email_id,user_properties):
        try:
            self.mp.people_set(user_email_id, user_properties, meta = {'$ignore_time' : False, '$ip' : 0})
            print(f"User {user_email_id} successfully stored into mixpanel.")      
        except Exception as error:
            print("Exception in mixpanel_store_user_profile() ",error) 

    def mixpanel_user_async(self,user_email_id,user_json_data, message, user_data):
        task_id = uuid.uuid4().hex  
        self.executor.submit_stored(task_id, self.mixpanel_store_user_profile, user_email_id,user_json_data)
        # return task_id
        task_id = uuid.uuid4().hex  
        self.executor.submit_stored(task_id, self.database_store_client_data, user_email_id,user_json_data, message, user_data)
        return task_id
    
    
    def mixpanel_event_async(self,distinct_id,event_name,event_properties, message = None, user_data = None):
        task_id = uuid.uuid4().hex  
        self.executor.submit_stored(task_id, self.mixpanel_store_events, distinct_id,event_name,event_properties)
         #return task_id
        task_id = uuid.uuid4().hex  
        self.executor.submit_stored(task_id, self.database_store_event_data, distinct_id,event_name,event_properties, message,user_data)
        return task_id
    
    def process_dict(self, email_id, event_name, temp_properties):
        try:
            final_dict = {    
                            '$distinct_id' : email_id, 
                            '$time': int(time.mktime(datetime.now().timetuple())),
                            '$os' : platform.system(),          
                            'Email' : email_id
                        }
            final_dict.update(temp_properties)
            return final_dict
        except Exception as e:
            print(f"Exception in mixpanel {event_name} event",e)
            return f"Exception in mixpanel {event_name} event"

    def task_status(self, task_id):
        try:
            if not self.executor.futures.done(task_id):
                return "running"
            self.executor.futures.pop(task_id)
        except Exception as error:
            print("Exception in task_status() ",error) 
        return "completed"
    
    def send_email_to_employer(self, professional_id, employer_id, job_id):
        from src.models.email.Send_email import send_job_applied_email
        try:
            details = {}
            query = "SELECT Concat(u1.first_name,' ', u1.last_name) as user_name, u1.email_id as professional_email, Concat(u1.city,', ',u1.country) as user_location, jp.job_title, jp.job_status, pp.about FROM users AS u1 JOIN job_post AS jp ON jp.id = %s JOIN professional_profile pp ON pp.professional_id = u1.user_id WHERE u1.user_id = %s;"
            values = (job_id, professional_id,) #u2.user_id as emp_id, u2.email_id as employer_email, 
            data = execute_query(query, values)
            if employer_id > 500000:
                emp_details_query = 'select email_id as employer_email from sub_users where sub_user_id = %s'
            else:
                emp_details_query = 'select email_id as employer_email from users where user_id = %s'
            values = (employer_id,)
            employer_email_dict = execute_query(emp_details_query, values)
            if employer_email_dict:
                employer_email = employer_email_dict[0]['employer_email']
            else:
                employer_email = ''
            if len(data) > 0:
                job_status = data[0]['job_status']
                # emp_id = data[0]['emp_id']
            redirect_url = f"https://devapp.2ndcareers.com/employer_dashboard/candidates?job_id={job_id}&&prof_id={professional_id}&&job_status={job_status}"
            if len(data) > 0:
                # details.update({"prof_first_name" : data[0]['first_name']})
                details.update({"job_title" : data[0]['job_title']})
                details.update({"user_name" : data[0]['user_name']})
                details.update({"user_location" : data[0]['user_location']})
                details.update({"prof_email" : data[0]['professional_email']})
                details.update({"emp_email" : employer_email})
                details.update({"about" : data[0]['about']})
                details.update({"url" : redirect_url})
            send_job_applied_email(details)
        except Exception as e:
            print(f"Exception in send_email_to_employer() ",e)
        return "Email sent"
    
    def admin_recmnd_email(self, job_id, user_id, role_id):
        from src.models.email.Send_email import send_sc_job_recmnd_email

        try:
            details = {}
            key = '2ndC'
            if role_id == 2:
                query = "select Concat(u.first_name,' ',u.last_name) as user_name, u.email_id, Concat(u.city,', ',u.country) as user_location, pp.professional_id, pp.about from users u join professional_profile pp on pp.professional_id = u.user_id where u.user_id = %s"
                values = (user_id,)
                professional_details = execute_query(query, values)
                query = "select jp.employer_id, u.email_id as emp_email from job_post jp left join users u on u.user_id = jp.employer_id where jp.id=%s"
                values = (job_id,)
                emp_email = execute_query(query, values)
                to_addr, user_name, user_location, about, professional_id = '','','','',''
                if len(emp_email) > 0:
                    to_addr = emp_email[0]['emp_email']
                else:
                    print("Error in admin recmnd email.")
                if len(professional_details) > 0:
                    user_name = professional_details[0]['user_name']
                    user_location = professional_details[0]['user_location']
                    about = professional_details[0]['about']
                    professional_id = professional_details[0]['professional_id']
                else:
                    print("Error in admin recmnd email.")
                details.update({"email_id" : to_addr, "user_name" : user_name, "user_location" : user_location, "about" : about, "professional_id" : professional_id})
            elif role_id == 3:
                query = "SELECT jp.job_title, jp.employer_id, CONCAT(SUBSTRING(jp.job_overview, 1, 100), '...') AS job_overview, CONCAT(COALESCE(jp.city, ''), ', ', COALESCE(jp.country, '')) AS job_location, COALESCE(ep.company_name, 'N/A') AS company_name, u.email_id FROM job_post jp LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id LEFT JOIN users u on u.user_id = jp.employer_id WHERE jp.id = %s"
                values = (job_id,)
                job_details = execute_query(query, values)
                email_query = "select email_id from users where user_id=%s"
                values = (user_id,)
                email_id_dict = execute_query(email_query, values)
                if len(job_details) > 0:
                    job_title = job_details[0]['job_title']
                    company_name = job_details[0]['company_name']
                    job_overview = job_details[0]['job_overview']
                    job_location = job_details[0]['job_location']
                    email_id = email_id_dict[0]['email_id'] if email_id_dict else ''
                    details.update({"job_id": job_id, "job_title" : job_title, "company_name" : company_name, "job_overview" : job_overview, "job_location" : job_location, "email_id" : email_id})
            send_sc_job_recmnd_email(details, role_id, key)
        except Exception as e:
            print(f"Exception in send_email_to_employer() ",e)
        return "Email sent"
        
    def ai_recmnd_email(self, job_id, professional_id):
        from src.models.email.Send_email import send_sc_job_recmnd_email
        try:
            details = {}
            key = 'AI'
            query = "SELECT jp.job_title, jp.employer_id, CONCAT(SUBSTRING(jp.job_overview, 1, 100), '...') AS job_overview, CONCAT(COALESCE(jp.city, ''), ', ', COALESCE(jp.country, '')) AS job_location, COALESCE(ep.company_name, 'N/A') AS company_name, u.email_id FROM job_post jp LEFT JOIN employer_profile ep ON ep.employer_id = jp.employer_id LEFT JOIN users u on u.user_id = jp.employer_id WHERE jp.id = %s"
            values = (job_id,)
            job_details = execute_query(query, values)
            email_query = "select email_id from users where user_id=%s"
            values = (professional_id,)
            email_id_dict = execute_query(email_query, values)
            if len(job_details) > 0:
                job_title = job_details[0]['job_title']
                company_name = job_details[0]['company_name']
                job_overview = job_details[0]['job_overview']
                job_location = job_details[0]['job_location']
                email_id = email_id_dict[0]['email_id'] if email_id_dict else ''
                details.update({"job_id": job_id, "job_title" : job_title, "company_name" : company_name, "job_overview" : job_overview, "job_location" : job_location, "email_id" : email_id})
            send_sc_job_recmnd_email(details, 3, key)
        except Exception as e:
            print(f"Exception in send_email_to_employer() ",e)
        return "Email sent"
    
    def send_email_job_apply_invite(self, professional_id, employer_id, job_id):
        from src.models.email.Send_email import send_job_apply_invite
        try:
            details = {}
            query = "SELECT u1.user_id AS prof_id, u1.email_id AS professional_email, u2.user_id AS emp_id, u2.email_id AS employer_email, ep.company_name, jp.id, jp.job_title, jp.job_status, CONCAT(COALESCE(jp.city, ''), ', ', COALESCE(jp.country, '')) AS job_location, CONCAT(SUBSTRING_INDEX(jp.job_desc, ' ', 20), '...') AS job_desc FROM users AS u1 JOIN users AS u2 ON u2.user_id = %s JOIN job_post AS jp ON jp.id = %s JOIN employer_profile AS ep ON ep.employer_id = u2.user_id WHERE u1.user_id = %s;"
            values = (employer_id, job_id, professional_id,)
            data = execute_query(query, values)
            if len(data) > 0:
                job_status = data[0]['job_status']
                prof_id = data[0]['prof_id']
                details.update({"professional_id" : prof_id})
                details.update({"job_id" : data[0]['id']})
                details.update({"job_title" : data[0]['job_title']})
                details.update({"to_email" : data[0]['professional_email']})
                details.update({"from_email" : data[0]['employer_email']})
                details.update({"job_desc" : data[0]['job_desc']})
                details.update({"job_location" : data[0]['job_location']})
                details.update({"company_name" : data[0]['company_name']})
                redirect_url = f"https://devapp.2ndcareers.com/professional/all_jobs?job_id={job_id}&&prof_id={prof_id}&&job_status={job_status}"
                details.update({"url" : redirect_url})
                send_job_apply_invite(details, "Invite To Apply")
            else:
                print("Error in send_email_job_apply_invite() - No data found")
                return "Email not sent"
        except Exception as e:
            print(f"Exception in send_email_to_employer()",e)
        return "Email sent"
    
    def send_email_for_shortlisted_candidates(self, professional_id, job_id, employer_user_id):
        from src.models.email.Send_email import send_shortlisted_email
        try:
            details = {}
            query = "SELECT u1.user_id AS prof_id, u1.email_id AS professional_email, u2.user_id AS emp_id, u2.email_id AS employer_email, ep.company_name, jp.id, jp.job_title, jp.job_status, CONCAT(COALESCE(jp.city, ''), ', ', COALESCE(jp.country, '')) AS job_location, CONCAT(SUBSTRING_INDEX(jp.job_desc, ' ', 20), '...') AS job_desc FROM users AS u1 JOIN users AS u2 ON u2.user_id = %s JOIN job_post AS jp ON jp.id = %s JOIN employer_profile AS ep ON ep.employer_id = u2.user_id WHERE u1.user_id = %s;"
            values = (employer_user_id, job_id, professional_id,)
            data = execute_query(query, values)
            if len(data) > 0:
                job_status = data[0]['job_status']
                prof_id = data[0]['prof_id']
                details.update({"professional_id" : prof_id})
                details.update({"job_id" : data[0]['id']})
                details.update({"job_title" : data[0]['job_title']})
                details.update({"to_email" : data[0]['professional_email']})
                details.update({"from_email" : data[0]['employer_email']})
                details.update({"job_desc" : data[0]['job_desc']})
                details.update({"job_location" : data[0]['job_location']})
                details.update({"company_name" : data[0]['company_name']})
                redirect_url = f"https://devapp.2ndcareers.com/professional/all_jobs?job_id={job_id}&&prof_id={prof_id}&&job_status={job_status}"
                details.update({"url" : redirect_url})
                send_shortlisted_email(details, "Send Shortlisted Email")
            else:
                print("Error in send_email_job_apply_invite() - No data found")
                return "Email not sent"
        except Exception as e:
            print(f"Exception in send_email_to_employer()",e)
        return "Email sent"
    
    def send_plan_end_email(self, email_id):
        from src.models.email.Send_email import notify_plan_end
        try:
            query = 'select first_name, last_name from users where email_id = %s'
            values = (email_id,)
            data = execute_query(query, values)
            if len(data) > 0:
                first_name = data[0]['first_name']
                last_name = data[0]['last_name']
                full_name = first_name + " " + last_name
            else:
                full_name = ''
            redirect_url = f"https://devapp.2ndcareers.com/employer_dashboard/pricing-plan"
            notify_plan_end(email_id, full_name, redirect_url, "Trial End Notification")  
        except Exception as e:
            print(f"Exception in send_plan_end_email()",e)
        return "Email sent"
    
    def send_plan_cancelled_email(self, email_id):
        from src.models.email.Send_email import notify_plan_cancelled
        try:
            query = 'select first_name, last_name, current_period_end from users where email_id = %s'
            values = (email_id,)
            data = execute_query(query, values)
            if len(data) > 0:
                first_name = data[0]['first_name']
                last_name = data[0]['last_name']
                full_name = first_name + " " + last_name
                end_date = data[0]['current_period_end']
                converted_date = datetime.fromtimestamp(end_date, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
                print(converted_date)
            else:
                full_name = ''
                converted_date = ''
            redirect_url = f"https://devapp.2ndcareers.com/employer_dashboard/pricing-plan"
            notify_plan_cancelled(email_id, full_name, redirect_url, converted_date, "Plan Cancelled Notification")  
        except Exception as e:
            print(f"Exception in send_plan_cancelled_email()",e)
        return "Email sent"
    
    def store_employer_details_in_meilisearch_cloud(self, documet):
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

    def get_employer_details(self, employer_id):
        try:
            query = "SELECT u.user_id, u.user_role_fk, u.email_active, u.first_name, u.last_name, u.email_id, u.dob, u.country_code, u.contact_number, u.country, u.state, u.gender, u.company_code, u.city, u.profile_percentage, u.pricing_category, u.is_active, u.payment_status, u.subscription_id, u.is_trial_started, u.old_plan, u.existing_pricing_key, ep.company_name, ep.designation, ep.company_description, ep.sector, ep.employer_type, ep.website_url FROM users u LEFT JOIN employer_profile ep ON ep.employer_id = u.user_id WHERE u.user_role_fk = 2 and u.user_id = %s ORDER BY u.user_id;"
            values = (employer_id,)
            emp_details_dict = execute_query(query, values)
            self.store_employer_details_in_meilisearch_cloud(emp_details_dict)
        except Exception as error:
            print(f"An error occurred while fetching employer details from DB to store in Meilisearch: {error}")

    def store_partner_details_in_meilisearch_cloud(self, documet):
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
            partner_details_dict = execute_query(query, values)
            self.store_partner_details_in_meilisearch_cloud(partner_details_dict)
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
            professional_details_dict = execute_query(query, values)
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

    def store_job_details_in_meilisearch_cloud(self, documet, index_name):
        try:
            # Store documents in Meilisearch
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
            query = """SELECT jp.`id` AS job_id, ep.`employer_id`, ep.company_name, ep.designation, ep.company_description, ep.sector, ep.employer_type, ep.website_url, u.city, u.country,
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
                    LEFT JOIN (SELECT sq.job_id, JSON_ARRAYAGG(sq.`custom_pre_screen_ques`) AS questions FROM pre_screen_ques sq GROUP BY sq.job_id) AS psq ON psq.job_id = jp.id where jp.id = %s
                    GROUP BY jp.`id`, ep.`employer_id`, ep.company_name, ep.designation, ep.company_description, ep.sector, ep.employer_type, ep.website_url, u.pricing_category;"""
            values = (job_id,)
            job_details_dict = execute_query(query, values)
            if job_details_dict:
                job_skills = job_details_dict[0]["skills"]
                if job_skills:
                    skills_list = [item.strip() for item in job_skills.split(",")]
                    job_details_dict[0].update({'skills':skills_list})
                else:
                    skills_list = []
            self.store_job_details_in_meilisearch_cloud(job_details_dict,MEILISEARCH_JOB_INDEX)
        except Exception as error:
            print(f"An error occurred while fetching job details from DB to store in Meilisearch: {error}")

    def database_store_event_data(self, distinct_id,event_name,event_json_data,message, user_data):
        try:    
            error = ""
            country = ""
            city = ""
            userrole = ""
            email_id = ""
            if(user_data != None):
                if 'email_id' in user_data:
                    email_id = user_data['email_id']
                if 'user_role' in user_data:
                    userrole = user_data['user_role']
                if 'Error' in event_json_data and event_json_data['Error']:
                    error = event_json_data['Error']
                    
                if 'city' in user_data and user_data["city"]:
                    city = user_data["city"]
                    
                if 'country' in user_data and user_data['country']:
                    country = user_data['country']
            query = '''
                    INSERT INTO event_data (distinct_id, email_id, user_role, created_at, event_name,message, city, country, error)
                    VALUES (%s, %s, %s, %s,%s, %s,%s, %s, %s)
                    '''        
            values = (distinct_id, email_id, userrole, (datetime.now(),),event_name,message, city, country, error)   
            res_obj = update_query_last_index(query,values)
            print(res_obj)
            if res_obj['row_count'] > 0:
                row_count = res_obj['last_index']
                if row_count > 0:  
                    print("Event data stored into the database.")
                else:
                    print("Event data not stored into the database")
            
        except Exception as e:
              print(f"Failed to insert event data into the database for {event_name}. Error "+ f'{e}')   

    def database_store_client_data(user_email_id,user_json_data, message, user_data):

        try:    
            error = ""
            country = ""
            city = ""
            userrole = ""
            email_id = ""
            if(user_data != None):
                distinct_id = user_data['email_id']
                email_id = user_data['email_id']
                userrole = user_data['user_role']
                if 'Error' in user_json_data and user_json_data['Error']:
                    error = user_json_data['Error']

                if 'city' in user_data and user_data["city"]:
                    city = user_data["city"]
                
                if 'country' in user_data and user_data['country']:
                    country = user_data['country']

                if 'Error' in user_json_data and user_json_data['Error']:
                    error = user_json_data['Error']

                if 'country' in user_data and user_data['country']:
                    country = user_data['country']
        
            query = '''
                    INSERT INTO client_data (distinct_id, email_id, user_role, created_at, message, city, country, error)
                    VALUES (%s, %s, %s, %s,%s, %s,%s, %s, %s)
                    '''        
            values = (distinct_id, email_id, userrole, (datetime.now(),),message, city, country, error)   
            res_obj = update_query_last_index(query,values)
            print(res_obj)
            if res_obj['row_count'] > 0:
                row_count = res_obj['last_index']
                if row_count > 0:  
                    print("Client data stored into the database.")
                else:
                    print("Client data not stored into the database")
            
        except Exception as e:
              print("Failed to insert event data into the database. Error "+ f'{e}')

    def get_admin_job_details(self, job_id):
        try:
            query = """SELECT ap.job_reference_id AS job_id, ap.job_title, ap.job_type, ap.job_overview, ap.company_name, ap.company_sector as sector,
                    ap.job_description AS job_desc, ap.skills, ap.country AS job_country, ap.state, ap.city AS job_city, ap.schedule AS work_schedule,
                    ap.workplace_type, ap.is_active, ap.functional_specification AS specialisation, ap.admin_job_status AS job_status,   

                    NULL AS pricing_category,
                    DATE_FORMAT(ap.`created_at`, '%%Y-%%m-%%d %%H:%%i:%%s') AS created_at, 
                    DATE_FORMAT(ap.`updated_at`, '%%Y-%%m-%%d %%H:%%i:%%s') AS updated_at, 
                    DATE_FORMAT(DATE_ADD(ap.`created_at`, INTERVAL 30 DAY), '%%Y-%%m-%%d %%H:%%i:%%s') AS closed_on
                    FROM `admin_job_post` ap WHERE ap.job_reference_id = %s;"""
            values = (job_id,)
            job_details_dict = execute_query(query, values)
            if job_details_dict:
                job_skills = job_details_dict[0]["skills"]
                if job_skills:
                    skills_list = [item.strip() for item in job_skills.split(",")]
                    job_details_dict[0].update({'skills':skills_list})
                else:
                    skills_list = []
            self.store_job_details_in_meilisearch_cloud(job_details_dict, MEILISEARCH_ADMIN_JOB_INDEX)
        except Exception as error:
            print(f"An error occurred while fetching job details from DB to store in Meilisearch: {error}")

    def delete_admin_job_details_in_meilisearch(self, job_ids):
        """
        Deletes multiple job documents from Meilisearch by their document IDs.
        :param job_ids: list of job reference IDs
        """

        client = Client(MEILISEARCH_URL, MEILISEARCH_MASTER_KEY)
        index = client.index(MEILISEARCH_ADMIN_JOB_INDEX)  # your index name

        # Ensure job_ids is a list (Meilisearch expects a list)
        if isinstance(job_ids, (str, int)):
            job_ids = [job_ids]

        result = index.delete_documents(job_ids)

        print(f"Documents deleted: {result}")
