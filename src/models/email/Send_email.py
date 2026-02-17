from src import app
import os
import time
import platform
from datetime import datetime as dt
from flask_executor import Executor
from src.models.background_task import BackgroundTask
from src.models.mysql_connector import update_query
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail,Personalization,To,Cc
from dotenv import load_dotenv
import base64

home_dir = "/home"
load_dotenv(home_dir+"/.env")
sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
executor = Executor(app)
background_runner = BackgroundTask(executor)

def sendgrid_mail(from_addr,to_addr,sub,body,event_name):
    from_email = "2nd Careers <" + from_addr + ">"
    message = Mail(
        from_email=from_email,
        to_emails= to_addr,
        subject=sub,
        html_content=body)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "Yes",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "No",
                'Email sent error' : str(e),
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
        print(error.message)

def sendgrid_mail_interview(from_addr,to_addr,cc,sub,body,event_name):
    from_email = "2nd Careers <" + from_addr + ">"
    message = Mail(
        from_email=from_email,
        subject=sub,
        html_content=body)
    personalization = Personalization()
    personalization.add_to(To(to_addr))

    for cc_address in cc:
        personalization.add_cc(Cc(cc_address))

    message.add_personalization(personalization)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "Yes",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "No",
                'Email sent error' : str(e),
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
        print(error.message)

def send_sc_job_recmnd_email(details,role_id, key):
    try:
        from_addr = os.environ.get('SENDER_EMAIL')
        from_email = "2nd Careers <" + from_addr + ">"
        to_addr = details['email_id']
        # to_addr = ""
        body, subject, event_name = '', '', ''
        if role_id == 2:
            index = open(os.getcwd()+"/templates/professional_recommendation.html",'r').read()
            # index = open("/home/applied-sw02/Documents/1_SC_SRC/1_old_src_oct/2ndcareers-back-end/second_careers_project/templates/professional_recommendation.html", 'r').read()
            index = index.replace("{professional_name}",details['user_name'] if details['user_name'] is not None else "")
            index = index.replace("{professional_about}",details['about'] if details['about'] is not None else "")
            index = index.replace("{professional_location}",details['user_location'] if details['user_location'] is not None else "")
            professional_id = details['professional_id']
            profile_redirect_url = f"https://devapp.2ndcareers.com/employer_dashboard/pool?prof_id={professional_id}"
            index = index.replace("{professional_link}",profile_redirect_url if profile_redirect_url is not None else "")
            body = index
            subject = "Profile Recommendation from 2nd Careers"
            event_name = "2nd careers Profile recommendation"
        elif role_id == 3:
            # index = open(os.getcwd()+"/templates/Email_verification.html",'r').read()
            index = open(os.getcwd()+"/templates/job_recommendation.html",'r').read()
            # index = open("/home/applied-sw02/Documents/1_SC_SRC/1_old_src_oct/2ndcareers-back-end/second_careers_project/templates/job_recommendation.html", 'r').read()
            # index = open("/home/applied-sw02/Documents/1_SC_SRC/1_old_src_oct/2ndcareers-back-end/second_careers_project/templates/job_recommendation.html", 'r').read()
            index = index.replace("{job_title}",details['job_title'] if details['job_title'] is not None else "")
            index = index.replace("{company_name}",details['company_name'] if details['company_name'] is not None else "")
            index = index.replace("{job_overview}",details['job_overview'] if details['job_overview'] is not None else "")
            index = index.replace("{job_location}",details['job_location'] if details['job_location'] is not None else "")
            job_id = str(details['job_id'])
            encoded_job_id = base64.b64encode(job_id.encode('utf-8')).decode('utf-8')
            job_redirect_url = f"https://devapp.2ndcareers.com/professional/recommended_jobs?job_id={encoded_job_id}"
            index = index.replace("{job_link}",job_redirect_url if job_redirect_url is not None else "")
            body = index
            subject = "Job Recommendation from 2nd Careers"
            if key == '2ndC':
                event_name = "2ndC Admin Job recommendation"
            else:
                event_name = "2ndC AI Job recommendation"
        message = Mail(
            from_email=from_email,
            to_emails= to_addr,
            subject= subject,
            html_content=body)
        try:
            sg = SendGridAPIClient(sendgrid_api_key)
            sg.send(message)
            try:
                event_properties = {    
                    '$distinct_id' : to_addr, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'is_email_sent' : "Yes",
                    'Email' : to_addr
                }
                background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
            except Exception as e:  
                print("Error in mixpanel_event_log : %s",str(e))
        except Exception as error:
            try:
                event_properties = {    
                    '$distinct_id' : to_addr, 
                    '$time': int(time.mktime(dt.now().timetuple())),
                    '$os' : platform.system(),          
                    'is_email_sent' : "No",
                    'Email sent error' : str(e),
                    'Email' : to_addr
                }
                background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
            except Exception as e:  
                print("Error in mixpanel_event_log : %s",str(e))
            print(error.message)
    except Exception as e:
        print("Error in send_job_recommended_email",str(e))

def send_job_applied_email(details):
    # from_addr = ''
    to_addr = ''
    subject = ''
    body = ''
    from_addr = os.environ.get('SENDER_EMAIL')
    from_email = "2nd Careers <" + from_addr + ">"
    to_addr = details['emp_email']
    url = details["url"]
    index = open(os.getcwd()+"/templates/professional_job_apply.html",'r').read()
    index = index.replace("{job_name}",details['job_title'] if details['job_title'] is not None else "")
    index = index.replace("{professional_name}",details['user_name'] if details['user_name'] is not None else "")
    index = index.replace("{professional_about}",details['about'] if details['about'] is not None else "")
    index = index.replace("{professional_location}",details['user_location'] if details['user_location'] is not None else "")
    index = index.replace("{professional_link}", url if url is not None else "")
    body = index
    subject = "New Application Received"
    event_name = "Professional Job Applied Email"
    message = Mail(
            from_email=from_email,
            to_emails= to_addr,
            subject= subject,
            html_content=body)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        try:
            event_name = "Professional Job Applied Email"
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "Yes",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "No",
                'Email sent error' : "Error in Professional Job Applied Email",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
        print(error.message)

def send_job_apply_invite(details,event_name):
    from_addr = os.environ.get('SENDER_EMAIL')
    from_email = "2nd Careers <" + from_addr + ">"
    to_addr = details['to_email']
    professional_id = details['professional_id']
    job_id = details['job_id']
    index = open(os.getcwd()+"/templates/job_invitation.html",'r').read()
    # index = open("/home/applied-sw02/Documents/1_SC_SRC/Nov_29_Dev/2ndcareers-back-end/second_careers_project/templates/job_invitation.html", 'r').read()
    index = index.replace("{company_name}",details['company_name'] if details['company_name'] is not None else "")
    index = index.replace("{job_title}",details['job_title'] if details['job_title'] is not None else "")
    index = index.replace("{job_desc}",details['job_desc'] if details['job_desc'] is not None else "")
    index = index.replace("{job_location}",details['job_location'] if details['job_location'] is not None else "")
    index = index.replace("{job_link}",details['url'] if details['url'] is not None else "")
    body = index
    subject = "Your Next Career Move Awaits - Apply Today!"
    message = Mail(
        from_email=from_email,
        to_emails= to_addr,
        subject=subject,
        html_content=body)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        query = 'update invited_jobs set is_invite_sent = %s where job_id = %s and professional_id = %s'
        values = ('Y', job_id, professional_id,)
        update_query(query, values)
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "Yes",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "No",
                'Email sent error' : str(e),
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
        print(error.message)

def send_shortlisted_email(details,event_name):
    from_addr = os.environ.get('SENDER_EMAIL')
    from_email = "2nd Careers <" + from_addr + ">"
    to_addr = details['to_email']
    index = open(os.getcwd()+"/templates/shortlisted_email.html",'r').read()
    # index = open("/home/applied-sw02/Documents/1_SC_SRC/July_31_live_src/2ndcareers-api/second_careers_project/templates/shortlisted_email.html",'r').read()
    # index = open("/home/applied-sw02/Documents/1_SC_SRC/July_31_live_src/2ndcareers-back-end/second_careers_project/templates/shortlisted_email.html", 'r').read()
    index = index.replace("{company_name}",details['company_name'] if details['company_name'] is not None else "")
    index = index.replace("{job_title}",details['job_title'] if details['job_title'] is not None else "")
    index = index.replace("{job_desc}",details['job_desc'] if details['job_desc'] is not None else "")
    index = index.replace("{job_location}",details['job_location'] if details['job_location'] is not None else "")
    index = index.replace("{job_link}",details['url'] if details['url'] is not None else "")
    body = index
    subject = "You have been shortlisted!"
    message = Mail(
        from_email=from_email,
        to_emails= to_addr,
        subject=subject,
        html_content=body)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "Yes",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "No",
                'Email sent error' : str(e),
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
        print(error.message)

def notify_plan_end(to_address, full_name, url, event_name):
    from_addr = os.environ.get('SENDER_EMAIL')
    from_email = "2nd Careers <" + from_addr + ">"
    to_addr = to_address
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
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "Yes",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "No",
                'Email sent error' : str(e),
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
        print(error.message)

def notify_plan_cancelled(to_address, full_name, url, end_date, event_name):
    from_addr = os.environ.get('SENDER_EMAIL')
    from_email = "2nd Careers <" + from_addr + ">"
    to_addr = to_address
    index = open(os.getcwd()+"/templates/cancel_subscripton.html",'r').read()
    # index = open("/home/applied-sw02/Documents/1_SC_SRC/Dec_30_dev_src/2ndcareers-back-end/second_careers_project/templates/cancel_subscripton.html", 'r').read()
    # index = index.replace("{plan}", plan if plan is not None else "")
    index = index.replace("{full_name}", full_name if full_name is not None else "")
    index = index.replace("{redirect_url}", url if url is not None else "")
    index = index.replace("{end_date}", end_date if end_date is not None else "")

    body = index
    subject = "Plan Cancellation Successful"
    message = Mail(
        from_email=from_email,
        to_emails= to_addr,
        subject=subject,
        html_content=body)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "Yes",
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
    except Exception as error:
        try:
            event_properties = {    
                '$distinct_id' : to_addr, 
                '$time': int(time.mktime(dt.now().timetuple())),
                '$os' : platform.system(),          
                'is_email_sent' : "No",
                'Email sent error' : str(e),
                'Email' : to_addr
            }
            background_runner.mixpanel_event_async(to_addr,event_name,event_properties)
        except Exception as e:  
            print("Error in mixpanel_event_log : %s",str(e))
        print(error.message)