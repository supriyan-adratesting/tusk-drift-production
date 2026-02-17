from src import app
from src.models.aws_resources import S3_Client
from flask import jsonify, request, redirect, url_for, session
from src.controllers.jwt_tokens.jwt_token_required import get_user_token,get_jwt_access_token
from src.models.user_authentication import get_user_data,isUserExist,api_json_response_format
import datetime
import json
import os
import boto3

g_log_file_path = os.getcwd()

USER_SESSION_BUCKET = os.environ.get('USER_SESSION_BUCKET')
s3_obj = S3_Client()


def s3_exists(s3_bucket, s3_key):    
    try:
        s3_cient = s3_obj.get_s3_client() 
        s3_cient.head_object(Bucket=s3_bucket,Key=s3_key)
        return True
    except Exception as e:
        print("s3_exists error : "+str(e))
        return False
    
def log_msg(response_data):        
    try: 
        email_id = None
        if not request.authorization == None:            
            email_id = request.authorization["username"]
            user_data = get_user_data(email_id)  
            email_id = user_data["email_id"]
            user_id =  user_data["user_id"]
            email_id = email_id.replace("@","_")
            dictionary = {
                "Date_time": str(datetime.datetime.now()),
                "url_name": request.endpoint,                
                "response": response_data.decode("utf-8")
            }    
                   
        else:  
            token_result = get_user_token(request)                                        
            if token_result["status_code"] == 200:                                
                user_data = get_user_data(token_result["email_id"])     
                email_id = user_data["email_id"]
                user_id =  user_data["user_id"]
                email_id = email_id.replace("@","_")
                dictionary = {
                    "Date_time": str(datetime.datetime.now()),
                    "url_name": request.endpoint,                    
                    "response": response_data.decode("utf-8")
                }
        if not email_id == None:
            if s3_exists(USER_SESSION_BUCKET, str(user_id)+"_"+email_id+".json"):     
                s3 = s3_obj.get_s3_resource() 
                obj = s3.Bucket(USER_SESSION_BUCKET).Object(str(user_id)+"_"+email_id+".json")
                json_file_content = obj.get()['Body'].read().decode('utf-8')        
                data = json.loads(json_file_content)
                data["data"].append(dictionary)
                json_data = json.dumps(data)
                s3_cient = s3_obj.get_s3_client() 
                s3_cient.put_object(Body=json_data,Bucket=USER_SESSION_BUCKET,Key=str(user_id)+"_"+email_id+".json")
                
            else:
                log_data = {"data":[dictionary]}
                json_data = json.dumps(log_data)
                s3_cient = s3_obj.get_s3_client() 
                s3_cient.put_object(Body=json_data,Bucket=USER_SESSION_BUCKET,Key=str(user_id)+"_"+email_id+".json")

    except Exception as error:
        print("Exception in log_msg() ",error)     

def log_msg_auto(user_id,email_id,response_data):  
    try:
        s3_cient = s3_obj.get_s3_client() 
        dictionary = {
                    "Date_time": str(datetime.datetime.now()),
                    "url_name": request.endpoint,                    
                    "response": response_data
                }
        email_id = email_id.replace("@","_")
        if s3_exists(USER_SESSION_BUCKET, str(user_id)+"_"+email_id+".json"):     
            s3 = s3_obj.get_s3_resource() 
            obj = s3.Bucket(USER_SESSION_BUCKET).Object(str(user_id)+"_"+email_id+".json")
            json_file_content = obj.get()['Body'].read().decode('utf-8')        
            data = json.loads(json_file_content)
            data["data"].append(dictionary)
            json_data = json.dumps(data)            
            s3_cient.put_object(Body=json_data,Bucket=USER_SESSION_BUCKET,Key=str(user_id)+"_"+email_id+".json")  
        else:
            log_data = {"data":[dictionary]}
            json_data = json.dumps(log_data)            
            s3_cient.put_object(Body=json_data,Bucket=USER_SESSION_BUCKET,Key=str(user_id)+"_"+email_id+".json")            

    except Exception as error:
        print("Exception in log_msg_auto() : ",error)