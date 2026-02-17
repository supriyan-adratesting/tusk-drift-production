from src import app
from flask_cors import CORS
from flask_executor import Executor
from src.models.background_task import BackgroundTask
from flask import request
from src.models.user_authentication import get_user_data
import jwt
import os

CORS(app)
# executor = Executor(app)
# executor.futures._max_workers = 10
# background_runner = BackgroundTask(executor)

# JWT_SECRET_KEY = os.environ.get('JWT_KEY')

# @app.after_request
# def log_responce_info(response):
#     try:        
#         if not request.authorization == None:
#             email_id = request.authorization["username"]
#             user_data = get_user_data(email_id)  
#             email_id = user_data["email_id"]
#             user_id =  user_data["user_id"]
#         else:
#             token = ""
#             if 'Authorization' in request.headers:
#                     bearer_token = request.headers["Authorization"].split(" ")
#                     if len(bearer_token) == 2 :
#                         if bearer_token[0] == "Bearer":
#                             token = bearer_token[1]                    
#                             token_details =  token_authentication(token)
#                             email_id = token_details['email_id']
#                             user_data = get_user_data(email_id)
#                             user_id =  user_data["user_id"]
#                         else:
#                             return {"status_code":401,"status":"Bearer Token Required"}     
#                     else:
#                         return {"status_code":401,"status":"Bearer Token Required "}
#             else:
#                 req = request.get_json()
#                 user_data = get_user_data(req["email_id"])
#                 email_id = user_data["email_id"]
#                 user_id =  user_data["user_id"]
        
#         background_runner.send_session_data_async(response.data, email_id, user_id)
#     except Exception as error:
#         print(error) 
#     return response
# def token_authentication(token):    
#     token_status = 0
#     try:                
#         decoded_data = jwt.decode(jwt=token,key=JWT_SECRET_KEY,algorithms=["HS256"])              
#         if 'sub' in decoded_data:
#             email_id = decoded_data["sub"]                         
#             return {"email_id":email_id,"status_code":200}
#     except Exception as error:
#         print(error)
#         if str(error) == "Signature has expired" or str(error) == "Invalid header padding":
#             token_status = 401
#             return {"status_code":token_status}
#         else:
#             token_status = -1             
#     return {"status_code":token_status}

if __name__ == '__main__':
    app.run()