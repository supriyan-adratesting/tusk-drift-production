import boto3
import os

S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
AWS_REGION = os.environ.get('AWS_REGION')

 

class S3_Client:        
 
    def get_s3_client(self):
        try:                                                             
                        
            s3_client__obj = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,region_name=AWS_REGION)             
            # s3_client__obj = boto3.client('s3')         
            return s3_client__obj
        except Exception as error:
            print("error in get_s3_client(): ",str(error))

    def get_s3_resource(self):
        try:                        
            # Use the new temporary credentials
            s3_resource_obj = boto3.resource('s3', aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,region_name=AWS_REGION)    
            # s3_resource_obj = boto3.resource('s3')         
            return s3_resource_obj
        except Exception as error:
            print("error in get_s3_resource(): ",str(error))

   