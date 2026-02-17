from flask import render_template
from flask import Flask, request, jsonify, session, url_for, redirect

from src import app
from src.controllers.authentication.manual import authentication_process as manual_auth
from src.controllers.jwt_tokens import jwt_token_required as jwt_token

@app.route('/' ,methods=['GET'])  
def start_api():      
    return "hi"

@app.route('/login' ,methods=['POST'])  
def login_process():      
    result = manual_auth.user_login()
    return result

@app.route('/renewal_access_token')
def renewal_access_token():
    result = manual_auth.renewal_access_token_process()
    return result 
  
@app.route('/get_token_email')
def get_token_email():
    result = manual_auth.token_mail_id()
    return result  

@app.route('/forgot_password',endpoint='forgot_password',methods=['POST'])
def forgot_password():
    resp = manual_auth.forgot_password()
    return resp

@app.route('/reset_password_token_validation',endpoint='reset_password_token_validation',methods=['GET'])
def reset_password_token_validation():
    url_redirect = manual_auth.reset_password_validation()
    return redirect(url_redirect)

@app.route('/resend_email',endpoint='resend_email',methods=['POST'])
@jwt_token.token_required
def resend_email():
    result = manual_auth.resend_email()
    return result

@app.route('/update_password',endpoint='update_password',methods=['POST'])
def update_password():
    result = manual_auth.update_password()
    return result

@app.route('/logout',endpoint='logout',methods=['POST','GET'])
@jwt_token.token_required
def user_logout():
    return manual_auth.user_logout()

@app.route('/professional_register',methods=['POST'])
def professional_register():
    return manual_auth.professional_register(request)    

@app.route('/about_you_data', methods = ['GET'])
def about_you_data():
    return manual_auth.about_you_data(request)   

@app.route('/your_career_story_data', methods = ['GET'])
def your_career_story_data():
    return manual_auth.your_career_story_data(request) 

@app.route('/delete_file', methods = ['POST'])
def delete_file():
    return manual_auth.delete_file(request)

@app.route('/employer_register',methods=['POST'])
def employer_register():
    return manual_auth.employer_register()    

@app.route('/partner_register',methods=['POST'])
def partner_register():
    return manual_auth.partner_register()

@app.route('/email_verification',endpoint='email_verification' ,methods=['GET'])
def email_verification():
    mail_res =  manual_auth.email_verification()    
    return mail_res


@app.route('/update_professional_account_details',endpoint='update_professional_account_details' ,methods=['POST'])
@jwt_token.token_required
def update_professional_account_details():
    return manual_auth.update_professional_account_details()

@app.route('/get_social_media_account_info',endpoint='get_social_media_account_info',methods=['GET'])
@jwt_token.token_required
def get_social_media_account_info():
    result = manual_auth.get_social_media_account_info()    
    return result

@app.route('/redirect_email',endpoint='redirect_email',methods=['POST'])
def redirect_email():
    result = manual_auth.redirect_email()    
    return result