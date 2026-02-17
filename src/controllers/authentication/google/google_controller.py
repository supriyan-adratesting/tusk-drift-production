import os
from flask import Flask, request, jsonify,redirect
from dotenv import load_dotenv
from src.controllers.authentication.google import google_process as google_auth
from src.models.user_authentication import api_json_response_format
from src import app
home_dir = "/home"
load_dotenv(home_dir+"/.env")
app.secret_key = os.environ.get('SECRET_KEY')
from src import app

@app.route('/google_signup')
def google_signup():        
    return google_auth.get_redirect_url_signup()

@app.route('/google_signin')
def google_login():    
    return google_auth.get_redirect_url_signin()   
     

@app.route('/oauth_google_web_signup')
def google_callback_signup():     
    auth_code = request.args.get('code')   
    url_redirect = google_auth.get_google_profile_signup(auth_code)    
    return redirect(url_redirect)

@app.route('/oauth_google_web_signin')
def google_callback_signin():     
    auth_code = request.args.get('code')   
    url_redirect = google_auth.get_google_profile_signin(auth_code)    
    return redirect(url_redirect)