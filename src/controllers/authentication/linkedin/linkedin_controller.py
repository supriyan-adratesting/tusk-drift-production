
from flask import request, redirect,jsonify
from src import app
from src.controllers.authentication.linkedin import linkedin_process as linkedin
from src.models.user_authentication import api_json_response_format

@app.route('/linkedin_signup')
def linkedin_signup():    
    authorization_url = linkedin.get_redirect_url_signup("")    
    """ if 'user_role' in request.args:
        user_role = request.args.get('user_role')                
        authorization_url = linkedin.get_redirect_url_signup(user_role)
    else:
         return api_json_response_format(False,"user_role required",204,{})    """          
    return redirect(authorization_url)
    # return authorization_url

@app.route('/linkedin_signin')
def linkedin_login():    
    authorization_url = linkedin.get_redirect_url_signin('')   
    return redirect(authorization_url)
    # return authorization_url

@app.route('/linkedin/callback/signup')
def callback_signup():        
    auth_code = request.args.get('code')           
    url_redirect = linkedin.get_linkedin_profile_signup(auth_code)                        
    return redirect(url_redirect)

@app.route('/linkedin/callback/signin')
def callback_signin():        
    auth_code = request.args.get('code')           
    url_redirect = linkedin.get_linkedin_profile_signin(auth_code)                        
    return redirect(url_redirect) 
    

