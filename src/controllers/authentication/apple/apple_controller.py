from flask import request, redirect,jsonify
from src import app
from src.controllers.authentication.apple import apple_process as apple
from src.models.user_authentication import api_json_response_format

@app.route('/apple_signup', methods = ['GET'])
def apple_signup():    
    authorization_url = apple.get_redirect_url_signup("")    
    """ if 'user_role' in request.args:
        user_role = request.args.get('user_role')                
        authorization_url = linkedin.get_redirect_url_signup(user_role)
    else:
         return api_json_response_format(False,"user_role required",204,{})    """          
    return redirect(authorization_url)
    # return authorization_url

@app.route('/test', methods = ['GET'])
def test_pages():    
    return "sssss"

@app.route('/apple_signin')
def apple_login():    
    authorization_url = apple.get_redirect_url_signin('')   
    return redirect(authorization_url)
    # return authorization_url

@app.route('/apple/callback/signup', methods = ['POST'])
def apple_callback_signup():        
    auth_code = request.args.get('code')
    # auth_code = ""        
    url_redirect = apple.apple_signup_callback(auth_code)                        
    return redirect(url_redirect)

@app.route('/apple/callback/signin', methods = ['POST'])
def apple_callback_signin():        
    auth_code = request.args.get('code')           
    url_redirect = apple.apple_signin_callback(auth_code)                        
    return redirect(url_redirect) 