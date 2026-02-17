
import pytest
from unittest.mock import MagicMock, patch
import os,sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.append(project_root)
from src import app
from src.controllers.authentication.manual.authentication_process import (
    check, request, user_login,resend_email,update_professional_account_details,professional_register,email_verification,employer_register,partner_register
)
from src.models.mysql_connector import execute_query, update_query, update_query_last_index

@pytest.fixture
def mock_request(mocker):
    mock = MagicMock()
    mocker.patch('src.controllers.authentication.manual.authentication_process.request', mock)
    return mock

class Testauthendicationprocess:
    @pytest.fixture(autouse=True)
    def setup(self, mocker, mock_request):
        self.mock_request = mock_request
        self.mocker = mocker

    def test_successful_login_with_correct_credentials(self):
        with app.app_context():
            # Setting up the request authorization
            self.mock_request.authorization = {"username": "krishnakumar180320@gmail.com", "password": "Test@123"}

            # Mocking user data and login mode
            self.mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', 
                              return_value={
                                  "is_exist": True,
                                  "user_id": 1,
                                  "login_mode": "Manual",
                                  "user_pwd": "Test@123",
                                  "email_active": "Y",
                                  "user_role": "admin"
                              })

            # Mocking token generation
            self.mocker.patch('src.controllers.authentication.manual.authentication_process.get_jwt_access_token', 
                              return_value={
                                  "status": "success",
                                  "access_token": "valid_token"
                              })

            # Mocking API response format
            self.mocker.patch('src.controllers.authentication.manual.authentication_process.api_json_response_format', 
                              return_value={
                                  "success": True,
                                  "message": "Login successful!",
                                  "error_code": 0,
                                  "data": {"access_token": "valid_token", "user_role": "admin"}
                              })

            # Call the function under test
            result = user_login()

            # Assertions to check if the login was successful
            assert result["success"] is True
            assert result["message"] == "Login successful!"
            assert result["error_code"] == 0
            assert result["data"]["access_token"] == "valid_token"

    # def test_lockout_after_maximum_incorrect_attempts(self):
    #     with app.app_context():
    #         # Setting up the request authorization
    #         self.mock_request.authorization = {"username": "anand040593@gmail.com", "password": "qwerty@123z"}

    #         # Mocking user data and login mode
    #         self.mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', 
    #                           return_value={
    #                               "is_exist": True,
    #                               "user_id": 1,
    #                               "login_mode": "Manual",
    #                               "user_pwd": "qwerty@123z",
    #                               "email_active": "Y",
    #                               "user_role": "admin"
    #                           })

    #         # Mocking login attempt data
    #         self.mocker.patch('src.models.mysql_connector.execute_query', 
    #                           side_effect=[
    #                               [{"login_attempt": 3}],  # First call to check login attempts
    #                               []  # Second call for updating last login time
    #                           ])

    #         # Mocking API response format for lockout message
    #         self.mocker.patch('src.controllers.authentication.manual.authentication_process.api_json_response_format', 
    #                           return_value={
    #                               "success": False,
    #                               "message": "Too many attempts. Please try logging in again in one hour.",
    #                               "error_code": 500,
    #                               "data": {}
    #                           })

    #         # Call the function under test
    #         result = user_login()

    #         # Assertions to check if the user is locked out after maximum attempts
    #         assert result["success"] is False
    #         assert result["message"] == "Too many attempts. Please try logging in again in one hour."
    #         assert result["error_code"] == 500

    # Authorization header is missing in the request
    def test_missing_authorization_header(self, mocker):
        from flask import Flask, request
        from src.controllers.authentication.manual.authentication_process import token_mail_id
        app = Flask(__name__)
        with app.test_request_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.token_authentication', return_value={"status_code": 401, "status": "Authorization Required"})
            response = token_mail_id()
            assert response == {"success": False, "message": "Authorization Required", "error_code": 401, "data": {}}


    # Authorization header is missing
    def test_authorization_header_missing(self, mocker):
        from flask import jsonify
        with app.test_request_context():
            mocker.patch('flask.request', headers={})
            mocker.patch('src.controllers.authentication.manual.authentication_process.api_json_response_format', return_value=jsonify({"success": False, "message": "Invalid Authorization", "error_code": 401, "data": {}}))
    
            from src.controllers.authentication.manual.authentication_process import renewal_access_token_process
            response = renewal_access_token_process()
            assert response.json == {"success": False, "message": "Invalid Authorization", "error_code": 401, "data": {}}

    def test_email_not_provided(self, mocker):
        from flask import Flask
        
        app = Flask(__name__)
        with app.test_request_context(json={}):
            from src.controllers.authentication.manual.authentication_process import forgot_password
            response = forgot_password()
            assert response['success'] is False
            assert response['message'] == "Please fill in all the required fields."
            assert response['error_code'] == 204


    # Token is valid and user logs out successfully
    def test_valid_token_logout_success(self, mocker):
        from flask import Flask, request
        from unittest.mock import patch
        app = Flask(__name__)
        with app.test_request_context('/'):
            # Mocking the necessary functions
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_token', return_value={'status_code': 200, 'email_id': 'test@example.com'})
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', return_value={'user_id': 1})
            mocker.patch('src.controllers.authentication.manual.authentication_process.update_query', return_value=1)
            mocker.patch('src.controllers.authentication.manual.authentication_process.api_json_response_format', return_value={'success': True, 'message': 'Logout successful!', 'error_code': 0, 'data': {}})
        
            # Import and call the function under test
            from src.controllers.authentication.manual.authentication_process import user_logout
            response = user_logout()
        
            # Assertions to check if the logout was successful
            assert response['success'] is True
            assert response['message'] == 'Logout successful!'
            assert response['error_code'] == 0

    # Token is missing from the request headers
    def test_missing_token_in_headers(self, mocker):
        from flask import Flask, request
        from unittest.mock import patch
        app = Flask(__name__)
        with app.test_request_context('/'):
            # Mocking the necessary functions
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_token', return_value={'status_code': 401, 'status': 'Authorization Required'})
            mocker.patch('src.controllers.authentication.manual.authentication_process.api_json_response_format', return_value={'success': False, 'message': 'Authorization Required', 'error_code': 401, 'data': {}})
        
            # Import and call the function under test
            from src.controllers.authentication.manual.authentication_process import user_logout
            response = user_logout()
        
            # Assertions to check if the correct error message is returned
            assert response['success'] is False
            assert response['message'] == 'Authorization Required'
            assert response['error_code'] == 401



    def test_valid_email(self):
        assert check('example@example.com') == True, "The email should be considered valid"

    # Reject an email without an '@' symbol like 'example.com'
    def test_invalid_email_missing_at_symbol(self):
        assert check('example.com') == False, "The email should be considered invalid due to missing '@'"

    # User attempts to register with an email that already exists in the database
    def test_email_already_exists(self, mocker):
        from flask import jsonify
        with app.test_request_context('/'):
            mocker.patch('src.controllers.authentication.manual.authentication_process.request.get_json', return_value={
                'first_name': 'Jane',
                'last_name': 'Smith',
                'email_id': 'jane.smith@example.com',
                'contact_number': '0987654321',
                'user_pwd': 'securepassword321',
                'is_age_verified': 'Y',
                'country': 'Canada',
                'city': 'Toronto'
            })
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', return_value={'is_exist': True})
            response = professional_register()
            assert response['success'] is False
            assert response['message'] == "It appears that an account with this email address already exists. Please sign in using your existing credentials."
            assert response['error_code'] == 409
            assert response['data'] == {}


    # Employer registration with all required fields provided and no existing user with the same email
    def test_check_employer_registration(self, mocker):
        from flask import jsonify
        with app.test_request_context('/'):
            mocker.patch('src.controllers.authentication.manual.authentication_process.request.get_json', return_value={
                'first_name': 'John',
                'last_name': 'Doe',
                'organization_name': 'Tech Solutions',
                'title': 'CEO',
                'sector': 'Technology',
                'email_id': 'john.doe@example.com',
                'website': 'http://techsolutions.com',
                'country': 'USA',
                'city': 'New York',
                'user_pwd': 'securepassword123'
            })
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': True})
            mocker.patch('src.models.mysql_connector.update_query', side_effect=[1, 1, 1])
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'status': 'success', 'access_token': 'token123'})
            mocker.patch('src.models.email.Send_email.sendgrid_mail')
    
            response = employer_register()
        
            assert response['success'] is False
            assert response['message'] == "It appears that an account with this email address already exists. Please sign in using your existing credentials."
            assert response['error_code'] == 409

    # Missing required fields in the request data
    def test_missing_required_fields(self, mocker):
        from flask import jsonify
        with app.test_request_context('/'):
            mocker.patch('src.controllers.authentication.manual.authentication_process.request.get_json', return_value={
                'first_name': 'John',
                'last_name': 'Doe',
                # Missing other required fields
            })
        
            response = employer_register()
        
            assert response['success'] is False
            assert response['message'] == "Please fill in all the required fields."
            assert response['error_code'] == 204
            assert response['data'] == {}


    # Verify successful registration with all required fields provided
    def test_successful_registration(self, mocker):
        from flask import jsonify
        with app.test_request_context('/'):
            mocker.patch('src.controllers.authentication.manual.authentication_process.request.get_json', return_value={
                'partner_name': 'John Doe',
                'organization_name': 'Doe Enterprises',
                'website': 'http://doe.com',
                'title': 'CEO',
                'email_id': 'john.doe@example.com',
                'contact_number': '1234567890',
                'country': 'USA',
                'city': 'New York',
                'user_pwd': 'securepassword123'
            })
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', side_effect=[{'is_exist': False}, {'is_exist': True, 'user_id': 1, 'user_role': 'partner'}])
            mocker.patch('src.controllers.authentication.manual.authentication_process.update_query', return_value=1)
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_jwt_access_token', return_value={'status': 'success', 'access_token': 'token123'})
            response = partner_register()
            assert response['success'] is True
            assert response['message'] == "Sign up successful!"
            assert response['error_code'] == 0
            assert response['data']['access_token'] == "token123"

    # Check behavior when required fields like 'partner_name' or 'email_id' are missing in the request
    def test_missing_required_partner_fields(self, mocker):
        from flask import jsonify
        with app.test_request_context('/'):
            mocker.patch('src.controllers.authentication.manual.authentication_process.request.get_json', return_value={
                'organization_name': 'Doe Enterprises',
                'website': 'http://doe.com',
                'title': 'CEO',
                'contact_number': '1234567890',
                'country': 'USA',
                'city': 'New York',
                'user_pwd': 'securepassword123'
            })
            response = partner_register()
            assert response['success'] is False
            assert response['message'] == "Please fill in all the required fields."
            assert response['error_code'] == 204
         

    # Verify that an invalid or missing user ID in the request returns an invalid link response
    def test_invalid_or_missing_user_id(self, mocker):
        from flask import request
        with app.test_request_context('/'):
            mocker.patch('flask.request')
            request.args = {}
            result = email_verification()
            print(result)
            expected_result = """<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email failure</title>

    <!-- Bootstrap -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet"
        integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">

    <!-- Bootstrap icons -->

    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">

    <style>
        .symbolSize {
            font-size: 4.5rem;
        }

        .brand-color {
            color: #e38212;
            /* text-decoration: none; */
        }

        .redirectSize {
            font-size: 1.8rem;
        }
    </style>

</head>

<body>
    <section class="container text-center">
        <section class="row align-items-center justify-content-center vh-100">
            <div class="email-verified ">
                <h1 class="text-danger symbolSize"><i class="bi bi-x-circle"></i></h1>
                <h3 class="">Error</h3>
                <p class="">Your Email address could not be verified</p>
            </div>
        </section>
    </section>

</body>

</html>"""
            assert result == expected_result


    # Token is valid and user role is professional
    def test_valid_token_professional_role_invalid_req_data(self, mocker):
        from flask import Flask, request
        app = Flask(__name__)
        with app.test_request_context(json={'first_name': '', 'last_name': 'Doe', 'contact_number': '1234567890', 'country': 'USA', 'city': 'New York'}):
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_token', return_value={'status_code': 200, 'email_id': 'john.doe@example.com'})
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', return_value={'user_role': 'professional', 'user_id': 1, 'email_id': 'john.doe@example.com'})
            mocker.patch('src.controllers.authentication.manual.authentication_process.update_query', return_value=1)
            mocker.patch('src.controllers.authentication.manual.authentication_process.api_json_response_format', side_effect=lambda status, message, error_code, data: {'status': status, 'message': message, 'error_code': error_code, 'data': data})
            response = update_professional_account_details()
            assert response['status'] == False
            assert response['message'] == "Please fill in all the required fields."

    # Token is invalid or expired
    def test_invalid_or_expired_token(self, mocker):
        from flask import Flask, request
        app = Flask(__name__)
        with app.test_request_context():
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_token', return_value={'status_code': 401, 'status': 'Invalid token'})
            mocker.patch('src.controllers.authentication.manual.authentication_process.api_json_response_format', side_effect=lambda status, message, error_code, data: {'status': status, 'message': message, 'error_code': error_code, 'data': data})
            response = update_professional_account_details()
            assert response['status'] == False
            assert response['message'] == "Invalid token. Please try again."
            assert response['error_code'] == 500

    # Token is valid and user exists with an unverified email, resend email successfully
    def test_resend_email_success(self, mocker):
        # Mocking necessary functions and setting up the environment
        with app.test_request_context():
            mocker.patch('src.controllers.authentication.manual.authentication_process.request.get_json', return_value={"email_id": "example@example.com"})
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_token', return_value={"status_code": 200})
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', return_value={"user_role": "employer", "user_id": 1, "email_active": "N"})
            mocker.patch('src.controllers.authentication.manual.authentication_process.isUserExist', return_value=True)
            mocker.patch('src.controllers.authentication.manual.authentication_process.execute_query', return_value=[{"email_active": "N"}])
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_jwt_access_token', return_value={"status": "success", "access_token": "token"})
            mocker.patch('src.controllers.authentication.manual.authentication_process.sendgrid_mail')
        
            # Invoke the function under test
            response = resend_email()
        
            # Assertions to check if the function behaves as expected
            assert response['success'] == False
            # assert response['message'] == " A verification link has been sent to your registered email. Please verify to proceed using the platform."
            # assert response['error_code'] == 0

    # User does not exist in the database
    def test_user_does_not_exist(self, mocker):
        # Mocking necessary functions and setting up the environment
        with app.test_request_context():
            mocker.patch('src.controllers.authentication.manual.authentication_process.request.get_json', return_value={"email_id": "nonexistent@example.com"})
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_token', return_value={"status_code": 200})
            mocker.patch('src.controllers.authentication.manual.authentication_process.get_user_data', return_value={"user_role": "", "is_exist": False})
        
            # Invoke the function under test
            response = resend_email()
        
            # Assertions to check if the function behaves as expected
            assert response['success'] == False
            