import pytest
from unittest.mock import MagicMock, patch
import os
import sys

# Set up the project root directory and add it to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.append(project_root)

from src import app
from src.controllers.authentication.linkedin.linkedin_process import (
    get_linkedin_profile_signup, get_linkedin_profile_signin
)
from src.models.mysql_connector import execute_query, update_query, update_query_last_index

@pytest.fixture
def mock_request():
    mock = MagicMock()
    return mock

class TestAuthenticationlinkedinProcess:
    @pytest.fixture(autouse=True)
    def setup(self, mocker, mock_request):
        self.mock_request = mock_request
        self.mocker = mocker


    # Successful LinkedIn profile retrieval and user creation
    def test_successful_linkedin_profile_retrieval_and_user_creation(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and requests.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'given_name': 'John', 'family_name': 'Doe', 'email': 'john.doe@example.com'}))
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': False})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'status': 'success', 'access_token': 'mock_token'})
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')
        
            result = get_linkedin_profile_signup('example_auth_code')
        
            assert 'https://app.2ndcareers.com?message' in result

    # LinkedIn profile retrieval fails
    def test_linkedin_profile_retrieval_fails(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication to simulate failure in getting access token
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', side_effect=Exception("Failed to retrieve access token"))
        
            result = get_linkedin_profile_signup('invalid_auth_code')
        
            assert "Something went wrong" in result



    # User role ID retrieval fails
    def test_user_role_id_retrieval_fails(self, mocker):
        with app.test_request_context():
            mocker.patch('src.models.user_authentication.get_user_roll_id', return_value=-1)
            result = get_linkedin_profile_signup('example_auth_code')
            assert 'Something went wrong. Please try again.' in result

    # Existing user with Google login mode is redirected correctly
    def test_existing_user_google_login_redirect(self, mocker):
        with app.test_request_context():
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'given_name': 'John', 'family_name': 'Doe', 'email': 'john.doe@example.com'}))
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': True, 'login_mode': 'Google', 'user_id': 123})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'status': 'success', 'access_token': 'mock_token'})
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            result = get_linkedin_profile_signup('example_auth_code')

            assert 'https://app.2ndcareers.com' in result

    # Incorrect or malformed authorization code provided
    def test_incorrect_authorization_code(self, mocker):
        with app.test_request_context():
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', side_effect=Exception("Invalid authorization code"))
            result = get_linkedin_profile_signup('invalid_auth_code')
            assert 'message=Something went wrong. Please try again.' in result

    # Handling of network issues during requests to LinkedIn API
    def test_handling_network_issues(self, mocker): 
        with app.test_request_context(): 
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', side_effect=Exception("Network issue"))
            result = get_linkedin_profile_signup('example_auth_code')
            assert 'Something went wrong. Please try again.' in result




    # Handling of invalid or expired LinkedIn authorization code
    def test_handling_invalid_or_expired_auth_code(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and requests.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', side_effect=Exception("Invalid or expired auth code"))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {}))
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': False})
            mocker.patch('src.models.mysql_connector.update_query', return_value=0)
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'status': 'error', 'access_token': ''})
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            result = get_linkedin_profile_signup('invalid_auth_code')

            assert 'message=Something went wrong. Please try again.' in result

    # Handling of missing or incorrect LinkedIn scope permissions
    def test_missing_or_incorrect_scope_permissions(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and requests.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {}))  # Simulating missing or incorrect scope permissions
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': False})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'status': 'success', 'access_token': 'mock_token'})
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            result = get_linkedin_profile_signup('example_auth_code')

            assert 'message=Something went wrong. Please try again.' in result



    # Verify successful LinkedIn profile retrieval and user exists in the database
    def test_successful_linkedin_profile_retrieval(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and request.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'email': 'test@example.com'}))
            # Mocking database and JWT token functions
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': True, 'user_id': '123', 'user_role': 'professional'})
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'access_token': 'jwt_token', 'status': 'success'})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            # Mocking background task runner
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')
        
            result = get_linkedin_profile_signin('valid_auth_code')
            assert 'token=jwt_token' in result 

    # Handle invalid or expired LinkedIn authorization code
    def test_invalid_linkedin_authorization_code(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication to simulate an invalid or expired token scenario
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', side_effect=Exception('Invalid authorization code'))
        
            result = get_linkedin_profile_signin('invalid_auth_code')
            assert 'Something went wrong' in result

    # Verify the function's response to an empty or null auth_code
    def test_empty_auth_code(self, mocker):
        with app.test_request_context():
            result = get_linkedin_profile_signin('')
            assert result == 'https://app.2ndcareers.com?message=Something went wrong. Please try again.'


    # Confirm access token generation and redirection for new user sign-up if user does not exist
    def test_confirm_access_token_generation_and_redirection_for_new_user_signup_if_user_does_not_exist(self, mocker):  
        with app.test_request_context():
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'email': 'test@example.com'}))
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': False, 'user_role': 'professional'})
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'access_token': 'jwt_token', 'status': 'success'})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')
        
            result = get_linkedin_profile_signin('valid_auth_code')
            assert 'https://app.2ndcareers.com/role_selection/professional_signup/social_media?token=jwt_token' in result 

    # Ensure successful update of user login status in the database
    def test_successful_user_login_status_update(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and request.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'email': 'test@example.com'}))
            # Mocking database and JWT token functions
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': True, 'user_id': '123', 'user_role': 'professional'})
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'access_token': 'jwt_token', 'status': 'success'})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            # Mocking background task runner
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            result = get_linkedin_profile_signin('valid_auth_code')
            assert 'https://app.2ndcareers.com/role_selection/professional_signup/social_media?token=jwt_token' in result 

    # Manage scenarios where the database operations fail
    def test_database_operations_fail(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and request.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'email': 'test@example.com'}))
            # Mocking database and JWT token functions to simulate database operation failure
            mocker.patch('src.models.user_authentication.get_user_data', side_effect=Exception("Database error"))
            # Mocking background task runner
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            result = get_linkedin_profile_signin('valid_auth_code')
            assert 'https://app.2ndcareers.com/' in result

    # Address the condition where the JWT token generation fails
    def test_jwt_token_generation_failure(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and request.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', return_value=mocker.Mock(access_token='mock_access_token'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'email': 'test@example.com'}))
            # Mocking database and JWT token functions
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': True, 'user_id': '123', 'user_role': 'professional'})
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'status': 'error'})
            # Mocking background task runner
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            result = get_linkedin_profile_signin('valid_auth_code')
            assert 'message=Something went wrong' in result


    # Ensure proper error handling and logging when an exception occurs
    def test_proper_error_handling(self, mocker):
        with app.test_request_context():
            # Mocking the LinkedIn authentication and request.get to simulate LinkedIn API response
            mocker.patch('linkedin.linkedin.LinkedInAuthentication.get_access_token', side_effect=Exception('Mocked error'))
            mocker.patch('requests.get', return_value=mocker.Mock(json=lambda: {'email': 'test@example.com'}))
            # Mocking database and JWT token functions
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'is_exist': True, 'user_id': '123', 'user_role': 'professional'})
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={'access_token': 'jwt_token', 'status': 'success'})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            # Mocking background task runner
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            result = get_linkedin_profile_signin('valid_auth_code')
            assert 'message=Something went wrong' in result