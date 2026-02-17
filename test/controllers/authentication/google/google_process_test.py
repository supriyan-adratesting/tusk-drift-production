import pytest
from unittest.mock import MagicMock, patch
import os
import sys

# Set up the project root directory and add it to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.append(project_root)

from src import app
from src.controllers.authentication.google.google_process import (
    get_google_profile_signup, get_google_profile_signin
)
from src.models.mysql_connector import execute_query, update_query, update_query_last_index

@pytest.fixture
def mock_request():
    mock = MagicMock()
    return mock

class TestAuthenticationGoogleProcess:
    @pytest.fixture(autouse=True)
    def setup(self, mocker, mock_request):
        self.mock_request = mock_request
        self.mocker = mocker

    # Handle invalid or missing authorization code gracefully
    def test_invalid_auth_code_handling(self, mocker):
        with app.test_request_context():
            auth_code = None
            expected_url = "https://app.2ndcareers.com?message=Something went wrong. Please try again."
            with patch('src.controllers.authentication.google.google_process.oauth_web_singnin.googleweb.authorize_access_token', side_effect=Exception("Invalid auth code")):
                result = get_google_profile_signin(auth_code)
                assert result == expected_url


    # Manage response when OAuth service returns an error or invalid data
    def test_oauth_service_error_handling(self, mocker):
        with app.test_request_context():
            auth_code = "valid_auth_code"
            expected_url = "https://app.2ndcareers.com?message=Something went wrong. Please try again."
            mocker.patch('src.controllers.authentication.google.google_process.oauth_web_singnin.googleweb.authorize_access_token', side_effect=Exception("OAuth service error"))
            result = get_google_profile_signin(auth_code)
            assert result == expected_url

    # Validate system response when token generation fails
    def test_token_generation_failure_response(self, mocker):
        with app.test_request_context():
            auth_code = "valid_auth_code"
            expected_url = "https://app.2ndcareers.com?message=Something went wrong. Please try again."
            mocker.patch('src.controllers.authentication.google.google_process.get_user_data', return_value={
                "is_exist": True,
                "user_id": "123",
                "user_role": "professional"
            })
            mocker.patch('src.controllers.authentication.google.google_process.jwt_token.get_jwt_access_token', return_value={
                "status": "error"
            })
            result = get_google_profile_signin(auth_code)
            assert result == expected_url

    # Test response when user role leads to no defined user table or column
    def test_user_role_no_defined_table_or_column(self, mocker):
        with app.test_request_context():
            auth_code = "valid_auth_code"
            expected_url = "https://app.2ndcareers.com?message=Something went wrong. Please try again."
            mocker.patch('src.controllers.authentication.google.google_process.get_user_data', return_value={
                "is_exist": True,
                "user_id": "123",
                "user_role": "unknown_role"
            })
            result = get_google_profile_signin(auth_code)
            assert result == expected_url

    # Validate handling of unexpected data formats from OAuth service
    def test_unexpected_data_formats_handling(self, mocker):
        with app.test_request_context():
            auth_code = "invalid_auth_code"
            expected_url = "https://app.2ndcareers.com?message=Something went wrong. Please try again."
            mocker.patch('src.controllers.authentication.google.google_process.get_user_data', return_value={
                "is_exist": False
            })
            result = get_google_profile_signin(auth_code)
            assert result == expected_url

    # Check for correct error logging when an exception is thrown
    def test_correct_error_logging_on_exception(self, mocker):
        with app.test_request_context():
            auth_code = "valid_auth_code"
            expected_url = "https://app.2ndcareers.com?message=Something went wrong. Please try again."
            mocker.patch('src.controllers.authentication.google.google_process.get_user_data', side_effect=Exception("Test Exception"))
            result = get_google_profile_signin(auth_code)
            assert result == expected_url

    # Ensure correct error handling when database updates fail
    def test_correct_error_handling_on_database_update_failure(self, mocker):
        with app.test_request_context():
            auth_code = "valid_auth_code"
            expected_url = "https://app.2ndcareers.com?message=Something went wrong. Please try again."
            mocker.patch('src.controllers.authentication.google.google_process.get_user_data', return_value={
                "is_exist": True,
                "user_id": "123",
                "user_role": "professional"
            })
            mocker.patch('src.controllers.authentication.google.google_process.jwt_token.get_jwt_access_token', return_value={
                "access_token": "valid_token",
                "status": "success"
            })
            mocker.patch('src.controllers.authentication.google.google_process.isUserExist', return_value=True)
            mocker.patch('src.controllers.authentication.google.google_process.update_query', return_value=-1)
            result = get_google_profile_signin(auth_code)
            print(result)
            assert result == expected_url

    # Verify successful user login with existing Google account and correct auth code
    def test_successful_google_login(self, mocker):
        with app.test_request_context():
            # Mock the OAuth and JWT token generation process
            mocker.patch('src.controllers.authentication.google.google_process.oauth_web_singnup.googleweb.authorize_access_token', return_value={
                'userinfo': {'email': 'test@example.com', 'given_name': 'Test', 'family_name': 'User'},
                'access_token': 'mock_access_token'
            })
            mocker.patch('src.models.user_authentication.get_user_data', return_value={
                'is_exist': True,
                'user_id': 1,
                'login_mode': 'Google'
            })
            mocker.patch('src.models.user_authentication.isUserExist', return_value=True)
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={
                'status': 'success',
                'access_token': 'mock_jwt_access_token'
            })
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            # Call the function
            auth_code = "valid_auth_code"
            result = get_google_profile_signup(auth_code)
            print(result)

            # Assert the expected URL redirect with token
            expected_url = os.environ.get('WEB_APP_URI') + "/role_selection/professional_signup/social_media?token=mock_jwt_access_token"
            assert result == expected_url

    # Handle invalid or expired authorization code during Google OAuth interaction
    def test_invalid_expired_auth_code(self, mocker):
        with app.test_request_context():
            # Mock the OAuth process to simulate an invalid/expired auth code
            mocker.patch('src.controllers.authentication.google.google_process.oauth_web_singnup.googleweb.authorize_access_token', side_effect=Exception("Invalid authorization code"))

            # Call the function
            auth_code = "invalid_auth_code"
            result = get_google_profile_signup(auth_code)

            # Assert the expected error message in URL redirect
            expected_url = os.environ.get('WEB_APP_URI') + "?message=Something went wrong. Please try again."
            assert result == expected_url


    # Confirm token generation and redirection for a new user signing up with Google
    def test_confirm_token_generation_and_redirection_for_new_user_signup_with_google(self, mocker):
        with app.test_request_context():
            mocker.patch('src.controllers.authentication.google.google_process.oauth_web_singnup.googleweb.authorize_access_token', return_value={
                'userinfo': {'email': 'test@example.com', 'given_name': 'Test', 'family_name': 'User'},
                'access_token': 'mock_access_token'
            })
            mocker.patch('src.models.user_authentication.get_user_data', return_value={
                'is_exist': False,
                'user_id': 1,
                'user_role': 'professional'
            })
            mocker.patch('src.models.user_authentication.isUserExist', return_value=False)
            mocker.patch('src.models.user_authentication.get_user_roll_id', return_value=1)
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={
                'status': 'success',
                'access_token': 'mock_jwt_access_token'
            })
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            auth_code = "valid_auth_code"
            result = get_google_profile_signup(auth_code)

            expected_url = os.environ.get('WEB_APP_URI') + "/role_selection/professional_signup/social_media?token=mock_jwt_access_token"
            assert result == expected_url

    # Respond to unexpected exceptions during the OAuth flow or user data processing
    def test_respond_to_unexpected_exceptions(self, mocker):
        with app.test_request_context():
            # Mock the OAuth process and raise an exception
            mocker.patch('src.controllers.authentication.google.google_process.oauth_web_singnup.googleweb.authorize_access_token', side_effect=Exception("OAuth error"))

            # Call the function with a valid auth code
            auth_code = "valid_auth_code"
            result = get_google_profile_signup(auth_code)

            # Assert the expected URL redirect due to exception
            expected_url = os.environ.get('WEB_APP_URI') + "?message=Something went wrong. Please try again."
            assert result == expected_url

    # Verify system behavior when external services like OAuth or database are down
    def test_oauth_down(self, mocker):
        with app.test_request_context():
            # Mock the OAuth authorization code process to simulate OAuth service being down
            mocker.patch('src.controllers.authentication.google.google_process.oauth_web_singnup.googleweb.authorize_access_token', side_effect=Exception("OAuth service is down"))
        
            # Call the function with a valid auth code
            auth_code = "valid_auth_code"
            result = get_google_profile_signup(auth_code)
        
            # Assert the expected URL redirect when OAuth service is down
            expected_url = os.environ.get('WEB_APP_URI') + "?message=Something went wrong. Please try again."
            assert result == expected_url



    # Test handling of partial data or incorrect data types from Google OAuth
    def test_handling_partial_data_or_incorrect_types(self, mocker):
        with app.test_request_context():
            # Mock the OAuth and JWT token generation process
            mocker.patch('src.controllers.authentication.google.google_process.oauth_web_singnup.googleweb.authorize_access_token', return_value={
                'userinfo': {'email': 'test@example.com', 'given_name': 'Test', 'family_name': 'User'},
                'access_token': 'mock_access_token'
            })
            mocker.patch('src.models.user_authentication.get_user_data', return_value={
                'is_exist': False
            })
            mocker.patch('src.models.user_authentication.get_user_roll_id', return_value=1)
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_jwt_access_token', return_value={
                'status': 'success',
                'access_token': 'mock_jwt_access_token'
            })
            mocker.patch('src.models.background_task.BackgroundTask.send_session_data_auto_async')

            # Call the function with partial data
            auth_code = "valid_auth_code"
            result = get_google_profile_signup(auth_code)

            # Assert the expected URL redirect with token for new user creation
            expected_url = os.environ.get('WEB_APP_URI') + "/role_selection/professional_signup/social_media?token=mock_jwt_access_token"
            assert result == expected_url
