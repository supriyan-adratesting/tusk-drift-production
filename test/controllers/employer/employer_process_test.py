import pytest
from unittest.mock import MagicMock, patch
import os,sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
src_path = os.path.join(project_root, 'second_careers_project')
sys.path.insert(0, src_path)
import datetime
from src import app
from src.controllers.employer.employer_process import (
    employer_job_post,get_job_posts,get_employer_profile_dashboard_data,get_org_description,update_org_description,get_hiring_team_details,delete_hiring_team_details,get_company_details,home_dashboard_view,close_job_posted,status_update_job_posted,update_interview_status,delete_job_post,update_company_details,on_click_load_more,pool_dashboard_view,candidates_dashboard_view,get_selected_professional_detail,update_application_status,update_custom_notes,filter_by_application_status,filter_professionals,job_post_draft
)

@pytest.fixture
def mock_request(mocker):
    mock = MagicMock()
    mocker.patch('src.controllers.employer.employer_process.request', mock)
    return mock

class Testemployerprocess:
    @pytest.fixture(autouse=True)
    def setup(self, mocker, mock_request):
        self.mock_request = mock_request
        self.mocker = mocker

    def test_invalid_token_employer_role(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': 1})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'job_title': 'Software Engineer', 'job_type': 'Full-time', 'work_schedule': '9-5', 'workplace_type': 'Remote', 'country': 'USA', 'city': 'New York', 'time_zone': 'EST', 'skills': 'Python, Flask', 'specialisation': 'Backend', 'job_desc': 'Develop APIs', 'required_resume': True, 'required_cover_letter': True, 'required_subcontract': False, 'time_commitment': '40 hours/week', 'duration': 'Permanent', 'job_status': 'Open', 'is_paid': True, 'is_active': True})
            mocker.patch('src.models.mysql_connector.update_query_last_index', return_value={'row_count': 1, 'last_index': 123})
            response = employer_job_post()
            assert response['success'] == False
            assert response['message'] == 'Please fill in all the required fields.'

    # Token is invalid or expired
    def test_invalid_or_expired_token(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 401, 'status': 'Invalid Token. Please try again'})
            response = employer_job_post()
            assert response['success'] == False
            assert response['message'] == 'Invalid Token. Please try again'
            assert response['error_code'] == 401

    # Token is valid and user is an employer with 'drafted' job status
    def test_invalid_token_employer_with_drafted_status(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'user_role': 'employer', 'user_id': 1})
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[{'id': 1, 'created_at': datetime.datetime.now()}])
            response = get_job_posts()
            assert response['success'] is False
            assert response['message'] == "Invalid Token. Please try again"

    # # Token is invalid or expired, leading to an error response
    def test_invalid_or_expired_token(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 401, 'status': 'Invalid Token. Please try again'})
            response = get_job_posts()
            assert response['success'] is False
            assert response['message'] == "Invalid Token. Please try again"
            assert response['error_code'] == 401

    # Token is valid and user is an employer with an existing profile
    def test_valid_token_employer_with_profile(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', return_value=True)
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[{'company_description': 'A great company'}])
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Your profile has been retrieved successfully!', 'error_code': 0, 'data': [{'company_description': 'A great company'}]})
            response = get_org_description()
            assert response['success'] is True
            assert response['message'] == 'Your profile has been retrieved successfully!'
            assert response['data'][0]['company_description'] == 'A great company'

    # User token is missing or invalid
    def test_missing_or_invalid_token(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 401, 'status': 'Invalid Token. Please try again.'})
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': False, 'message': 'Invalid Token. Please try again.', 'error_code': 401, 'data': {}})
            response = get_org_description()
            assert response['success'] is False
            assert response['message'] == 'Invalid Token. Please try again.'
            assert response['error_code'] == 401
            assert response['data'] == {}
    
    # Employer profile is found and company description is retrieved successfully
    def test_employer_profile_found_and_description_retrieved_successfully(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', return_value=True)
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[{'company_description': 'A great company'}])
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Your profile has been retrieved successfully!', 'error_code': 0, 'data': [{'company_description': 'A great company'}]})
        
            response = get_org_description()
            assert response['success'] is True
            assert response['message'] == 'Your profile has been retrieved successfully!'
            assert response['data'][0]['company_description'] == 'A great company'

    # Check response when required fields are missing in the request data
    def test_missing_required_fields(self, mocker):
        with app.app_context():
            with app.test_request_context(json={'subject': 'Interview'}):  # Missing other required fields
                mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
                response = update_interview_status()
                assert response['success'] is False
                assert response['error_code'] == 204
                assert 'Please fill in all the fields' in response['message']

    # Validate that user notifications are inserted correctly
    def test_user_notifications_inserted_correctly(self, mocker):
        with app.app_context():
            with app.test_request_context(json={'subject': 'Interview', 'message': 'Please attend', 'job_id': '1', 'email_id': 'candidate@example.com', 'professional_id': '2'}):
                mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
                mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer'})
                mocker.patch('src.controllers.employer.employer_process.sendgrid_mail_interview')
                mocker.patch('src.controllers.employer.employer_process.execute_query')
                mocker.patch('src.controllers.employer.employer_process.update_query', return_value=1)
                response = update_interview_status()
                assert response['success'] is False
                assert response['message'] == 'Please fill in all the fields(professional_id)'
    
    # Employer has no hiring team but has a profile, returns no records found message
    def test_employer_no_hiring_team_with_profile(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_id': '123', 'user_role': 'employer'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', side_effect=[True, False])
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': False, 'message': 'No records found for this id', 'error_code': 204, 'data': {}})
            result = get_hiring_team_details()
            assert result['success'] is False
            assert result['message'] == 'No records found for this id'
            assert result['error_code'] == 204
            assert result['data'] == {}

    # Handling of null or 'N/A' values in hiring team data
    def test_handling_null_values_in_hiring_team_data(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_id': '123', 'user_role': 'employer'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', side_effect=[True, True])
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[{'team_member': 'John Doe', 'role': 'N/A'}])
            mocker.patch('src.controllers.employer.employer_process.replace_empty_values', return_value=[{'team_member': 'John Doe', 'role': ''}])
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Your profile has been retrieved successfully!', 'error_code': 0, 'data': [{'team_member': 'John Doe', 'role': ''}]})
            result = get_hiring_team_details()
            assert result['success'] is True
            assert result['message'] == 'Your profile has been retrieved successfully!'
            assert result['data'][0]['team_member'] == 'John Doe'
            assert result['data'][0]['role'] == ''
    
    # Handling of unexpected exceptions during the process
    def test_handling_unexpected_exceptions(self):
        with app.app_context():
            with patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', side_effect=Exception('Unexpected error')), \
                patch('src.models.user_authentication.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'}), \
                patch('src.models.user_authentication.isUserExist', return_value=True), \
                patch('src.models.mysql_connector.update_query', return_value=1):
                response = update_org_description()
                assert response == {'success': False, 'message': 'Invalid Token. Please try again.', 'error_code': 401, 'data': {}}

    # Token is invalid or expired
    def test_invalid_or_expired_token(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 401, 'status': 'Invalid Token. Please try again.'})
            from src.controllers.employer.employer_process import update_hiring_team_details
            response = update_hiring_team_details()
            assert response == {'success': False, 'message': 'Invalid Token. Please try again.', 'error_code': 401, 'data': {}}

    # Function successfully deletes hiring team details when all conditions are met
    def test_unsuccessful_deletion(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'test@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_id': '1', 'user_role': 'employer'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', side_effect=[True, True])
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[])
            mocker.patch('src.controllers.employer.employer_process.replace_empty_values', return_value=[])
            response = delete_hiring_team_details()
            assert response['success'] is False
            assert response['message'] == "Please fill in all the required fields."

    # Returns error if 'emp_id' is not provided in the request data
    def test_missing_emp_id(self, mocker):
        with app.app_context():
            response = delete_hiring_team_details()
            assert response['success'] is False
            assert response['message'] == "Please fill in all the required fields."
            assert response['error_code'] == 204

    # Employer profile data is correctly fetched and formatted
    def test_employer_profile_data_fetched_and_formatted(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', return_value=True)
            mocker.patch('src.controllers.employer.employer_process.execute_query', side_effect=[
                [{'website_url': 'http://example.com', 'sector': 'Tech', 'employer_type': 'Startup'}],
                [{'city': 'New York', 'country': 'USA'}]
            ])
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Your profile has been retrieved successfully!', 'error_code': 0, 'data': [{'website_url': 'http://example.com', 'sector': 'Tech', 'employer_type': 'Startup', 'location': [{'city': 'New York', 'country': 'USA'}]}]})
            response = get_company_details()
            assert response['success'] is True
            assert response['message'] == 'Your profile has been retrieved successfully!'
            assert response['error_code'] == 0
            assert response['data'][0]['website_url'] == 'http://example.com'

    # All data fields are present and non-null in the database results
    def test_all_data_fields_present_and_non_null(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', return_value=True)
            mocker.patch('src.controllers.employer.employer_process.execute_query', side_effect=[
                [{'website_url': 'http://example.com', 'sector': 'Tech', 'employer_type': 'Startup'}],
                [{'city': 'New York', 'country': 'USA'}]
            ])
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Your profile has been retrieved successfully!', 'error_code': 0, 'data': [{'website_url': 'http://example.com', 'sector': 'Tech', 'employer_type': 'Startup', 'location': [{'city': 'New York', 'country': 'USA'}]}]})
            response = get_company_details()
            assert response['success'] is True
            assert response['message'] == 'Your profile has been retrieved successfully!'
            assert response['error_code'] == 0
            assert response['data'][0]['website_url'] == 'http://example.com'
    
    # User location data is successfully updated in the users table
    def test_user_location_data_updated_successfully(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'user_role': 'employer', 'user_id': 1})
            mocker.patch('src.models.user_authentication.isUserExist', return_value=True)
            mocker.patch('src.models.mysql_connector.update_query', side_effect=[1, 1])
            response = update_company_details()
            assert response['success'] is False
            assert response['message'] == "Invalid Token. Please try again."

    # Hiring team data is successfully aggregated and added to the response
    def test_hiring_team_data_aggregated(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'})
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[{'id': '1', 'first_name': 'John', 'last_name': 'Doe', 'designation': 'Manager', 'email_id': 'johndoe@example.com', 'contact_number': '1234567890', 'company_name': 'Example Corp', 'company_description': 'An Example Company', 'employer_type': 'Private', 'sector': 'Technology', 'website_url': 'http://example.com', 'city': 'Example City', 'state': 'Example State', 'country': 'Example Country', 'profile_image': 'image.jpg'}])
            mocker.patch('src.controllers.employer.employer_process.replace_empty_values1', side_effect=lambda x: x if x != None else '')
            from src.controllers.employer.employer_process import get_employer_profile_dashboard_data
            response = get_employer_profile_dashboard_data()
            assert response['success'] == True
            assert response['message'] == "Details fetched successfully!"
            assert response['error_code'] == 0
            assert response['data']['company_name'] == 'Example Corp'
    
    # Response handling when the database connection is lost
    def test_database_connection_lost_handling(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'})
            mocker.patch('src.models.mysql_connector.execute_query', side_effect=Exception("Database connection lost"))
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': False, 'message': 'Database connection lost', 'error_code': 500, 'data': {}})
            response = get_employer_profile_dashboard_data()
            assert response['success'] == False
            assert response['message'] == 'Database connection lost'
            assert response['error_code'] == 500
            assert response['data'] == {}

    # Employer has job posts that match the job status filter
    def test_employer_job_posts_matching_filter(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': 1})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'job_status': 'active'})
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[{'job_id': 1, 'job_title': 'Software Developer'}])
            response = home_dashboard_view()
            assert response['success'] is True
            assert response['message'] == 'Details fetched successfully!'
            assert response['error_code'] == 0
            assert len(response['data']) == 1
    
    # Request contains all required fields
    def test_request_contains_all_required_fields(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': 1})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'job_status': 'active'})
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[{'job_id': 1, 'job_title': 'Software Developer'}])
            response = home_dashboard_view()
            assert response['success'] is True
            assert response['message'] == 'Details fetched successfully!'
            assert response['error_code'] == 0
            assert len(response['data']) == 1

    # Handling of unexpected exceptions during the process
    def test_handling_unexpected_exceptions(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={"status_code": 200, "email_id": "employer@example.com"})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', side_effect=Exception("Database connection error"))
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={"success": False, "message": "Sorry! We encountered an unexpected error. Please try again later.", "error_code": 500, "data": {}})
            response = status_update_job_posted()
            assert response["success"] is False
            assert response["message"] == "Sorry! We encountered an unexpected error. Please try again later."
            assert response["error_code"] == 500

    # Network or database connectivity issues during the operation
    def test_network_or_database_connectivity_issues(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer'})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'is_role_filled': True, 'candidate_id': '123', 'feedback': 'Good fit', 'job_id': '1'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', side_effect=Exception("Database connection error"))
            response = close_job_posted()
            assert response['success'] == False
            assert response['message'] == "Database connection error"
            assert response['error_code'] == 500

    # Candidate ID is provided and job activity is updated accordingly
    def test_candidate_id_provided_and_job_activity_updated(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer'})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'is_role_filled': True, 'candidate_id': '123', 'feedback': 'Good fit', 'job_id': '1'})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', return_value=True)
            mocker.patch('src.controllers.employer.employer_process.update_query', return_value=1)
            response = close_job_posted()
            assert response['success'] == True
            assert response['message'] == "The job has been successfully closed. We would like to thank you for posting the job."
            assert response['error_code'] == 0
    
    
    # Experience, education, skills, languages, and additional info data are grouped and formatted correctly
    def test_experience_education_skills_languages_additional_info_grouping(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer'})
            mocker.patch('src.controllers.employer.employer_process.execute_query', return_value=[{'about': 'N/A', 'preferences': 'N/A', 'experience_id': 1, 'company_name': 'ABC Inc.', 'job_title': 'Software Engineer', 'start_month': 1, 'start_year': 2020, 'end_month': 12, 'end_year': 2021, 'job_description': 'Developed software', 'job_location': 'New York', 'education_id': 1, 'institute_name': 'XYZ University', 'degree_level': 'Bachelor', 'specialisation': 'Computer Science', 'start_month': 1, 'start_year': 2016, 'end_month': 12, 'end_year': 2020, 'institute_location': 'California', 'skill_id': 1, 'skill_name': 'Python', 'skill_level': 'Advanced', 'language_id': 1, 'language_known': 'English', 'language_level': 'Fluent', 'additional_info_id': 1, 'additional_info_title': 'Certification', 'additional_info_description': 'Certified in Python'}])
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Details fetched successfully!', 'error_code': 0, 'data': {}})
            response = on_click_load_more()
            assert response['success'] is True
            assert response['message'] == 'Details fetched successfully!'
            assert response['error_code'] == 0

    # Handle invalid or expired tokens appropriately
    def test_invalid_or_expired_token_handling(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 401, 'status': 'Invalid Token'})
            response = pool_dashboard_view()
            assert response['success'] is False
            assert response['message'] == "Invalid Token. Please try again."
            assert response['error_code'] == 401
            assert response['data'] == {}

    # Request data is missing 'job_id' or 'professional_id'
    def test_missing_job_id_or_professional_id(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'professional_id': 1})
            response = get_selected_professional_detail()
            assert response['success'] is False
    
    # Job ID is provided and valid job details are fetched
    def test_job_id_provided_valid_details_fetched(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.request', **{'get_json.return_value': {'job_id': '123', 'job_status': 'active'}, 'headers': {'Authorization': 'Bearer validtoken'}})
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '1'})
            mocker.patch('src.controllers.employer.employer_process.execute_query', side_effect=[[], []])  # Simulate no job posts and no professional details
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Details fetched successfully!', 'error_code': 0, 'data': {}})
            result = candidates_dashboard_view()
            assert result['success'] is True
            assert result['message'] == 'Details fetched successfully!'

    # Job status filter is applied but no matching records are found
    def test_job_status_filter_no_matching_records(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.request', **{'get_json.return_value': {'job_id': '', 'job_status': 'active'}, 'headers': {'Authorization': 'Bearer validtoken'}})
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '1'})
            mocker.patch('src.controllers.employer.employer_process.execute_query', side_effect=[[], []])  # Simulate no job posts and no professional details
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'No records found', 'error_code': 0, 'data': {}})
            result = candidates_dashboard_view()
            assert result['success'] is True
            assert result['message'] == 'No records found'
    
    # Notification fails to create after status is set to 'shortlisted'
    def test_notification_creation_failure_shortlisted(self, mocker):  
        with app.app_context():
            with app.test_request_context(json={'professional_id': '123', 'job_id': '456', 'status': 'shortlisted'}):
                with patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token') as mock_get_user_token:
                    mock_get_user_token.return_value = {'status_code': 200, 'email_id': 'test@example.com'}
                    with patch('src.models.user_authentication.get_user_data') as mock_get_user_data:
                        mock_get_user_data.return_value = {'user_role': 'employer'}
                        with patch('src.models.mysql_connector.execute_query') as mock_execute_query:
                            mock_execute_query.return_value = [{'job_title': 'Software Engineer'}]
                            with patch('src.models.mysql_connector.update_query') as mock_update_query:
                                mock_update_query.return_value = 0
                                result = update_application_status()
                                assert result['success'] == False
                                assert result['message'] == "Please fill in all the fields(professional_id)"

    # Check successful update of custom notes in the database
    def test_unsuccessful_update_custom_notes(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 200, 'email_id': 'test@example.com'})
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'user_role': 'employer'})
            mocker.patch('src.models.mysql_connector.update_query', return_value=1)
            mocker.patch('src.models.user_authentication.api_json_response_format', return_value={'success': True, 'message': 'Candidate status has been updated successfully!', 'error_code': 0, 'data': {}})
            response = update_custom_notes()
            assert response['success'] is False

    # Handle missing 'job_id' in request data
    def test_handle_missing_job_id(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'test@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer'})
            mocker.patch('src.models.user_authentication.api_json_response_format', return_value={'success': False, 'message': 'Please fill in all the fields(job_id)', 'error_code': 204, 'data': {}})
            response = update_custom_notes()
            assert response['success'] is False      
    
    # Request JSON does not contain 'status'
    def test_request_json_missing_status(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': False, 'message': 'Please fill in all the fields(status)', 'error_code': 204, 'data': {}})
            req_data = {
                "job_id": 123
            }
            with app.test_request_context(json=req_data):
                result = filter_by_application_status()
                assert result['success'] is False
                assert result['message'] == 'Please fill in all the fields(status)'
                assert result['error_code'] == 204
                assert isinstance(result['data'], dict) and not result['data'] 

    # Status is 'Applied' and there are matching records in the database
    def test_status_applied_matching_records(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': '123'})
            mocker.patch('src.controllers.employer.employer_process.execute_query', side_effect=[[], [{'professional_id': 1, 'invite_to_interview': 'Yes', 'application_status': 'Applied', 'custom_notes': 'Good candidate'}]])  # Mocking database responses
            mocker.patch('src.controllers.employer.employer_process.api_json_response_format', return_value={'success': True, 'message': 'Details fetched successfully!', 'error_code': 0, 'data': {}})
            req_data = {
                "status": "Applied",
                "job_id": 123
            }
            with app.test_request_context(json=req_data):
                result = filter_by_application_status()
                assert result['success'] is True
                assert result['message'] == 'Details fetched successfully!'
                assert result['error_code'] == 0
                assert isinstance(result['data'], dict)

    # User role is not 'employer', leading to an unauthorized access response
    def test_unauthorized_user_access(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 200, 'email_id': 'user@example.com'})
            mocker.patch('src.models.user_authentication.get_user_data', return_value={'user_role': 'user'})
            response = filter_professionals()
            assert response['success'] is False
            assert response['message'] == 'Invalid Token. Please try again'
            assert response['error_code'] == 401

    # Request does not contain a job_id
    def test_request_missing_job_id(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={"status_code": 200, "email_id": "employer@example.com"})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={"user_role": "employer", "user_id": "123"})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={})
            response = delete_job_post()
            assert response == {"success": False, "message": "Please fill in all the required fields.", "error_code": 204, "data": {}}

    # Successful deletion returns a success message and status code 0
    def test_successful_deletion_returns_success_message_and_status_code_0(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={"status_code": 200, "email_id": "employer@example.com"})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={"user_role": "employer", "user_id": "123"})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={"job_id": "456"})
            mocker.patch('src.controllers.employer.employer_process.isUserExist', side_effect=[True, True])
            mocker.patch('src.controllers.employer.employer_process.update_query', return_value=1)
            response = delete_job_post()
            assert response == {"success": True, "message": "The job post has been updated successfully!", "error_code": 0, "data": {}}
    
    # Pre-screen questions data malformed or incomplete, affecting database operations
    def test_pre_screen_questions_data_malformed(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.jwt_tokens.jwt_token_required.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.models.user_authentication.get_user_data', return_value={
                'user_role': 'employer',
                'user_id': 1
            })
            mocker.patch('src.models.mysql_connector.update_query_last_index', return_value={'row_count': 1, 'last_index': 1})
            mocker.patch('src.models.user_authentication.isUserExist', return_value=True)
            response = job_post_draft()
            assert response['success'] is False
            assert response['message'] == "Invalid Token. Please try again."
    
    # Required job post fields are missing or invalid
    def test_required_fields_missing_or_invalid(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': 1})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'job_title': 'Software Engineer', 'work_schedule': '9-5', 'workplace_type': 'Remote', 'country': 'USA', 'city': 'New York', 'time_zone': 'EST', 'skills': 'Python, Flask', 'specialisation': 'Backend', 'job_desc': 'Develop APIs', 'required_resume': True, 'required_cover_letter': True, 'required_subcontract': False, 'time_commitment': '40 hours/week', 'duration': 'Permanent', 'job_status': 'Open', 'is_paid': True, 'is_active': True})
            result = employer_job_post()
            assert result['success'] == False
            assert result['message'] == 'Please fill in all the required fields.'

    # Meilisearch indexing fails
    def test_meilisearch_indexing_fails(self, mocker):
        with app.app_context():
            mocker.patch('src.controllers.employer.employer_process.get_user_token', return_value={'status_code': 200, 'email_id': 'employer@example.com'})
            mocker.patch('src.controllers.employer.employer_process.get_user_data', return_value={'user_role': 'employer', 'user_id': 1})
            mocker.patch('src.controllers.employer.employer_process.request.get_json', return_value={'job_title': 'Software Engineer', 'job_type': 'Full-time', 'work_schedule': '9-5', 'workplace_type': 'Remote', 'country': 'USA', 'city': 'New York', 'time_zone': 'EST', 'skills': 'Python, Flask', 'specialisation': 'Backend', 'job_desc': 'Develop APIs', 'required_resume': True, 'required_cover_letter': True, 'required_subcontract': False, 'time_commitment': '40 hours/week', 'duration': 'Permanent', 'job_status': 'Open', 'is_paid': True, 'is_active': True})
            mocker.patch('src.models.mysql_connector.update_query_last_index', return_value={'row_count': -1, 'last_index': 0})
            mocker.patch('src.models.user_authentication.api_json_response_format', return_value={'success': False, 'message': 'Sorry, we encountered an issue. Please try again.', 'error_code': 500, 'data': {}})
            result = employer_job_post()
            assert result['success'] == False
            assert result['message'] == 'Please fill in all the required fields.'
            assert result['error_code'] == 204
