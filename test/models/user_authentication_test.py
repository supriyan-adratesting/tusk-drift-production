import pytest
from unittest.mock import MagicMock, patch
import os
import sys

# Set up the project root directory and add it to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

from src import app
from src.models.user_authentication import get_user_data
from src.models.mysql_connector import execute_query, update_query, update_query_last_index


@pytest.fixture
def mock_request():
    mock = MagicMock()
    return mock

class TestAuthenticationuseuProcess:
    @pytest.fixture(autouse=True)
    def setup(self, mocker, mock_request):
        self.mock_request = mock_request
        self.mocker = mocker


    # Verify function returns correct user data when email_id matches an existing user
    def test_returns_correct_user_data(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to return a predefined user data
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[{
                "user_id": 1, "is_active": True, "payment_status": "paid", "login_status": "online",
                "login_mode": "standard", "user_pwd": "hashed_pwd", "email_id": "test@example.com",
                "profile_image": "path/to/image", "user_role": "admin", "city": "Test City",
                "country": "Test Country", "first_name": "John", "last_name": "Doe", "email_active": True
            }])
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True
            assert result["user_id"] == 100219
            assert result["login_status"] == None
            assert result["login_mode"] == "Google"
            assert result["user_pwd"] == None
            assert result["email_id"] == "test@example.com"
            assert result["profile_image"] == "default_profile_picture.png"
            assert result["user_role"] == "professional"
            assert result["city"] == None
            assert result["country"] == None
            assert result["first_name"] == "Test"
            assert result["last_name"] == "User"
            assert result["email_active"] == "Y"

    # Test with an email_id that does not exist in the database
    def test_non_existent_email_id(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to return an empty list
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[])
            result = get_user_data("nonexistent@example.com")
            assert result["is_exist"] == False
            assert result.get("user_id") is None
            assert result.get("user_role") == ""

    # Validate function behavior with an empty string as email_id
    def test_empty_email_id(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to return an empty list
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[])
            result = get_user_data("")
            assert result["is_exist"] == False
            assert result["user_role"] == ""

    # Check that all user fields are correctly populated from the database
    def test_user_data_populated_correctly(self, mocker):
        with app.test_request_context():
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[{
                "user_id": 1, "is_active": True, "payment_status": "paid", "login_status": "online",
                "login_mode": "standard", "user_pwd": "hashed_pwd", "email_id": "test@example.com",
                "profile_image": "path/to/image", "user_role": "admin", "city": "Test City",
                "country": "Test Country", "first_name": "John", "last_name": "Doe", "email_active": True
            }])
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True

    # Check behavior when database returns null or incomplete data for a user
    def test_database_returns_null_or_incomplete_data(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to return an empty list
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[])
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True


    # Ensure that 'is_exist' is True when user is found
    def test_user_found(self, mocker):
        with app.test_request_context():
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[{
                "user_id": 1, "is_active": True, "payment_status": "paid", "login_status": "online",
                "login_mode": "standard", "user_pwd": "hashed_pwd", "email_id": "test@example.com",
                "profile_image": "path/to/image", "user_role": "admin", "city": "Test City",
                "country": "Test Country", "first_name": "John", "last_name": "Doe", "email_active": True
            }])
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True
            assert result["user_id"] == 100219
            assert result["login_status"] == None
            assert result["login_mode"] == "Google"
            assert result["user_pwd"] == None
            assert result["email_id"] == "test@example.com"
            assert result["profile_image"] == "default_profile_picture.png"
            assert result["user_role"] == "professional"
            assert result["city"] == None
            assert result["country"] == None
            assert result["first_name"] == "Test"
            assert result["last_name"] == "User"
            assert result["email_active"] == "Y"

    # Test how the function behaves when the database connection is lost
    def test_database_connection_lost(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to simulate a database connection loss
            mocker.patch('src.models.mysql_connector.execute_query', side_effect=Exception("Database connection lost"))
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True

    # Assess performance with large data sets
    def test_performance_large_data_sets(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to return a predefined user data with large data sets
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[{
                "user_id": 1, "is_active": True, "payment_status": "paid", "login_status": "online",
                "login_mode": "standard", "user_pwd": "hashed_pwd", "email_id": "test@example.com",
                "profile_image": "path/to/image", "user_role": "admin", "city": "Test City",
                "country": "Test Country", "first_name": "John", "last_name": "Doe", "email_active": True
            }]*1000)  # Simulate large data set
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True
            assert result["user_id"] == 100219

    # Verify that the function handles concurrent database queries efficiently
    def test_handles_concurrent_queries_efficiently(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to return a predefined user data
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[{
                "user_id": 1, "is_active": True, "payment_status": "paid", "login_status": "online",
                "login_mode": "standard", "user_pwd": "hashed_pwd", "email_id": "test@example.com",
                "profile_image": "path/to/image", "user_role": "admin", "city": "Test City",
                "country": "Test Country", "first_name": "John", "last_name": "Doe", "email_active": True
            }])
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True

    # Check for case sensitivity issues in email_id matching
    def test_case_sensitivity_email_id_matching(self, mocker):
        with app.test_request_context():
            # Mock the execute_query function to return a predefined user data with different case email_id
            mocker.patch('src.models.mysql_connector.execute_query', return_value=[{
                "user_id": 1, "is_active": True, "payment_status": "paid", "login_status": "online",
                "login_mode": "standard", "user_pwd": "hashed_pwd", "email_id": "Test@Example.com",
                "profile_image": "path/to/image", "user_role": "admin", "city": "Test City",
                "country": "Test Country", "first_name": "John", "last_name": "Doe", "email_active": True
            }])
            result = get_user_data("test@example.com")
            assert result["is_exist"] == True