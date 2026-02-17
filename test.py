# import requests

# def get_geolocation(ip_address):
#     try:
#         response = requests.get(f"https://ipinfo.io/{ip_address}/json")
#         data = response.json()
#         return data
#     except Exception as e:
#         return {"error": str(e)}

# # Example usage
# ip_address = "117.255.117.93"  # Replace with the IP address you want to look up
# location_data = get_geolocation(ip_address)

# if "error" not in location_data:
#     print(f"IP: {location_data['ip']}")
#     print(f"City: {location_data['city']}")
#     print(f"Region: {location_data['region']}")
#     print(f"Country: {location_data['country']}")
#     print(f"Location: {location_data['loc']}")
#     print(f"Organization: {location_data['org']}")
# else:
#     print(f"Error: {location_data['error']}")

# from geopy.geocoders import Nominatim

# def get_country(latitude, longitude):
#     geolocator = Nominatim(user_agent="geoapiExercises")
#     location = geolocator.reverse((latitude, longitude), exactly_one=True)
#     address = location.raw['address']
#     country = address.get('country', '')
#     return country

# # Example usage
# latitude = 11.0055
# longitude = 76.9661  # Coordinates for London, UK
# country = get_country(latitude, longitude)
# print(f"The country is: {country}")


# import requests
# import socket
# import requests
# from ip2geotools.databases.noncommercial import DbIpCity
# from geopy.distance import distance


# def printDetails(ip):
#     res = DbIpCity.get(ip, api_key="free")
#     print(f"IP Address: {res.ip_address}")
#     print(f"Location: {res.city}, {res.region}, {res.country}")
#     print(f"Coordinates: (Lat: {res.latitude}, Lng: {res.longitude})")
    
# printDetails("103.99.148.120")

# import meilisearch

# client = meilisearch.Client(url='https://melisearchdev.2ndcareers.com:7700/')  # Update with your Meilisearch URL
# index_name = 'Resume2'  # Replace with your index name

# # Get the index
# index = client.get_index(index_name)

# # Delete all documents
# index.delete_all_documents()
# from langchain_community.vectorstores import Meilisearch
# from langchain_openai import OpenAIEmbeddings
# import json, uuid, os
# import mysql.connector
# from mysql.connector import Error
# from  openai import OpenAI
# from datetime import datetime

# OPENAI_API_KEY = ''

# JOB_POST_INDEX = 'Job_posted_Dev'

# import meilisearch

# # Initialize Meilisearch client
# MEILI_HTTP_ADDR = 'https://melisearchdev.2ndcareers.com:7700/'  # Default Meilisearch address
# # MEILI_API_KEY = os.environ.get("MEILI_API_KEY")  # API key, if required

# client = meilisearch.Client(MEILI_HTTP_ADDR)

# # Ensure the index exists
# index_name = JOB_POST_INDEX
# client.create_index(index_name)
# # if not client.index(index_name).get_settings():

# embeddings = OpenAIEmbeddings(api_key=OPENAI_API_KEY)
# embedders = {
#             "adra": {
#                 "source": "userProvided",
#                 "dimensions": 1536,
#             }
#         }

# vector_store = Meilisearch(
#                 client=client,
#                 embedding=embeddings,
#                 embedders=embedders,
#                 index_name=JOB_POST_INDEX
#             )

# class DocumentWrapper:
#     def __init__(self, document):
#         self.document = document
#         self.page_content = json.dumps(document)  # Serialize the document to a JSON string
#         self.metadata = document  # Use the document itself as metadata
#         self.id = str(uuid.uuid4())  # Generate a unique ID for each document

#     def to_dict(self):
#         return self.document

# def serialize_document(doc):
#     """Helper function to convert non-serializable fields in a document."""
#     for key, value in doc.items():
#         if isinstance(value, datetime):
#             doc[key] = value.isoformat()  # Convert datetime to ISO string
#     return doc

# def store_in_meilisearch(documents):
#     try:
#         job_id = documents[0]['id']
#         # if isinstance(new_date, datetime):
#         #     return new_date.isoformat()
        
#         embedder_name = "adra"
#         index_name = JOB_POST_INDEX
#         serialized_documents = [serialize_document(doc) for doc in documents]
#         wrapped_documents = [DocumentWrapper(doc) for doc in serialized_documents]

#         # Ensure each document has an 'id' field
#         for doc in wrapped_documents:
#             if not hasattr(doc, 'id') or doc.id is None:
#                 raise ValueError(f"Document {doc} is missing an 'id' field.")
        
#         # Attempt to store in Meilisearch
#         index = vector_store.from_documents(
#             client = client,
#             documents=wrapped_documents,
#             embedding=embeddings,
#             embedders=embedders,
#             embedder_name="adra",
#             index_name=JOB_POST_INDEX
#         )
#         print(f"Documents successfully stored in Meilisearch for {job_id}.")

#     except ValueError as ve:
#         print(f"ValueError: {ve}")
#     except Exception as error:
#         print(f"An error occurred while storing documents in Meilisearch: {error}")

# def execute_query(connection, query, values=None):
#     try:
#         cursor = connection.cursor(dictionary=True)  # Return results as dictionary
#         if values:
#             cursor.execute(query, values)
#         else:
#             cursor.execute(query)
#         results = cursor.fetchall()  # Fetch all rows
#         return results
#     except Error as e:
#         print(f"Error: {e}")
#         return None
#     finally:
#         cursor.close()  #

# def create_mysql_connection(host, database, user, password):
#     try:
#         connection = mysql.connector.connect(
#             host=host,
#             database=database,
#             user=user,
#             password=password
#         )
#         if connection.is_connected():
#             print("Connection successful")
#             return connection
#     except Error as e:
#         print(f"Error: {e}")
#         return None

# def main():
#     cursor = create_mysql_connection('localhost', 'test_nov_dev_db_28', 'root', 'Applied@123')
#     query = """select id, employer_id, job_title, job_type, work_schedule, job_overview, workplace_type, country, city, specialisation, required_subcontract, 
#                 skills, job_desc, required_resume, required_cover_letter, required_background_check, time_commitment, timezone, duration, job_status, is_paid,
#                 is_active, created_at from job_post"""
#     values = ()
#     rslt = execute_query(cursor,query, values)
#     for r in rslt:
#         store_in_meilisearch([r])
#     # print(rslt)

# main()
