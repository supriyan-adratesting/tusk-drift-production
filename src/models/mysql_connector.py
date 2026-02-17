import os
from src import app
from dotenv import load_dotenv
from flask_mysqldb import MySQL
from MySQLdb.cursors import DictCursor

load_dotenv()
try:
    app.config['MYSQL_HOST'] = os.environ.get('MYSQL_HOST')
    app.config['MYSQL_USER'] = os.environ.get('MYSQL_USER')
    app.config['MYSQL_PASSWORD'] = os.environ.get('MYSQL_PASSWORD')
    app.config['MYSQL_DB'] = os.environ.get('MYSQL_DB')
    app.config['MYSQL_AUTOCOMMIT'] = True
    app.config['MYSQL_CONNECT_TIMEOUT'] = 60
    print(os.environ.get('MYSQL_HOST'))
    print(os.environ.get('MYSQL_DB'))
    mysql = MySQL(app)
except Exception as e: 
    print("MySQL Connection Error : "+str(e))

def api_json_response_format(status,message,error_code,data):
    result_json = {"success" : status,"message" : message,"error_code" : error_code,"data": data}
    return result_json

def execute_query(query, values=None):
    cursor = mysql.connect.cursor()
    data = []
    try:
        cursor.execute(query, values)
        result = cursor.fetchall()  # Or fetchone() if you expect only one row
        columns = [column[0] for column in cursor.description]
        # Fetch all rows and convert each row into a dictionary
        for row in result:
            row_dict = {}
            for idx, value in enumerate(row):
                row_dict[columns[idx]] = value
            data.append(row_dict)
        return data
    except Exception as e:
        print("Error executing query:", e)
        return None
    finally:
        cursor.close()
        return data

def view_execute_query(query, values=None):
    cursor = mysql.connect.cursor()
    data = []

    try:
        # Execute query
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)

        # Check if query is SELECT
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
            if cursor.description:  # Only if columns exist
                columns = [col[0] for col in cursor.description]
                for row in result:
                    data.append(dict(zip(columns, row)))
            return data
        else:
            # For INSERT/UPDATE/DELETE/CREATE VIEW etc.
            mysql.connect.commit()
            return True

    except Exception as e:
        print("Error executing query:", e)
        return None

    finally:
        cursor.close()

def update_many(query, values = None):
    try:
        cursor = mysql.connect.cursor()
        cursor.executemany(query, values)   # <-- bulk update
        mysql.connect.commit()
        return True
    except Exception as e:
        print("Error while executing bulk update:", e)
        mysql.connect.rollback()
        return False


def update_query(query, values, args=None):
    cursor = mysql.connect.cursor()
    try:
        if args:
            cursor.execute(query, args)
        else:
            cursor.execute(query, values)
        mysql.connect.commit()
        row_count = cursor.rowcount
        if row_count == 0:
            row_count = 1
    except Exception as e:
        row_count = -1
        print("Error executing query:", e)
    finally:
        cursor.close()
        return row_count

def update_query_last_index( query, values):
    updt_dict = {}
    cursor = mysql.connect.cursor()
    try:
        cursor.execute(query, values)
        row_count = cursor.rowcount
        last_index = cursor.lastrowid
        updt_dict["row_count"] = row_count
        updt_dict["last_index"] = last_index
        if row_count == 0:
            row_count = 1
            updt_dict["row_count"] = row_count
            updt_dict["last_index"] = last_index
    except Exception as e:
        row_count = -1
        updt_dict["row_count"] = row_count
        updt_dict["last_index"] = 0
    finally:
        cursor.close()
        return updt_dict

def chat_bot_execute_query(query, values,client_name="",service_name="",conversation_id="",alias_name=""):
    cursor = mysql.connect.cursor(DictCursor)
    result = []
    try:
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
    except Exception as e:
        error = "Error executing query:", e
        print(error)
        result = -1
        return api_json_response_format(False,str(error),500,{})        
    finally:
        cursor.close()
        return result

def chat_bot_update_query_last_index( query, values,client_name="",service_name="",conversation_id="",alias_name=""):
    updt_dict = {}
    cursor = mysql.connect.cursor()
    try:
        cursor.executemany(query, values)
        row_count = cursor.rowcount
        last_index = cursor.lastrowid
        updt_dict["row_count"] = row_count
        updt_dict["last_index"] = last_index
        if row_count == 0:
            row_count = 1
            updt_dict["row_count"] = row_count
            updt_dict["last_index"] = last_index
    except Exception as e:
        row_count = -1
        updt_dict["row_count"] = row_count
        updt_dict["last_index"] = 0
        updt_dict["error"] = str(e)
        # task_id = uuid.uuid4().hex  
        # executor.submit_stored(task_id, update_log_async, client_name,service_name,conversation_id,MODULE_NAME,"update_query_last_index",alias_name,"Exception occured in update query. Error : "+str(e),500)
    finally:
        cursor.close()
        return updt_dict
    
def run_query(query):
    cursor = mysql.connect.cursor(DictCursor)
    result = []
    try:
        cursor.execute(query)
        result = cursor.fetchall()
    except Exception as e:
        error = "Error executing query:", e
        print(error)
        # task_id = uuid.uuid4().hex  
        # executor.submit_stored(task_id, update_log_async, client_name,service_name,conversation_id,MODULE_NAME,"execute_query",alias_name,"Exception occured in query execution. Error : "+str(e),500)   
        return api_json_response_format(False,str(error),500,{})        
    finally:
        cursor.close()
        return result
