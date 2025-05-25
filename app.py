import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import mysql.connector # Or SQLAlchemy

import pandas as pd # New import for Excel handling
import time # New import for unique filenames

from tasks import celery_app, process_excel_task

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
# Enable CORS for all routes (for development).
# In production, you'd restrict this to your frontend's domain.
CORS(app)

# Database Configuration (using environment variables)
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT')),
    'database': os.getenv('MYSQL_DB'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD')
}

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        # In a real app, you'd log this error and handle it gracefully
        return None

@app.route('/')
def home():
    return "User Management Backend is running!"

@app.route('/test_db_connection')
def test_db_connection():
    conn = None
    try:
        conn = get_db_connection()
        if conn and conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            db_version = cursor.fetchone()[0]
            cursor.close()
            return jsonify({"status": "success", "message": f"Connected to MySQL DB version: {db_version}"}), 200
        else:
            return jsonify({"status": "error", "message": "Failed to connect to MySQL DB"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"}), 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route('/upload_users', methods=['POST'])
def upload_users():
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part in the request"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400

    # Validate file extension
    allowed_extensions = ('.xlsx', '.xls')
    if not file.filename.lower().endswith(allowed_extensions):
        return jsonify({"status": "error", "message": "Invalid file type. Please upload an Excel file (.xlsx or .xls)."}), 400

    try:
        # Create a temporary directory if it doesn't exist within the backend app's root
        upload_folder = os.path.join(app.root_path, 'uploads')
        os.makedirs(upload_folder, exist_ok=True)

        # Generate a unique filename to prevent overwrites or conflicts
        # Using a timestamp is simple; for stronger uniqueness, consider uuid.uuid4()
        timestamp = int(time.time())
        original_filename_parts = os.path.splitext(file.filename)
        unique_filename = f"{original_filename_parts[0]}_{timestamp}{original_filename_parts[1]}"
        filepath = os.path.join(upload_folder, unique_filename)
        file.save(filepath)

        # Offload the Excel processing to a Celery task
        # This makes the API response fast while the heavy work happens in the background
        process_excel_task.delay(filepath)

        return jsonify({
            "status": "success",
            "message": "File uploaded successfully. Processing started in background."
        }), 202 # 202 Accepted status indicates asynchronous processing

    except Exception as e:
        # General error handling for file upload issues
        print(f"Error during file upload: {str(e)}") # Log the error for debugging
        return jsonify({"status": "error", "message": f"File upload failed: {str(e)}"}), 500
    
@app.route('/users', methods=['GET'])
def get_all_users():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"status": "error", "message": "Database connection failed"}), 500

        cursor = conn.cursor(dictionary=True) # Return rows as dictionaries
        cursor.execute("SELECT id, username, email, role, created_at, updated_at FROM users")
        users = cursor.fetchall()

        # Convert datetime objects to string for JSON serialization
        for user in users:
            if user['created_at']:
                user['created_at'] = user['created_at'].isoformat()
            if user['updated_at']:
                user['updated_at'] = user['updated_at'].isoformat()

        return jsonify({"status": "success", "data": users}), 200

    except Exception as e:
        print(f"Error fetching users: {e}")
        return jsonify({"status": "error", "message": "Failed to fetch users", "details": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


@app.route('/update_users_batch', methods=['POST'])
def update_users_batch():
    updated_users_data = request.json # Expects a JSON array of user objects
    if not isinstance(updated_users_data, list):
        return jsonify({"status": "error", "message": "Request body must be a JSON array"}), 400

    conn = None
    cursor = None
    successful_updates = 0
    failed_updates = 0
    update_results = []

    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"status": "error", "message": "Database connection failed"}), 500

        cursor = conn.cursor()

        # SQL for updating user data. We can update email and role based on id.
        # username is usually fixed, password would be handled via a separate flow.
        sql = """
        UPDATE users
        SET email = %s, role = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s;
        """
        # Also, we might consider updating based on username or email if ID is not available.
        # For simplicity, we assume ID is passed for updates.

        for user_data in updated_users_data:
            user_id = user_data.get('id')
            new_email = user_data.get('email')
            new_role = user_data.get('role')

            # Basic validation
            if not all([user_id, new_email, new_role]):
                update_results.append({"id": user_id, "status": "failed", "message": "Missing ID, email, or role"})
                failed_updates += 1
                continue

            # Validate role against ENUM values
            valid_roles = ['employee', 'manager', 'intern', 'hr', 'director']
            if new_role.lower() not in valid_roles:
                update_results.append({"id": user_id, "status": "failed", "message": f"Invalid role: {new_role}"})
                failed_updates += 1
                continue

            try:
                cursor.execute(sql, (new_email, new_role.lower(), user_id))
                if cursor.rowcount > 0:
                    successful_updates += 1
                    update_results.append({"id": user_id, "status": "success"})
                else:
                    failed_updates += 1
                    update_results.append({"id": user_id, "status": "failed", "message": "User not found or no change"})
            except mysql.connector.Error as db_err:
                failed_updates += 1
                update_results.append({"id": user_id, "status": "failed", "message": f"DB error: {str(db_err)}"})
                print(f"Error updating user {user_id}: {db_err}")

        conn.commit() # Commit all changes at once
        print(f"Batch update completed: {successful_updates} successful, {failed_updates} failed.")

        return jsonify({
            "status": "success",
            "message": f"Batch update processed. {successful_updates} users updated, {failed_updates} failed.",
            "results": update_results
        }), 200

    except Exception as e:
        print(f"Error during batch update: {e}")
        conn.rollback() # Rollback if a critical error occurs before commit
        return jsonify({"status": "error", "message": f"Batch update failed: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


if __name__ == '__main__':
    # When running with debug=True, the server will restart on code changes.
    # host='0.0.0.0' makes the Flask app accessible from your Windows machine via localhost.
    app.run(debug=True, host='0.0.0.0', port=5000)