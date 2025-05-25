import os
import pandas as pd
import mysql.connector
from celery import Celery
from dotenv import load_dotenv

# Load environment variables from .env file (for worker's context)
load_dotenv()

# --- Celery Configuration ---
# IMPORTANT:
# For local development, 'redis://localhost:6379/0' is fine.
# For Aiven Redis, uncomment the REDIS_URL from your .env
# and ensure it's loaded here. Example Aiven URL for secure SSL connection:
# REDIS_URL=rediss://avnadmin:your_aiven_redis_password@your-aiven-redis-host.aivencloud.com:23456/defaultdb?ssl_cert_reqs=required
# If using Aiven Redis, make sure you installed 'redis' package with SSL support:
# pip install redis[hiredis]

CELERY_BROKER_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

celery_app = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

# --- Database Configuration for tasks ---
# This ensures your worker can connect to the Aiven MySQL database
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT')),
    'database': os.getenv('MYSQL_DB'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    # If Aiven requires SSL, you might need to add:
    # 'ssl_ca': '/path/to/your/aiven_ca.pem'
    # Download the CA certificate from Aiven and point to it.
    # For initial local testing, this might not be strictly necessary if your Aiven config is flexible.
}

def get_db_connection_task():
    """Establishes and returns a database connection for Celery tasks."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL in task: {err}")
        # In a real application, you'd log this error to a file/service
        # and perhaps use a retry mechanism or alert system.
        raise # Re-raise the exception to mark the task as failed

@celery_app.task(bind=True, max_retries=3, default_retry_delay=60) # Add retry logic
def process_excel_task(self, filepath):
    """
    Celery task to process the uploaded Excel file, parse user data,
    and insert/update records in the MySQL database.
    """
    print(f"[{self.request.id}] Starting to process Excel file: {filepath}")
    conn = None
    cursor = None
    try:
        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(filepath)
        total_rows = len(df)
        processed_count = 0
        skipped_count = 0

        conn = get_db_connection_task()
        cursor = conn.cursor()

        # SQL statement for inserting/updating users
        # ON DUPLICATE KEY UPDATE is crucial for handling existing users
        # assuming 'username' and 'email' are UNIQUE constraints in your 'users' table.
        sql = """
        INSERT INTO users (username, email, password, role)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            password = VALUES(password), -- IMPORTANT: Hash passwords in a real app!
            role = VALUES(role),
            updated_at = CURRENT_TIMESTAMP;
        """

        data_to_insert = []
        for index, row in df.iterrows():
            try:
                # Basic validation: ensure required columns exist and are not empty
                required_cols = ['username', 'email', 'password', 'role']
                if not all(col in row and pd.notna(row[col]) and str(row[col]).strip() != '' for col in required_cols):
                    print(f"[{self.request.id}] Skipping row {index + 2} (Excel row number): Missing or empty required columns.")
                    skipped_count += 1
                    continue

                # Clean and validate data types
                username = str(row['username']).strip()
                email = str(row['email']).strip()
                # HASH PASSWORDS HERE IN A REAL APPLICATION!
                # For this example, we're taking it as-is, but this is a critical security step.
                password = str(row['password']).strip()
                role = str(row['role']).strip().lower() # Ensure lowercase for ENUM matching

                # Basic validation for role against predefined ENUM values
                valid_roles = ['employee', 'manager', 'intern', 'hr', 'director']
                if role not in valid_roles:
                    print(f"[{self.request.id}] Skipping row {index + 2}: Invalid role '{row['role']}'. Must be one of {valid_roles}.")
                    skipped_count += 1
                    continue

                # Add data to the batch list
                data_to_insert.append((username, email, password, role))
                processed_count += 1

            except Exception as row_error:
                print(f"[{self.request.id}] Error processing row {index + 2}: {row_error}")
                skipped_count += 1
                continue # Continue to the next row even if one fails

        if data_to_insert:
            # Execute the batch insert/update
            cursor.executemany(sql, data_to_insert)
            conn.commit()
            print(f"[{self.request.id}] Successfully processed {processed_count} out of {total_rows} rows.")
            if skipped_count > 0:
                print(f"[{self.request.id}] Skipped {skipped_count} rows due to validation issues.")
        else:
            print(f"[{self.request.id}] No valid data found in Excel file to insert/update. Total rows: {total_rows}")

        # Clean up the uploaded file after successful processing
        os.remove(filepath)
        print(f"[{self.request.id}] Finished processing and deleted file: {filepath}")

    except pd.errors.EmptyDataError:
        print(f"[{self.request.id}] Error: The Excel file '{filepath}' is empty.")
        self.retry(exc=pd.errors.EmptyDataError, countdown=10) # Retry after 10 seconds
    except FileNotFoundError:
        print(f"[{self.request.id}] Error: Excel file not found at '{filepath}'.")
        self.retry(exc=FileNotFoundError, countdown=30) # Retry if file not found
    except Exception as e:
        print(f"[{self.request.id}] Critical error processing Excel file {filepath}: {str(e)}")
        # Log this error extensively for debugging
        # You might want to prevent file deletion here if processing failed completely.
        # Consider retrying the task after a delay, or sending a failure notification.
        try:
            self.retry(exc=e, countdown=60) # Retry after 60 seconds on general error
        except self.MaxRetriesExceededError:
            print(f"[{self.request.id}] Max retries exceeded for task. Final failure for {filepath}")
            # Here you might want to send an email notification about the failure
            # and potentially move the problematic file to an 'error' directory.
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()