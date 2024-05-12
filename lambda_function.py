import os  # Import the os module for interacting with the operating system
import requests  # Import the requests module for HTTP requests
import toml  # Import the toml module to work with TOML files
from dotenv import load_dotenv  # Import the load_dotenv function from the dotenv module for loading environment variables
import snowflake.connector as sf  # Import the Snowflake connector module for Snowflake data warehouse

def load_config():
   """Load configuration from TOML file."""
   return toml.load('config.toml')  # Load configuration from the 'config.toml' TOML file

def download_file(url, destination_path):
   """Download a file from a URL to a specified path."""
   response = requests.get(url)  # Send an HTTP GET request to the specified URL
   response.raise_for_status()  # Raise an HTTPError for an unsuccessful status code
   with open(destination_path, 'wb') as file:  # Open the destination file in binary write mode
       file.write(response.content)  # Write the content of the response to the file

def establish_snowflake_connection(config):
   """Establish a connection to Snowflake using configuration."""
   return sf.connect(  # Connect to Snowflake using the provided configuration
       user=os.getenv('SNOWFLAKE_USER'),  # Get the Snowflake user from environment variables
       password=os.getenv('SNOWFLAKE_PASSWORD'),  # Get the Snowflake password from environment variables
       account=config['snowflake']['account'],  # Access the Snowflake account from the configuration
       warehouse=config['snowflake']['warehouse'],  # Access the Snowflake warehouse from the configuration
       database=config['snowflake']['database'],  # Access the Snowflake database from the configuration
       schema=config['snowflake']['schema'],  # Access the Snowflake schema from the configuration
       role=config['snowflake']['role']  # Access the Snowflake role from the configuration
   )

def prepare_snowflake_stage(cursor, stage_name, schema, table, file_path, file_name):
   """Prepare the Snowflake stage for file upload."""
   cursor.execute("CREATE OR REPLACE FILE FORMAT COMMA_CSV TYPE ='CSV' FIELD_DELIMITER = ',';")  # Create or replace a file format for CSV files
   cursor.execute(f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = COMMA_CSV")  # Create or replace a stage in Snowflake with the specified file format
   cursor.execute(f"PUT 'file://{file_path}' @{stage_name}")  # Put the file at the specified file path into the Snowflake stage
   cursor.execute(f"LIST @{stage_name}")  # List the files in the Snowflake stage
   cursor.execute(f"TRUNCATE TABLE {schema}.{table};")  # Truncate the specified table in Snowflake
   cursor.execute(f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT = COMMA_CSV, ON_ERROR = 'CONTINUE';")  # Copy the file data into the specified table in Snowflake

def upload_file_to_snowflake():
   config = load_config()  # Load the configuration from the TOML file
   
   local_file_path = os.path.join(config['local']['destination_folder'], config['local']['file_name'])  # Set the local file path
   
   download_file(config['source']['url'], local_file_path)  # Download the file from the source URL to the local file path
   
   with establish_snowflake_connection(config) as conn:  # Establish a connection to Snowflake
       with conn.cursor() as cursor:  # Create a cursor to execute SQL statements
           prepare_snowflake_stage(  # Prepare the Snowflake stage for file upload
               cursor,
               config['snowflake']['stage_name'],
               config['snowflake']['schema'],
               config['snowflake']['table'],
               local_file_path,
               config['local']['file_name']
           )

   print("File uploaded to Snowflake successfully.")  # Print a success message after uploading the file

def lambda_handler(event, context):
   try:
       upload_file_to_snowflake()  # Call the function to upload file to Snowflake
       return {'statusCode': 200, 'body': 'File uploaded to Snowflake successfully.'}  # Return a success response
   except Exception as e:
       print(f"Error: {str(e)}")  # Print the error message
       return {'statusCode': 500, 'body': f'Error uploading file to Snowflake: {str(e)}'}  # Return an error response if an exception occurs
