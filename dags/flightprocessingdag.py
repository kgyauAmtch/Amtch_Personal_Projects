# Importing necessary modules 
import pendulum 
from airflow.decorators import dag, task
import pandas as pd
import os 
import mysql.connector 
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
import logging 
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import re


#load variables from the env file
load_dotenv()

#define my databse connection parameters

MYSQL_HOST = os.getenv('MYSQL_HOST','mysql_db2')
MYSQL_USER = os.getenv('MYSQL_USER','flightuser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD','myuserpass')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE','flightdb')

#postgres connection parameters

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

# Configure logging
logging.basicConfig(
    filename='/usr/local/airflow/plugins/log_file.log',  
    level=logging.INFO,                   
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dag(
    start_date= datetime(2025, 5, 17,20,15,00,00),
    dag_id='flight_price_processing_Kwame_Ofori_Gyau',
    description='A DAG to process flight price data from csv to mysql to postgres',
    schedule='@daily',
    tags=['flight_data'],
    max_consecutive_failed_dag_runs=3,
    catchup=False # prevent airflow from cathcing up.

)

def my_flight_process():
    @task
    
    #first task data ingestion
    def data_ingestion():
        #read data from csv file into mysql
        
        logging.info("Start of Data ingestion into mysql")
        
        csvfile_path='/usr/local/airflow/include/Flight_Price_Dataset_of_Bangladesh.csv'
        
        df=pd.read_csv(csvfile_path)
        
        logging.info(f'Number of rows is{len(df)}')
        
        #using my sql connector because sqlite is for light work as this is a little more comprehensive
        
        mysql_hook = MySqlHook(mysql_conn_id="mysql_flightprice")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
      
        # connection(cursor acts liek an actual cursor communicates between the python and the mysql)
        cursor = conn.cursor()

        # insert query to isert the data into the sql database 
        insert_query ="""
        INSERT INTO stagingflightprices(
        Airline,
        Source,
        Source_Name,
        Destination,
        Destination_Name,
        Departure_Date_Time,
        Arrival_Date_Time,
        Duration_hrs,
        Stopovers,
        Aircraft_Type,
        Class,
        Booking_Source,
        Base_Fare_BDT,
        Tax_Surcharge_BDT,
        Total_Fare_BDT,
        Seasonality,
        Days_Before_Departure) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        # loop through the rows and nsert into databse
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
       
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully loaded data into MySQL staging table")
        
        return 'Data Ingestion Task complete'
        #  ingestion_task = data_ingestion()
    
    @task
    def data_validation():
        
        #   task 2 data validation
        logging.info("Starting validation") 
        
        # Connect to MySQL database
        mysql_hook = MySqlHook(mysql_conn_id="mysql_flightprice")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
    
      # Required columns and their expected data types
        required_columns = {
            'Airline': str,
            'Source': str,
            'Source_Name': str,
            'Destination': str,
            'Destination_Name': str,
            'Departure_Date_Time': 'datetime',
            'Arrival_Date_Time': 'datetime',
            'Duration_hrs': float,
            'Stopovers': str,
            'Aircraft_Type': str,
            'Class': str,
            'Booking_Source': str,
            'Base_Fare_BDT': float,
            'Tax_Surcharge_BDT': float,
            'Total_Fare_BDT': float,
            'Seasonality': str,
            'Days_Before_Departure': int
        }
    
        # Get column information
        column_query = "SHOW COLUMNS FROM flightdb.stagingflightprices"
        try:
            columns_info = mysql_hook.get_records(column_query)
            existing_columns = [col[0] for col in columns_info]
            
            # Check for missing columns
            missing_columns = [col for col in required_columns.keys() if col not in existing_columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            logging.info("All required columns exist in the database.")
        except Exception as e:
            logging.error(f"Error checking columns: {e}")
            raise
        
        # Query all data for validation
        query = "SELECT * FROM flightdb.stagingflightprices"
        df = mysql_hook.get_pandas_df(query)
        
        # Store validation results
        validation_results = {
            'missing_values': {},
            'type_issues': {},
            'inconsistencies': {}
        }
        
        # Check for missing values
        for col in required_columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                validation_results['missing_values'][col] = missing_count
                logging.warning(f"Column {col} has {missing_count} missing values")
        
        # Type validation
        for col, expected_type in required_columns.items():
            # Skip columns that don't exist (handled in previous step)
            if col not in df.columns:
                continue
                
            # Handle special cases
            if expected_type == 'datetime':
                non_datetime = df[~pd.to_datetime(df[col], errors='coerce').notnull()].shape[0]
                if non_datetime > 0:
                    validation_results['type_issues'][col] = f"{non_datetime} non-datetime values"
            elif expected_type == float:
                # Check if values can be converted to float
                try:
                    non_numeric = df[pd.to_numeric(df[col], errors='coerce').isnull()].shape[0]
                    if non_numeric > 0:
                        validation_results['type_issues'][col] = f"{non_numeric} non-numeric values"
                except:
                    validation_results['type_issues'][col] = "Contains non-numeric values"
            elif expected_type == int:
                # Check if values can be converted to int
                try:
                    non_int = df[pd.to_numeric(df[col], errors='coerce').isnull()].shape[0]
                    if non_int > 0:
                        validation_results['type_issues'][col] = f"{non_int} non-integer values"
                except:
                    validation_results['type_issues'][col] = "Contains non-integer values"
            elif expected_type == str:
                # Check for empty strings
                empty_strings = (df[col] == '').sum()
                if empty_strings > 0:
                    validation_results['type_issues'][col] = f"{empty_strings} empty strings"
        
        # Check for inconsistencies 
        
        # Check for negative fares
        for fare_col in ['Base_Fare_BDT', 'Tax_Surcharge_BDT', 'Total_Fare_BDT']:
            if fare_col in df.columns:
                negative_fares = df[df[fare_col] < 0]
                negative_count = len(negative_fares)
                if negative_count > 0:
                    validation_results['inconsistencies'][fare_col] = f"{negative_count} negative values"
                    logging.warning(f"Found {negative_count} negative values in {fare_col}")
                    
                    # Log some examples of negative fares for debugging
                    if negative_count > 0:
                        examples = negative_fares.head(min(5, negative_count))[fare_col].tolist()
                        logging.warning(f"Examples of negative {fare_col}: {examples}")

        
        # Log summary of validation issues
        if any(validation_results[k] for k in validation_results):
            logging.warning("Data validation issues found:")
            for category, issues in validation_results.items():
                if issues:
                    logging.warning(f"  {category}: {issues}")
                    
            # Return validation results for potential downstream tasks
            return validation_results
        else:
            logging.info("All data passed validation checks.")
            return {"status": "valid"}
        
            # return 'Data Ingestion Task complete'
    
    
    @task 
    def data_transformation():
        logging.info("Starting data transformation task")
        
        try:
            # Connect to MySQL database
            mysql_hook = MySqlHook(mysql_conn_id="mysql_flightprice")
            
            # query the data from the staging table
            query = """
            SELECT * FROM flightdb.stagingflightprices
            """
            df = mysql_hook.get_pandas_df(query)
            logging.info(f"Retrieved {len(df)} rows for transformation")
            
            # Create a connection for writing transformation results
            conn = mysql_hook.get_conn()
            cursor = conn.cursor()
            
        
            # Calculate average fare by airline
            logging.info("Calculating average fares by airline")
            avg_fares_by_airline = df.groupby('Airline')[['Base_Fare_BDT', 'Tax_Surcharge_BDT', 'Total_Fare_BDT']].mean()
            
            # Create a table to store average fares if it doesn't exist
            create_avg_fares_table = """
            CREATE TABLE IF NOT EXISTS avg_fares_by_airline (
                id INT AUTO_INCREMENT PRIMARY KEY,
                airline VARCHAR(100),
                avg_base_fare FLOAT,
                avg_tax_surcharge FLOAT,
                avg_total_fare FLOAT
            )
            """
            cursor.execute(create_avg_fares_table)
            conn.commit()
            
            # Clear existing data and insert new averages
            cursor.execute("TRUNCATE TABLE avg_fares_by_airline")
            
            for airline, row in avg_fares_by_airline.iterrows():
                insert_query = """
                INSERT INTO avg_fares_by_airline (airline, avg_base_fare, avg_tax_surcharge, avg_total_fare)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    airline, 
                    row['Base_Fare_BDT'],
                    row['Tax_Surcharge_BDT'],
                    row['Total_Fare_BDT']
                ))
            conn.commit()
            logging.info(f"Stored average fares for {len(avg_fares_by_airline)} airlines")
            
            #Analyze seasonal fare variations
            logging.info("Analyzing seasonal fare variations")
            
            # Define peak seasons based on the Seasonality column
            # Assuming 'Seasonality' column already contains this information
            seasonal_avg_fares = df.groupby('Seasonality')[['Total_Fare_BDT']].agg(['mean', 'count'])
            seasonal_avg_fares.columns = seasonal_avg_fares.columns.droplevel()
            
            # Create a table to store seasonal fares if it doesn't exist
            create_seasonal_fares_table = """
            CREATE TABLE IF NOT EXISTS seasonal_fare_analysis (
                id INT AUTO_INCREMENT PRIMARY KEY,
                season VARCHAR(50),
                avg_fare FLOAT,
                booking_count INT
            )
            """
            cursor.execute(create_seasonal_fares_table)
            conn.commit()
            
            # Clear existing data and insert new seasonal analysis
            cursor.execute("TRUNCATE TABLE seasonal_fare_analysis")
            
            for season, row in seasonal_avg_fares.iterrows():
                insert_query = """
                INSERT INTO seasonal_fare_analysis (season, avg_fare, booking_count)
                VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (
                    season, 
                    row['mean'],
                    row['count']
                ))
            conn.commit()
            logging.info(f"Stored seasonal fare analysis for {len(seasonal_avg_fares)} seasons")
            
            #Count bookings by airline
            logging.info("Counting bookings by airline")
            booking_counts = df['Airline'].value_counts().reset_index()
            booking_counts.columns = ['Airline', 'Booking_Count']
            
            # Create a table to store booking counts if it doesn't exist
            create_booking_counts_table = """
            CREATE TABLE IF NOT EXISTS airline_booking_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                airline VARCHAR(100),
                booking_count INT
            )
            """
            cursor.execute(create_booking_counts_table)
            conn.commit()
            
            # Clear existing data and insert new booking counts
            cursor.execute("TRUNCATE TABLE airline_booking_counts")
            
            for _, row in booking_counts.iterrows():
                insert_query = """
                INSERT INTO airline_booking_counts (airline, booking_count)
                VALUES (%s, %s)
                """
                cursor.execute(insert_query, (
                    row['Airline'], 
                    row['Booking_Count']
                ))
            conn.commit()
            logging.info(f"Stored booking counts for {len(booking_counts)} airlines")
            
            #Identify most popular routes
            logging.info("Identifying most popular routes")
            df['Route'] = df['Source'] + '-' + df['Destination']
            popular_routes = df['Route'].value_counts().reset_index()
            popular_routes.columns = ['Route', 'Frequency']
            
            # Get top 10 routes with source-destination details
            top_routes = popular_routes.head(10)
            
            # Create a more detailed dataframe with source and destination names
            top_routes_detailed = []
            for _, route_row in top_routes.iterrows():
                route = route_row['Route']
                source, destination = route.split('-')
                
                # Find a sample row for this route to get the names
                sample_row = df[(df['Source'] == source) & (df['Destination'] == destination)].iloc[0]
                
                top_routes_detailed.append({
                    'Source_Code': source,
                    'Source_Name': sample_row['Source_Name'],
                    'Destination_Code': destination,
                    'Destination_Name': sample_row['Destination_Name'],
                    'Frequency': route_row['Frequency'],
                    'Avg_Fare': df[(df['Source'] == source) & (df['Destination'] == destination)]['Total_Fare_BDT'].mean()
                })
            
            top_routes_df = pd.DataFrame(top_routes_detailed)
            
            # Create a table to store popular routes if it doesn't exist
            create_popular_routes_table = """
            CREATE TABLE IF NOT EXISTS popular_routes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source_code VARCHAR(50),
                source_name VARCHAR(100),
                destination_code VARCHAR(50),
                destination_name VARCHAR(100),
                frequency INT,
                avg_fare FLOAT
            )
            """
            cursor.execute(create_popular_routes_table)
            conn.commit()
            
            # Clear existing data and insert new popular routes
            cursor.execute("TRUNCATE TABLE popular_routes")
            
            for _, row in top_routes_df.iterrows():
                insert_query = """
                INSERT INTO popular_routes (
                    source_code, source_name, destination_code, destination_name, frequency, avg_fare
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    row['Source_Code'],
                    row['Source_Name'],
                    row['Destination_Code'],
                    row['Destination_Name'],
                    row['Frequency'],
                    row['Avg_Fare']
                ))
            conn.commit()
            logging.info(f"Stored {len(top_routes_df)} popular routes")
            
            # Close connections
            cursor.close()
            conn.close()
            
            # Return summary of transformations
            return {
                "rows_processed": len(df),
                "airlines_analyzed": len(avg_fares_by_airline),
                "seasons_analyzed": len(seasonal_avg_fares),
                "popular_routes_identified": len(top_routes_df)
            }
            
        except Exception as e:
            logging.error(f"Error in data transformation: {str(e)}")
            raise
    
    @task
    def postgres_ingestion():
        
                
        try:
            # Connect to MySQL to retrieve transformed data
            mysql_hook = MySqlHook(mysql_conn_id="mysql_flightprice")
            
            # Connect to PostgreSQL
            postgres_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DATABASE,
                port=POSTGRES_PORT
            )
            postgres_cursor = postgres_conn.cursor()
            
            # Define the tables we want to transfer
            tables_to_transfer = [
                {
                    'name': 'avg_fares_by_airline',
                    'create_sql': """
                        CREATE TABLE IF NOT EXISTS avg_fares_by_airline (
                            id SERIAL PRIMARY KEY,
                            airline VARCHAR(100),
                            avg_base_fare FLOAT,
                            avg_tax_surcharge FLOAT,
                            avg_total_fare FLOAT
                        )
                    """,
                    'columns': ['airline', 'avg_base_fare', 'avg_tax_surcharge', 'avg_total_fare']
                },
                {
                    'name': 'seasonal_fare_analysis',
                    'create_sql': """
                        CREATE TABLE IF NOT EXISTS seasonal_fare_analysis (
                            id SERIAL PRIMARY KEY,
                            season VARCHAR(50),
                            avg_fare FLOAT,
                            booking_count INT
                        )
                    """,
                    'columns': ['season', 'avg_fare', 'booking_count']
                },
                {
                    'name': 'airline_booking_counts',
                    'create_sql': """
                        CREATE TABLE IF NOT EXISTS airline_booking_counts (
                            id SERIAL PRIMARY KEY,
                            airline VARCHAR(100),
                            booking_count INT
                        )
                    """,
                    'columns': ['airline', 'booking_count']
                },
                {
                    'name': 'popular_routes',
                    'create_sql': """
                        CREATE TABLE IF NOT EXISTS popular_routes (
                            id SERIAL PRIMARY KEY,
                            source_code VARCHAR(50),
                            source_name VARCHAR(100),
                            destination_code VARCHAR(50),
                            destination_name VARCHAR(100),
                            frequency INT,
                            avg_fare FLOAT
                        )
                    """,
                    'columns': ['source_code', 'source_name', 'destination_code', 'destination_name', 'frequency', 'avg_fare']
                }
            ]
            
            # Create a flight data table in PostgreSQL
            complete_data = """
            CREATE TABLE IF NOT EXISTS complete_flight_data (
                id SERIAL PRIMARY KEY,
                airline VARCHAR(100),
                source_code VARCHAR(50),
                source_name VARCHAR(100),
                destination_code VARCHAR(50),
                destination_name VARCHAR(100),
                departure_datetime TIMESTAMP,
                arrival_datetime TIMESTAMP,
                duration_hrs FLOAT,
                stopovers VARCHAR(50),
                aircraft_type VARCHAR(100),
                class VARCHAR(50),
                booking_source VARCHAR(100),
                base_fare_bdt FLOAT,
                tax_surcharge_bdt FLOAT,
                total_fare_bdt FLOAT,
                seasonality VARCHAR(50),
                days_before_departure INT,
                route VARCHAR(100)
            )
            """
            postgres_cursor.execute(complete_data)
            postgres_conn.commit()
            
            # Process each table
            transfer_summary = {}
            
            for table_info in tables_to_transfer:
                table_name = table_info['name']
                logging.info(f"Processing table: {table_name}")
                
                # Create the table in PostgreSQL if it doesn't exist
                try:
                    postgres_cursor.execute(table_info['create_sql'])
                    postgres_conn.commit()
                    logging.info(f"Created table {table_name} in PostgreSQL (if it didn't exist)")
                except Exception as e:
                    logging.error(f"Error creating PostgreSQL table {table_name}: {str(e)}")
                    raise
                
                # Clear existing data in PostgreSQL table
                try:
                    postgres_cursor.execute(f"TRUNCATE TABLE {table_name}")
                    postgres_conn.commit()
                    logging.info(f"Cleared existing data from PostgreSQL table {table_name}")
                except Exception as e:
                    logging.error(f"Error truncating PostgreSQL table {table_name}: {str(e)}")
                    raise
                
                # Fetch data from MySQL
                query = f"SELECT * FROM {table_name}"
                mysql_data = mysql_hook.get_pandas_df(query)
                
                if mysql_data.empty:
                    logging.warning(f"No data to transfer for table {table_name}")
                    transfer_summary[table_name] = 0
                    continue
                
                # Insert data into PostgreSQL
                placeholders = ', '.join(['%s'] * len(table_info['columns']))
                columns = ', '.join(table_info['columns'])
                
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                row_count = 0
                for _, row in mysql_data.iterrows():
                    # Extract just the columns we need
                    values = [row[col] for col in table_info['columns']]
                    postgres_cursor.execute(insert_query, values)
                    row_count += 1
                    
                    # Commit in batches for better performance
                    if row_count % 100 == 0:
                        postgres_conn.commit()
                
                # Final commit
                postgres_conn.commit()
                transfer_summary[table_name] = row_count
                logging.info(f"Transferred {row_count} rows to PostgreSQL table {table_name}")
            
            # Transfer a dataset for comprehensive analyses
            logging.info("Preparing complete flight data for PostgreSQL")
            
            # Fetch all necessary data from MySQL
            completedata_query = """
            SELECT 
                s.Airline as airline, 
                s.Source as source_code, 
                s.Source_Name as source_name,
                s.Destination as destination_code, 
                s.Destination_Name as destination_name,
                s.Departure_Date_Time as departure_datetime,
                s.Arrival_Date_Time as arrival_datetime,
                s.Duration_hrs as duration_hrs,
                s.Stopovers as stopovers,
                s.Aircraft_Type as aircraft_type,
                s.Class as class,
                s.Booking_Source as booking_source,
                s.Base_Fare_BDT as base_fare_bdt,
                s.Tax_Surcharge_BDT as tax_surcharge_bdt,
                s.Total_Fare_BDT as total_fare_bdt,
                s.Seasonality as seasonality,
                s.Days_Before_Departure as days_before_departure,
                CONCAT(s.Source, '-', s.Destination) as route
            FROM stagingflightprices s
            """
            complete_data = mysql_hook.get_pandas_df(completedata_query)
            
            # Clear existing data in  table
            postgres_cursor.execute("TRUNCATE TABLE complete_flight_data")
            postgres_conn.commit()
            
            # Insert  data
            completedata_cols = ['airline', 'source_code', 'source_name', 'destination_code', 
                                'destination_name', 'departure_datetime', 'arrival_datetime', 
                                'duration_hrs', 'stopovers', 'aircraft_type', 'class', 
                                'booking_source', 'base_fare_bdt', 'tax_surcharge_bdt', 
                                'total_fare_bdt', 'seasonality', 'days_before_departure', 'route']
            
            placeholders = ', '.join(['%s'] * len(completedata_cols))
            columns = ', '.join(completedata_cols)
            
            insert_query = f"INSERT INTO complete_flight_data ({columns}) VALUES ({placeholders})"
            
            row_count = 0
            for _, row in complete_data.iterrows():
                # Extract just the columns we need, handling potential datetime conversion issues
                values = []
                for col in completedata_cols:
                    if col in ['departure_datetime', 'arrival_datetime'] and pd.notnull(row[col]):
                        # Ensure datetime format is compatible with PostgreSQL
                        values.append(pd.to_datetime(row[col]))
                    else:
                        values.append(row[col])
                
                postgres_cursor.execute(insert_query, values)
                row_count += 1
                
                # Commit in batches for better performance
                if row_count % 100 == 0:
                    postgres_conn.commit()
                    logging.info(f"Transferred {row_count}  rows to PostgreSQL")
            
            # Final commit
            postgres_conn.commit()
            transfer_summary['complete_flight_data'] = row_count
            logging.info(f"Transferred {row_count} rows to PostgreSQL complete_flight_data table")
            
            
            # Close connections
            postgres_cursor.close()
            postgres_conn.close()
            
            return {
                "tables_transferred": len(tables_to_transfer) + 1,  # +1 for  table
                "transfer_details": transfer_summary
            }
            
        except Exception as e:
            logging.error(f"Error in PostgreSQL ingestion: {str(e)}")
            # Log full stack trace for debugging
            import traceback
            logging.error(traceback.format_exc())
            raise
    

     #set dependencies
    data_ingestion() >> data_validation() >> data_transformation() >> postgres_ingestion()

my_flight_processDAG = my_flight_process()