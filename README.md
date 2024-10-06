# ETLDesignProject
This README serves as a summary of the ETL (Extract, Transform, Load) process for moving data from a CSV file into a PostgreSQL database and the automation process for a hourly scheduler of the ETL script.  

Instructions
etl.py - is the main ETL script containing all the functions for the E-T-L of the data from CSV file to a PostgreSQL DB. 
Extract Data: The tool or library used for this purpose is Pandas, to read and extract data from the CSV file.

Transform Data: Modify and shape the extracted data as needed for your specific use case, for example, data type conversions, data validation, or filtering.

Load Data: Establish a connection to your PostgreSQL database, create a table to match your data, and insert the transformed data into the table using SQL statements.


