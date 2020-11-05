# Sparkify Database

Sparkify database is a Redshift database that stores the user activity and songs metadata on Sparkify's new streaming application. Previously this information resided in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. ETL pipeline is built that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.

## Purpose

- The database provides Sparkify's analytical team with access to their processes and data on cloud.
- The design of the database tables is optimized for queries on song play analysis.

## Database Schema Design

Sparkify database tables form a star schema. This database design separates facts and dimensions yielding a subject-oriented design where data is stored according to logical relationships, not according to how the data was entered. 

- Fact And Dimension Tables

    The database includes:
    - Fact table:
        
        1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
            - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
            
    - Dimension tables:
        
        2. **users** - users in the app
            - user_id, first_name, last_name, gender, level
        3. **songs** - songs in music database
            - song_id, title, artist_id, year, duration
        4. **artists** - artists in music database
            - artist_id, name, location, latitude, longitude
        5. **time** - timestamps of records in songplays broken down into specific units
            - start_time, hour, day, week, month, year, weekday

## ETL Pipeline

An ETL pipeline is built using python. The python script loads data from S3 to staging tables on Redshift. It also inserts data from staging tables into fact and dimensional tables. 
    
## Running Python Scripts
    
The following python scripts are used to setup Redshift cluster, create staging and analytics tables on Redshift and to load data from S3 to staging tables using COPY queries and to insert the records from staging tables into the database tables:

1. **setup.py** 
This script performs setup actions such as creating IAM role for Redshift cluster, attaching policy to the role, creating Redshift cluster, and allowing ingress of incoming traffic to cluster's default security group. Run this file to setup Redshift cluster with required permissions before running create_tables script.
*Run below command in terminal to execute this script:*
    `python setup.py`

2. **create_tables.py** 
This script drops and creates database tables both staging and analytics tables on Redshift. This script imports and uses drop and create statements from *sql_queries.py* script. Run this file to reset the database tables before running ETL scripts.
*Run below command in terminal to execute this script:*
    `python create_tables.py`

3. **etl.py**
This script connects to Sparkify database and executes queries to load data from S3 to staging tables and insert records into dimension and fact tables in Redshift database. After running this script data should be inserted into analytics tables on Redshift and can be used for further analysis.
*Run below command in terminal to execute this script:*
        `python etl.py`

4. **teardown.py**
This script can be used for teardown actions such as deleting IAM role for Redshift cluster, detaching policy to the role, deleting Redshift cluster, and revoking ingress of incoming traffic to cluster's default security group. 
*Run below command in terminal to execute this script:*
        `python teardown.py`







    

