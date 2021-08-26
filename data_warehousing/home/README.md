
# Sparkify Data Pipeline 

[Schema Design in GIT](https://github.com/aoyerinde/Data_Modelling_Project/blob/main/project_3/Sparkify_DB.png)


## Fact Tables
1. songplays - records in log data associated with song plays. Below are the field and data types 
* songplay_id ::integer
* start_time ::text
* user_id  ::text
* level ::text
* song_id ::text
* artist_id ::text
* session_id ::text
* location  ::text
* user_agent ::text

## Dimension Tables
2. users - users in the app. Below are the fields and data types.
* user_id ::varchar
* first_name ::text
* last_name ::text
* gender ::text
* level ::text

3. songs - songs in music database
* song_id ::text
* title ::text
* artist_id ::text
* year ::int
* duration ::double precision

4. artists - artists in music database
* artist_id ::text
* name ::text
* location ::text
* latitude  ::double precision
* longitude  ::double precision

5. time - timestamps of records in songplays broken down into specific units
* start_time ::timestamp
* hour ::int
* day ::int
* week ::int 
* month ::varchar 
* year ::int 
* weekday ::varchar

## Important key to join & filter on
* user table
    * user_id -> songplay table, 
    
* song table
    * song_id -> songplay table
    * artist_id -> artist table
 
* artist_id 
    * artist_id -> song table 
  


# ETL pipeline

## Extraction
The extraction process has been done from two main files: song_data and log_data from the S3 Bucket.

## Load (Using the pipeline)
To run the pipeline in the terminal, the following steps should be taken.
 * Open terminal and run "python create_tables.py". This will set up the database connection and create the neccesary tables
 
 * run "python etl.py" to load data from the s3 bucket to the staging tables and then the production ones
 
