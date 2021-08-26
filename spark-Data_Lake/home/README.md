# Project: Data Lake

## Introduction
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Project Description
In this project, we'll apply what we have learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, we  load data from S3, process the data into analytics tables using Spark, and load them back into S3.

## Document Process
* Process Song Data
    *  Firstly we Load .Json files and create a view for songs data
    *  We then Use spark to extract dataset for songs and artist tables from songs data view
    *  Finally we write the data to an S3 Bucket 
    
* Process Log Data 
    *  Firstly we Load .Json files and create a view for log data
    *  We then Use spark to extract dataset for user, time and sonplay tables from logs data view
    *  Finally we write the data to an S3 Bucketsa as parquet
 
* Intiate Read, Process and Write 
    * The main function runs the process song and long data methods