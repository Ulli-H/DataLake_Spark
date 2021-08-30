# DataLake Spark

Data Lake with Spark and Amazon s3


*An Udacity Data Engineer Nanodegree project*

## Content
- [Description](#description)
- [Data](#data)
- [Tables](#tables)
- [ELT-process and instructions](#etl-process_and_instructions)
- [Links](#links)

## Description  

The subject of the project is the fictional music streaming provider "Sparkify", which has grown out of their current data warehouse solution and wants to move to a Data Lake Infrastructure. With the help of Spark they will analyze their data generated from the application in a distributed approach, in-memory, which will increase the speed drastically compared to the old solution. The client requests for a service hosted in the AWS cloud. The data is loaded from an s3 bucket and the materialized tables are also written to an s3 output bucket.



## Data  

The data used for this project consists of a song dataset and log dataset, which are both stored in s3 buckets. Both datasets are multiple files in JSON format. 
The song data files are partitioned by the first 3 letters of each song's track ID and contain information on each song and the artist of the song. 

The logdata files contain event data for every event of user activity that happended in the app during one day. The files are partitioned by year and date. 

All data was provided by Udacity as part of the data engineer nanodegree. 



## Tables 

The following materialized tables where created from the data:

#### - fact table: songplays (records in the log data associated with song plays)  
columns: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### - dimension table: users (data on app users)  
columns: *user_id, first_name, last_name, gender, level*

#### - dimension table: songs (data on all songs in library/app)  
columns: *song_id, title, artist_id, year, duration*

#### - dimension table: artists (data on all artists in library/app)  
columns: *artist_id, name, location, latitude, longitude*

#### - dimension table: time (timestamps of songplays in log data brocken down in different units)  
columns: *start_time, hour, day, week, month, year, weekday*



## ELT-process and instructions
The etl.py script loads and processes the input data and writes the materialized tables into the output bucket in s3.
  
1) Create an AWS IAM role (s3 read write access)
2) Enter IAM's credentials in dl.cfg
3) Create s3 bucket 
4) Enter bucket-URL in etl.py as output_data.
5) Run etl.py 



## Links

[Repository](https://github.com/Ulli-H/DataLake_Spark) 
