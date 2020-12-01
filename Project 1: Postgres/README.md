# Project 1: Relational Data Modeling with PostgreSQL

## Introduction

A startup called Sparkify (think Spotify) wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in **understanding what songs users are listening to**. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Data Preview:
#### Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
#### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what a single log file looks like.
```
{"artist":null,"auth":"Logged In", "firstName":"Jordan", "gender":"F", "itemInSession":0, "lastName":"Hicks", "length":null, "level":"free", "location":"Salinas, CA", "method":"GET", "page":"Home", "registration":1540008898796.0, "sessionId":240, "song":null, "status":200,"ts":1541480984796, "userId":"37", "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.78.2 (KHTML, like Gecko) Version\/7.0.6 Safari\/537.78.2\""}
```

### Task
Acting as Sparkify's Data Engineer, develop a relational database with Postgres and build an ETL pipeline using Python to facilitate efficient data querying by the Analytics team. 
Detailed tasks include:
    - define fact and dimension tables for a star schema 
    - write an ETL pipeline to transfer data from files in two local directories into these tables 


### Schema for Song Play Analysis
The schema used for this exercise is the Star Schema with one main fact table and 4 dimentional tables
![alt text](https://github.com/SumerBangera/Data-Engineering/blob/main/Star%20Schema.png?raw=true)

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
1. **users** - users in the app
    - user_id, first_name, last_name, gender, level
2. **songs** - songs in music database
    - song_id, title, artist_id, year, duration
3. **artists** - artists in music database
    - artist_id, name, location, latitude, longitude
4. **time** - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

# Files
Details of the files used for the project:
1. **sql_queries.py** contains all the sql queries imported into the files bellow.
2. **create_tables.py** drops and creates tables. Run this file to reset the tables before each time the ETL scripts are run.
3. **test.ipynb** displays the first few rows of each table to check the database.
4. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into the tables. 
5. **etl.py** reads and processes all files from song_data and log_data and loads them into your tables. 