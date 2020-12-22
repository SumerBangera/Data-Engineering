# Project 2: NoSQL Data Modeling with Apache Cassandra

## Introduction
A startup called Sparkify (think Spotify) wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in **understanding what songs users are listening to**. Currently, there is no easy way to query the data to generate the results, since the **data reside in a directory of CSV files on user activity on the app**.

### Task
Acting as Sparkify's Data Engineer, develop a non-relational (NoSQL) database with Apache Cassandra and build an ETL pipeline using Python to facilitate efficient data querying by the Analytics team. 

Primarily, the teams wants to achieve the following:
1. Get the artist, song title and song's length in the music app history that was heard during a speific 'sessionId' and 'itemInSession'
2. Get the name of artist, song (sorted by itemInSession) and user (first and last name) for a given 'userid' and 'sessionid'
3. Get every user name (first and last) in the music app history who listened to a particular song e.g. 'All Hands Against His Own'

## Data
For this project, we will be working with ```event_data``` dataset. The directory of CSV files is partitioned by date. Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

### Schema
Each file in the dataset consists of the following columns

- artist 
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>

<img src="images/image_event_datafile_new.jpg">


## Project Steps

### Modeling the NoSQL database with Apache Cassandra

1. Design tables and define schema to answer the queries outlined by the Analytics team
2. Define correct PRIMARY KEY - Partition Keys, Clustering Columns and Composite Primary Keys
3. Develop CREATE statement for building denormalized tables optimized for the queries
4. Load the data with INSERT statement for each of the tables
5. Test by running the proper SELECT statements with the correct WHERE clause and compare with expected results

### Build ETL Pipeline with Python

1. Implement the logic to iterate through each event file in the dataset to process and create a new streamlined CSV file
2. Include Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in the data model
3. Test by running SELECT statements after running the queries on your database


## Files
Details of the files used for the project:
1. **event_data** - directory of CSV files partitioned by date
2. **event_datafile_new.csv** - denormalized dataset used to insert data into the Apache Cassandra tables
3. **Project_1B_ Project_Template** - Python code for creating tables and developing the ETL process

## References:
https://github.com/danieldiamond/udacity-dend
https://github.com/Flor91/Data-engineering-nanodegree
