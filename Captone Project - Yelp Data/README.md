# Data Engineering Capstone Project - Yelp Dataset
### Project Summary
This project aims at employing skills developed through the nanodegree to build an efficient data pipeline using the Yelp dataset. 
Primarily the project focuses on: 
* collecting and cleaning data from an external source 
* loading the data to Amazon S3 for storage
* defining an efficient data model using Star Schema
* building datawarehouses using Amazon Redshift
* automating the ETL pipeline using Apache Airflow

The project includes the following steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Additional details about the project

### Step 1: Scope the Project and Gather Data
#### Scope
This project uses the Yelp dataset to develop a cloud-based database to support analytical requirements of data analysts/scientists. In this project, we only focus on the restaurant businesses in the dataset. The end solution of this project is aimed at building a single-source of truth for analytics dashboard and support NLP-related tasks.

#### Describe and Gather Data
The original dataset contains information about various businesses, user reviews, users, check-in details, etc. in separate JSON and CSV files. The original dataset has been sampled to only included restuarants businesses. 

The dataset for this analysis has been prepared using the Kaggle kernal to filter the relevant data only. Please refer the yelp_data_prep.ipynb (Jupyter Notebook) for detailed steps about the data collection and preparation steps.

The final prepared files include: filetype (rows, columns)
1. restaurant_df - CSV (43965, 11)
2. reviews_df - JSON (1100000, 9)
3. checkin_df - CSV (43039, 4)
4. users_df - JSON (540335, 21)

These files were then uploaded to an Amazon S3 bucket for data storage before creating the data warehouse on Amazon Redshift.


### Step 2: Explore and Assess the Data
Please refer the yelp_data_prep.ipynb (Jupyter Notebook) for detailed steps

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
The schema used for this project is the Star Schema with one main fact table and 3 dimentional tables as described below

<img src="https://github.com/SumerBangera/Data-Engineering/blob/main/Captone Project - Yelp Data/images/Schema.png"/>


#### Fact Table
**reviews** - records of each review given to the restaurants in the database
    - review_id, user_id, business_id, stars, date, review_text, useful, funny, cool
    
#### Dimension Tables
1. **restaurants** - details about restaurants in the database
    - business_id, business_name, address, city, state, postal code, latitude, longitude, stars, review_count, categories, total_checkins, first_checkin, latest_checkin
2. **users** - details about users in the database
    - user_id, user_name, review_count, yelping_since, useful_total, funny_total, cool_total, fans, average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos
3. **date_table** - timestamps of records in the database broken down into specific units
    - date, day, week, month, year, dayOfWeek
    

#### Staging Tables
For using Redshift to store the staging data, the staging_events and staging_songs table need to be created. Data from these staging tables will be extracted and moved into the above fact and dimensionals tables

1. **staging_reviews** 
    - review_id, user_id, business_id, stars, useful, funny, cool, text, date
2. **staging_restaurants** 
    - business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count,categories
3. **staging_checkin**
    - business_id, count, min, max
4. **staging_users**
    - user_id, name, review_count, yelping_since, useful, funny, cool, elite,	fans,	average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos


#### 3.2 Mapping Out Data Pipelines
List of steps necessary to pipeline the data into the above data model
1. Run the yelp_data_prep.ipynb to extract relevant data from the Yelp dataset on Kaggle (this is better than locally downloading the files and then cleaning them as we can use Kaggle kernal to execute the big files)
2. Upload the output files from step 1 into an Amazon S3 bucket
3. Setup an Amazon Redshift cluster for data warehousing
4. Create connections to the Redshift cluster using Airflow
5. Run the Airflow DAG named "yelp_dag" which will automate the entire data pipeline

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
To enhance and automate the data pipeline, Apache Airflow is employed with relevant operators and helper functions. The ETL DAG graph created for this project is as given below:

<img src="https://github.com/SumerBangera/Data-Engineering/blob/main/Captone%20Project%20-%20Yelp%20Data/images/DAG.png"/>

Details about each operator and helper function in the DAG is given below:
1. **create_tables.py** - contains all the necessray CREATE statements to create the data model tables
2. **sql_queries.py** - contains all the necessary INSERT statements to populate the tables 
3. **StageToRedshiftOperator** - uses Airflow's PostgreSQL & S3 hooks to read and copy data into the staging tables in Redshift
4. **LoadFactOperator** & **LoadDimensionOperator** - uses Airflow's PostgreSQL hook and relevant SQL statements to transform the staging data into the defined Star Schema for loading data into appropriate fact and dimension tables defined above
5. **DataQualityOperator** - uses Airflow's PostgreSQL hook to to detect discrepancies within the newly formed data warehouse. Custom SQL statements can be passed to check data quality as required

#### 4.2 Data Quality Checks
Data quality is ensured through integrity constraints on the relational database (e.g., unique key, data type, etc.)

Furthermore, additional quality checks have been implemented within the data pipeline DAG using the DataQualityOperator. These include:
 * Source/Count checks to ensure completeness
 * Unit tests for the scripts to ensure they are doing the right thing using custom SQL statementements in the DataQualityOperator

#### 4.3 Data dictionary 
Refer [data_dictionary.txt](https://github.com/SumerBangera/Data-Engineering/blob/main/Captone%20Project%20-%20Yelp%20Data/data_dictionary.txt) which provides a brief description of each field. 


### Step 5: Additional details about the project
#### Tools
**Python** is used as the programming language for its versatility and production-readiness
**PostgreSQL** is used for the data warehouse due to its well-suppored relational database management system
**Amazon S3** is used as the cloud data storage option to protect and provide easy access to data
**Amazon Redshift** is used as the data warehouse to easily connect to the data in S3 buckets and ability to scale up/down the number of nodes as required
**Airflow** is used to run the ETL due to its powerful scheduling and monitoring features


#### Scenario Analysis:
Eventually this project may have to address the following scenarios as the data grows:

**The data was increased by 100x**: This project uses cloud technologies like Amazon S3 and Redshift to store and process the data. The main advantage of these technologies is the ability to scale up/down the resources required to store and process the data. Amazon EMR can be explored as well. 

**The pipelines would be run on a daily basis by 7am every day**: Apache Airflow implemented in the project provides great flexibility and ability to schedule the data pipelines. To meet this requirement, the parameters in the DAG can be changed to run daily using the schedule_interval parameter. 

**The database needed to be accessed by 100+ people**: Given the above cloud technologies, data can be accessed without any issues.   


## Future work:
1. Design an Analytics dashboard in Tableau/Metabase
2. Conduct NLP analysis of the reviews
