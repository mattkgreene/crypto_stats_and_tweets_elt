# Crypto Stats and Tweets Data Pipeline using Airflow

## Introduction

### Project Overview

#### This project was brought upon through Udacity's nanodegree program.  
#### For the capstone project within the nanodegree, the ultimate goal is to build a data pipeline
#### that uses the technologies and applications covered in the the program.
#### With the recent rise of crypto currency interests and the evolution of crypto twitter into
#### the media spotlight, revolving my capstone project around these two areas seemed like a good idea.

#### The ultimate goal of this project is to create both crypto statistics and crypto tweets datasets that can be used in downstream applications.  
#### That goal was accomplished through this project.  However, I have further goals for this project, which will be discussed later.

### Project Requirements
#### At least 2 data sources
##### twitter.com accessed through snscrape tweets libary
##### coingecko public API resulting in crypto currency statistical data starting in 2015.
#### More than 1 million lines of data.
##### The snscrape_tweets_hist dataset has over 1.5 million rows
##### The coin_stats_hist has over 250k rows.
#### At least two data sources/formats (csv, api, json)
##### Stored in S3 (mkgpublic)
##### mkgpublic/capstone/tweets/tweets.parquet
##### mkgpublic/capstone/crypto/cg_hourly.csv 


### Data Ingestion Process
#### Tweets
##### The original data ingestion process ran into few snafus. As I decided to use the twitter API to get the tweets
##### side of the data at first; however, due to limitations within the twitter API, I couldn't get more than 1000 tweets per call.
##### Thus, I decided to use the snscrape tweets python library instead, which provided a much easier method to get a ton of tweets in a
##### reasonable amount of time.
##### Through using the snscrape tweets python library the tweets were gathered running a library function.
##### The tweets were than stored in a MongoDB database as an intermediary storage solution.
##### Data was continuously ingested between using this process until enough tweets about various crypto currencies was gathered.
##### After storing the tweets in MongoDB the tweets were then pulled from the MongoDB database, stored in a pandas dataframe and written to
##### the mkgpublic s3 bucket as a parquet file.

#### Crypto
##### Using the coingecko api, crypto currency statistical data was pulled and stored in a pandas dataframe.  
##### After storing in the pandas df the data was written to the MongoDB database used for tweets.
##### Data is continously ingested through this process until enough statistical data about various crypto currencies was stored.
##### Finally the crypto currency statistical data is pulled from the MongoDB database, stored in a pandas dataframe and written to 
##### the mkgpublic s3 bucket as a CSV.
##### *** Note ***
###### I stored the data as a CSV because two sets of data formats were requested.  I originally choose to store the crypto stats data as a 
###### json file, but even when partitioning the file into several JSON files, the files were too big for airflow to handle.  Thus, I went with
###### the csv format.

### Crypto Stats and Tweets ELT

#### Now we get into the udacity capstone data ingestion and processing part of this project.

#### Ultimately I choose to follow a similar process to what is in the mkg_airflow repository where I am using airflow to run a 
#### sequence of tasks.

#### Data Model

<blockquote class="imgur-embed-pub" lang="en" data-id="a/M82fKpe"  ><a href="//imgur.com/a/M82fKpe">Udacity Capstone Project Data Model</a>

#### Steps

<blockquote class="imgur-embed-pub" lang="en" data-id="a/egP96PR"  ><a href="//imgur.com/a/egP96PR">Airflow Udacity Capstone Dag</a></blockquote>

1. Create Redshift Cluster
2. Create Crypto, Tweets, and Dim Schemas
3. Create Crypto/Tweets staging and Dim Tables
4. Staging
  1. Stage Coingecko Token List Mapping Table
  2. Stage Coingecko hourly crypto currency statistical table
  3. Stage snscrape tweets crypto twitter table
5. Load Dimensions
  1. Load Coingecko Token List Mapping Table
  2. Load Date Dim with date information from Coingecko hourly crypto currency statistical staging table
  3. Load Date Dim with date information from Stage snscrape tweets crypto twitter staging table
6. Create Fact Tables
7. Load Fact Tables
  1. Load crypto currency statistics history table
  2. Load snscrape tweets history table
8. Run Data Quality Checks
  1. Select Statements that make sure data is actually present
  2. Build an Aggregate table with min statistic and max statistic values per month from the coin_stats_hist table
9. Store resulting dim, fact and aggregate tables in S3
10. Delete Redshift Cluster

### Future Work and Final Thoughts

#### Some questions for future work:
* What if the data was increased by 100x.
  * I would use a spark emr cluster to process the data for me as that would speed up both the data ingestion and processing parts of the project.
  * This is likely going to happen in my future steps for this process, so ultimately this will be added in future versions.
* What if the pipelines would be run on a daily basis by 7 am every day.
  * I need a way to get the first part of this process easier.  The issue is sometimes either the coingecko or the snscrape tweets api breaks.
  Thus, if this pipeline would need to be run every day at 7am I would need to fix the initial data ingestion into my S3 bucket, 
  as in, making the process more automated.  Nonetheless, if we are just referring to the S3-->Redshift-->S3 part of the process, then I would
  set airflow to run the process daily as the initial api --> MongoDB --> S3 part of the process would be taken care of.  I would also need to add
  in a extra step so that the pipeline combines the data previously stored in the S3 bucket with the new data added.
* What if the database needed to be accessed by 100+ people.
  * If the database needs to be accessed by 100+ people than I would need to either need to:
    * constantly run a redshift cluster with the tables stored in said cluster (this requires additional IAM configuration and security protocols)
    * store the results in MongoDB so everyone can just pull from that database using pandas (requires adding everyones IP to the MongoDB Network)
    * have users simply pull from the mkgpublic S3 Bucket (just need the S3 URI)

#### Future Work
##### Ultimately, I want to use these datasets built out through this datapipline in a dashboard hosted on a website.
##### I want to incoporate reddit data as well into the mix and then run sentiment analysis on both the tweets and reddit thread datasets
##### to determine the current crypto market sentiment.
##### Work will be done over the next few months on the above tasks.
    




