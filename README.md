# Crypto Stats and Tweets Data Pipeline using Airflow

## Introduction

### Project Overview

#### This project was brought upon through Udacity's nanodegree program.  
#### For the capstone project within the nanodegree, the ultimate goal is to build a data pipeline that uses the technologies and applications covered in the the program.
#### With the recent rise of crypto currency interests and the evolution of crypto twitter into the media spotlight, revolving my capstone project around these two areas seemed like a good idea.

#### The ultimate goal of this project is to create both crypto statistics and crypto tweets datasets that can be used in downstream applications.  
#### That goal was accomplished through this project.  However, I have further goals for this project, which will be discussed later.

### Project Requirements
#### At least 2 data sources
  * twitter.com accessed through snscrape tweets libary
  * coingecko public API resulting in crypto currency statistical data starting in 2015.
#### More than 1 million lines of data.
  * The snscrape_tweets_hist dataset has over 1.5 million rows
  * The coin_stats_hist has over 250k rows.
#### At least two data sources/formats (csv, api, json)
  * Stored in S3 (mkgpublic)
    * mkgpublic/capstone/tweets/tweets.parquet
    * mkgpublic/capstone/crypto/cg_hourly.csv 


### Data Ingestion Process
#### Tweets
The original data ingestion process ran into few snafus. As I decided to use the twitter API to get the tweets side of the data at first; however, due to limitations within the twitter API, I couldn't get more than 1000 tweets per call.

Thus, I decided to use the snscrape tweets python library instead, which provided a much easier method to get a ton of tweets in a reasonable amount of time.

Through using the snscrape tweets python library, the tweets were gathered running a library function.

The tweets were than stored in a MongoDB database as an intermediary storage solution.

Data was continuously ingested using this process until enough tweets about various crypto currencies was gathered.

After storing the tweets in MongoDB the tweets were then pulled from the MongoDB database, stored in a pandas dataframe and written to the mkgpublic s3 bucket as a parquet file.

#### Crypto

Using the coingecko api, crypto currency statistical data was pulled and stored in a pandas dataframe.  

After storing the data in the pandas df, the data was written to the MongoDB database used for tweets.

Data is continously ingested through this process until enough statistical data about various crypto currencies was stored.

Finally the crypto currency statistical data is pulled from the MongoDB database, stored in a pandas dataframe and written to the mkgpublic s3 bucket as a CSV.
*** Note ***
I stored the data as a CSV because two sets of data formats were requested.  I originally choose to store the crypto stats data as a json file, but even when partitioning the file into several JSON files, the files were too big for airflow to handle.  Thus, I went with the csv format.

### Crypto Stats and Tweets ELT

Now we get into the udacity capstone data ingestion and processing part of this project.

Ultimately, I choose to follow a similar process to what is in the mkg_airflow repository where I am using airflow to run a sequence of tasks.

#### Main Scripts

* dags/tweets_and_crypto_etl.py
* plugins/helpers/sql_queries.py
* plugins/operators/stage_redshift.py
* plugins/operators/load_dimension.py
* plugins/operators/load_fact.py
* plugins/helpers/analysis.py
* plugins/operators/data_quality.py
* plugins/operators/null_data_quality_check.py

#### Data Model

<blockquote class="imgur-embed-pub" lang="en" data-id="a/M82fKpe"  ><a href="//imgur.com/a/M82fKpe">Udacity Capstone Project Data Model</a></blockquote>

1. Data is loaded into the staging tables cg_coin_list_stg, snscrape_tweets_stg, and cg_hourly_stg on a Redshift Cluster from the S3 bucket
2. Date information is loaded into Date Dim
3. Data is loaded into the cg_coin_list table from cg_coin_list_stg
4. Data is loaded into coin_stats_hist using a join between date_dim, cg_hourly_stg, and cg_coin_list using date_keys and coin names as parameters to get foreign key allocation
5. Data is loaded into snscrape_tweets_hist using a join between date_dim, snscrape_tweets_stg and cg_coin_list using date_keys and coin names as parameters to get foreign key allocation

Ultimately, this data model was chosen as the end state will be combining crypto price action with tweet sentiment to determine how the market reacts to price action.  So, we need a relationship between the crypto and tweets datasets in order to one day achieve this future state result.


#### Steps

<blockquote class="imgur-embed-pub" lang="en" data-id="a/egP96PR"  ><a href="//imgur.com/a/egP96PR">Airflow Udacity Capstone Dag</a></blockquote>

1. Create Redshift Cluster
2. Create Crypto, Tweets, and Dim Schemas
3. Create Crypto/Tweets staging and Dim Tables
4. Staging
  * Stage Coingecko Token List Mapping Table
  * Stage Coingecko hourly crypto currency statistical table
  * Stage snscrape tweets crypto twitter table
5. Load Dimensions
  * Load Coingecko Token List Mapping Table
  * Load Date Dim with date information from Coingecko hourly crypto currency statistical staging table
  * Load Date Dim with date information from Stage snscrape tweets crypto twitter staging table
6. Run Primary Key Data Quality Check
  * Checks both Date Dimension and Coingecko Coinlist Dimension for primary key validility
7. Create Fact Tables
8. Load Fact Tables
  * Load crypto currency statistics history table
  * Load snscrape tweets history table
9. Run Foreign Key Data Quality Check
  * Checks both snscrape_tweets_hist and coin_stats_hist for foreign key validility
10. Run Aggregate Analysis Quality Checks
  * Select Statements that make sure data is actually present
  * Build an Aggregate table with min statistic and max statistic values per month from the coin_stats_hist table
11. Store resulting dim, fact and aggregate tables in S3
12. Delete Redshift Cluster

#### Data Quality Check Explanation
* In step 6, we do a primary key validility check to ensure both date_key in date_dim and coin_key in cg_coin_list are not null.
The reason why we do this primary key check is to ensure the primary keys were instantiated correctly, and no row in either table will
have any nulls.  If the table does have nulls then an exception is raised through the NullDataQuality operator.

* In step 9, we do a foreign key validility check to ensure that both date_key and coin_key in the tables snscrape_tweets_hist and coin_stats_hist are valid.  We would like both foreign keys to be accurate and non-null for accurate analysis to be done when joining together with the dimensional tables.  Furthermore, in future work we will be using these foreign keys to run additional analysis, so they must be non-null for said purposes.

* In step 10, we run select statements on each of the fact and dimensional tables to just do a quick analytical check of the tables.  Afterwards we build an aggregate table based on the min statistic and max static values per month from the coin_stats_hist table.  Eventually, these stats will be combined with a monthly tweet sentiment to determine crypto twitter's market sentiment based off monthly price action.

### Data Dictionary and Data Model Validility

#### Data Model Validility
Regarding data model validility this select statement will test all the relationships we have between our end-result tables:

``` sql
SELECT dt."date", coin_symbol, tweet, price, market_cap, total_volumes
FROM crypto.coin_stats_hist coin
JOIN tweets.snscrape_tweets_hist tweet
	ON coin.date_key = tweet.date_key
    AND coin.coin_key = tweet.coin_key
JOIN dim.date_dim dt
	ON dt.date_key = coin.date_key
    AND dt.date_key = tweet.date_key
LIMIT 5 ;
```

Results in:

```json
[
    {"date":"2017-01-22 00:00:00","coin_symbol":"eth","tweet":"#Bitcoin: $924.155 | €864.014 \n#Litecoin: $3.87 | ฿0.00424 \n#Ethereum: $10.8 | ฿0.0118 \n$BTC $LTC $ETH #ASX #NZSX #JPK","price":"10.74535","market_cap":"947279384.77","total_volumes":"5158451.37"},
    {"date":"2017-01-14 00:00:00","coin_symbol":"eth","tweet":"#Bitcoin: $828.085 | €783.523 \n#Litecoin: $3.88 | ฿0.00476 \n#Ethereum: $9.68 | ฿0.0117 \n$BTC $LTC $ETH #ASX #NZSX #JPK","price":"9.69526","market_cap":"852254946.17","total_volumes":"4879177.65"},
    {"date":"2017-01-14 00:00:00","coin_symbol":"ltc","tweet":"#Bitcoin: $828.085 | €783.523 \n#Litecoin: $3.88 | ฿0.00476 \n#Ethereum: $9.68 | ฿0.0117 \n$BTC $LTC $ETH #ASX #NZSX #JPK","price":"3.96449","market_cap":"195592082","total_volumes":"157639764.72"},
    {"date":"2017-01-14 00:00:00","coin_symbol":"ltc","tweet":"#Bitcoin: $828.085 | €783.523 \n#Litecoin: $3.88 | ฿0.00476 \n#Ethereum: $9.68 | ฿0.0117 \n$BTC $LTC $ETH #ASX #NZSX #JPK","price":"3.96449","market_cap":"195592082","total_volumes":"157639764.72"},
    {"date":"2017-01-29 00:00:00","coin_symbol":"eth","tweet":"#Bitcoin: $921.163 | €862.116 \n#Litecoin: $3.83 | ฿0.00419 \n#Ethereum: $10.5 | ฿0.0115 \n$BTC $LTC $ETH #ASX #NZSX #JPK","price":"10.43044","market_cap":"921810453.39","total_volumes":"3291646.73"}
]
```

#### Data Dictionary

Column Names and Definitions for Final Dimension and Fact Tables

##### Dimensions

crypto.cg_coin_list:
| Column Name | Description                                               |
|-------------|-----------------------------------------------------------|
| coin_key    | primary key generated through Redshifts identity function |
| coin_id     | crypto currency coin identifier used by coingecko         |
| symbol      | crypto currency symbol                                    |
| name        | crypto currency name                                      |

dim.date_dim:
| Column Name | Description                                               |
|-------------|-----------------------------------------------------------|
| date_key    | primary key generated through Redshifts identity function |
| date        | date snapshot for tweet data and crypto statistics data   |
| hour        | hour extracted from the date attribute                    |
| day         | day extracted from the date attribute                     |
| week        | week extracted from the date attribute                    |
| month       | month extracted from the date attribute                   |
| year        | year extracted from the date attribute                    |
| weekday     | weekday extracted from the date attribute                 |

##### Fact Tables

crypto.coin_stats_hist:
| Column Name   | Description                                                 |
|---------------|-------------------------------------------------------------|
| date_key      | foreign key that references date_key in dim.date_dim        |
| coin_key      | foreign key that references coin_key in crypto.cg_coin_list |
| date          | date snapshot for crypto statistics                         |
| coin_name     | crypto currency coin name for statistics extracted          |
| coin_symbol   | crypto currency coin symbol for statistics extracted        |
| price         | crypto currency's price during snapshot                     |
| market_cap    | crypto currency's market cap during snapshot                |
| total_volumes | crypto currency's total volumes during snapshot             |

tweets.snscrape_tweets_hist:
| Column Name | Description                                                            |
|-------------|------------------------------------------------------------------------|
| date_key    | foreign key that references date_key in dim.date_dim                   |
| coin_key    | foreign key that references coin_key in crypto.cg_coin_list            |
| date        | date snapshot for tweet data                                           |
| tweet_id    | twitter's tweet identifier for the tweet in this row                   |
| coin_name   | crypto currency coin name for statistics extracted                     |
| tweet       | the text of the tweet wrangled from twitter.com                        |
| username    | the username of the account that sent the tweet                        |
| search_term | search term being used in the snscrape_tweets twitter data aggregation |

### Future Work and Final Thoughts

#### Some questions for future work:
* What if the data was increased by 100x.
  * I would use a spark emr cluster to process the data as that would speed up both the data ingestion and the processing parts of the project.
  * This is likely going to happen in my future steps for this project, so ultimately this will be added in future versions.
* What if the pipelines would be run on a daily basis by 7 am every day.
  * I need a way to get the first part of this process easier.  The issue is sometimes either the coingecko or the snscrape tweets api breaks.
  Thus, if this pipeline would need to be run every day at 7am I would need to fix the initial data ingestion into my S3 bucket, as in, making the process more automated.  
  * Nonetheless, if we are just referring to the S3-->Redshift-->S3 part of the process, then I would set airflow to run the current elt process daily as the initial api --> MongoDB --> S3 part of the process would be taken care of.  
  * I would also need to add in an extra step so that the pipeline combines the data that is previously stored in the S3 bucket with the new data added.
* What if the database needed to be accessed by 100+ people.
  * If the database needs to be accessed by 100+ people than I would need to either:
    * constantly run a redshift cluster with the tables stored in said cluster (this requires additional IAM configuration and security protocols)
    * store the results in MongoDB so everyone can just pull from that database using pandas (requires adding everyones IP to the MongoDB Network)
    * have users simply pull from the mkgpublic S3 Bucket (just need the S3 URI) and using a platform like Databricks for users to run analysis

#### Future Work
Ultimately, I want to use these datasets as the backend to a dashboard hosted on a website.

I want to incoporate reddit data as well into the mix.  Afterwards, I want to run sentiment analysis on both the tweets and reddit thread datasets to determine the current crypto market sentiment.

Work will be done over the next few months on the above tasks.
    




