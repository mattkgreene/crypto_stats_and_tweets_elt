class SqlQueries:
    create_schemas = """
        CREATE SCHEMA IF NOT EXISTS tweets;
        CREATE SCHEMA IF NOT EXISTS crypto;
        CREATE SCHEMA IF NOT EXISTS dim;
    """
    
    create_stg_tables = """
        CREATE TABLE IF NOT EXISTS tweets.snscrape_tweets_stg (
        datetime timestamp NOT NULL,
        tweet_id BIGINT NOT NULL,
        text VARCHAR(10000) NOT NULL,
        username varchar(256),
        crypto varchar(256)
        );

        CREATE TABLE IF NOT EXISTS crypto.cg_hourly_stg (
            "date" timestamp NOT NULL,
            price numeric(18,5) NOT NULL,
            market_cap numeric(18,2) NOT NULL,
            total_volumes numeric(18,2) NOT NULL,
            coin VARCHAR(256) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS crypto.cg_coin_list_stg (
            coin_id varchar(256),
            symbol varchar(256) NOT NULL,
            name varchar(256) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS crypto.cg_coin_list (
            coin_key INTEGER identity(0,1),
            coin_id varchar(256),
            symbol varchar(256) NOT NULL,
            name varchar(256) NOT NULL,
            PRIMARY KEY (coin_key)
        );

        CREATE TABLE IF NOT EXISTS dim.date_dim (
            date_key INTEGER identity(0,1),
            "date" timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            PRIMARY KEY (date_key)
        ) ;
    """

    create_fact_tables = """

        DROP TABLE IF EXISTS crypto.coin_stats_hist;

        CREATE TABLE IF NOT EXISTS crypto.coin_stats_hist ( 
                    date_key INTEGER NOT NULL,
                    coin_key INTEGER NOT NULL,
                    "date" timestamp NOT NULL,
                    coin_name VARCHAR(256) NOT NULL,
                    coin_symbol VARCHAR(256) NOT NULL,
                    price numeric(18,5) NOT NULL,
                    market_cap numeric(18,2) NOT NULL,
                    total_volumes numeric(18,2) NOT NULL,
                    FOREIGN KEY(date_key) REFERENCES dim.date_dim (date_key),
                    FOREIGN KEY(coin_key) REFERENCES crypto.cg_coin_list (coin_key)
                );


        CREATE TABLE IF NOT EXISTS tweets.snscrape_tweets_hist (
            date_key INTEGER NOT NULL,
            coin_key INTEGER NOT NULL,
            "date" timestamp NOT NULL,
            tweet_id BIGINT NOT NULL,
            coin_name VARCHAR(256),
            tweet VARCHAR(10000) NOT NULL,
            username varchar(256),
            search_term varchar(256),
            FOREIGN KEY(date_key) REFERENCES dim.date_dim (date_key),
            FOREIGN KEY(coin_key) REFERENCES crypto.cg_coin_list (coin_key)
        );

    """

    cg_coin_list_insert = (
        """
        INSERT INTO crypto.cg_coin_list (
            coin_id,symbol, name
        )
        SELECT coin_id, symbol, name
        FROM crypto.cg_coin_list_stg
        WHERE coin_id IS NOT NULL;
        """
    )

    date_dim_tweets_insert = (
        """
        INSERT INTO dim.date_dim (
            "date",
            hour,
            day,
            week,
            "month",
            year,
            weekday
            )
        SELECT datetime as "date", extract(hour from "date") as hour, extract(day from "date") as day, extract(week from "date") as week, 
               extract(month from "date") as month, extract(year from "date") as year, extract(dayofweek from "date") as dayofweek
        FROM tweets.snscrape_tweets_stg
        """
        )

    date_dim_crypto_insert = (
        """
        INSERT INTO dim.date_dim (
            "date",
            hour,
            day,
            week,
            "month",
            year,
            weekday
            )
        SELECT "date", extract(hour from "date") as hour, extract(day from "date") as day, extract(week from "date") as week, 
               extract(month from "date") as month, extract(year from "date") as year, extract(dayofweek from "date") as dayofweek
        FROM crypto.cg_hourly_stg
        """
        )

    coin_stats_hist_insert = (
        """
        INSERT INTO crypto.coin_stats_hist (
            date_key,
            coin_key,
            "date",
            coin_name,
  			coin_symbol,
            price,
            market_cap,
            total_volumes
        )
        
        SELECT date_dim.date_key as date_key, cg_coin_list.coin_key as coin_key,
                        cg_hourly_stg.date, cg_coin_list.name as coin_name, cg_coin_list.symbol as coin_symbol,
                        cg_hourly_stg.price,
                        cg_hourly_stg.market_cap, cg_hourly_stg.total_volumes
        FROM crypto.cg_hourly_stg
        JOIN dim.date_dim
            ON date_dim.date = cg_hourly_stg.date
        JOIN crypto.cg_coin_list
            ON (UPPER(cg_hourly_stg.coin) = UPPER(cg_coin_list.coin_id)
            OR UPPER(cg_hourly_stg.coin) = UPPER(cg_coin_list.name))
            
        WHERE cg_coin_list.name IS NOT NULL;
        """
    )

    snscrape_tweets_hist_insert = (
        """
        INSERT INTO tweets.snscrape_tweets_hist (
            date_key,
            coin_key,
            "date",
            tweet_id,
            coin_name,
            tweet,
            username,
            search_term
        )
        SELECT date_dim.date_key as date_key, cg_coin_list.coin_key as coin_key,
        snscrape_tweets_stg.datetime as "date", snscrape_tweets_stg.tweet_id as tweet_id, 
        cg_coin_list.name as coin_name, snscrape_tweets_stg.text as tweet,
        snscrape_tweets_stg.username, snscrape_tweets_stg.crypto as search_term
        FROM tweets.snscrape_tweets_stg
        JOIN dim.date_dim
            ON date_dim.date = snscrape_tweets_stg.datetime
        JOIN crypto.cg_coin_list
            ON snscrape_tweets_stg.crypto = cg_coin_list.name
            OR snscrape_tweets_stg.crypto = cg_coin_list.symbol
        WHERE snscrape_tweets_stg.tweet_id IS NOT NULL;
        """
    )

