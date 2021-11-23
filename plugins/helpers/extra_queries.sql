SELECT * FROM stl_load_errors
ORDER BY starttime DESC;

SELECT datetime as "date", extract(hour from "date") as hour, extract(day from "date") as day, extract(week from "date") as week, 
               extract(month from "date") as month, extract(year from "date") as year, extract(dayofweek from "date") as dayofweek
        FROM tweets.snscrape_tweets_stg

SELECT "date", extract(hour from "date") as hour, extract(day from "date") as day, extract(week from "date") as date, 
               extract(month from "date") as month, extract(year from "date") as year, extract(dayofweek from "date") as dayofweek
        FROM crypto.cg_hourly_stg


DROP TABLE IF EXISTS crypto.coin_stats_hist;

CREATE TABLE IF NOT EXISTS crypto.coin_stats_hist (
            date_key INTEGER NOT NULL,
            coin_key INTEGER NOT NULL,
            "date" timestamp NOT NULL,
            coin_name VARCHAR(256) NOT NULL,
            price numeric(18,5) NOT NULL,
            market_cap numeric(18,2) NOT NULL,
            total_volumes numeric(18,2) NOT NULL,
            FOREIGN KEY(date_key) REFERENCES dim.date_dim (date_key),
            FOREIGN KEY(coin_key) REFERENCES crypto.cg_coin_list (coin_key)
        );

  INSERT INTO crypto.coin_stats_hist (
            date_key,
            coin_key,
            "date",
            coin_name,
            price,
            market_cap,
            total_volumes
        )
        
SELECT date_dim.date_key as date_key, cg_coin_list.coin_key as coin_key,
        cg_hourly_stg.date, cg_coin_list.name as coin_name, cg_hourly_stg.price,
        cg_hourly_stg.market_cap, cg_hourly_stg.total_volumes
        FROM crypto.cg_hourly_stg
        JOIN dim.date_dim
            ON date_dim.date = cg_hourly_stg.date
        JOIN crypto.cg_coin_list
            ON cg_hourly_stg.coin = cg_coin_list.coin_id
        WHERE cg_coin_list.name IS NOT NULL;

DROP TABLE IF EXISTS crypto.usd_price_per_month_stats;

CREATE TABLE IF NOT EXISTS crypto.usd_price_per_month_stats (
  coin_name VARCHAR(256), month VARCHAR(10),
  year VARCHAR(10), max_usd_price NUMERIC(18,5), min_usd_price NUMERIC(18,5)
  );
  
INSERT INTO crypto.usd_price_per_month_stats (
  coin_name, "month", "year", max_usd_price, min_usd_price
  )

SELECT hist.coin_name, dt.month, dt.year, MAX(price) as max_usd_price, MIN(price) as min_usd_price
FROM crypto.coin_stats_hist hist
JOIN dim.date_dim dt
	ON dt.date_key = hist.date_key
GROUP BY coin_name, year, month
ORDER BY coin_name, year, month ASC
;

INSERT INTO crypto.avg_price_per_month (
  coin_name, "month", "year", avg_usd_price
  )
SELECT hist.coin_name, dt.month, dt.year, AVG(price) as avg_usd_price
FROM crypto.coin_stats_hist hist
JOIN dim.date_dim dt
	ON dt.date_key = hist.date_key
GROUP BY coin_name, year, month
ORDER BY coin_name, year, month ASC
;