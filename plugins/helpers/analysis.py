class analysis_queries:
    songplays_check = """
    SELECT * FROM public.songplays WHERE playid IS NULL;
    """

    users_check = """
    SELECT * FROM public.users WHERE userid IS NULL;
    """

    songs_check = """
    SELECT * FROM public.songs WHERE songid IS NULL;
    """

    artists_check = """
    SELECT * FROM public.artists WHERE artistid IS NULL;
    """

    cg_coin_list_check = """
        SELECT * FROM crypto.cg_coin_list
        LIMIT 10;
    """

    coin_stats_hist_check = """
        SELECT * FROM crypto.coin_stats_hist
        LIMIT 10;
    """

    snscrape_tweets_hist_check = """
        SELECT * FROM tweets.snscrape_tweets_hist
        LIMIT 10;
    """

    max_and_min_price_per_month_usd = """
    DROP TABLE IF EXISTS crypto.usd_price_per_month_stats;

    CREATE TABLE IF NOT EXISTS crypto.usd_price_per_month_stats (
    coin_name VARCHAR(256), coin_symbol VARCHAR(20), month VARCHAR(10),
    year VARCHAR(10), max_usd_price NUMERIC(18,5), min_usd_price NUMERIC(18,5),
    max_mcap NUMERIC(18,5), min_mcap NUMERIC(18,5),
    max_total_vol NUMERIC(18,5), min_total_vol NUMERIC(18,5)
    );
    
    INSERT INTO crypto.usd_price_per_month_stats (
    coin_name, coin_symbol, "month", "year", max_usd_price, min_usd_price, max_mcap, min_mcap, max_total_vol, min_total_vol
    )

    SELECT hist.coin_name, hist.coin_symbol, dt.month, dt.year, MAX(price) as max_usd_price, MIN(price) as min_usd_price,
    MAX(market_cap) as max_mcap, MIN(market_cap) as min_mcap, MAX(total_volumes) as max_total_vol, MIN(total_volumes) as min_total_vol
    FROM crypto.coin_stats_hist hist
    JOIN dim.date_dim dt
        ON dt.date_key = hist.date_key
    GROUP BY coin_name, coin_symbol, year, month
    ORDER BY coin_name, coin_symbol, CAST(year AS INT), CAST(month AS INT)
    ;
    """