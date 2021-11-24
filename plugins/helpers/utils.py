from pycoingecko import CoinGeckoAPI
import pandas as pd
import pymongo
from pymongo import MongoClient

class dt_utils:
    def ts_to_date(self, df, stat_val):
        t = df
        t = t.rename(columns={0:'ts', 1: stat_val})
        t['ts'] = pd.to_datetime(t['ts'], unit='ms')
        date_dim = {}
        for i in t.index.tolist():
                try: date_dim[i] = {'date': t['ts'][i].isoformat(),
                                    stat_val: t[stat_val][i]
                                }
                except: print("Error at time extraction index: " + i)
        return date_dim

class coin_gecko:
    def get_cg_data(self, coin, currency, by_ts, days=None, from_ts=None,to_ts=None):
        cg = CoinGeckoAPI()
        if by_ts:
            coin_chart = cg.get_coin_market_chart_range_by_id(coin, 'usd', from_ts, to_ts)
        else:
            coin_chart = cg.get_coin_market_chart_by_id(id=coin, vs_currency=currency, days=days)
        
        
        chart_df_mkt_cap = pd.DataFrame.from_dict(coin_chart['market_caps'])
        chart_df_prices = pd.DataFrame.from_dict(coin_chart['prices'])
        chart_df_total_volume = pd.DataFrame.from_dict(coin_chart['total_volumes'])
        
        chart_mkt_cap_date = dt_utils.ts_to_date(chart_df_mkt_cap,'market_cap')
        chart_price_date = dt_utils.ts_to_date(chart_df_prices,'price')
        chart_total_volume_date = dt_utils.ts_to_date(chart_df_total_volume,'total_volumes')
        
        chart_price_date_df = pd.DataFrame.from_dict(chart_price_date, orient='index')
        chart_mkt_cap_date_df = pd.DataFrame.from_dict(chart_mkt_cap_date, orient='index')
        chart_total_volume_date_df = pd.DataFrame.from_dict(chart_total_volume_date, orient='index')
        
        mkt_cap_and_price = chart_price_date_df.merge(chart_mkt_cap_date_df,on='date')\
                            .merge(chart_total_volume_date_df, on='date')
        mkt_cap_and_price['coin'] = coin
        return mkt_cap_and_price


class mongo:
    def _connect_mongo(self, host, username, password, db):
        """ A util for making a connection to mongo """

        if username and password:
            mongo_uri = 'mongodb+srv://%s:%s@%s/%s?retryWrites=true&w=majority' % (username, password, host, db)
            print(mongo_uri)
            conn = MongoClient(mongo_uri)
        else:
            conn = MongoClient(host, 27070)


        return conn[db]


    def write_mongo(self, db, collection, df, host='localhost', username=None, password=None, no_id=True):
        db = self._connect_mongo(host=host, username=username, password=password, db=db)
        
        cg_collection = db[collection]
        
        cg_collection.insert_many(df.to_dict('records'))


    def read_mongo(self, db, collection, query={}, host='localhost', username=None, password=None, no_id=True):
        """ Read from Mongo and Store into DataFrame """

        # Connect to MongoDB
        db = self._connect_mongo(host=host, username=username, password=password, db=db)

        # Make a query to the specific DB and Collection
        cursor = db[collection].find(query)

        # Expand the cursor and construct the DataFrame
        df =  pd.DataFrame(list(cursor))

        # Delete the _id
        if no_id:
            del df['_id']

        return df

class build_dfs:
    def build_df(coin_list, currency, days):
        ret = []
        for coin in coin_list:
            ret.append(coin_gecko.get_cg_data(coin, currency, False, days=days))
        full_chart = pd.concat(ret,ignore_index=True)
        return full_chart

    def build_ts_df(coin_list, currency, from_ts, to_ts):
        ret = []
        from_ts = pd.to_datetime([from_ts]).view(int)[0] / 10**9
        print(from_ts)
        to_ts = pd.to_datetime([to_ts]).view(int)[0] / 10**9
        print(to_ts)
        for coin in coin_list:
            ret.append(coin_gecko.get_cg_data(coin, currency, True, from_ts=from_ts, to_ts=to_ts))
        full_chart = pd.concat(ret,ignore_index=True)
        return full_chart