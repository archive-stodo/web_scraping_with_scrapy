import pandas as pd
import numpy as np


class ValuationDataCleaner:

    def __init__(self,  filename='valuation.txt', location='../'):
        self.df = self.load_scraped_file()

    def load_scraped_file(self, filename='valuation.txt', location='../'):
        header_names = ['id', 'ticker', "market_cap", "p_to_s", "p_to_b", "p_to_c", "price"]
        df = pd.read_csv(
            location + filename,
            names=header_names,
            index_col="id"
            # na_values="-" - nice option, but let's do it in other way
        )

        return df

    def insert_nan_when_data_missing(self):
        self.df[self.df == '-'] = np.nan
        # or
        # self.df.replace('-', np.nan, inplace=True)

        return self.df

    def convert_market_cap_col_to_100k_ints(self):
        self.df['market_cap'] = self.df['market_cap'].apply(self.convert_market_cap_col)

    def convert_market_cap_col(self, market_cap):
        if market_cap == np.nan:
            pass
        elif str(market_cap).endswith("M"):
            string_value = market_cap[:-1]
            return int(float(string_value))
        elif str(market_cap).endswith("B"):
            string_value = market_cap[:-1]
            return int(float(string_value) * 1000)

    def drop_ticker(self):
        self.df.drop("ticker", axis=1, inplace=True)

    def to_numeric_where_needed(self):
        self.df['p_to_s'] = self.df['p_to_s'].astype(float)
        self.df['p_to_b'] = self.df['p_to_b'].astype(float)
        self.df['p_to_c'] = self.df['p_to_c'].astype(float)
        self.df['price'] = self.df['price'].astype(float)

    def convert_df_to_db_ready_one(self):
        self.load_scraped_file()
        self.insert_nan_when_data_missing()
        self.convert_market_cap_col_to_100k_ints()
        self.to_numeric_where_needed()

        return self.df


cleaner = ValuationDataCleaner()
cleaner.convert_df_to_db_ready_one()
cleaner.drop_ticker()
