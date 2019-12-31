import pandas as pd
import numpy as np


class OverviewDataCleaner:

    def __init__(self,  filename='overview.txt', location='../'):
        self.df = self.load_scraped_file()

    def load_scraped_file(self, filename='overview.txt', location='../'):
        header_names = ['id', 'ticker', "sector", "p_to_e"]
        df = pd.read_csv(
            location + filename,
            names=header_names,
            index_col="id",
            na_values=["-"]
        )

        return df



    def convert_df_to_db_ready_one(self):
        self.load_scraped_file()

        return self.df


cleaner = OverviewDataCleaner()
df = cleaner.convert_df_to_db_ready_one()

print(df)

df.to_csv('overview_prep.csv')
