import pandas as pd
import numpy as np


class FinancialDataCleaner:

    def __init__(self,  filename='financial.txt', location='../'):
        self.df = self.load_scraped_file()

    def load_scraped_file(self, filename='financial.txt', location='../'):
        header_names = ['id', 'ticker', "dividend", "roa", "roe", "roi", "currR", 'quickR',
                        'ltDebt_to_eq', 'debt_to_eq', 'grossM', 'operM', 'profitM', 'volume']
        self.df = pd.read_csv(
            location + filename,
            names=header_names,
            index_col='id',
            na_values=["-"]
        )

        self.df.drop("volume", axis=1, inplace=True)

        return self.df

    def convert_percentages_to_numbers(self):
        def convert(percentage):
            str_perc = str(percentage)

            if str_perc == np.nan:
                return np.nan
            elif str_perc != np.nan and str_perc.endswith('%'):
                return round(float(str_perc[:-1]) / 100, 3)
            else:
                return percentage

        self.df = self.df.applymap(convert)

        return self.df

    def convert_df_to_db_ready_one(self):
        self.load_scraped_file()
        self.convert_percentages_to_numbers()

        return self.df


cleaner = FinancialDataCleaner()
df = cleaner.convert_df_to_db_ready_one()
# print(df.dtypes)
print(df.loc[:, 'roi': 'ltDebt_to_eq'])
df.to_csv('financial_prep.csv')