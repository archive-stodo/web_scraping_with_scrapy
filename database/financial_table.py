from sqlalchemy import Column, String, Integer, Numeric, BigInteger, Float, Table, create_engine
from sqlalchemy.orm import sessionmaker

from finviz.data_manipulations.financial_data_cleaning import FinancialDataCleaner
from finviz.data_manipulations.valuation_data_cleaning import ValuationDataCleaner
from finviz.database.Database import Base, Database


class Financial(Base):
    __tablename__ = 'financial'

    id = Column(BigInteger, primary_key=True)
    ticker = Column(String)
    dividend = Column(Float)
    market_cap = Column(Float)
    roa = Column(Float)
    roe = Column(Float)
    roi = Column(Float)
    currR = Column(Float)
    quickR = Column(Float)
    ltDebt_to_eq = Column(Float)
    debt_to_eq = Column(Float)
    grossM = Column(Float)
    operM = Column(Float)
    profitM = Column(Float)


    def __init__(self):
        self.Session = sessionmaker(bind=Database.engine)


    def __repr__(self):
        return f"id: {self.id}, ticker: {self.ticker}"

    def bulk_insert(self):
        cleaner = FinancialDataCleaner()
        df = cleaner.convert_df_to_db_ready_one()

        print(df)

        df.to_sql('financial', con=Database.engine, if_exists='append', chunksize=1000)


financial = Financial()
financial.bulk_insert()