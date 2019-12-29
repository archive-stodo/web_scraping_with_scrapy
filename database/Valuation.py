from sqlalchemy import Column, String, Integer, Numeric, BigInteger, Float, Table, create_engine
from sqlalchemy.orm import sessionmaker

from finviz.data_manipulations.valuation_data_cleaning import ValuationDataCleaner
from finviz.database.Database import Base, Database


class Valuation(Base):
    __tablename__ = 'valuation'

    id = Column(BigInteger, primary_key=True)
    ticker = Column(String)
    market_cap = Column(Float)
    p_to_s = Column(Float)
    p_to_b = Column(Float)
    p_to_c = Column(Float)
    price = Column(Float)

    def __init__(self):
        self.Session = sessionmaker(bind=Database.engine)

    def __repr__(self):
        return f"id: {self.id}, ticker: {self.ticker}, market_cap: {self.market_cap}, p_to_s: {self.p_to_s}, " \
               f"p_to_b: {self.p_to_b}, p_to_c: {self.p_to_c}, price: {self.price}"

    def get_all(self):
        session = self.Session()
        result = session.query(Valuation).all()
        [print(row) for row in result]

    def bulk_insert(self):
        cleaner = ValuationDataCleaner()
        df = cleaner.convert_df_to_db_ready_one()

        df.index.names = ['id']
        print(df)

        # beautifully easy!
        df.to_sql('valuation', con=Database.engine, if_exists='append', chunksize=1000)


valuation = Valuation()
# valuation.bulk_insert()
valuation.get_all()

