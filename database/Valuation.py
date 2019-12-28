from sqlalchemy import Column, String, Integer, Numeric
from sqlalchemy.orm import sessionmaker
from finviz.database.Database import Base, Database


class Valuation(Base):
    __tablename__ = 'valuation'

    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    market_cap = Column(Integer)
    p_to_s = Column(Numeric)
    p_to_b = Column(Numeric)
    p_to_c = Column(Numeric)
    price = Column(Numeric)

    def __init__(self):
        self.Session = sessionmaker(bind=Database.engine)

    def __repr__(self):
        return f"id: {self.id}, ticker: {self.ticker}, market_cap: {self.market_cap}, p_to_s: {self.p_to_s}, " \
               f"p_to_b: {self.p_to_b}, p_to_c: {self.p_to_c}, price: {self.price}"

    def get_all(self):
        session = self.Session()
        result = session.query(Valuation).all()

        for row in result:
           print (result)


valuation = Valuation()
valuation.get_all()


