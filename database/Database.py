import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

class Database:
    engine = db.create_engine('postgresql://postgres:postgres@localhost/stock_fundamental', echo=True)
    session = sessionmaker(bind=engine)

    def __init__(self):
        self.connection = self.engine.connect()
        print('DB instance created')


    def fetchQuery(self, query):
        fetchQuery = self.connection.execute(query)

        for data in fetchQuery.fetchall():
            print(data)


Base = declarative_base()

# db = Database()
# result = db.fetchQuery("SELECT * from valuation")
# print(result)