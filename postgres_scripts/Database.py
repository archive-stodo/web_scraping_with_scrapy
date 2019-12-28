import sqlalchemy as db

class Database:
    engine = db.create_engine('postgresql://postgres:postgres@localhost/stock_fundamental')

    def __init__(self):
        self.connection = self.engine.connect()
        print('DB instance created')


    def fetchQuery(self, query):
        fetchQuery = self.connection.execute(query)

        for data in fetchQuery.fetchall():
            print(data)

db = Database()
result = db.fetchQuery("SELECT * from valuation")

print(result)