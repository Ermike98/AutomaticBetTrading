from mysql import connector

# class C:
#     def __init__(self, i):
#         self.i = i
#
#     def f(self):
#         print(self.i)
#         return self.i
#
# l = [C(i) for i in range(10)]
# futures = [client.submit(c.f) for c in l]
# results = [f.result() for f in futures]

username = 'root'
password = 'root'
host = 'localhost'

conn = connector.connect(
        host=host,
        user=username,
        password=password,
        database='bets'
    )

# cursor = conn.cursor(buffered=True, dictionary=True)
cursor = conn.cursor()

sql = """select idmatch
 from `match`;"""
cursor.execute(sql)
print(cursor.fetchone())
conn.close()

