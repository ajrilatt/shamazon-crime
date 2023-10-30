import psycopg2 as p

#Forms connection and sets up cursor to access SHAMAZON
conn = p.connect(database = 'shamazon',
                 host = 'localhost',
                 user = input('Username: '),
                 password = input('Password: '),
                 port = '5432')
cursor = conn.cursor()

#Returns all packages
cursor.execute("SELECT * FROM schmackages")
print(cursor.fetchone())
