import pyodbc
import psycopg2

# read database configuration from file
with open('db_conf.txt') as f:
    config = dict(line.strip().split('=') for line in f)

# set up SQL Server database connection
server = config['sql_server']
database = config['sql_database']
username = config['sql_username']
password = config['sql_password']
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

# set up SQL query and execute
sql_query = 'SELECT Acc_azur FROM [ContentumTEST].[dbo].[IsklyucheniyaO365]'
cursor = cnxn.cursor()
#cursor.execute(sql_query)

# set up PostgreSQL database connection
pg_host = config['pg_host']
pg_database = config['pg_database']
pg_user = config['pg_user']
pg_password = config['pg_password']
pg_port = config['pg_port']
pg_conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password, port=pg_port)

# check row count in PostgreSQL table
pg_cursor2 = pg_conn.cursor()
pg_cursor2.execute('SELECT COUNT(*) FROM tbl_user_except')
pg_row_count = pg_cursor2.fetchone()[0]

# check row count in SQL Server table
cursor.execute('SELECT COUNT(*) FROM [ContentumTEST].[dbo].[IsklyucheniyaO365]')
sql_row_count = cursor.fetchone()[0]
#print(config)

# compare row counts and proceed accordingly
if pg_row_count == sql_row_count:
    print(f"Row counts are equal. {sql_row_count} rows fetched from SQL Server and {pg_row_count} rows fetched from PostgreSQL.")
else:
    print(f"Row counts are different. {sql_row_count} rows fetched from SQL Server and {pg_row_count} rows fetched from PostgreSQL. Copying data to PostgreSQL table.")

    # set up table and insert data
    table_name = 'tbl_user_except'
    insert_query000 = 'truncate table tbl_user_except'
    insert_query = f"INSERT INTO {table_name} (Acc_azur) VALUES (%s)"

    # insert data into PostgreSQL table
    pg_cursor1 = pg_conn.cursor()
    pg_cursor1.execute(insert_query000)
    
	
    
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    for row in rows:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(insert_query, row)
        pg_conn.commit()

# close database connections
cnxn.close()
pg_conn.close()
