import os
import ibm_db
from dotenv import load_dotenv
import ibm_db_dbi
import pandas as pd

def create_and_insert_table(cursor, df, table_name):
    # Check if the table exists and drop it if it does
    check_table_sql = f"SELECT TABNAME FROM SYSCAT.TABLES WHERE TABNAME='{table_name.upper()}'"
    cursor.execute(check_table_sql)
    if cursor.fetchone():
        drop_table_sql = f"DROP TABLE {table_name}"
        cursor.execute(drop_table_sql)
        print(f"Table {table_name} dropped successfully.")

    # Create the CREATE TABLE SQL query
    columns = ', '.join([f'"{col}" VARCHAR(128)' for col in df.columns])
    create_table_sql = f'CREATE TABLE {table_name} ({columns})'

    # Execute the table creation statement
    cursor.execute(create_table_sql)
    print(f"Table {table_name} created successfully.")

    # Insert data from DataFrame into the table
    insert_sql = f"INSERT INTO {table_name} ({', '.join([f'\"{col}\"' for col in df.columns])}) VALUES ({', '.join(['?' for _ in df.columns])})"
    for index, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
    cursor.connection.commit()

    print("Data inserted successfully.")


if __name__ == '__main__':
    # Load environment variables
    load_dotenv()

    dsn_hostname = os.getenv('DB_HOST')
    dsn_uid = os.getenv('DB_UID') 
    dsn_pwd = os.getenv('DB_PWD') 
    dsn_port = os.getenv('DB_PORT') 
    dsn_database = "bludb" 
    dsn_driver = "{IBM DB2 ODBC DRIVER}"
    dsn_protocol = "TCPIP"
    dsn_security = "SSL"

    dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security
    )


    # Connect to the database
    conn = ibm_db.connect(dsn, '', '')

    # Create a cursor
    cursor = ibm_db_dbi.Connection(conn).cursor()

    # Load the data
    df = pd.read_csv('./data/Danh_Sach_Sinh_Vien.csv')

    # Create and insert the table
    create_and_insert_table(cursor, df, 'STUDENTS')

    # Close the cursor and the connection
    cursor.close()