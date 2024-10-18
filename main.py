from flask import Flask, render_template, request, redirect, url_for, flash
import ibm_db
import os
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Set your IBM Db2 credentials in the environment variables or manually assign them in the code below.
dsn_hostname = os.getenv('DB_HOST')
dsn_uid = os.getenv('DB_UID') 
dsn_pwd = os.getenv('DB_PWD') 
dsn_port = os.getenv('DB_PORT') 
dsn_database = "bludb" 
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_protocol = "TCPIP"
dsn_security = "SSL"

# Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

conn = ibm_db.connect(dsn, "", "")

@app.route('/')
def index():
    try:
        sql = "SELECT * FROM STUDENTS"
        stmt = ibm_db.exec_immediate(conn, sql)
        rows = []
        row = ibm_db.fetch_assoc(stmt)
        while row:
            rows.append(row)
            row = ibm_db.fetch_assoc(stmt)
        return render_template('index.html', rows=rows)
    except Exception as e:
        flash(str(e))
        return render_template('index.html', rows=[])

@app.route('/update', methods=['POST'])
def update():
    try:
        id = request.form['id']
        column = request.form['column']
        new_value = request.form['new_value']
        sql = f"UPDATE STUDENTS SET {column} = '{new_value}' WHERE ID = '{id}'"
        ibm_db.exec_immediate(conn, sql)
        return redirect(url_for('index'))
    except Exception as e:
        flash(str(e))
        return redirect(url_for('index'))

@app.route('/delete', methods=['POST'])
def delete():
    try:
        id = request.form['id']
        sql = f"DELETE FROM STUDENTS WHERE ID = {id}"
        ibm_db.exec_immediate(conn, sql)
        return redirect(url_for('index'))
    except Exception as e:
        flash(str(e))
        return redirect(url_for('index'))

@app.route('/query', methods=['POST'])
def query():
    try:
        query = request.form['query']
        stmt = ibm_db.exec_immediate(conn, query)
        rows = []
        row = ibm_db.fetch_assoc(stmt)
        while row:
            rows.append(row)
            row = ibm_db.fetch_assoc(stmt)
        return render_template('index.html', rows=rows)
    except Exception as e:
        flash(str(e))
        return render_template('index.html', rows=[])
    
@app.route('/insert', methods=['POST'])
def insert():
    form_items = request.form.items()

    columns = []
    values_list = []

    for item in form_items:
        columns.append(item[0])
        values_list.append(f"'{item[1]}'")

    sql = f"INSERT INTO STUDENTS ({', '.join([column for column in columns])}) VALUES ({', '.join(values_list)})"
    ibm_db.exec_immediate(conn, sql)
    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(debug=True)
