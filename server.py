#!/usr/bin/python

from flask import Flask, Response
from flask import request
from app.connector import PostgresConnector
from app.pgconfig import PG_CONNECTION_CREDENTIALS

app = Flask(__name__)
pgc = PostgresConnector(**PG_CONNECTION_CREDENTIALS)

@app.route("/")
def hello():
    return '''
        <html><body>
        For valentines data: <a href="/promotion/valentine">Click me.</a><br>
        For christmas data: <a href="/promotion/christmas">Click me.</a>
        </body></html>
        '''

@app.route("/promotion/valentine")
def get_valentines_data():
    pgc.connect()
    df = pgc.query_to_pandas("select customer_id, order_date from mrt.valentines_promotion")
    csv = df.to_csv()
    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=valentines.csv"})
    pgc.close()

@app.route("/promotion/christmas")
def get_christmas_data():
    pgc.connect()
    df = pgc.query_to_pandas("select customer_id, order_date from mrt.christmas_promotion")
    csv = df.to_csv()
    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=christmas.csv"})
    pgc.close()

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    
@app.get('/shutdown')
def shutdown():
    shutdown_server()
    return 'Server shutting down...'

app.run()