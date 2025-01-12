from flask import Flask, jsonify, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd


app = Flask(__name__) 

@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

def get_all_stations(conn):
    query = "SELECT * FROM stations"
    return pd.read_sql_query(query, conn)

@app.route('/stations/<station_id>')
def route_station_by_id(station_id):
    conn = make_connection()
    station = get_station_by_id(station_id, conn)
    return station.to_json()


@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

def insert_into_stations(data, conn):
    cursor = conn.cursor()
    cursor.execute(QUERY_INSERT_STATION, data)
    conn.commit()
    return {'message': 'Station added successfully'}

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE trip_id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result
    
def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/json', methods = ['POST'])
def json_example():
    request_data = request.get_json(force=True) # parse the incoming JSON data as Dictionary
    name = request_data['name']
    age = request_data['age']
    address = request_data['address']
    return (f"""Hello {name}, your age is {age} and your address in {address}""")

################### FUNCTIONS ####################


DB_PATH = 'austin_bikeshare.db'
QUERY_INSERT_TRIP = '''
    INSERT INTO trips (
    id, 
    subscriber_type, 
    bikeid, 
    start_time, 
    start_station_id,
    start_station_name, 
    end_station_id, 
    end_station_name, 
    duration_minutes
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

QUERY_INSERT_STATION = '''
    INSERT INTO stations (
        station_id, 
        name, 
        status, 
        address,
        alternate_name,
        city_asset_number,
        property_type, 
        number_of_docks,
        power_type, 
        footprint_length, 
        footprint_width, 
        notes,
        council_district, 
        modified_date
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db');
    return connection

# Routes
@app.route('/add_trip', methods=['POST'])
def add_trip_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'})

    file = request.files['file']
    trips = pd.read_csv(file)
    trips = trips.where(pd.notnull(trips), None)

    conn = make_connection()
    cursor = conn.cursor()

    for _, row in tqdm(trips.iterrows(), total=trips.shape[0], desc="Adding Trips"):
        data = tuple(row.fillna('').values)
        cursor.execute(QUERY_INSERT_TRIP, data)

    conn.commit()
    conn.close()

    return {'message': 'Trips added successfully'}


@app.route('/add_station', methods=['POST'])
def add_station_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    stations = pd.read_csv(file)
    stations = stations.where(pd.notnull(stations), None) 

    conn = make_connection()
    cursor = conn.cursor()

    for _, row in tqdm(stations.iterrows(), total=stations.shape[0], desc="Adding Stations"):
        data = tuple(row.fillna('').values)

        cursor.execute(QUERY_INSERT_STATION, data)

    conn.commit()
    conn.close()

    return jsonify({'message': 'Stations added successfully'})
    

# Scoring 2
@app.route('/stations/<station_id>', methods=['GET'])
def get_station_by_id(station_id):
    conn = make_connection()
    query_station_id = f"""
        SELECT * FROM stations
        WHERE station_id = {station_id}
        """
    result = pd.read_sql_query(query_station_id, conn)
    conn.close()
    return result.to_json()

# Scoring 3
@app.route('/stations/add', methods=['POST'])
def add_station():
    data = pd.Series(request.get_json())
    data = tuple(data.fillna('').values)

    conn = make_connection()
    cursor = conn.cursor()
    
    cursor.execute(QUERY_INSERT_STATION, data)
    conn.commit()
    conn.close()

    return {'message': 'Station added successfully'}

@app.route('/trips/add', methods=['POST'])
def add_trip():
    data = pd.Series(request.get_json())
    data = tuple(data.fillna('').values)

    conn = make_connection()
    cursor = conn.cursor()
    
    cursor.execute(QUERY_INSERT_TRIP, data)
    conn.commit()
    conn.close()

    return {'message': 'Trip added successfully'}

# Scoring 4
@app.route('/trips/average_trip_duration', methods=['GET'])
def average_trip_duration_v2():
    conn = make_connection()
    cursor = conn.cursor()

    query = '''
        SELECT AVG(duration_minutes) AS avg_duration
        FROM trips
    '''

    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    return {
        'alltime_average': result[0][0]
    }

# Scoring 5
@app.route('/trips/average/<year>/<month>', methods=['GET'])
def daily_average_v2(year, month):
    conn = make_connection()
    cursor = conn.cursor()

    query = f'''
        SELECT substr(start_time, 1, 10) AS trip_date, 
                AVG(duration_minutes) AS avg_duration
        FROM trips
        WHERE start_time LIKE '{year}-{month}-%'
        GROUP BY trip_date
        ORDER BY trip_date
    '''

    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    daily_averages = [
        {'date': row[0], 'average_duration': row[1]} for row in result
    ]

    return {
        'year': year,
        'month': month,
        'daily_averages': daily_averages
    }

# Scoring 6
@app.route('/profile_v2', methods=['POST'])
def build_profile_v2():
    data = request.get_json()

    name = data.get('name')
    bikeid = data.get('bikeid')

    conn = make_connection()
    cursor = conn.cursor()

    query = f"""
        SELECT AVG(duration_minutes) AS avg_duration
        FROM trips
        WHERE bikeid = '{bikeid}'
    """

    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    return {
        'name': name,
        'bikeid': bikeid,
        'alltime_average': result[0][0]
    }

if __name__ == '__main__':
    app.run(debug=True, port=5000)