from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import pandas as pd

app = Flask(__name__)

# Create engine using the `hawaii.sqlite` database file
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect an existing database into a new model
Base = automap_base()

# Reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Station = Base.classes.station
Measurement = Base.classes.measurement

@app.route('/')
def home():
    return '''
    <h1>Available Routes:</h1>
    <ul>
        <li>/api/v1.0/precipitation</li>
        <li>/api/v1.0/stations</li>
        <li>/api/v1.0/tobs</li>
        <li>/api/v1.0/<start></li>
        <li>/api/v1.0/<start>/<end></li>
    </ul>
    '''

@app.route('/api/v1.0/precipitation')
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]

    # Calculate the date one year from the last date in data set.
    prev_year_date = (pd.to_datetime(recent_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= prev_year_date).all()

    # Close Session
    session.close()

    # Convert the query results to a dictionary using date as the key and prcp as the value
    precipitation_data = {result.date: result.prcp for result in results}

    # Return the JSON representation of your dictionary
    return jsonify(precipitation_data)

@app.route('/api/v1.0/stations')
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query results
    results = session.query(Station.station).all()

    # Close Session
    session.close()

    # Return a JSON list of stations from the dataset
    stations = [result.station for result in results]
    return jsonify(stations)

@app.route('/api/v1.0/tobs')
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]

        # Calculate the date one year from the last date in data set.
    prev_year_date = (pd.to_datetime(recent_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')

    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    most_active_stations = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    most_active_station_id = most_active_stations[0][0]

    # Query the last 12 months of temperature observation data for this station
    results = session.query(Measurement.tobs).filter(Measurement.date >= prev_year_date).filter(Measurement.station == most_active_station_id).all()

    # Close Session
    session.close()

    # Return a JSON list of temperature observations for the previous year
    tobs_data = [result.tobs for result in results]
    return jsonify(tobs_data)

@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def start_end(start, end=None):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    if end:
        # For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
        temp_data = {
            'TMIN': results[0][0],
            'TAVG': results[0][1],
            'TMAX': results[0][2]
        }
        return jsonify(temp_data)
    else:
        # For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start).all()
        temp_data = {
            'TMIN': results[0][0],
            'TAVG': results[0][1],
            'TMAX': results[0][2]
        }
        return jsonify(temp_data)

if __name__ == '__main__':
    app.run(debug=True)


