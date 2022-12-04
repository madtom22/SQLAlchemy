import csv
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, select, Float
from datetime import date


engine = create_engine('sqlite:///database.db', echo=True)

meta = MetaData()


def load_items_from_csv(csvfile):
    datas = []
    with open(csvfile, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            datas.append(row)
    return datas


stations_datas = load_items_from_csv('clean_stations.csv')
measure_datas = load_items_from_csv('clean_measure.csv')

print(measure_datas)


stations = Table(
            'stations', meta,
            Column('station', String, primary_key=True),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('elevation', Float),
            Column('name', String),
            Column('country', String),
            Column('state', String)
            )

measure = Table(
            'measure', meta,
            Column('station', String),
            Column('date', Integer),
            Column('precip', Float),
            Column('tobs', Integer),
            )

stations_datas_to_insert = stations.insert().values(stations_datas)
measure_datas_to_insert = measure.insert().values(measure_datas)

conn = engine.connect()

conn.execute(measure_datas_to_insert)
