import pandas as pd
import csv
import sqlite3
from pathlib import Path


def db_connect():
    dbPath = Path('database')
    try:
        print("\nStart:- Connecting to belongQETest DB....")
        conn = sqlite3.connect(dbPath / 'belongQE.db')
        print("\nSuccess:- Connecting to belongQETest DB....")
        return conn
    except Exception as err:
        print("\nError :- Connecting to belongQETest DB unsuccessful....")
        print(err)
        return None


def test_setup():
    try:
        generatedFilePath = Path('generatedTopPredictions')
        print("\nStart:- Reading daily csv file....")
        top10dailyDF = pd.read_csv(
            filepath_or_buffer=generatedFilePath/'daily/part-00000-8f32ad2c-58c7-42f5-909e-399d5842a9df-c000.csv',
            header=None)
        top10dailyDF.drop(top10dailyDF.columns[[7,8,9,10]],axis=1, inplace=True)
        top10dailyDF.columns = ['Year','Month','mDate','Loc_Name','Daily_Cnt','Longitude','Latitude']
        print(top10dailyDF)
        print("\nStart:- Reading monthly csv file....")
        top10monthlyDF = pd.read_csv(
            filepath_or_buffer=generatedFilePath/'monthly/part-00000-ea30b018-09c3-4c97-8d05-d14ee6cb468b-c000.csv',
            header=None)
        top10monthlyDF.drop(top10monthlyDF.columns[[5,6, 7, 8]], axis=1, inplace=True)
        top10monthlyDF.columns = ['Year','Month','Monthly_Cnt','Longitude','Latitude']
        print(top10monthlyDF)
    except Exception as err:
        print("\nError :- Reading generated csv files....")
        print(err)

    conn = db_connect()

    try:
        print("\nStart :- Writing generated daily data to table prod_top_ten_daily....")

        top10dailyDF.to_sql(
            name='prod_top_ten_daily',
            con=conn,
            if_exists='replace',
            index=False,
            dtype={
                'Year': 'real',
                'Month': 'text',
                'Date': 'real',
                'Loc_Name': 'text',
                'Daily_Cnt':'real',
                'Longitude':'real',
                'Latitude':'real'
            }
        )

        dailyrecs = conn.execute('Select * from prod_top_ten_daily')

        for recs in dailyrecs:
            print(recs)


        print("\nStart :- Writing generated monthly data to table prod_top_ten_monthly....")
        top10monthlyDF.to_sql(
            name='prod_top_ten_monthly',
            con=conn,
            if_exists='replace',
            index=False,
            dtype={
                'Year': 'real',
                'Month': 'text',
                'Monthly_Cnt': 'real',
                'Longitude': 'real',
                'Latitude': 'real'
            }
        )

        monthlyrecs = conn.execute('Select * from prod_top_ten_monthly')

        for recs in monthlyrecs:
            print(recs)

    except Exception as err:
        print("\nError :- Writing generated data into db tables....")
        print(err)

    try:
        print("\nStart:- Reading sensor count data from URL....")
        sourcedataDF = pd.read_json('https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json')
        sourcedataDF.drop(['id','day','time'],axis = 1, inplace = True)
        print(sourcedataDF.columns)

        print("\nStart:- Reading sensor location data from URL....")
        sourcedatalocDF = pd.read_json('https://data.melbourne.vic.gov.au/resource/h57g-5234.json')
        sourcedatalocDF.drop(['installation_date','location','direction_1','direction_2','note'],axis = 1, inplace = True)
        print(sourcedatalocDF.columns)


    except Exception as err:
        print("\nError :- Reading sensor data from URL's....")
        print(err)

    try:
        print("\nStart :- Writing source sensor count data to table prod_source_sensor_cnt....")
        sourcedataDF.to_sql(
            name='prod_source_sensor_cnt',
            con=conn,
            if_exists='replace',
            index=False,
            dtype={
                'Date_Time':'text',
                'Year': 'real',
                'Month': 'text',
                'Date': 'real',
                'Sensor_Id': 'int',
                'Loc_Name': 'text',
                'Daily_Cnt':'real'
            }
        )

        countrecs = conn.execute('Select * from prod_source_sensor_cnt')

        for recs in countrecs:
            print(recs)


        print("\nStart :- Writing source sensor location data to table prod_source_sensor_loc....")
        sourcedatalocDF.to_sql(
            name='prod_source_sensor_loc',
            con=conn,
            if_exists='replace',
            index=False,
            dtype={
                'Sensor_Id': 'int',
                'Sensor_Desc': 'text',
                'Sensor_Name': 'text',
                'Status': 'text',
                'Latitude': 'real',
                'Longitude': 'real'
            }
        )

        locrecs = conn.execute('Select * from prod_source_sensor_loc')

        for recs in locrecs:
            print(recs)

    except Exception as err:
        print("\nError :- Writing sensor source data into db tables....")
        print(err)












