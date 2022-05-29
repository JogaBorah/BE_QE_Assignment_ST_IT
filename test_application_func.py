import pandas as pd
import csv
import sqlite3
from pathlib import Path
import test_dbConnection as db
import pytest_check as validate

def test_only_10_recs_gen_for_day():
    conn = db.db_connect()
    query = "Select count(*) as rec_cnt from prod_top_ten_daily"
    dailyCntDF = pd.read_sql(query,conn)
    dailyCnt = dailyCntDF.iloc[0]['rec_cnt']
    validate.equal(dailyCnt,10,'The daily prediction is not for Top 10, it is more/less than 10')
    print("Successfully validated that generated daily top 10 is of 10 records only")


def test_only_10_recs_gen_for_month():
    conn = db.db_connect()
    query = "Select count(*) as rec_cnt from prod_top_ten_monthly"
    dailyCntDF = pd.read_sql(query,conn)
    dailyCnt = dailyCntDF.iloc[0]['rec_cnt']
    validate.equal(dailyCnt,10,'The monthly prediction is not for Top 10, it is more/less than 10')
    print("Successfully validated that generated monthly top 10 is of 10 records only")


def test_top_10_ped_by_day_gen():
    conn = db.db_connect()
    actualQuery = "Select Loc_Name,Year,Month,mDate,Daily_Cnt,Longitude,Latitude from prod_top_ten_daily " \
                  "order by Daily_Cnt desc limit 10"
    testQuery = "Select Loc_Name as Loc_Name,Year as Year,Month as Month,mDate as mDate,Daily_Cnt as Daily_Cnt," \
                "Longitude as Longitude,Latitude as Latitude from (Select *, " \
                "row_number() over(order by daily_cnt desc) daily_rnk from (" \
                "Select cnt.sensor_id,cnt.sensor_name as loc_name,cnt.year,cnt.month,cnt.mdate," \
                "loc.longitude, loc.latitude,sum(hourly_counts) as daily_cnt " \
                "from prod_source_sensor_cnt cnt join prod_source_sensor_loc loc on cnt.sensor_id = loc.sensor_id " \
                "group by cnt.sensor_id,cnt.sensor_name,cnt.year,cnt.month,cnt.mdate,loc.longitude, " \
                "loc.latitude order by sum(hourly_counts) desc)) where daily_rnk <= 10"
    dailyActualDF = pd.read_sql(actualQuery,conn)
    print(dailyActualDF.columns)
    dailyTestDF = pd.read_sql(testQuery, conn)
    print(dailyTestDF.columns)
    compareDF = dailyActualDF.merge(dailyTestDF, how='outer',
                                    indicator=True).loc[lambda x: x['_merge'] != 'both']
    dailyCnt = len(compareDF.index)
    validate.equal(dailyCnt,0,'The daily prediction data is not correct for Top 10')
    print("Successfully validated that generated daily top 10 has expected data")


def test_top_10_ped_by_month_gen():
    conn = db.db_connect()
    actualQuery = "Select Year,Month,Monthly_Cnt,Longitude,Latitude from prod_top_ten_monthly " \
                  "order by Monthly_Cnt desc limit 10;"
    testQuery = "Select Year as Year,Month as Month,Monthly_Cnt as Monthly_Cnt," \
                "Longitude as Longitude,Latitude as Latitude from ( " \
                "Select cnt.sensor_id,cnt.sensor_name,cnt.year,cnt.month,loc.longitude, " \
                "loc.latitude,sum(hourly_counts) as monthly_cnt from prod_source_sensor_cnt cnt, " \
                " prod_source_sensor_loc loc where cnt.sensor_id = loc.sensor_id " \
                "group by cnt.sensor_id,cnt.sensor_name,cnt.year,cnt.month,loc.longitude, loc.latitude " \
                "order by sum(hourly_counts) desc) limit 10"
    monthlyActualDF = pd.read_sql(actualQuery,conn)
    monthlyTestDF = pd.read_sql(testQuery, conn)
    compareDF = monthlyActualDF.merge(monthlyTestDF, how='outer',
                                    indicator=True).loc[lambda x: x['_merge'] != 'both']
    monthlyCnt = len(compareDF.index)
    validate.equal(monthlyCnt,0,'The monthly prediction data is not correct')
    print("Successfully validated that generated monthly top 10 has expected data")


