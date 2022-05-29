# Belong_QE_Tests

# System/Integration Data Tests using Pytest(Python), SQL and HTML Reports

## Getting Started

These instructions will help in setting up the project on your local machine for Data test automation purpose.

### Pre-requisite softwares

Below software should be installed before project setup,

* python
* Pycharm/Vscode Preferable

### Project setup

```
* Clone the project artefacts from github
* Open the project folder in Pycharm/Vscode, now open a terminal window using Pycharm/Vscode
* Run pip install pipenv
* Run pipenv install
* Run pipenv shell
* Run the tests using the commands below:
    * pytest
* Now open the report in reports folder by right clicking on the html file and opening with Browser or directly from file location in any browser
```
### Database and Tables Created

A SQLITE3 DB has been created for testing purpose.
```
DB Name: **belongQE.db**
DB Location: Belong_QE_Tests\database\belongQE.db
Application for interacting with UI can be downloaded from https://sqlitebrowser.org/

```
Four Tables has been created.

```
1. Actual Data For Daily Top 10: **prod_top_ten_daily**
2. Actual Data For Daily Top 10: **prod_top_ten_monthly**
3. Source Data For Sensor Count From URL(https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json): **prod_source_sensor_cnt**
4. Source Data For Sensor Location From URL(https://data.melbourne.vic.gov.au/resource/h57g-5234.json): **prod_source_sensor_loc**

```


### Test Queries Used
1. Daily Top 10 
   *Actual Data
    ```Sql
   Select Loc_Name,Year,Month,mDate,Daily_Cnt,Longitude,Latitude from prod_top_ten_daily order by Daily_Cnt desc limit 10;
   
   ```
   *Expected Data
    ```SQL
    Select * from (
    Select *, row_number() over(order by daily_cnt desc) daily_rnk from ( Select cnt.sensor_id,
    cnt.sensor_name as loc_name,cnt.year,cnt.month,cnt.mdate,loc.longitude, loc.latitude,sum(hourly_counts) as daily_cnt
    from prod_source_sensor_cnt cnt join prod_source_sensor_loc loc on cnt.sensor_id = loc.sensor_id
    group by cnt.sensor_id,cnt.sensor_name,cnt.year,cnt.month,cnt.mdate,loc.longitude, loc.latitude
    order by sum(hourly_counts) desc)) where daily_rnk <= 10
    
    ```
   
2. Monthly Top 10
   *Actual Data
    ```
   Select Year,Month,Monthly_Cnt,Longitude,Latitude from prod_top_ten_monthly order by Monthly_Cnt desc limit 10
   ```
   *Expected Data
    ```
   Select Year,Month,Monthly_Cnt,Longitude,Latitude from ( Select cnt.sensor_id,cnt.sensor_name,cnt.year,
   cnt.month,loc.longitude, loc.latitude,sum(hourly_counts) as monthly_cnt 
   from prod_source_sensor_cnt cnt, prod_source_sensor_loc loc where cnt.sensor_id = loc.sensor_id 
   group by cnt.sensor_id,cnt.sensor_name,cnt.year,cnt.month,loc.longitude, loc.latitude 
   order by sum(hourly_counts) desc) limit 10
   ```