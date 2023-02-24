# ETL-Apache-Airflow-API-MySQL-Backfilling

This is an ETL workflow that fetches USD currency conversion data from apilayer, parse the nested JSON string, format it into tabular data, and finally insert it in the MySQL database. 

To reload the back date/history data, Airflow backfilling feature is used. To do so, Airflow execution date or "DS" is used in the tasks to make it dynamic.  
