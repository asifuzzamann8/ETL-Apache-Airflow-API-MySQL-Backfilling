#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  6 12:42:28 2023

@author: asif
"""
import pendulum
from airflow.decorators import dag, task
#from textwrap import dedent
#from airflow.operators.pyhton import PythonOperator

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal


@dag(
    schedule_interval="0 11 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="EST"),
    catchup=False,
    #depends_on_past=True,
    #wait_for_downstream=True,
    default_args={
        "depends_on_past": True,
        "wait_for_downstream": True,
        "retries": 0,
    },
    tags=["ETL_currency_api"],
)



def ETL_currency_api():
    '''
    Fetch daily conversion rate from API Layer and load it into MySQL after required transformation
    '''
    @task()
    def extract(ds=None, base="USD"):
        
        event_date = ds #take the Airflow execution date        
        event_date = str(event_date)
        
        '''
        with open("/home/hadoop/nifi_data/event_date.txt", "w") as text_file:
            text_file.write(event_date)
        '''
                
        event_date = datetime.strptime(event_date, '%Y-%m-%d').date() #date conversion        
        start_date = event_date - timedelta(days=3) #back date for range
        
        #api url preparation
        url = "https://api.apilayer.com/exchangerates_data/timeseries?" + \
            "base=" + base + \
            "&start_date=" + start_date.strftime('%Y-%m-%d') + \
            "&end_date=" + event_date.strftime('%Y-%m-%d')
   
        payload = {}
        headers= {
              "apikey": "" #input api key here
            }
    
        #api call    
        response = requests.request("GET", url, headers=headers, data = payload)
        
        #parsing api response status
        flag = response.status_code
        
        #returning the response if success
        if flag == 200:
            result = json.loads(response.text)
            return result
        else:
            raise Exception("Response code: "+flag)
    
    @task()
    def transform(result: dict):
        
        df1 = pd.DataFrame.from_dict(result['rates'], orient='index') #convert in dataframe
        df1 = df1.reset_index().rename(columns={'index': 'event_date'}) #convert date index into a column
        df1_melted = df1.melt(id_vars='event_date', var_name='currency_name', value_name='currency_value') #transpose the data
        df1_melted['currency_value'] = df1_melted['currency_value'].apply(Decimal)
        df1_melted.to_csv("/home/hadoop/nifi_data/ext_daily_currency.csv", index=False)
        
    @task()
    def load_raw():
        
        import pymysql
        conn = pymysql.connect(host="localhost", user="admin", password="1040", port=3306, autocommit=True, local_infile=True)
        
        with conn.cursor() as cursor:
            
            cursor.execute("truncate table prod.tmp_currency_values")
            
            cursor.execute("LOAD DATA LOCAL INFILE '/home/hadoop/nifi_data/ext_daily_currency.csv' \
                                INTO TABLE prod.tmp_currency_values \
                                FIELDS TERMINATED BY ',' \
                                LINES TERMINATED BY '\n' \
                                IGNORE 1 ROWS")
            
    @task()        
    def load_final(ds=None):
        
        event_date = ds #take the Airflow execution date        
        event_date = str(event_date)
        
        event_date = datetime.strptime(event_date, '%Y-%m-%d').date() #date conversion        
        start_date = event_date - timedelta(days=3) #back date for range

        import pymysql
        conn = pymysql.connect(host="localhost", user="admin", password="1040", port=3306, autocommit=True, local_infile=True)
        
        with conn.cursor() as cursor:
            
            cursor.callproc('prod.insert_currency_values',(''+start_date.strftime('%Y-%m-%d')+'', ''+event_date.strftime('%Y-%m-%d')+'', ''))
            db_response = cursor.fetchall()
            
            #if db_response[0][0] != "Success":
            #    raise Exception("Data not inserted in DB final table")
        
               
    result = extract()
    transform(result) >> load_raw() >> load_final()
    
    
ETL_currency_api()
