import json
import awswrangler as wr
import datetime
import numpy as np
import pandas as pd
import random
from random import randint
import psycopg2

def lambda_handler(event, context):
    
    
    conn = psycopg2.connect(host = hostname, dbname=database, user = username, password= pwd)
    
    Cars = wr.s3.read_csv(path='s3://acadataengineeringproject/Final_cars.csv')
    Restaurants = wr.s3.read_csv(path='s3://acadataengineeringproject/Final_restaurant.csv')
    Tours = wr.s3.read_csv(path='s3://acadataengineeringproject/Final_Tours.csv')
    Booking = pd.read_sql("select * from booking", conn)

    def gen_service():
        rand=random.choice(random.choice((Cars['Car_id'].values,Restaurants['Restaurant_id'].values,Tours['Tour_id'])))
        if rand.startswith('C'):
            data=[("S"+ str(randint(10000000,99999999))),random.choice(Booking['booking_id'].values),rand,"Car_renting"]
            return data
        elif rand.startswith('T'):
            data=[("S"+ str(randint(10000000,99999999))),random.choice(Booking['booking_id'].values),rand,'Tour_booking']
            return data
        else:
            data=[("S"+ str(randint(10000000,99999999))),random.choice(Booking['booking_id'].values),rand,'Restaurant_rservation']
            return data
            
    def dataframe_service():
        df = pd.DataFrame(columns=["Booking_service_id","Booking_id","Service_id","Service_type"])
        for i in range(10):
            df.loc[i] = gen_service()
        return df
        
    Service_booking=dataframe_service()
    
    def execute_many(conn, df, table):
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s)" % (table, cols)
        cursor = conn.cursor()
        try:
            cursor.executemany(query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}" )
            conn.rollback()
            cursor.close()
            return 1
        print("done")
        cursor.close()
        
        
    
    execute_many(conn, Service_booking, 'service_booking')
    

    return {
        'statusCode': 200,
        'body': json.dumps('done')
    }
