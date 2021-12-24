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
    
    Payment_status = wr.s3.read_csv(path='s3://acadataengineeringproject/Payment_status.csv')
    Payment_type = wr.s3.read_csv(path='s3://acadataengineeringproject/Payment_type.csv')
    Booking = pd.read_sql("select * from booking", conn)
    
    Booking_status_conf=Booking[Booking["status_id"]==1]
    Currency= ["USD","EUR"]
    
    def gen_payment():
        data=[("P"+str(randint(10000000,99999999))),random.choice(Booking_status_conf['booking_id'].values),
              random.choice(Payment_type['Payment_type_id'].values),
             random.choice(Payment_status['Payment_status_id'].values),random.choice(Currency)]
        return data
        
    def dataframe_payment():
        df = pd.DataFrame(columns=["Payment_id","Booking_id","Payment_type_id","Payment_status_id","Payment_Currency"])
        for i in range(10):
            df.loc[i] = gen_payment()
        return df
        
    Payment=dataframe_payment()
    
    def execute_many(conn, df, table):
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
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
        

    
    execute_many(conn, Payment, 'payment')
    
    return {
        'statusCode': 200,
        'body': json.dumps('done')
    }
