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
    
    Room_review = wr.s3.read_csv(path='s3://acadataengineeringproject/hotel-reviews.csv')
    Room_review_happy=Room_review[Room_review['Is_Response']=='happy']
    Room_review_not_happy=Room_review[Room_review['Is_Response']=='not happy'] 
    Booking = pd.read_sql("select * from booking", conn)
    Booking_status_conf=Booking[Booking["status_id"]==1]

    def gen_review():
        rand=randint(1,10)
        if rand <= 5:
            data=[("R"+ str(randint(10000000,99999999))),random.choice(Booking_status_conf['booking_id'].values),
                  rand,random.choice(Room_review_not_happy['Description'].values)]
            return data
        else:
            data=[("R"+ str(randint(10000000,99999999))),random.choice(Booking_status_conf['booking_id'].values),
                  rand,random.choice(Room_review_happy['Description'].values)]
            return data
            
            
    def dataframe_review():
        df = pd.DataFrame(columns=["Review_id","Booking_id","Review_score","Review_text"])
        for i in range(5):
            df.loc[i] = gen_review()
        return df
    
    Review=dataframe_review()
   
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
        
       
    
    execute_many(conn, Review, 'review')
    
    return {
        'statusCode': 200,
        'body': json.dumps('done')
    }
