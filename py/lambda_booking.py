import json
import awswrangler as wr
import datetime
import numpy as np
import pandas as pd
import random
from random import randint
import psycopg2



def lambda_handler(event, context):
    
    
    Rooms = wr.s3.read_csv(path='s3://acadataengineeringproject/Final_Room.csv')
    Tour_Agent = wr.s3.read_csv(path='s3://acadataengineeringproject/tour_agent.csv')
    Guest = wr.s3.read_csv(path='s3://acadataengineeringproject/data_guest.csv')
    
    List=[1,1,1,1,2]
    
    Guest_id = Guest['Guest_ID'].values
    Na = ["Nan","Nan","Nan","Nan","Nan","Nan","Nan","Nan","Nan","Nan"]
    Guest_id=np.concatenate((Guest_id, Na), axis=0)
    m=pd.date_range(start="2010-01-01",end="2021-12-01").to_pydatetime().tolist()
    
    def end_date(x):
       return(x+datetime.timedelta(days=randint(7,14)))
    
    
    def gen():
        rand = random.choice(Guest_id)
        if rand == "Nan":
            data = [randint(10000000, 99999999), random.choice(Rooms['Room_id'].values), 0,
                random.choice(Tour_Agent['Tour_Agent'].values), random.choice(m), random.choice(List)]
            return(data)
        else:
            data = [randint(10000000, 99999999), random.choice(Rooms['Room_id'].values), rand, 0, random.choice(m),
                random.choice(List)]
            return(data)
        
    def dataframe():
        df = pd.DataFrame(columns=["Booking_id","Room_id","Guest_id","Agent_id","Start_date","Status_id"])
        for i in range(10):
            df.loc[i] = gen()
        df["End_date"]=df["Start_date"].apply(end_date)
        df=df[["Booking_id","Room_id","Guest_id","Agent_id","Start_date","End_date","Status_id"]]
        return(df)
    
    Booking=dataframe()
    
    def execute_many(conn, df, table):
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
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
    

    conn = psycopg2.connect(host = hostname, dbname=database, user = username, password= pwd)
    execute_many(conn, Booking, 'booking')
    
    
    return {
        'statusCode' : 200,
         'body' : json.dumps('done')
    }
    
    