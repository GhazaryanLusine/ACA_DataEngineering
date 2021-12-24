import pandas as pd
import numpy as np
from random import randint
import random


Guest= pd.read_csv(r"data_guest.csv", index_col=False)
Tour_Agent= pd.read_csv(r"tour_agent.csv", index_col=False)
Country=pd.read_csv(r"Countries.csv")
Restaurants = pd.read_csv(r"Final_restaurant.csv", index_col=False)
Cars = pd.read_csv(r"Final_cars.csv", index_col=False)
Tours = pd.read_csv(r"Final_Tours.csv", index_col=False)
Hotels = pd.read_csv(r"Final_Hotel.csv", index_col=False)
Rooms = pd.read_csv(r"Final_Room.csv", index_col=False)
Room_types = pd.read_csv(r"Final_Room_type.csv", index_col=False)
Room_review = pd.read_csv(r"C:\Users\admin\Desktop\New folder\hotel-reviews.csv", index_col=False)
Payment_type=pd.read_csv("Payment_type.csv")
Payment_status=pd.read_csv("Payment_status.csv")



Guest_id = Guest['Guest_ID'].values
Na = ["Nan","Nan","Nan","Nan","Nan","Nan","Nan","Nan","Nan","Nan"]
Guest_id=np.concatenate((Guest_id, Na), axis=0)
List=[1,1,1,1,2]


import datetime
m=pd.date_range(start="2010-01-01",end="2021-12-01").to_pydatetime().tolist()

def end_date(x):
    return(x+datetime.timedelta(days=randint(7,14)))

def gen():
    rand = random.choice(Guest_id)
    if rand == "Nan":
        data = [randint(10000000, 99999999), random.choice(Rooms['Room_id'].values), 0,
                random.choice(Tour_Agent['Tour_Agent'].values), random.choice(m), random.choice(List)]
        return data
    else:
        data = [randint(10000000, 99999999), random.choice(Rooms['Room_id'].values), rand, 0, random.choice(m),
                random.choice(List)]
        return data
    
def dataframe():
    df = pd.DataFrame(columns=["Booking_id","Room_id","Guest_id","Agent_id","Start_date","Status_id"])
    for i in range(10):
        df.loc[i] = gen()
    df["End_date"]=df["Start_date"].apply(end_date)
    df=df[["Booking_id","Room_id","Guest_id","Agent_id","Start_date","End_date","Status_id"]]
    return df

Booking=dataframe()

Booking_status_conf=Booking[Booking["Status_id"]==1]


Room_review['Description']=Room_review['Description'].str.replace(",", ";")
Room_review['Description']=Room_review['Description'].str.replace("\r\n", "")

Room_review=Room_review[Room_review['Description'].str.len() < 300]
Room_review_happy=Room_review[Room_review['Is_Response']=='happy']
Room_review_not_happy=Room_review[Room_review['Is_Response']=='not happy'] 

def gen_review():
    rand=randint(1,10)
    if rand <= 5:
        data=[("R"+ str(randint(10000000,99999999))),random.choice(Booking_status_conf['Booking_id'].values),
              rand,random.choice(Room_review_not_happy['Description'].values)]
        return data
    else:
        data=[("R"+ str(randint(10000000,99999999))),random.choice(Booking_status_conf['Booking_id'].values),rand,
              random.choice(Room_review_happy['Description'].values)]
        return data
    
    
def dataframe_review():
    df = pd.DataFrame(columns=["Review_id","Booking_id","Review_score","Review_text"])
    for i in range(200):
        df.loc[i] = gen_review()
    return df

Review=dataframe_review()

def gen_service():
    rand=random.choice(random.choice((Cars['Car_id'].values,Restaurants['Restaurant_id'].values,Tours['Tour_id'])))
    if rand.startswith('C'):
        data=[("S"+ str(randint(10000000,99999999))),random.choice(Booking['Booking_id'].values),rand,"Car_renting"]
        return data
    elif rand.startswith('T'):
        data=[("S"+ str(randint(10000000,99999999))),random.choice(Booking['Booking_id'].values),rand,'Tour_booking']
        return data
    else:
        data=[("S"+ str(randint(10000000,99999999))),random.choice(Booking['Booking_id'].values),rand,'Restaurant_rservation']
        return data

def dataframe_service():
    df = pd.DataFrame(columns=["Booking_service_id","Booking_id","Service_id","Service_type"])
    for i in range(1000):
        df.loc[i] = gen_service()
    return df

Service_booking=dataframe_service()
Service_booking.to_csv("Service_booking.csv", index= False)

Currency= ["USD","EUR"]

def gen_payment():
        data=[("P"+ str(randint(10000000,99999999))),random.choice(Booking_status_conf['Booking_id'].values),
             random.choice(Payment_type['Payment_type_id'].values),
             random.choice(Payment_status['Payment_status_id'].values),random.choice(Currency)]
        return data
    
def dataframe_payment():
    df = pd.DataFrame(columns=["Payment_id","Booking_id","Payment_type_id","Payment_status_id","Payment_Currency"])
    for i in range(1000):
        df.loc[i] = gen_payment()
        df.drop_duplicates(subset ="Booking_id",
                     keep = "first", inplace = True)
    return df

Payment=dataframe_payment()
Payment.to_csv("Payment.csv", index = False)
 













