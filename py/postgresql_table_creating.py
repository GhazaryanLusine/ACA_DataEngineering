hostname = "database-1.cdxkfmr78jdg.eu-central-1.rds.amazonaws.com"
database = "postgres"
username = "postgres"
pwd = "Walcott1994"

import psycopg2
conn = psycopg2.connect(host = hostname, dbname=database, user = username, password= pwd)

cur = conn.cursor()

cur.execute(''' CREATE TABLE guest(
          Guest_ID INTEGER PRIMARY KEY,
          Guest_name VARCHAR(10) NOT NULL,
          Guest_surname VARCHAR(20) NOT NULL,
          Guest_email VARCHAR(35) NOT NULL,
          Guest_gender VARCHAR(6) NOT NULL,
          Guest_country VARCHAR(35) NOT NULL,
          Guest_date_of_birth DATE NOT NULL
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE tour_agent(
          Agent_id INT PRIMARY KEY,
          Agent_name VARCHAR(25) NOT NULL,
          Agent_email VARCHAR(35) NOT NULL,
          Agent_phone VARCHAR(15) NOT NULL      
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE hotels(
          Hotel_id VARCHAR(5) PRIMARY KEY,
          Hotel_name VARCHAR(25) NOT NULL,
          Hotel_star INT NOT NULL,
          Hotel_country VARCHAR(5) NOT NULL      
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE country(
          Country_name VARCHAR(25) PRIMARY KEY,
          Country_code VARCHAR(5) NOT NULL
           
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Restaurants(
          Restaurant_id VARCHAR(5) PRIMARY KEY,
          Name VARCHAR(50) NOT NULL,
          Cuisine_Style VARCHAR(150),
          Rating FLOAT NOT NULL      
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Cars(
          Cars_id VARCHAR(5) PRIMARY KEY,
          Car_make VARCHAR(15) NOT NULL,
          Car_model VARCHAR(25) NOT NULL
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Tours(
          Tour_id VARCHAR(5) PRIMARY KEY,
          name VARCHAR(120) NOT NULL,
          Tour_price VARCHAR(10) NOT NULL,
          Rating FLOAT 
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Room_types(
          Roomtype_id VARCHAR(5) PRIMARY KEY,
          Roomtype VARCHAR(20) NOT NULL,
          RT_Desc VARCHAR(220) NOT NULL
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Rooms(
          Room_id VARCHAR(5) PRIMARY KEY,
          Hotel_id VARCHAR(5) NOT NULL,
          Room_typeid VARCHAR(5) NOT NULL,
          Room_number INT NOT NULL,
          Smoking_YN VARCHAR(5) NOT NULL,
          FOREIGN KEY (Hotel_id)
          REFERENCES hotels (Hotel_id),
          FOREIGN KEY (Room_typeid)
          REFERENCES room_types (Roomtype_id)
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Payment_type(
          Paymen_type_id VARCHAR(5) PRIMARY KEY,
          Payment_type VARCHAR(15) NOT NULL
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Payment_status(
          Payment_status_id VARCHAR(5) PRIMARY KEY,
          Booking_id VARCHAR(8) NOT NULL
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Review(
          Review_id VARCHAR(9) PRIMARY KEY,
          Booking_id VARCHAR(8) NOT NULL,
          Review_score INT NOT NULL,
          Review_text VARCHAR(300) NOT NULL,
          FOREIGN KEY (Booking_id)
          REFERENCES booking (Booking_id)
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Booking_status(
          Status_id INT PRIMARY KEY,
          Status VARCHAR(10) NOT NULL
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Booking(
          Booking_id VARCHAR(8) PRIMARY KEY,
          Room_id VARCHAR(5) NOT NULL,
          Guest_id INT,
          Agent_id INT,
          Start_date DATE NOT NULL,
          End_date DATE NOT NULL,
          Status_id VARCHAR(2),
          FOREIGN KEY (Room_id)
          REFERENCES rooms (Room_id),
          FOREIGN KEY (Guest_id)
          REFERENCES guest (Guest_id),
          FOREIGN KEY (Agent_id)
          REFERENCES tour_agent (Agent_id),
          FOREIGN KEY (Status_id)
          REFERENCES booking_status(Status_id)
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Service_booking(
          Booking_service_id VARCHAR(9) PRIMARY KEY,
          Booking_id VARCHAR(8) NOT NULL,
          Service_id VARCHAR(5) NOT NULL,
          Service_type VARCHAR(25) NOT NULL,
          FOREIGN KEY (Booking_id)
          REFERENCES booking (Booking_id)
        )
      ''')
conn.commit()

cur.execute(''' CREATE TABLE Payment(
          Payment_id VARCHAR(9) PRIMARY KEY,
          Booking_id VARCHAR(8) NOT NULL,
          Payment_type_id VARCHAR(5) NOT NULL,
          Payment_status_id VARCHAR(2) NOT NULL,
          Payment_Currency VARCHAR(5) NOT NULL,
          FOREIGN KEY (Booking_id)
          REFERENCES booking (Booking_id),
          FOREIGN KEY (Payment_type_id)
          REFERENCES payment_type (Paymen_type_id),
          FOREIGN KEY (Payment_status_id)
          REFERENCES payment_status (Payment_status_id)
        )
      ''')
conn.commit()

cur.execute(''' ALTER TABLE tours ALTER COLUMN Rating DROP NOT NULL
      ''')
conn.commit()

with open('data_guest.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'guest', sep=',')
conn.commit()
   
with open('tour_agent.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'tour_agent', sep=',')
conn.commit()

with open('Final_Hotel.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'hotels', sep=',')
    
with open('Countries.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'hotels', sep=',')
conn.commit()  

with open('Final_restaurant.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'restaurants', sep=',')
conn.commit()

with open('Final_Tours.csv', 'r', encoding="utf8") as f:
    next(f) 
    cur.copy_from(f, 'tours', sep=',')
conn.commit()

cur.execute(''' ALTER TABLE review
alter column Review_text type VARCHAR(400);
      ''')
conn.commit()

with open('Final_Room_type.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'room_types', sep=',')
conn.commit()

with open('Final_Room.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'rooms', sep=',')
conn.commit()

with open('Payment_type.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'payment_type', sep=',')
conn.commit()

with open('Payment_status.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'payment_status', sep=',')
conn.commit()

with open('booking_status.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'booking_status', sep=',')
conn.commit()

with open('Booking.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'booking', sep=',')
conn.commit()

with open('Review.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'review', sep=',')
conn.commit()

with open('Service_booking.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'service_booking', sep=',')
conn.commit()

with open('Payment.csv', 'r') as f:
    next(f) 
    cur.copy_from(f, 'payment', sep=',')
conn.commit()
    
    
    
    
    
