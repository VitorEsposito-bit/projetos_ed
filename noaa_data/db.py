#Import library
import sqlite3 as sq
import csv


#Create a connection to the database
con = sq.connect("noaa_data.db")


#Create cursos for the current session
cur = con.cursor()


#Create table
cur.execute("""
CREATE TABLE noaaprcp (
    date TEXT,        -- Keeping as TEXT for easy date handling
    datatype TEXT,    
    station TEXT,     
    value REAL,      
    city TEXT
)
""")


#Check if table exists in the database
res = cur.execute("SELECT name FROM sqlite_master")
res.fetchone()


#Opening the csv file and storing data in a list-object
with open("noaa_data.csv","r",encoding="utf-8") as csvfile:
    reader = csv.reader(csvfile, delimiter=",")
    next(reader, None)
    data = []
    
    for i in reader:
        i[3] = float(i[3])
        data.append(tuple(i))


#Insert data values into noaprcp table
cur.executemany("INSERT into noaaprcp VALUES(?,?,?,?,?)",data)


#Commit
con.commit()


#Select query number 1
sq1= cur.execute("SELECT COUNT(city) FROM noaaprcp")
sq1.fetchall()


#Select query number 2
sq2 = cur.execute("SELECT * FROM noaaprcp LIMIT 10")
sq2.fetchall()


#Closing connection
con.close()