import mysql.connector
import numpy as np
import pandas as pd
import quandl
import csv
import json 

df = quandl.get("BSE/BOM542248", authtoken={'YOUR API KEY'})
print(df.head())
print(df.columns)

#df = df[['Open', 'High', 'Low', 'Close', 'WAP', 'No. of Shares', 'No. of Trades', 'Total Turnover', 'Deliverable Quantity', '% Deli. Qty to Traded Qty', 'Spread H-L', 'Spread C-O']]
df.dropna(inplace=True)
print(df.head())

#WRITE TO CSV
file = open('bse_csv.csv', 'w')
my_writer = csv.writer(file)

my_writer.writerow(['Open', 'High', 'Low', 'Close', 'WAP', 'No. of Shares', 'No. of Trades', 'Total Turnover', 'Deliverable Quantity', '% Deli. Qty to Traded Qty', 'Spread H-L', 'Spread C-O'])

for Open, High, Low, Close, WAP, SharesNum, TradesNum, TotalTurnover, DeliverableQty, percentDeliQtytoTradedQty, SpreadHL, SpreadCO in zip(df['Open'], df['High'], df['Low'], df['Close'], df['WAP'], df['No. of Shares'], df['No. of Trades'], df['Total Turnover'], df['Deliverable Quantity'], df['% Deli. Qty to Traded Qty'], df['Spread H-L'], df['Spread C-O']):
    my_writer.writerow([Open, High, Low, Close, WAP, SharesNum, TradesNum, TotalTurnover, DeliverableQty, percentDeliQtytoTradedQty, SpreadHL, SpreadCO])
file.close()

#CONVERT CSV TO JSON
def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []
      
    #read csv file
    with open(csvFilePath, encoding='utf-8') as csvfile: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvfile) 

        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            jsonArray.append(row)
  
    #cnvert python jsonArray to JSON String and write to file
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonfile: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonfile.write(jsonString)
          
csvFilePath = r'bse_csv.csv'
jsonFilePath = r'bse_json.json'
csv_to_json(csvFilePath, jsonFilePath)

df = pd.read_json('bse_json.json')
#print(df.to_string()) 
print('\nPRINTING THE FIRST 12 ROWS OF THE DATASET\n')
print(df.head(12)) 
print('\nPRINTING THE LAST 5 ROWS OF THE DATASET\n')
print(df.tail()) 
print(df.info()) 
print(df.describe)

#ESTABLISH DATABASE CONNECTION
mydb = mysql.connector.connect(
  host="localhost",
  user="{Your Database username}",
  password="{Your Database Password}",
  database="{Your Model's Name}" #returns an error if DB does not exist
)
print(mydb)
new_db = mydb.cursor()

#CREATE TABLE
new_db.execute("CREATE TABLE records (id INT(11) NOT NULL AUTO_INCREMENT, Open VARCHAR(50), High VARCHAR(50), Low VARCHAR(50), Close VARCHAR(50), WAP VARCHAR(50), SharesNum VARCHAR(50), TradesNum VARCHAR(50), TotalTurnover VARCHAR(50), DeliverableQty VARCHAR(50), percentDeliQtytoTradedQty VARCHAR(50), SpreadHL VARCHAR(50), SpreadCO VARCHAR(50), PRIMARY KEY (id))")

#WRITE TO MYSQL DATABASE
sql = "INSERT INTO records (Open, High, Low, Close, WAP, SharesNum, TradesNum, TotalTurnover, DeliverableQty, percentDeliQtytoTradedQty, SpreadHL, SpreadCO) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
val = []
for i in range(len(df['Open'])):
    val.append((df['Open'][i], df['High'][i], df['Low'][i], df['Close'][i], df['WAP'][i], df['No. of Shares'][i].item(), df['No. of Trades'][i].item(), df['Total Turnover'][i].item(), df['Deliverable Quantity'][i].item(), df['% Deli. Qty to Traded Qty'][i].item(), df['Spread H-L'][i], df['Spread C-O'][i]))

new_db.executemany(sql, val)

mydb.commit()

print(new_db.rowcount, "records inserted.")
new_db.execute("SHOW TABLES")
