# PYTHON_API_CSV_JSON_MySQL
This code fetch data from an API, writes the dataset to CSV, converts it to JSON and then saves the data in MySQL database

Language used - PYTHON

Database used - MySQL
Install MySQL and configure your database and user parameters
Be carefull to remeber your log in parameters
Also install MySQL Workbench for data visualization

pip install the following modules python modules
- mysql.connector
- numpy
- pandas
- quandl

Find this block of code in the program file and fill them with your MySQL database parameters

mydb = mysql.connector.connect(
  host="localhost",
  user="{Your Database username}",
  password="{Your Databse Password}",
  database="{Your Model's Name}" #returns an error if DB does not exist
)

Delete bsa_csv.csv and bse_json.json - I only left them for you to see the output, the code should generate your own output

You may also need to install other necessary modules as stated in the program

Data source - https://data.nasdaq.com/data/BSE/BOM542248-deccan-health-care-ltd-eod-prices

Cheers!