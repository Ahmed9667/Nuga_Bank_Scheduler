#import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
import psycopg2
import os
 spark = SparkSession.builder.appName('Nuga Scheduler')\
        .config('spark.jars' , 'postgresql-42.7.3.jar'  )\
        .getOrCreate()

spark

# import dataset
df = spark.read.csv('nuga_bank_transactions.csv', header=True, inferSchema=True)
df.show()

#check null values
for i in df.columns:
    null = df.filter(df[i].isNull()).count()
    print(i,' NULLS = ',null)

df.printSchema()

#column Last_Updated has timestamp datatype so we will drop records of null values
df = df.na.drop(subset='Last_Updated')

#fill null values
for i in df.columns:
    data_types = dict(df.dtypes)[i]
    if data_types == 'string':
        df = df.fillna({i:'unknow'})
    elif data_types == 'double':
        df = df.fillna({i : 0.0})
    else:
        df = df.fillna({i : 0})

#check for null values
for i in df.columns:
    null = df.filter(df[i].isNull()).count()
    print(i,' Nulls = ',null)

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col

#create table transaction
transaction = df.select('Transaction_Date','Amount','Transaction_Type')

#create column transaction_id
w = Window.orderBy(f.lit(1))
transaction = transaction.withColumn('transaction_id', row_number().over(w))\
              .select('transaction_id','Transaction_Date','Amount','Transaction_Type')

transaction.show(5)

# create table customer
customer = df.select('Customer_Name',
 'Customer_Address',
 'Customer_City',
 'Customer_State',
 'Customer_Country','Email',
 'Phone_Number')

w = Window.orderBy(f.lit(1))

customer = customer.withColumn('customer_id', row_number().over(w))\
            .select('customer_id','Customer_Name','Customer_Address','Customer_City','Customer_State', 'Customer_Country','Email','Phone_Number')

customer.show(5)

#create table employee
employee = df.select('Company','Job_Title','Gender','Marital_Status')

w= Window.orderBy(f.lit(1))
employee = employee.withColumn('employee_id', row_number().over(w))\
           .select('employee_id','Company','Job_Title','Gender','Marital_Status')

employee.show(5)

#create fact_table
fact = df.join(transaction, on=['Transaction_Date','Amount','Transaction_Type'], how='left')\
         .join(customer,on=['Customer_Name','Customer_Address','Customer_City','Customer_State', 'Customer_Country','Email','Phone_Number'],how='left')\
         .join(employee,on=['Company','Job_Title','Gender','Marital_Status'],how='left')\
         .select('transaction_id','customer_id','employee_id','Credit_Card_Number','IBAN','Currency_Code','Random_Number','Category','Group','Is_Active','Last_Updated','Description')

fact.show()

import psycopg2

# Define database connection parameters including the database name
db_params = {
    'username':'postgres',
    'password':'ahly9667',
    'host':'localhost',
    'port':'5432',
    'database':'nuga_bank_scheduled'
}

default_db_url =f"postgresql://{db_params['username']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/postgres"

#create database
try:
    # open the connection
    conn = psycopg2.connect(default_db_url)
    conn.autocommit = True
    cur = conn.cursor()

#check if the database is already existed
    cur.execute(f"select 1 from pg_catalog.pg_database where datname='{db_params['database']}'")
    exists = cur.fetchone()
    if not exists:
        # Create the database
        cur.execute(f"create database {db_params['database']}")
        print(f"Database {db_params['database']} created successfully")

    else:
        print(f"Database {db_params['database']} already existed")

    # Close the cnnection
    cur.close()
    conn.close()
except exception as e :
    print(f"an error {e} occurred")


# Connect to the new created database alayta_bank
def db_connected():
    connection = psycopg2.connect(user = 'postgres', 
                                  host= 'localhost',
                                  password = 'ahly9667',
                                  port = 5432,
                                  database ='nuga_bank_scheduled')

    return connection

conn = db_connected()
print(f"Database {db_params['database']} connected successfully")


#create a function to create related tables of schema
def create_table():
    conn = db_connected()
    cursor= conn.cursor()
    query = """
                 drop table if exists transaction;
                 drop table if exists customer;
                 drop table if exists employss;
                 drop table if exists fact_table;

                 create table transaction(
                 transaction_id bigint primary key,
                 Transaction_Date date,
                 Amount float,
                 Transaction_Type varchar(10000));

                 create table customer(
                 customer_id bigint primary key,
                 Customer_Name varchar(10000),
                 Customer_Address varchar(10000) ,
                 Customer_City varchar(10000),
                 Customer_State varchar(10000),
                 Customer_Country varchar(10000),
                 Email varchar(10000),
                 Phone_Number varchar(10000));

                 create table employee(
                 employee_id bigint primary key,
                 Company varchar(10000),
                 Job_Title varchar(10000),
                 Gender varchar(10000),
                 Marital_Status varchar(10000));

                 create table fact_table(
                 transaction_id bigint,
                 customer_id bigint,
                 employee_id bigint,
                 Credit_Card_Number bigint,
                 IBAN varchar(10000),
                 Currency_Code varchar(10000),
                 Random_Number float,
                 Category varchar(10000),
                 "Group" varchar(10000),
                 Is_Active varchar(10000),
                 Last_Updated date,
                 Description varchar(10000),
                 foreign key (transaction_id) references transaction(transaction_id),
                 foreign key (customer_id) references customer(customer_id) ,
                 foreign key (employee_id ) references employee(employee_id ) ) ;
                """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

create_table()
print('Tables created successfully')


#load data into tables
my_url = "jdbc:postgresql://localhost:5432/nuga_bank_scheduled"
my_properties = {'user' : 'postgres',
              'password' : 'ahly9667',
              'driver' : 'org.postgresql.Driver'}

customer.write.jdbc( url= my_url , table = 'customer' , mode ='append' ,properties= my_properties)
transaction.write.jdbc( url= my_url , table = 'transaction' , mode ='append' ,properties= my_properties)
employee.write.jdbc( url= my_url , table = 'employee' , mode ='append' ,properties= my_properties)
fact.write.jdbc( url= my_url , table = 'fact_table' , mode ='append' ,properties= my_properties)

print('Records added successfully to tables of database')