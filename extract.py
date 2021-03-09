import pandas as pd
import json    
import string
from pymongo import MongoClient
import pymongo
client=MongoClient('localhost',27017)
customerdetails_col = client.telecom.customerDetailsCol
customerfinance_col = client.telecom.customerFinanceCol

#*********************************************************************
# Extraction of Data
#*********************************************************************

df_customerDetails = pd.read_csv("CustomerDetails.csv")
df_customerFinance = pd.read_csv("CustomerFinance.csv") 

df_customerDetails = df_customerDetails.to_dict('records')
df_customerFinance = df_customerFinance.to_dict('records')

# print(df_customerFinance)

# the Raw data is stored to the Mongodb
customerdetails_col.insert_many(df_customerDetails)
customerfinance_col.insert_many(df_customerFinance)