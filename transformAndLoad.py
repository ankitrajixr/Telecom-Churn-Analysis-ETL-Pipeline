#Importing libraries
import pymongo 
import pprint
import pandas as pd

#connecting mongodb and python
mongo_uri = "mongodb://localhost:27017/"  
client = pymongo.MongoClient(mongo_uri)
db = client.telecom
print(db.list_collection_names())

#*********************************************************************
# Transformations
#*********************************************************************

table_cust = db.customerDetailsCol.find({})
df_cust = pd.DataFrame(table_cust)
#print(df_cust.head(100))

table_cust_fin = db.customerFinanceCol.find({})
df_cust_fin = pd.DataFrame(table_cust_fin)
#print(df_cust_fin.head(100))


df_join_right = pd.merge(df_cust, df_cust_fin, how='right', on=['PaymentID'])

print(df_join_right)

#Validity Check
df_join_right["Internet Scheme ID"] = df_join_right["Internet Scheme ID"].fillna("NO")

df_join_right['TotalCharges'] = df_join_right['TotalCharges'].replace(" ", 0).astype('float32')

#Validity Check for negative time

print((df_join_right['tenure'] < 0).any().any())

print((df_join_right['MonthlyCharges'] < 0).any().any())

import numpy as np
#Consistency Check
if not np.array_equal(df_join_right.tenure, df_join_right.tenure.astype(float)):
    df = df_join_right [df_join_right.tenure.str.contains("(\\-?|\\+?)?\d+(\\.\d+)?")]

#Completeness
df_join_right.dropna(inplace = True)

#Loading the Preprocessed data to the Database
df_join_right.to_csv("pre_processed.csv", index = False)


#*********************************************************************
# Creation of Star Schema
#*********************************************************************


#Exporting the preprocessed data from the data base to create the Star Schema

df_pre_processed = pd.read_csv("E:/SRH/3rd Semester/Data Management 2/Data Management 2 Source Code/pre_processed.csv")

# Split the columns as per requirement from df_pre_processed dataframe into different dataframes.
 

# Select columns which are relevant to user information from df_pre_processed dataFrame to prepare the fact table
# and populate it into a new subset DataFrame (df_cust_fact)
df_cust_fact = df_pre_processed.loc[ : , ['customerID', 'PaymentID', 'Contract ID', 'TotalCharges', 'Churn', 'tenure', 'MultipleLines', 'Internet Scheme ID'] ]
print(df_cust_fact)

# Select columns which are relevant to user information from df_pre_processed dataFrame to prepare the dimension table
# and populate it into a new subset DataFrame (df_cust_details)
df_cust_details = df_pre_processed.loc[ : , ['customerID', 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'OnlineSecurity', 'OnlineBackup' , 'DeviceProtection',  'StreamingTV', 'StreamingMovies', 'TechSupport'] ]
print(df_cust_details)

# Select columns which are relevant to payment_detailsfrom df_pre_processed dataFrame
# and populate it into a new subset DataFrame (df_payment_details)
df_payment_details = df_pre_processed.loc[ : , ['PaymentID', 'MonthlyCharges', 'PaymentMethod', 'PaperlessBilling'] ]
print(df_payment_details)

# Select columns which are relevant to customer information about contract type from df_pre_processed
# dataFrame and populate it into a new subset DataFrame (df_user_movie_fact)
df_contract_type = df_pre_processed.loc[ : , ['Contract ID', 'Contract'] ]
print(df_contract_type)

# Select columns which are relevant to customer information about Internet Type from df_pre_processed
# dataFrame and populate it into a new subset DataFrame (df_user_movie_fact)
df_internet_type = df_pre_processed.loc[ : , ['Internet Scheme ID', 'InternetService'] ]
print(df_internet_type)


# df_payment_details dataframe represent Payment Dimension Table,
# df_internet_type dataframe represent Internet type Dimension Table,
# df_contract_type dataframe represent Contract Type Dimension Table,
# df_cust_details dataframe represent Customer Details Dimension Table,
# df_cust_fact represent Customer Details Fact Table.

# Requirement states that all table must have uniqe records and they must be sorted.

# Sort the df_users dataframe in ascending order based on User ID and 
# remove duplicate Payment ID records
df_payment_details = df_payment_details.sort_values(by = ['PaymentID'], ascending = True, na_position = 'last').drop_duplicates(['PaymentID'],keep = 'first')
print(df_payment_details)

# Sort the df_movies dataframe in ascending order based on Contract ID and 
# remove duplicate Contract ID records
df_contract_type = df_contract_type.sort_values(by = ['Contract ID'], ascending = True, na_position = 'last').drop_duplicates(['Contract ID'],keep = 'first')
print(df_contract_type)

# Sort the df_user_movie_fact dataframe in ascending order based on Internet Scheme ID also 
# remove duplicate record for Internet Scheme ID
df_internet_type = df_internet_type.sort_values(by = ['Internet Scheme ID'], ascending = True, na_position = 'last').drop_duplicates(['Internet Scheme ID'],keep = 'first')
print(df_internet_type)

#*********************************************************************
# CLoading into the Data warehouse
#*********************************************************************

# Export the Star Schema and save them in CSV file respectively
df_cust_fact.to_csv("file_cust_fact.csv", index = False)
df_cust_details.to_csv("file_cust_details.csv", index = False)
df_payment_details.to_csv("file_payment_details.csv", index = False)
df_contract_type.to_csv("file_contract_type.csv", index = False)
df_internet_type.to_csv("file_internet_type.csv", index = False)