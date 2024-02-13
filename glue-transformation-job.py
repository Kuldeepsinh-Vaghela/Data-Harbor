import sys
import boto3 
import pandas as pd
from io import StringIO

bucket_name = 'slick-deals-product-data-raw'
file_name = 'raw-data/product_data.csv'

s3 = boto3.client('s3')
obj = s3.get_object(Bucket = bucket_name, Key= file_name)
df = pd.read_csv(obj['Body'])

### removing rows with advertisements
ad_rows_indx = df.index[df['Store Name'].str.contains('Advertiser')]
for indx in ad_rows_indx:
    df = df.drop(indx)

del_rows = df.index[df['Upload Time'].str.contains('http')]
for row in del_rows:
    df = df.drop(row)

### standardizing the upload time to hours
# Minutes to Hours
time_in_min_indx = df.index[df['Upload Time'].str.contains('minutes') ]
for x in time_in_min_indx:
    df.at[x,'Upload Time'] = str(round(int(df.at[x,'Upload Time'].split(' ')[0])/60.0,2))
# Days to Hours
time_in_days_indx = df.index[df['Upload Time'].str.contains('day')]
for x in time_in_days_indx:
    df.at[x,'Upload Time'] = str(int(df.at[x,'Upload Time'].split(' ')[0])*24.0)
# Month to Hours
time_in_month_indx = df.index[df['Upload Time'].str.contains('month')]
for x in time_in_month_indx:
    df.at[x,'Upload Time'] = str(int(df.at[x,'Upload Time'].split(' ')[0])*24.0*30.0)
# Extracting the hour value
time_in_hour_indx = df.index[df['Upload Time'].str.contains('hour')]
for x in time_in_hour_indx:
    df.at[x,'Upload Time'] = df.at[x,'Upload Time'].split(' ')[1]

### Changing datatype of Upload time column to numeric
df['Upload Time'].astype('float')

### Removing $ from New Price and Original Price columns
df['New Price'] = df['New Price'].str.replace('$','')
df['Original Price'] = df['Original Price'].str.replace('$','')

# Replacing Free with 0 in New Price column or in Original Price Column
df['New Price'] = df['New Price'].str.replace('Free','0')
df['Original Price'] = df['Original Price'].str.replace('Free','0')

#Getting the indexes of the rows with duplicate values and removing them
duplicate_indexes = df.index[df['Product'].duplicated(keep='first')]
for index in duplicate_indexes:
    df.drop(index, inplace=True)

### Creating a Product_data DF
product_data_df = df[['Product','Product image URL','Hot Favourite']]
product_data_df.loc[:,'ProductID'] = range(len(product_data_df))


### Creating Store_data DF
store_data_df = pd.DataFrame()
store_data_df['Store Name'] = df['Store Name'].unique()
store_data_df.loc[:,'StoreID'] = range(len(store_data_df))


### Creating deals df
deals_df = df[['Product','New Price','Original Price','Upload Time','Store Name']]
#Mapping product and store IDs
deals_df = deals_df.merge(product_data_df, left_on='Product',right_on='Product')
deals_df = deals_df.merge(store_data_df, left_on='Store Name',right_on='Store Name')
deals_df = deals_df[['ProductID','New Price','Original Price','Upload Time','StoreID']]

product_data_df.to_csv('s3://slick-deals-product-data-raw/transformed-data/dimension-tables/product-table/',index=False)
store_data_df.to_csv('s3://slick-deals-product-data-raw/transformed-data/dimension-tables/store-table/', index=False)
deals_df.to_csv('s3://slick-deals-product-data-raw/transformed-data/fact-tables/deals-table/', index=False)