import pandas as pd
from datetime import datetime

#  #   Column         Non-Null Count  Dtype 
# ---  ------         --------------  -----
#  0   TransactionID  80 non-null     object
#  1   Date           80 non-null     object
#  2   ProductID      80 non-null     object
#  3   ProductName    80 non-null     object
#  4   Quantity       80 non-null     int64
#  5   UnitPrice      80 non-null     object
#  6   CustomerID     78 non-null     object
#  7   Region         79 non-null     object


log_file = open('output/application.log', "a")

def log(message, addTime=False):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if addTime == True:
        log_file.write(f"{timestamp} | {message}\n")
    else:
        log_file.write(f"{message}\n")
    

##AXZ log("Application started", True)

df = pd.read_csv('data/sales_data.txt', sep='|')
#print (df.info())
init_record_count = df.shape[0]
##AXZ log("Total " + str(init_record_count) + " records read from sales_data.txt")

#missing customer IDs or region
empty_count = df['CustomerID'].isnull().sum()
#print(empty_count)
##AXZ log( "Blank customer IDs found : " + str(empty_count))

#Region
empty_count = df['Region'].isnull().sum()
#print(empty_count)
##AXZ log( "Blank Region found : " + str(empty_count))

#Remove NUll values (will affect CustomerID and Region)
df = df.dropna()
record_count = df.shape[0]
#print(record_count)
##AXZ log( "Record count after Region and CustomerID cleanup : " + str(record_count))

# Filter the DataFrame, remove Quantity <= 0
condition_to_keep = df['Quantity'] > 0
df = df[condition_to_keep]
record_count = df.shape[0]
# print(record_count)
##AXZ log( "Record count after Quanity cleanup : " + str(record_count))

#print(df.info()

#Remove commas from UnitPrice and convert to integer
df['UnitPrice'] = df['UnitPrice'].str.replace(',', '').astype(int)

# Filter the DataFrame, remove Unitprice <= 0
condition_to_keep = df['UnitPrice'] > 0
df = df[condition_to_keep]
record_count = df.shape[0]
print(record_count)
##AXZ log( "Record count after UnitPrice cleanup : " + str(record_count))

#Remove commas from ProductName and convert to integer
df['ProductName'] = df['ProductName'].str.replace(',', '')

#Transaction IDs not starting with 'T'
df = df[df["TransactionID"].astype(str).str.startswith("T")]

final_record_count = df.shape[0]
#print('After removing non-Ts')
#print(final_record_count)

#df.to_csv('output.csv', index=False) 

log("Total records parsed: " + str(init_record_count))
log("Invalid records removed: " + str(init_record_count-final_record_count))
log("Valid records after cleaning: " + str(final_record_count))







##AXZlog("Application finished", False)
log_file.close()