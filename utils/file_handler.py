"""
    Reads sales data from file handling encoding issues

    Returns: list of raw lines (strings)

    Expected Output Format:
    ['T001|2024-12-01|P101|Laptop|2|45000|C001|North', ...]

    Requirements:
    - Use 'with' statement
    - Handle different encodings (try 'utf-8', 'latin-1', 'cp1252')
    - Handle FileNotFoundError with appropriate error message
    - Skip the header row
    - Remove empty lines
"""

import pandas as pd
import json
from typing import Literal, Annotated

def read_sales_data(filename):
    """
    Task 1.1 Read Sales Data with Encoding Handling
    Reads sales data from file handling encoding issues

    Returns: list of raw lines (strings)

    Expected Output Format:
    ['T001|2024-12-01|P101|Laptop|2|45000|C001|North', ...]

    Requirements:
    - Use 'with' statement
    - Handle different encodings (try 'utf-8', 'latin-1', 'cp1252')
    - Handle FileNotFoundError with appropriate error message
    - Skip the header row
    - Remove empty lines
    """

    encodings = ["utf-8", "latin-1", "cp1252"]

    for encoding in encodings:
        try:
            with open(filename, mode="r", encoding=encoding) as file:
                lines = file.readlines()
                # Skip header and remove empty lines
                data_lines = [
                    line.strip()
                    for line in lines[1:]     # skip header
                    if line.strip()           # remove empty lines
                ]

                return data_lines

        except UnicodeDecodeError:
            # Try next encoding
            continue

        except FileNotFoundError:
            print(f"Error: File '{filename}' not found.")
            return []

    print("Error: Unable to read file with supported encodings.")
    return []

#df = pd.DataFrame(lines)
#record_count = df.shape[0]
#print(record_count)
#df.to_csv('outputL.csv', index=False)


def parse_transactions(raw_lines):
    """
    Task 1.2 Parse and Clean Data
    Parses raw lines into clean list of dictionaries

    Returns: list of dictionaries with keys:
    ['TransactionID', 'Date', 'ProductID', 'ProductName',
     'Quantity', 'UnitPrice', 'CustomerID', 'Region']

    Expected Output Format:
    [
        {
            'TransactionID': 'T001',
            'Date': '2024-12-01',
            'ProductID': 'P101',
            'ProductName': 'Laptop',
            'Quantity': 2,           # int type
            'UnitPrice': 45000.0,    # float type
            'CustomerID': 'C001',
            'Region': 'North'
        },
        ...
    ]

    Requirements:
    - Split by pipe delimiter '|'
    - Handle commas within ProductName (remove or replace)
    - Remove commas from numeric fields and convert to proper types
    - Convert Quantity to int
    - Convert UnitPrice to float
    - Skip rows with incorrect number of fields
    """


    sales_dict = {}
    values= []
    keys = ['TransactionID', 'Date', 'ProductID', 'ProductName', 'Quantity', 'UnitPrice', 'CustomerID', 'Region']
    #print(keys)
    # iterate thru the Lines
    for line in raw_lines:
        #print (line)
        listRec = line.split("|")
        #items from 0 to 7
        #print(listRec[7])
        strTransactionID    = listRec[0] 
        strDate             = listRec[1]         
        strProductID        = listRec[2]   
        strProductName      = str(listRec[3]).replace(',', '')
        strQuantity         = int(listRec[4])
        strUnitPrice        = float(str(listRec[5]).replace(',', ''))
        strCustomerID       = listRec[6] 
        strRegion           = listRec[7] 
        value = [strTransactionID,strDate, strProductID,strProductName, strQuantity, strUnitPrice,strCustomerID,strRegion ]
        if all(str(v).strip() != "" for v in value):
            values.append(value)
   
 
    sales_dict = [dict(zip(keys, value)) for value in values]
    return sales_dict
    #print (sales_dict)



def validate_and_filter(transactions, region: Literal['East', 'North', 'South', 'West']=None, 
                        min_amount: Annotated[int, "gt=500", "le=900000"]=None, max_amount=None):
    """
    type_: SimulationType = "solar"


    Task 1.3 Data Validation and Filtering
    Validates transactions and applies optional filters

    Parameters:
    - transactions: list of transaction dictionaries
    - region: filter by specific region (optional)
    - min_amount: minimum transaction amount (Quantity * UnitPrice) (optional)
    - max_amount: maximum transaction amount (optional)

    Returns: tuple (valid_transactions, invalid_count, filter_summary)

    Expected Output Format:
    (
        [list of valid filtered transactions],
        5,  # count of invalid transactions
        {
            'total_input': 100,
            'invalid': 5,
            'filtered_by_region': 20,
            'filtered_by_amount': 10,
            'final_count': 65
        }
    )

    Validation Rules:
    - Quantity must be > 0
    - UnitPrice must be > 0
    - All required fields must be present
    - TransactionID must start with 'T'
    - ProductID must start with 'P'
    - CustomerID must start with 'C'

    Filter Display:
    - Print available regions to user before filtering
    - Print transaction amount range (min/max) to user
    - Show count of records after each filter applied
    """

    header_fields = {
        "TransactionID", "Date", "ProductID", "ProductName", "Quantity", "UnitPrice", 
        "CustomerID", "Region"
    }

    valid_transactions = []
    invalid_count = 0

    for txn in transactions:
        try:
            # Required fields check
            if not header_fields.issubset(txn.keys()):
                invalid_count += 1
                continue

            # Field-level validation
            if (
                not txn["TransactionID"].startswith("T")
                or not txn["ProductID"].startswith("P")
                or not txn["CustomerID"].startswith("C")
                or txn["Quantity"] <= 0
                or txn["UnitPrice"] <= 0
            ):
                invalid_count += 1
                continue

            valid_transactions.append(txn)

        except Exception:
            invalid_count += 1

    # -----------------------------
    # DISPLAY AVAILABLE FILTER INFO
    # -----------------------------
    available_regions = sorted(
        {txn["Region"] for txn in valid_transactions}
    )
    #print("Available Regions:", available_regions)
    # available_regions = ['East', 'West', 'North', 'South']
    amounts = [
        txn["Quantity"] * txn["UnitPrice"]
        for txn in valid_transactions
    ]

    #if amounts:
        #print(f"Transaction Amount Range: "  f"{min(amounts)} to {max(amounts)}" )

    # -----------------------------
    # FILTERING PHASE
    # -----------------------------
    filter_summary = {
        "total_input": len(transactions),
        "invalid": invalid_count,
        "filtered_by_region": 0,
        "filtered_by_amount": 0,
        "final_count": 0
    }

    filtered_transactions = valid_transactions

    # Region filter
    if region:
        before = len(filtered_transactions)
        filtered_transactions = [
            txn for txn in filtered_transactions
            if txn["Region"] == region
        ]
        filter_summary["filtered_by_region"] = (
            before - len(filtered_transactions)
        )
        # print(
        #     f"Records after region filter ({region}): "
        #     f"{len(filtered_transactions)}"
        # )

    # Amount filter
    if min_amount is not None or max_amount is not None:
        before = len(filtered_transactions)

        def amount_in_range(txn):
            amount = txn["Quantity"] * txn["UnitPrice"]
            if min_amount is not None and amount < min_amount:
                return False
            if max_amount is not None and amount > max_amount:
                return False
            return True

        filtered_transactions = [
            txn for txn in filtered_transactions
            if amount_in_range(txn)
        ]

        filter_summary["filtered_by_amount"] = (
            before - len(filtered_transactions)
        )
        # print(
        #     "Records after amount filter:",
        #     len(filtered_transactions)
        # )

    filter_summary["final_count"] = len(filtered_transactions)

    return filtered_transactions, invalid_count, filter_summary


#print('start')
lines = read_sales_data('data/sales_data.txt')

a = parse_transactions(lines)
#print(a)

#pretty_json = json.dumps(a, indent=4)
#print(pretty_json)

b = validate_and_filter(a, min_amount=800000 )
#pretty_json = json.dumps(b, indent=4)
#print(pretty_json)