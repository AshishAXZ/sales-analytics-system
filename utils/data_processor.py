
import file_handler
import json

def filterValidTransactions(transactions):
    invalid_count = 0
    required_fields = {
    "TransactionID", "Date", "ProductID", "ProductName",
    "Quantity", "UnitPrice", "CustomerID", "Region"
    }

    valid_transactions = []
    invalid_count = 0

    for txn in transactions:
        # Check required fields
        if not required_fields.issubset(txn.keys()):
            invalid_count += 1
            continue

        # Apply validation rules
        if (
            txn["Quantity"] <= 0 or
            txn["UnitPrice"] <= 0 or
            not txn["TransactionID"].startswith("T") or
            not txn["ProductID"].startswith("P") or
            not txn["CustomerID"].startswith("C")
        ):
            invalid_count += 1
            continue

        # If all checks pass → valid record
        valid_transactions.append(txn)
    return valid_transactions

def calculate_total_revenue(transactions):
    """
    Task 2.1 Sales Summary Calculator
    Calculates total revenue from all transactions

    Returns: float (total revenue)

    Expected Output: Single number representing sum of (Quantity * UnitPrice)
    Example: 1545000.50
    """
    invalid_count = 0
    header_fields = {
        "TransactionID", "Date", "ProductID", "ProductName", "Quantity", "UnitPrice", 
        "CustomerID", "Region"
    }
    totRevenue = 0.00
    for txn in a:
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

            revenue = txn["Quantity"] * txn["UnitPrice"]
            totRevenue = totRevenue + revenue
        except Exception as e:
            print(f"An error occurred: {e}")
    
    return totRevenue



def region_wise_sales(transactions):
    """
    Analyzes sales by region

    Returns: dictionary with region statistics

    Expected Output Format:
    {
        'North': {
            'total_sales': 450000.0,
            'transaction_count': 15,
            'percentage': 29.13
        },
        'South': {...},
        ...
    }

    Requirements:
    - Calculate total sales per region
    - Count transactions per region
    - Calculate percentage of total sales
    - Sort by total_sales in descending order
    """
    #------------------------------
    fNorthPercent = 0
    fSouthPercent = 0
    fEastPercent = 0
    fWestPercent = 0
    #------------------------------
    iNorthRev = 0
    iSouthRev = 0
    iEastRev = 0
    iWestRev = 0
    #------------------------------
    iNorthCount = 0
    iSouthCount = 0
    iEastCount = 0
    iWestCount = 0
    
    invalid_count = 0
    header_fields = {
        "TransactionID", "Date", "ProductID", "ProductName", "Quantity", "UnitPrice", 
        "CustomerID", "Region"
    }

    # North = 0
    # South = 1
    # East  = 2
    # West  = 3

    #header_return = {"Region", 'total_sales','transaction_count','percentage' }

    totRevenue = 0.00
    for txn in a:
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
            
            revenue = txn["Quantity"] * txn["UnitPrice"]
            totRevenue = totRevenue + revenue
            
            match txn["Region"]:
                case "North":
                    iNorthRev = iNorthRev + revenue
                    iNorthCount += 1
                case "South":
                    iSouthRev = iSouthRev + revenue
                    iSouthCount += 1
                case "East" :
                    iEastRev = iEastRev + revenue
                    iEastCount += 1
                case "West" :
                    iWestRev = iWestRev + revenue
                    iWestCount += 1
        except Exception as e:
            print(f"An error occurred: {e}")

    fNorthPercent = 100 * iNorthRev / totRevenue
    fSouthPercent = 100 * iSouthRev / totRevenue
    fEastPercent = 100 * iEastRev / totRevenue
    fWestPercent = 100 * iWestRev / totRevenue

    region_summary = {
        "North": {
            "total_sales": float(iNorthRev),
            "transaction_count": int(iNorthCount),
            "percentage": round(fNorthPercent, 2)
        },
        "South": {
            "total_sales": float(iSouthRev),
            "transaction_count": int(iSouthCount),
            "percentage": round(fSouthPercent, 2)
        },
        "East": {
            "total_sales": float(iEastRev),
            "transaction_count": int(iEastCount),
            "percentage": round(fEastPercent, 2)
        },
        "West": {
            "total_sales": float(iWestRev),
            "transaction_count": int(iWestCount),
            "percentage": round(fWestPercent, 2)
        }
    }

    region_summary = dict(
        sorted(
            region_summary.items(),
            key=lambda item: item[1]["total_sales"],
            reverse=True
        )
    )

    return region_summary
    
        

def top_selling_products(transactions, n=5):
    """
    Finds top n products by total quantity sold

    Returns: list of tuples

    Expected Output Format:
    [
        ('Laptop', 45, 2250000.0),  # (ProductName, TotalQuantity, TotalRevenue)
        ('Mouse', 38, 19000.0),
        ...
    ]

    Requirements:
    - Aggregate by ProductName
    - Calculate total quantity sold
    - Calculate total revenue for each product
    - Sort by TotalQuantity descending
    - Return top n products
    """
    valid_transactions = filterValidTransactions(transactions)
    #print (valid_transactions)
    #================================================
    product_summary = {}

    # -----------------------------
    # AGGREGATION PHASE
    # -----------------------------
    for txn in valid_transactions:
        product = txn["ProductName"]
        quantity = txn["Quantity"]
        revenue = txn["Quantity"] * txn["UnitPrice"]

        if product not in product_summary:
            product_summary[product] = {
                "total_quantity": 0,
                "total_revenue": 0.0
            }

        product_summary[product]["total_quantity"] += quantity
        product_summary[product]["total_revenue"] += revenue

    # -----------------------------
    # SORT BY TOTAL QUANTITY DESC
    # -----------------------------
    sorted_products = sorted(
        product_summary.items(),
        key=lambda item: item[1]["total_quantity"],
        reverse=True
    )

    # -----------------------------
    # BUILD FINAL OUTPUT (TOP N)
    # -----------------------------
    result = [
        (
            product,
            data["total_quantity"],
            round(data["total_revenue"], 2)
        )
        for product, data in sorted_products[:n]
    ]

    return result



def customer_analysis(transactions):
    """
    Analyzes customer purchase patterns
    Returns: dictionary of customer statistics

    Expected Output Format:
    {
        'C001': {
            'total_spent': 95000.0,
            'purchase_count': 3,
            'avg_order_value': 31666.67,
            'products_bought': ['Laptop', 'Mouse', 'Keyboard']
        },
        'C002': {...},
        ...
    }

    Requirements:
    - Calculate total amount spent per customer
    - Count number of purchases
    - Calculate average order value
    - List unique products bought
    - Sort by total_spent descending
    """
    valid_transactions = filterValidTransactions(transactions)
    #print (valid_transactions)

    customer_summary = {}

    # -----------------------------
    # AGGREGATION PHASE
    # -----------------------------
    for txn in valid_transactions:
        customer_id = txn["CustomerID"]
        amount = txn["Quantity"] * txn["UnitPrice"]
        product = txn["ProductName"]

        if customer_id not in customer_summary:
            customer_summary[customer_id] = {
                "total_spent": 0.0,
                "purchase_count": 0,
                "products_bought": set()  # use set to keep products unique
            }

        customer_summary[customer_id]["total_spent"] += amount
        customer_summary[customer_id]["purchase_count"] += 1
        customer_summary[customer_id]["products_bought"].add(product)

    # -----------------------------
    # POST-PROCESSING
    # -----------------------------
    for customer_id, data in customer_summary.items():
        data["avg_order_value"] = round(
            data["total_spent"] / data["purchase_count"], 2
        )
        data["products_bought"] = sorted(list(data["products_bought"]))

    # -----------------------------
    # SORT BY TOTAL_SPENT DESC
    # -----------------------------
    sorted_summary = dict(
        sorted(
            customer_summary.items(),
            key=lambda item: item[1]["total_spent"],
            reverse=True
        )
    )

    return sorted_summary


def daily_sales_trend(transactions):
    """
    Analyzes sales trends by date

    Returns: dictionary sorted by date

    Expected Output Format:
    {
        '2024-12-01': {
            'revenue': 125000.0,
            'transaction_count': 8,
            'unique_customers': 6
        },
        '2024-12-02': {...},
        ...
    }

    Requirements:
    - Group by date
    - Calculate daily revenue
    - Count daily transactions
    - Count unique customers per day
    - Sort chronologically
    """
    valid_transactions = filterValidTransactions(transactions)
    #print (valid_transactions)

    date_summary = {}

    # -----------------------------
    # AGGREGATION PHASE
    # -----------------------------
    for txn in valid_transactions:
        txnDate = txn["Date"]
        amount = txn["Quantity"] * txn["UnitPrice"]
        customer = txn["CustomerID"]
        

        if txnDate not in date_summary:
            date_summary[txnDate] = {
                "revenue": 0.0,
                "transaction_count": 0,
                "unique_customers": set()  # use set to keep products unique
            }

        date_summary[txnDate]["revenue"] += amount
        date_summary[txnDate]["transaction_count"] += 1
        date_summary[txnDate]["unique_customers"].add(customer)

    # -----------------------------
    # POST-PROCESSING
    # -----------------------------
    for txnDate, data in date_summary.items():
        data["unique_customers"] = len(data["unique_customers"])
    #print(date_summary)
    # -----------------------------
    # SORT BY DATE ASC
    # -----------------------------
    sorted_summary = dict(
        sorted(
            date_summary.items(),
            key=lambda item: item[0]
        )
    )

    return sorted_summary

def find_peak_sales_day(transactions):
    """
    Identifies the date with highest revenue

    Returns: tuple (date, revenue, transaction_count)

    Expected Output Format:
    ('2024-12-15', 185000.0, 12)
    """
    valid_transactions = filterValidTransactions(transactions)

    daily_summary = {}
    # -----------------------------
    # AGGREGATION PHASE
    # -----------------------------
    for txn in transactions:
        txn_date = txn["Date"]
        amount = txn["Quantity"] * txn["UnitPrice"]

        if txn_date not in daily_summary:
            daily_summary[txn_date] = {
                "revenue": 0.0,
                "transaction_count": 0
            }

        daily_summary[txn_date]["revenue"] += amount
        daily_summary[txn_date]["transaction_count"] += 1

    #print(daily_summary)
    
    # -----------------------------
    # FIND PEAK SALES DAY
    # -----------------------------
    peak_date, peak_data = max(
        daily_summary.items(),
        key=lambda item: item[1]["revenue"]
    )

    return (
        peak_date,
        round(peak_data["revenue"], 2),
        peak_data["transaction_count"]
    )


def low_performing_products(transactions, threshold=10):
    """
    Identifies products with low sales
    Returns: list of tuples
    Expected Output Format:
    [
        ('Webcam', 4, 12000.0),  # (ProductName, TotalQuantity, TotalRevenue)
        ('Headphones', 7, 10500.0),
        ...
    ]
    Requirements:
    - Find products with total quantity < threshold
    - Include total quantity and revenue
    - Sort by TotalQuantity ascending
    """

    valid_transactions = filterValidTransactions(transactions)
    
    product_summary = {}

    # -----------------------------
    # AGGREGATION PHASE
    # -----------------------------
    for txn in valid_transactions:
        product = txn["ProductName"]
        quantity = txn["Quantity"]
        revenue = txn["Quantity"] * txn["UnitPrice"]

        if product not in product_summary:
            product_summary[product] = {
                "total_quantity": 0,
                "total_revenue": 0.0
            }

        product_summary[product]["total_quantity"] += quantity
        product_summary[product]["total_revenue"] += revenue
    
    # -----------------------------
    # FILTER LOW-PERFORMING PRODUCTS
    # -----------------------------
    low_products = [
        (
            product,
            data["total_quantity"],
            round(data["total_revenue"], 2)
        )
        for product, data in product_summary.items()
        if data["total_quantity"] < threshold
    ]

    # -----------------------------
    # SORT BY TOTAL QUANTITY ASC
    # -----------------------------
    low_products.sort(key=lambda item: item[1])

    return low_products



import json
print('start')
lines = file_handler.read_sales_data('data/sales_data.txt')
#print(lines)
a = file_handler.parse_transactions(lines)
#print(a)

# ========================================
#totRevenue = calculate_total_revenue(a)
#print(totRevenue)
# ========================================
#b = top_selling_products(a)
#print (b)
# ========================================
# c= customer_analysis(a)
# #print (c)
# pretty_json = json.dumps(c, indent=4)
# print(pretty_json)
# ========================================
#d=daily_sales_trend(a)
#pretty_json = json.dumps(d, indent=4)
#print(pretty_json)
# ========================================

# e=find_peak_sales_day(a)
# print(e)

f= low_performing_products(a)
#print (f)
# pretty_json = json.dumps(f, indent=4)
# print(pretty_json)