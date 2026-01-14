import requests
import json
import os
import re
import file_handler
import data_processor

# data['products'] contains list of all products
# data['total'] gives total count

def fetch_all_products():
    """ Task 3.1 Fetch all Products
    Fetches all products from DummyJSON API
    Returns: list of product dictionaries
    Expected Output Format:
    [
        {
            'id': 1,
            'title': 'iPhone 9',
            'category': 'smartphones',
            'brand': 'Apple',
            'price': 549,
            'rating': 4.69
        },
        ...
    ]

    Requirements:
    - Fetch all available products (use limit=100)
    - Handle connection errors with try-except
    - Return empty list if API fails
    - Print status message (success/failure)
    """
    params = {'limit': 300}
    url = 'https://dummyjson.com/products'
    
    try:
        response = requests.get(url, params=params, timeout=10)
        # Raise an exception for HTTP errors (4xx or 5xx)
        response.raise_for_status()
        # Parse JSON and extract the list of products
        data = response.json()
        products = data.get('products', [])
        
        result = []
        for p in products:
            result.append({
                "id": p.get("id"),
                "title": p.get("title"),
                "category": p.get("category"),
                "brand": p.get("brand"),
                "price": p.get("price"),
                "rating": p.get("rating")
            })

        print(f"Success: Fetched {len(result)} products.")
        return result

    except requests.exceptions.RequestException as e:
        # Handles connection errors, timeouts, and HTTP errors
        print(f"Failure: Could not fetch products. Error: {e}")
        return []

def create_product_mapping(api_products):
    """
    Creates a mapping of product IDs to product info

    Parameters: api_products from fetch_all_products()

    Returns: dictionary mapping product IDs to info

    Expected Output Format:
    {
        1: {'title': 'iPhone 9', 'category': 'smartphones', 'brand': 'Apple', 'rating': 4.69},
        2: {'title': 'iPhone X', 'category': 'smartphones', 'brand': 'Apple', 'rating': 4.44},
        ...
    }
    """
    product_mapping = {}

    if not api_products:
        print("No products received to create mapping")
        return product_mapping

    for product in api_products:
        product_id = int(product.get("id"))   #axz

        if product_id is None:
            continue  # skip invalid records

        product_mapping[product_id] = {
            "title": product.get("title"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "rating": product.get("rating")
        }

    print(f"Created product mapping for {len(product_mapping)} products")
    return product_mapping

def enrich_sales_data(transactions, product_mapping):
    """
    Enriches transaction data with API product information
    """
    enriched_transactions = []

    # Ensure output directory exists
    os.makedirs("data", exist_ok=True)
    output_file = "data/enriched_sales_data.txt"

    for tx in transactions:
        enriched_tx = tx.copy()

        try:
            product_id_raw = tx.get("ProductID", "")

            # Extract numeric ID (P101 -> 101, P5 -> 5)
            match = re.search(r"\d+", product_id_raw)
            numeric_id = int(match.group()) if match else None
            #print(numeric_id)
            product_info = product_mapping.get(numeric_id)
            
            if product_info:
                enriched_tx.update({
                    "API_Category": product_info.get("category"),
                    "API_Brand": product_info.get("brand"),
                    "API_Rating": product_info.get("rating"),
                    "API_Match": True
                })
            else:
                enriched_tx.update({
                    "API_Category": None,
                    "API_Brand": None,
                    "API_Rating": None,
                    "API_Match": False
                })

        except Exception:
            enriched_tx.update({
                "API_Category": None,
                "API_Brand": None,
                "API_Rating": None,
                "API_Match": False
            })

        enriched_transactions.append(enriched_tx)

    # Write to pipe-delimited file
    if enriched_transactions:
        headers = enriched_transactions[0].keys()

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("|".join(headers) + "\n")
            for row in enriched_transactions:
                f.write("|".join(str(row.get(h, "")) for h in headers) + "\n")

    print(f"✅ Enriched {len(enriched_transactions)} transactions")
    print(f"📄 Output saved to {output_file}")

    return enriched_transactions


def save_enriched_data(enriched_transactions, filename='data/enrhd_sales_data.txt'):
    """
    Saves enriched transactions back to file

    Expected File Format:
    TransactionID|Date|ProductID|ProductName|Quantity|UnitPrice|CustomerID|Region|API_Category|API_Brand|API_Rating|API_Match
    T001|2024-12-01|P101|Laptop|2|45000.0|C001|North|laptops|Apple|4.7|True
    ...

    Requirements:
    - Create output file with all original + new fields
    - Use pipe delimiter
    - Handle None values appropriately
    """
    # Ensure output directory exists
    output_file = filename
    # Write to pipe-delimited file
    if enriched_transactions:
        headers = enriched_transactions[0].keys()

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("|".join(headers) + "\n")
            for row in enriched_transactions:
                f.write("|".join(str(row.get(h, "")) for h in headers) + "\n")

    print(f"📄 Output saved to {output_file}")


from datetime import datetime
from collections import defaultdict, Counter

def generate_sales_report(transactions, enriched_transactions, output_file='output/sales_report.txt'):
    # -------------------------------
    # Helper functions
    # -------------------------------
    def fmt_currency(value):
        return f"₹{value:,.2f}"

    def parse_date(d):
        return datetime.strptime(d, "%Y-%m-%d")

    # -------------------------------
    # Basic metrics
    # -------------------------------
    total_transactions = len(transactions)
    total_revenue = sum(t["Quantity"] * t["UnitPrice"] for t in transactions)
    avg_order_value = total_revenue / total_transactions if total_transactions else 0

    dates = [parse_date(t["Date"]) for t in transactions]
    min_date = min(dates).date()
    max_date = max(dates).date()

    # -------------------------------
    # Region-wise performance
    # -------------------------------
    region_sales = defaultdict(float)
    region_tx_count = defaultdict(int)

    for t in transactions:
        revenue = t["Quantity"] * t["UnitPrice"]
        region_sales[t["Region"]] += revenue
        region_tx_count[t["Region"]] += 1

    region_summary = sorted(
        region_sales.items(),
        key=lambda x: x[1],
        reverse=True
    )

    # -------------------------------
    # Top 5 products
    # -------------------------------
    product_qty = defaultdict(int)
    product_rev = defaultdict(float)

    for t in transactions:
        product_qty[t["ProductName"]] += t["Quantity"]
        product_rev[t["ProductName"]] += t["Quantity"] * t["UnitPrice"]

    top_products = sorted(
        product_rev.items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]

    # -------------------------------
    # Top 5 customers
    # -------------------------------
    customer_spend = defaultdict(float)
    customer_orders = defaultdict(int)

    for t in transactions:
        customer_spend[t["CustomerID"]] += t["Quantity"] * t["UnitPrice"]
        customer_orders[t["CustomerID"]] += 1

    top_customers = sorted(
        customer_spend.items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]

    # -------------------------------
    # Daily sales trend
    # -------------------------------
    daily_rev = defaultdict(float)
    daily_tx = defaultdict(int)
    daily_customers = defaultdict(set)

    for t in transactions:
        daily_rev[t["Date"]] += t["Quantity"] * t["UnitPrice"]
        daily_tx[t["Date"]] += 1
        daily_customers[t["Date"]].add(t["CustomerID"])

    daily_dates = sorted(daily_rev.keys())

    # -------------------------------
    # Product performance analysis
    # -------------------------------
    best_day = max(daily_rev.items(), key=lambda x: x[1])

    low_perf_products = [
        p for p, qty in product_qty.items() if qty < 5
    ]

    avg_tx_region = {
        r: region_sales[r] / region_tx_count[r]
        for r in region_sales
    }

    # -------------------------------
    # API enrichment summary
    # -------------------------------
    enriched = [t for t in enriched_transactions if t.get("API_Match")]
    failed = [t for t in enriched_transactions if not t.get("API_Match")]

    success_rate = (len(enriched) / len(enriched_transactions) * 100) if enriched_transactions else 0

    failed_products = sorted(
        {t["ProductID"] for t in failed}
    )

    # -------------------------------
    # Write report
    # -------------------------------
    with open(output_file, "w", encoding="utf-8") as f:

        # HEADER
        f.write("=" * 44 + "\n")
        f.write("           SALES ANALYTICS REPORT\n")
        f.write(f"     Generated: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
        f.write(f"     Records Processed: {total_transactions}\n")
        f.write("=" * 44 + "\n\n")

        # OVERALL SUMMARY
        f.write("OVERALL SUMMARY\n")
        f.write("-" * 44 + "\n")
        f.write(f"Total Revenue:        {fmt_currency(total_revenue)}\n")
        f.write(f"Total Transactions:   {total_transactions}\n")
        f.write(f"Average Order Value:  {fmt_currency(avg_order_value)}\n")
        f.write(f"Date Range:           {min_date} to {max_date}\n\n")

        # REGION-WISE PERFORMANCE
        f.write("REGION-WISE PERFORMANCE\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Region':<10}{'Sales':<15}{'% of Total':<15}{'Tx Count'}\n")
        for region, sales in region_summary:
            pct = (sales / total_revenue) * 100 if total_revenue else 0
            f.write(f"{region:<10}{fmt_currency(sales):>13}{pct:>7.2f}%      {region_tx_count[region]}\n")
        f.write("\n")

        # TOP 5 PRODUCTS
        f.write("TOP 5 PRODUCTS\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Rank':<6}{'Product':<20}{'Qty':<8}{'Revenue'}\n")
        for i, (prod, rev) in enumerate(top_products, 1):
            f.write(f"{i:<6}{prod:<20}{product_qty[prod]:<8}{fmt_currency(rev)}\n")
        f.write("\n")

        # TOP 5 CUSTOMERS
        f.write("TOP 5 CUSTOMERS\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Rank':<6}{'Customer':<12}{'Spent':<15}{'Orders'}\n")
        for i, (cust, spent) in enumerate(top_customers, 1):
            f.write(f"{i:<6}{cust:<12}{fmt_currency(spent):<15}{customer_orders[cust]}\n")
        f.write("\n")

        # DAILY SALES TREND
        f.write("DAILY SALES TREND\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Date':<12}{'Revenue':<15}{'Tx':<6}{'Customers'}\n")
        for d in daily_dates:
            f.write(
                f"{d:<12}{fmt_currency(daily_rev[d]):<15}"
                f"{daily_tx[d]:<6}{len(daily_customers[d])}\n"
            )
        f.write("\n")

        # PRODUCT PERFORMANCE ANALYSIS
        f.write("PRODUCT PERFORMANCE ANALYSIS\n")
        f.write("-" * 44 + "\n")
        f.write(f"Best Selling Day: {best_day[0]} ({fmt_currency(best_day[1])})\n")
        f.write("Low Performing Products:\n")
        for p in low_perf_products:
            f.write(f"  - {p}\n")
        f.write("Average Transaction Value per Region:\n")
        for r, v in avg_tx_region.items():
            f.write(f"  {r}: {fmt_currency(v)}\n")
        f.write("\n")

        # API ENRICHMENT SUMMARY
        f.write("API ENRICHMENT SUMMARY\n")
        f.write("-" * 44 + "\n")
        f.write(f"Total Products Enriched: {len(enriched)}\n")
        f.write(f"Success Rate: {success_rate:.2f}%\n")
        f.write("Products Not Enriched:\n")
        for p in failed_products:
            f.write(f"  - {p}\n")

    print(f"✅ Sales report generated at {output_file}")


AllProds = fetch_all_products()
#print(AllProds)

PrdMaps = create_product_mapping(AllProds)
#print(PrdMaps)


#print('start')
lines = file_handler.read_sales_data('data/sales_data.txt')
#print(lines)

trxs = file_handler.parse_transactions(lines)
#print(trxs)

validTs = data_processor.filterValidTransactions (trxs)

EnRchd = enrich_sales_data (validTs, PrdMaps)
#print(c) 
pretty_json = json.dumps(EnRchd, indent=4)
#print(pretty_json)

#### Saving enriched data is already being done in Function "enrich_sales_data"
#####a = save_enriched_data(EnRchd)

generate_sales_report ( validTs, EnRchd )
print("Done with text report")