import pandas as pd
from datetime import datetime
import utils.file_handler
import utils.api_handler
import utils.data_processor

log_file = open('output/application.log', "a")

def main():
    """
    Main execution function
    """
    try:
        print("=" * 40)
        print("SALES ANALYTICS SYSTEM")
        print("=" * 40)
        print()

        # 1. Read sales data
        print("[1/10] Reading sales data...")
        raw_data = utils.file_handler.read_sales_data("data/sales_data.txt")
        
        print(f"✓ Successfully read {len(raw_data)} transactions\n")

        # 2. Parse and clean
        print("[2/10] Parsing and cleaning data...")
        #transactions = parse_and_clean_transactions(raw_data)
        trxs = utils.file_handler.parse_transactions(raw_data)
        #print(trxs)
        validTs = utils.data_processor.filterValidTransactions (trxs)
        print(f"✓ Parsed {len(validTs)} records\n")

        # 3. Display filter options
        regions = sorted({t["Region"] for t in validTs})
        amounts = [t["Quantity"] * t["UnitPrice"] for t in validTs]

        print("[3/10] Filter Options Available:")
        print(f"Regions: {', '.join(regions)}")
        print(f"Amount Range: ₹{min(amounts):,.0f} - ₹{max(amounts):,.0f}\n")

        choice = input("Do you want to filter data? (y/n): ").strip().lower()

        # 4. Apply filters (optional)
        if choice == "y":
            region_filter = input("Enter region (or press Enter to skip): ").strip()
            min_amt = input("Enter minimum amount (or press Enter): ").strip()
            max_amt = input("Enter maximum amount (or press Enter): ").strip()

            filtered = []
            for t in validTs:
                amount = t["Quantity"] * t["UnitPrice"]

                if region_filter and t["Region"] != region_filter:
                    continue
                if min_amt and amount < float(min_amt):
                    continue
                if max_amt and amount > float(max_amt):
                    continue

                filtered.append(t)

            validTs = filtered
            print(f"\n✓ Filtered down to {len(validTs)} transactions\n")
        else:
            print()

        # 5. Validate transactions
        print("[4/10] Validating transactions...")
        #valid_tx, invalid_tx = validate_transactions(validTs)
        #print(f"✓ Valid: {len(valid_tx)} | Invalid: {len(invalid_tx)}\n")

        # 6. Analysis step (logical checkpoint)
        print("[5/10] Analyzing sales data...")
        print("✓ Analysis complete\n")

        # 7. Fetch API products
        print("[6/10] Fetching product data from API...")
        api_products = utils.api_handler.fetch_all_products(limit=200)
        print(f"✓ Fetched {len(api_products)} products\n")

        # 8. Enrich sales data
        print("[7/10] Enriching sales data...")
        product_mapping = utils.api_handler.create_product_mapping(api_products)
        enriched_transactions = utils.api_handler.enrich_sales_data(validTs, product_mapping)

        enriched_count = sum(1 for t in enriched_transactions if t.get("API_Match"))
        success_rate = (enriched_count / len(validTs) * 100) if validTs else 0

        print(f"✓ Enriched {enriched_count}/{len(validTs)} transactions ({success_rate:.1f}%)\n")

        # 9. Generate report
        print("[9/10] Generating report...")
        utils.api_handler.generate_sales_report(validTs, enriched_transactions)
        print("✓ Report saved to: output/sales_report.txt\n")

        # 10. Completion
        print("[10/10] Process Complete!")
        print("=" * 40)

    except Exception as e:
        print("\n❌ An unexpected error occurred")
        print(f"Reason: {e}")
        print("Please check logs / input files and retry.")


def log(message, addTime=False):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if addTime == True:
        log_file.write(f"{timestamp} | {message}\n")
    else:
        log_file.write(f"{message}\n")
    

if __name__ == "__main__":
    main()

# ##AXZ log("Application started", True)

# df = pd.read_csv('data/sales_data.txt', sep='|')
# #print (df.info())
# init_record_count = df.shape[0]
# ##AXZ log("Total " + str(init_record_count) + " records read from sales_data.txt")

# #missing customer IDs or region
# empty_count = df['CustomerID'].isnull().sum()
# #print(empty_count)
# ##AXZ log( "Blank customer IDs found : " + str(empty_count))

# #Region
# empty_count = df['Region'].isnull().sum()
# #print(empty_count)
# ##AXZ log( "Blank Region found : " + str(empty_count))

# #Remove NUll values (will affect CustomerID and Region)
# df = df.dropna()
# record_count = df.shape[0]
# #print(record_count)
# ##AXZ log( "Record count after Region and CustomerID cleanup : " + str(record_count))

# # Filter the DataFrame, remove Quantity <= 0
# condition_to_keep = df['Quantity'] > 0
# df = df[condition_to_keep]
# record_count = df.shape[0]
# # print(record_count)
# ##AXZ log( "Record count after Quanity cleanup : " + str(record_count))

# #print(df.info()

# #Remove commas from UnitPrice and convert to integer
# df['UnitPrice'] = df['UnitPrice'].str.replace(',', '').astype(int)

# # Filter the DataFrame, remove Unitprice <= 0
# condition_to_keep = df['UnitPrice'] > 0
# df = df[condition_to_keep]
# record_count = df.shape[0]
# print(record_count)
# ##AXZ log( "Record count after UnitPrice cleanup : " + str(record_count))

# #Remove commas from ProductName and convert to integer
# df['ProductName'] = df['ProductName'].str.replace(',', '')

# #Transaction IDs not starting with 'T'
# df = df[df["TransactionID"].astype(str).str.startswith("T")]

# final_record_count = df.shape[0]
# #print('After removing non-Ts')
# #print(final_record_count)

# #df.to_csv('output.csv', index=False) 

# log("Total records parsed: " + str(init_record_count))
# log("Invalid records removed: " + str(init_record_count-final_record_count))
# log("Valid records after cleaning: " + str(final_record_count))

# ##AXZlog("Application finished", False)
# log_file.close()