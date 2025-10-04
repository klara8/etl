import pandas as pd
from tabulate import tabulate

csv_file = "/Users/klarakopecka/ETL_project/etl/dataL/amazon.csv"

def extract(csv_file):
    df_sales = pd.read_csv(csv_file)
    return df_sales

df_sales = extract(csv_file)
#print(df_sales.head())
#df_sales.head().to_csv("datal.csv", index=False)
#print(tabulate(df_sales.head(1), headers="keys", tablefmt="psql"))


def transform(df_sales):
    columns_to_keep = df_sales.loc[:,["product_id","product_name","category","discounted_price","actual_price","discount_percentage","rating","rating_count"]]
    price_columns = ["discounted_price","actual_price"]

    for col in price_columns:
        columns_to_keep[col] = (
            columns_to_keep[col]
            .astype(str)
            .str.replace("₹", "", regex=False)
            .str.replace(",","", regex=False)
            .str.strip()
        )
        columns_to_keep[col] = pd.to_numeric(columns_to_keep[col], errors="coerce")

    deduplicated = columns_to_keep.drop_duplicates()

    missing = deduplicated.isnull().sum()
    missing_percent = (missing/len(deduplicated)) * 100

    col_to_drop_na = missing_percent[missing_percent < 5].index.tolist()
    
    if col_to_drop_na:
        dropped = deduplicated.dropna(subset=col_to_drop_na)
    else:
        dropped = deduplicated
        print("more than 5% to drop/fill")

    return dropped
    
    

df_clean = transform(df_sales)
    
#print(tabulate(df_clean.head(1), headers="keys", tablefmt="psql")) 
print(df_clean.info())
print(df_clean.isnull().sum())

#print(df_clean.duplicated())
#print(df_clean.duplicated().sum())

print(df_clean[df_clean["discounted_price"] < 0 ])
