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
    #vybrani sloupcu, ktere budu vyiuzivat
    columns_to_keep = df_sales.loc[:,["product_id","product_name","category","discounted_price","actual_price","discount_percentage","rating","rating_count"]]
    price_columns = ["discounted_price","actual_price","discount_percentage"]
    #ocisteni a zmena typu na cislo cenovych sloupcu
    for col in price_columns:
        columns_to_keep[col] = (
            columns_to_keep[col]
            .astype(str)
            .str.replace("₹", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.replace(",","", regex=False)
            .str.strip()
        )
        columns_to_keep[col] = pd.to_numeric(columns_to_keep[col], errors="coerce")
 
    #dropnuti radku s chybejicimi hodnotami pokud jich je mene nez 5% z datasetu
    missing = columns_to_keep.isnull().sum()
    missing_percent = (missing/len(columns_to_keep)) * 100

    col_to_drop_na = missing_percent[missing_percent < 5].index.tolist()
    
    if col_to_drop_na:
        dropped = columns_to_keep.dropna(subset=col_to_drop_na)
    else:
        dropped = columns_to_keep
        print("more than 5% to drop/fill")

    dropped.loc[dropped["discount_percentage"] >= 0, "disc_label"] = "not that great discount"    
    dropped.loc[dropped["discount_percentage"] >= 30, "disc_label"] = "ok discount"    
    dropped.loc[dropped["discount_percentage"] >= 60, "disc_label"] = "good discount"
    dropped.loc[dropped["discount_percentage"] >= 80, "disc_label"] = "great discount"



    return dropped
    
    

df_clean = transform(df_sales)
    
#print(tabulate(df_clean.head(1), headers="keys", tablefmt="psql")) 
#print(df_clean.info())
#print(df_clean.isnull().sum())

#print(df_clean.duplicated())
#print(df_clean.duplicated().sum())

#print(df_clean[df_clean["discounted_price"] < 0 ])

def load(df_clean):
    df_clean.to_csv("/Users/klarakopecka/ETL_project/etl/dataL/dataL.csv")

load(df_clean)