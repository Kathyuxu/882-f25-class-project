from datetime import datetime
from airflow.decorators import dag, task
import pandas as pd
import random

CLIENT_FILE = "https://storage.googleapis.com/qst-public/ba882/sales.xlsx"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lab6"],
)
def lab6():

    @task
    def extract():
        xls = pd.read_excel(CLIENT_FILE)
        print("Columns found:", xls.columns)
        return xls.columns.to_list()  # list of column names

    @task
    def print_info(col_names:list):
        for name in col_names:
            print(f"Found column: {name}")

    @task
    def summarize_orders():
        df = pd.read_excel(CLIENT_FILE, sheet_name="Orders")
        summary = df.groupby("Customer Segment")["Sales"].sum().reset_index()
        print(summary.head())
        # return all categories as a list for mapping
        return df["Product Category"].dropna().unique().tolist()

    @task
    def summarize_category(category: str):
        df = pd.read_excel(CLIENT_FILE, sheet_name="Orders")
        sub = df[df["Product Category"] == category]
        print(f"=== Category: {category} ===")
        print(f"Orders: {len(sub)}")
        print(f"Total Sales: {sub['Sales'].sum():,.2f}")
        print(f"Average Sales: {sub['Sales'].mean():,.2f}")

     # define the pipeline
    col_names = extract()
    pi = print_info(col_names=col_names)
    cats = summarize_orders()               
    pi >> cats                              
    summarize_category.expand(category=cats)
    
lab6()
