# =================================================================
# Title: Extract Script for Retail Analytics Pipeline
# Description: Reads retail sales data from a CSV file and loads it into a pandas DataFrame.
# Author: Yugandhar Ailuri
# Date: 2025-10-14
# =================================================================

# Testing the extract module
import pandas as pd
import os

RAW_DATA_PATH = r"c:/Users/YUGANDHAR REDDY/OneDrive/Desktop/retail_analytics_pipeline/data/raw"
# List of CSV files to be read
FILES = ["Features.csv", "Sales.csv", "Stores.csv"]


def load_data(file_name):
    " Loading a single CSV file into a pandas DataFrame "
    path = os.path.join(RAW_DATA_PATH, file_name)
    print(f"Loading file: {file_name}")
    df = pd.read_csv(path)
    print(f"Loaded {file_name} - shape: {df.shape}")
    return df


def main():
    # Read all CSVs
    features_df = load_data("Features.csv")
    sales_df = load_data("Sales.csv")
    stores_df = load_data("Stores.csv")

    # Display of head of each dataframe
    print("\n Features Sample:")
    print(features_df.head())

    print("\n Sales Sample:")
    print(sales_df.head())

    print("\n Stores Sample:")
    print(stores_df.head())

    # missing value check
    print("\n Missing values summary:")
    for name, df in zip(FILES, [features_df, sales_df, stores_df]):
        print(f"{name}: {df.isnull().sum().sum()} missing values")


if __name__ == "__main__":
    main()
