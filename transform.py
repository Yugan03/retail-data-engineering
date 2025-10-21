# ==============================================================
# Transformation Step - Retail Analytics Pipeline
# Description: Cleans and Merges retail sales data for analysis.
# Author: Yugandhar Ailuri
# Date: 2025-10-14
# ==============================================================

import os
import pandas as pd

RAW_DATA_PATH = r"c:/Users/YUGANDHAR REDDY/OneDrive/Desktop/retail_analytics_pipeline/data/raw"
PROCESSED_PATH = r"c:/Users/YUGANDHAR REDDY/OneDrive/Desktop/retail_analytics_pipeline/data/processed"


def load_data():
    """Loading all data sets"""
    features = pd.read_csv(os.path.join(RAW_DATA_PATH, "Features.csv"))
    sales = pd.read_csv(os.path.join(RAW_DATA_PATH, "Sales.csv"))
    stores = pd.read_csv(os.path.join(RAW_DATA_PATH, "Stores.csv"))
    return features, sales, stores


def clean_data(features, sales, stores):
    """Clean and merge datasets"""
    # Handle missing values
    features = features.fillna({
        "Temperature": features["Temperature"].median(),
        "Fuel_Price": features["Fuel_Price"].median(),
        "MarkDown1": features["MarkDown1"].median(),
        "MarkDown2": features["MarkDown2"].median(),
        "MarkDown3": features["MarkDown3"].median(),
        "MarkDown4": features["MarkDown4"].median(),
        "MarkDown5": features["MarkDown5"].median(),
        "CPI": features["CPI"].median(),
        "Unemployment": features["Unemployment"].median()
    })

    # Merge datasets
    merged_df = sales.merge(stores, on="Store", how="left")
    merged_df = merged_df.merge(
        features, on=["Store", "Date", "IsHoliday"], how="left")

    # Convert Date to datetime
    merged_df["Date"] = pd.to_datetime(merged_df["Date"], dayfirst=True)

    # Add new columns
    merged_df["Year"] = merged_df["Date"].dt.year
    merged_df["Month"] = merged_df["Date"].dt.month

    return merged_df


def save_data(df):
    """Saving the cleaned data to a CSV file"""
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    output_path = os.path.join(PROCESSED_PATH, "retail_clean.csv")
    df.to_csv(output_path, index=False)
    print(f"Clean dataset saved at: {output_path}")


def main():
    print("Transforming raw data")
    features, sales, stores = load_data()
    clean_df = clean_data(features, sales, stores)
    save_data(clean_df)
    print(f"Final shape: {clean_df.shape}")


if __name__ == "__main__":
    main()
