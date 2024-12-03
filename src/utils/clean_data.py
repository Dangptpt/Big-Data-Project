import pandas as pd
from datetime import datetime

def clean_bicycle_data(df):
    df_clean = df.copy()
    df_clean.loc[df_clean['PREMISES_TYPE'].isin(['Apartment', 'House']), 'PREMISES_TYPE'] = 'Residential'
    df_clean.loc[df_clean['PREMISES_TYPE'] == 'Transit', 'PREMISES_TYPE'] = 'Other'
    df_clean.loc[df_clean['PREMISES_TYPE'] == 'Educational', 'PREMISES_TYPE'] = 'Commercial'

    datetime_columns = ['OCC_DATE', 'REPORT_DATE']
    for col in datetime_columns:
        df_clean[col] = pd.to_datetime(df_clean[col])
    
    df_clean['BIKE_COST'] = pd.to_numeric(df_clean['BIKE_COST'], errors='coerce')
    df_clean['BIKE_COST'] = df_clean['BIKE_COST'].fillna(0)
    
    df_clean['BIKE_SPEED'] = pd.to_numeric(df_clean['BIKE_SPEED'], errors='coerce')
    df_clean['BIKE_SPEED'] = df_clean['BIKE_SPEED'].fillna(df_clean['BIKE_SPEED'].median())
    
    df_clean['BIKE_TYPE'] = df_clean['BIKE_TYPE'].fillna('UNKNOWN')
    
    df_clean['LOCATION_TYPE'] = df_clean['LOCATION_TYPE'].fillna('UNKNOWN')
    df_clean['PREMISES_TYPE'] = df_clean['PREMISES_TYPE'].fillna('Other')
    
    df_clean['REPORT_DELAY'] = (df_clean['REPORT_DATE'] - df_clean['OCC_DATE']).dt.total_seconds() / 3600
    
    df_clean['BIKE_MAKE'] = df_clean['BIKE_MAKE'].str.strip().str.upper()
    df_clean['BIKE_MODEL'] = df_clean['BIKE_MODEL'].str.strip().str.upper()
        
    return df_clean