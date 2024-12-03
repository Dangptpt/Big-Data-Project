import pandas as pd
from datetime import datetime

def clean_bicycle_data(df):
    df_clean = df.copy()
    
    datetime_columns = ['OCC_DATE', 'REPORT_DATE']
    for col in datetime_columns:
        df_clean[col] = pd.to_datetime(df_clean[col])
    
    df_clean['BIKE_COLOUR'] = df_clean['BIKE_COLOUR'].str.strip().str.upper()
    color_mapping = {
        'BLK': 'BLACK',
        'WHI': 'WHITE',
        'SILRED': 'SILVER RED',
        '': 'UNKNOWN'
    }
    df_clean['BIKE_COLOUR'] = df_clean['BIKE_COLOUR'].map(lambda x: color_mapping.get(x, x))
    
    df_clean['BIKE_COST'] = pd.to_numeric(df_clean['BIKE_COST'], errors='coerce')
    df_clean['BIKE_COST'] = df_clean.groupby('BIKE_TYPE')['BIKE_COST'].transform(
        lambda x: x.fillna(x.median())
    )
    df_clean['BIKE_COST'] = df_clean['BIKE_COST'].fillna(df_clean['BIKE_COST'].median())
    
    df_clean['BIKE_SPEED'] = pd.to_numeric(df_clean['BIKE_SPEED'], errors='coerce')
    df_clean['BIKE_SPEED'] = df_clean['BIKE_SPEED'].fillna(df_clean['BIKE_SPEED'].median())
    
    df_clean['BIKE_TYPE'] = df_clean['BIKE_TYPE'].fillna('UNKNOWN')
    bike_type_mapping = {
        'MT': 'MOUNTAIN',
        'RG': 'ROAD',
        'RC': 'RACING',
        'OT': 'OTHER'
    }
    df_clean['BIKE_TYPE'] = df_clean['BIKE_TYPE'].map(lambda x: bike_type_mapping.get(x, x))
    
    df_clean['LOCATION_TYPE'] = df_clean['LOCATION_TYPE'].fillna('UNKNOWN')
    df_clean['PREMISES_TYPE'] = df_clean['PREMISES_TYPE'].fillna('UNKNOWN')

    df_clean = df_clean[
        (df_clean['LAT_WGS84'].between(43.5, 44.0)) & 
        (df_clean['LONG_WGS84'].between(-80.0, -79.0))
    ]
    
    df_clean['REPORT_DELAY'] = (df_clean['REPORT_DATE'] - df_clean['OCC_DATE']).dt.total_seconds() / 3600  # Số giờ
    
    df_clean['BIKE_MAKE'] = df_clean['BIKE_MAKE'].str.strip().str.upper()
    df_clean['BIKE_MODEL'] = df_clean['BIKE_MODEL'].str.strip().str.upper()
    
    valid_statuses = ['STOLEN', 'RECOVERED']
    df_clean['STATUS'] = df_clean['STATUS'].apply(lambda x: x if x in valid_statuses else 'UNKNOWN')
    
    return df_clean

