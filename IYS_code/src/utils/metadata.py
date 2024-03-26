import os
import pandas as pd
import nexussdk as nexus
import requests
import datetime
from requests.exceptions import HTTPError

def generate_dats_metadata(dataframes, csv_info_df):
    dats_metadata = []

    for df_name, df in dataframes.items():
        # Find the corresponding row in csv_info_df
        csv_info = csv_info_df[csv_info_df['filename'] == df_name + '.csv'].iloc[0]

        # Extract metadata
        file_metadata = {
            'title': df_name,
            'size': csv_info['filesize'],
            'formats': ['CSV'],
            'rows': len(df),
            'accessURL': csv_info['s'],
            'format': 'CSV',
            'size': csv_info['filesize'],
            'columns': list(df.columns)
            
            }
            # Additional fields can be added here as per DATS specifications

            
        dats_metadata.append(file_metadata)
        # Convert the list of dictionaries to a DataFrame
    metadata_df = pd.DataFrame(dats_metadata)
    return metadata_df
