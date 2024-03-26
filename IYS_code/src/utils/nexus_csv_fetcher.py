import os
import pandas as pd
import nexussdk as nexus
import requests
import datetime
from requests.exceptions import HTTPError

from src.utils import NexusSparqlQuery as qns

def fetch_csv_files(sparqlview_wrapper, org, project):
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    data_folder = f'./{org}_{project}_data_{current_date}/'
    #print(f"Creating data folder: {data_folder}")  # Debug print

    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        #print(f"Folder created: {data_folder}")  # Debug print
    else:
        pass #print(f"Folder already exists: {data_folder}")  # Debug print

        # SPARQL query to fetch file names
    csv_names_query = '''
    prefix nxv: <https://bluebrain.github.io/nexus/vocabulary/>
    SELECT DISTINCT ?s ?self ?filename ?filesize
    WHERE {
      ?s nxv:self ?self . 
      ?s a nxv:File .
      ?s nxv:bytes ?filesize .
      ?s nxv:filename ?filename .
    }
    '''

    df = qns.sparql2dataframe(qns.query_sparql(csv_names_query, sparqlview_wrapper))

    for index, row in df.iterrows():
        file_url = row['s']
        filename = row['filename']
        out_filepath = os.path.join(data_folder, filename)
        #print(f"Downloading file: {filename} to {out_filepath}")  # Debug print

        try:
            nexus.files.fetch(org, project, file_url, out_filepath=out_filepath)
            #print(f"Downloaded file: {filename} (File ID: {file_url})")
        except HTTPError as e:
            pass
            #print(f"Error fetching file: {filename} (File ID: {file_url})")
            #print(f"Error details: {e}")

    return data_folder

def load_dataframes(data_folder):
    csv_files = [f for f in os.listdir(data_folder) if f.endswith('.csv')]
    dataframes = {}

    for csv_file in csv_files:
        df_name = os.path.splitext(os.path.basename(csv_file))[0].replace(" ", "_")
        df = pd.read_csv(os.path.join(data_folder, csv_file))
        dataframes[df_name] = df

    return dataframes
