#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Standard library imports
import json
import os
import hashlib
import uuid
import requests
from requests.auth import HTTPBasicAuth
from time import sleep
from datetime import datetime, timedelta
import configparser
import sqlite3
import random as rd
import getpass
import sys
from urllib.request import urlopen
from urllib.parse import quote 
from urllib.parse import urljoin
from dotenv import load_dotenv


# Third-party imports
import nexussdk as nexus
from SPARQLWrapper import JSON, POST
import pandas as pd
import numpy as np
from requests.adapters import HTTPAdapter, Retry
from fhirclient.models.fhirdate import FHIRDate


# In[ ]:


from base_dir import get_local_folder
rt_fldr = get_local_folder()
sys.path.append(rt_fldr)


# In[ ]:


from src.utils import common_utilities
from src.utils import NexusSparqlQuery as qns


# In[ ]:


from src.fhir_dataclasses.base import Base #, Patient, Encounter, Questionnaire,  QuestionnaireItem, QuestionnaireResponse, QuestionnaireResponseItem, AnswerOption
from src.fhir_dataclasses.patient import Patient
from src.fhir_dataclasses.encounter import Encounter
from src.fhir_dataclasses.questionnaire import Questionnaire
from src.fhir_dataclasses.questionnaire_item import QuestionnaireItem
from src.fhir_dataclasses.questionnaire_response import QuestionnaireResponse
from src.fhir_dataclasses.questionnaire_responseitem import QuestionnaireResponseItem
from src.fhir_dataclasses.answer_option import AnswerOption
from src.fhir_dataclasses.org import Organization


# In[ ]:


from src.dataframe_transformations.create_org_df import create_org_dataframe
from src.dataframe_transformations.patient_df import create_patient_df
from src.dataframe_transformations.study_df import create_research_study_df
from src.dataframe_transformations.questionnaire_df import create_questionnaire_df
from src.dataframe_transformations.questionnaire_item_df import create_questionnaire_item_df
from src.dataframe_transformations.answer_option_df import create_answer_option_df
from src.dataframe_transformations.encounter_df import create_encounter_df
from src.dataframe_transformations.questionnaire_response_df import create_questionnaire_response_df
from src.dataframe_transformations.questionnaire_response_item_df import create_questionnaire_response_item_df


# In[ ]:


from src.utils.nexus_config import initialize_nexus_connection


# In[ ]:


from src.utils.nexus_csv_fetcher import fetch_csv_files, load_dataframes
from src.utils.nexus_config import get_nexus_uri_base
from src.utils.batch_processor import process_dataframe_in_batches,process_batch
from src.utils.error_logging import log_errors_to_csv, read_error_file_and_fetch_records

from src.utils.common_utilities import generate_uri
from src.utils.common_utilities import gen_enc_uri
from src.utils.common_utilities import generate_uri2
from src.utils.common_utilities import generate_uri3
from src.utils.common_utilities import generate_uri4


# ## ACCESS RAW DATA FILES FROM RAW DATA FILES PROJECT

# In[ ]:


# load_dotenv('.env')  # specify path or just load_dotenv() if the file is named '.env' and in the same directory 
# token = os.getenv('token')


# In[ ]:


# Specify org here
org = ''


# In[ ]:


nexus_deployment = ''
project = ''

# Initialize Nexus Connection
sparql_client, metadata = initialize_nexus_connection(nexus_deployment, org, project, token)


# In[ ]:


# import the context needed for nexus
url = "https://bluebrainnexus.io/contexts/metadata.json"
response = urlopen(url)
metadata_dict = json.loads(response.read())


# In[ ]:


sparqlview_endpoint = f"{nexus_deployment}/views/{org}/{project}/graph/sparql"
sparqlview_wrapper = qns.create_sparql_client(sparql_endpoint=sparqlview_endpoint, token=token, http_query_method=POST, result_format=JSON)

data_folder = fetch_csv_files(sparqlview_wrapper, org, project)


# In[ ]:


dataframes = load_dataframes(data_folder)


# ### Generate NEXUS URI base

# In[ ]:


# GENERATE Nexus URI
project = f'{org}-fhir-data'
nexus_uri_base = get_nexus_uri_base(org)


# ## Dropping PHI if exist

# In[ ]:


from src.dataframe_transformations.reg_preprocesssing import filter_dataframe_columns


# In[ ]:


filter_dataframe_columns(dataframes, 'REGISTRATION', org)


# ## Create organization table

# In[ ]:


# Define the organization data - Please fill-n your organization data
org_data = {
    "org_id": [],
    "official_name": [""],
    "description": [""],
    "email": [""],
    "website_url": [""],
#    "org_type": ["Non-profit"],
    "province": [""]
}

# Create the organization dataframe
org_df = create_org_dataframe(org_data, nexus_uri_base, generate_uri2)


# 
# 
# ## Reserach Study Table

# In[ ]:


# Assuming 'dataframes' is your dictionary of DataFrames and 'org_df' contains the organization data
org_uri = org_df['org_uri'].iloc[0]  # Assuming the organization URI is in the first row of org_df
research_study = create_research_study_df(dataframes, org_uri)


# ## Patient DataFrame

# In[ ]:


registration_df = dataframes['REGISTRATION']
org_uri = org_df['org_uri'].unique()[0]
study_title = research_study['study_title'].unique()[0]

patient = create_patient_df(registration_df,org, org_uri, study_title, generate_uri3, nexus_uri_base)


# In[ ]:


patient.head()


# ## Questionnaire

# In[ ]:


DHM = dataframes['DHM']
questionnaire = create_questionnaire_df(DHM, org_uri, generate_uri, nexus_uri_base)


# ## Questionnaire Item Table

# In[ ]:


questionnaire_item= create_questionnaire_item_df(dataframes, questionnaire, generate_uri2, nexus_uri_base)


# ## Questionnaire Item Answer Option Table

# In[ ]:


answer_option = create_answer_option_df(questionnaire_item, dataframes, org, generate_uri3, nexus_uri_base)


# ## Encounter

# In[ ]:


encounter = create_encounter_df(dataframes, questionnaire, patient, gen_enc_uri, nexus_uri_base)


# ## Questionnaire Response

# In[ ]:


ques_response = create_questionnaire_response_df(dataframes, questionnaire, patient, encounter, generate_uri2, nexus_uri_base)


# ## Questionnaire_resposne ITEM

# In[ ]:


response_item = create_questionnaire_response_item_df(dataframes, questionnaire, patient, encounter, questionnaire_item, ques_response,answer_option, generate_uri4, nexus_uri_base)


# ## Sending the data to the Demo Project

# In[ ]:


project = 'fhir-data'

# Initialize Nexus Connection
sparql_client, metadata = initialize_nexus_connection(nexus_deployment, org, project, token)


# ## Organization resource to FHIR and Nexus

# In[ ]:


df=org_df.copy()
#df.drop('subject_id',axis=1,inplace=True)
for index, row in df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    org_instance = Organization(**row.to_dict())
    nexus_url = nexus_deployment
    # Transform to FHIR representation and send to Nexus.
    org_instance.update_to_nexus(nexus_deployment, org, project,token)
del org_df


# ## Patient resource to FHIR and Nexus

# In[ ]:


df=patient.copy()
df.drop('subject_id',axis=1,inplace=True)
for index, row in df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    patient_instance = Patient(**row.to_dict())
    nexus_url = nexus_deployment
    # Transform to FHIR representation and send to Nexus.
    patient_instance.update_to_nexus(nexus_deployment, org, project,token)
del patient


# ## Adding encounter reource to Nexus
# 

# In[ ]:


df=encounter.copy()
for index, row in df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    encounter_instance = Encounter(**row.to_dict())
    
    # Transform to FHIR representation and send to Nexus.
    encounter_instance.update_to_nexus(nexus_deployment, org, project,token)
del encounter


# ## Questionnaire resource to FHIR and Nexus

# In[ ]:


df=questionnaire.copy()
for index, row in df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    questionnaire_instance = Questionnaire(**row.to_dict())
    
    # Transform to FHIR representation and send to Nexus.
    questionnaire_instance.update_to_nexus(nexus_deployment, org, project,token)
del questionnaire


# ## Questionnaire_item to FHIR and Nexus

# In[ ]:


df =questionnaire_item.copy()
for index, row in df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    quesitem_instance = QuestionnaireItem(**row.to_dict())
    
    # Transform to FHIR representation and send to Nexus.
    quesitem_instance.update_to_nexus(nexus_deployment, org, project,token)
del questionnaire_item


# ## Answer options table to FHIR and NEXUS

# In[ ]:


df=answer_option.copy()
for index, row in df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    options_instance = AnswerOption(**row.to_dict())
    
    # Transform to FHIR representation and send to Nexus.
    options_instance.update_to_nexus(nexus_deployment, org, project,token)
del answer_option


# ## Questionnaire_response to FHIR and Nexus

# In[ ]:


df=ques_response.copy()
for index, row in df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    response_instance = QuestionnaireResponse(**row.to_dict())
    
    # Transform to FHIR representation and send to Nexus.
    response_instance.update_to_nexus(nexus_deployment, org, project,token)
del ques_response


# ## Questionnaire_response_item to FHIR and Nexus

# In[ ]:


df=response_item.copy()
f_df = df.dropna(subset=['value'])
f_df.shape

for index, row in f_df.iterrows():
    # Convert the row to a dictionary and unpack it to initialize the dataclass.
    response_item_instance = QuestionnaireResponseItem(**row.to_dict())
    
    # Transform to FHIR representation and send to Nexus.
    response_item_instance.update_to_nexus(nexus_deployment, org, project,token)
#del answer_option


# In[ ]:





# In[ ]:





# In[ ]:


# #df=response_item.copy()
# # Drop rows where the 'value' column is null
# f_df = df.dropna(subset=['value'])
# f_df.shape

# # # Reset index and drop the old index column
# # f_df = f_df.reset_index(drop=True)
# #del response_item


# # Assuming 'f_df', 'nexus', 'org', 'project', and 'batch_size' are defined
# error_list = process_dataframe_in_batches(f_df, 500, nexus_deployment, org, project,token)


# In[ ]:


# with open('error_log.txt', 'w') as f:
#     for error in error_list:
#         f.write(f"{error}\n")


# In[ ]:


# error_file, error_df = log_errors_to_csv(error_list,org)


# In[ ]:


# # Reading the error file and fetching matching records from the main DataFrame
# matching_records = read_error_file_and_fetch_records(error_list, f_df)


# In[ ]:


# matching_records.to_csv('mr_org.csv')

