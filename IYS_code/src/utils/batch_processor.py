# batch_processor.py

import pandas as pd
from src.fhir_dataclasses.questionnaire_responseitem import QuestionnaireResponseItem
from dataclasses import dataclass
import pandas as pd
from urllib.parse import quote 
from urllib.parse import urljoin
import requests
import logging

   
def process_dataframe_in_batches(f_df, batch_size,  nexus_deployment, org, project, token, starting_batch_index=0):

    """
    Processes a DataFrame in batches and handles errors.

    :param f_df: The DataFrame to be processed.
    :param batch_size: The number of records to process in each batch.
    :param nexus: The Nexus instance.
    :param org: The organization name.
    :param project: The project name.
    :param starting_batch_index: The starting index for batching (default is 0).
    :return: A list of error information.
    """
    error_list = []
    total_expected_batches = (len(f_df) + batch_size - 1) // batch_size
    total_successful_updates = 0
    total_errors = 0

    print(f"Total Expected Batches: {total_expected_batches}")
    for batch_start in range(starting_batch_index, len(f_df), batch_size):
        batch_df = f_df.iloc[batch_start:batch_start + batch_size]
        batch_successful_updates, batch_errors, batch_error_list = process_batch(batch_df, nexus_deployment, org, project,token)
        total_successful_updates += batch_successful_updates
        total_errors += batch_errors
        error_list.extend(batch_error_list)

    print(f"Total Batches Processed: {total_expected_batches}")
    print("Total Successful Updates:", total_successful_updates)
    print("Total Errors:", total_errors)

    return error_list


# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, filename='batch_processing.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_batch(batch_df,  nexus_deployment, org, project,token):
    batch_successful_updates = 0
    batch_errors = 0
    batch_error_list= []
    print(f"Processing batch of size: {len(batch_df)}")  # Debugging statement

    for index, row in batch_df.iterrows():
        #print(f"Processing row {index}")  # Debugging statement
        try:
            ques_response_item_instance = QuestionnaireResponseItem(**row.to_dict())
            #ques_response_item_instance = QuestionnaireResponseItem.from_row(row)
            logging.info(f"Processing record at index {index} with data: {row.to_dict()}")
            ques_response_item_instance.update_to_nexus(nexus_deployment, org, project,token)
            batch_successful_updates += 1
            logging.info(f"Successfully updated record at index {index}.")
        except Exception as e:
            error_info = {'RecordIndex': index, 'RecordData': row.to_dict(), 'ErrorMessage': str(e)}
            batch_error_list.append(error_info)
            batch_errors += 1
            logging.error(f"Failed to process record at index {index}: {e}")
    return batch_successful_updates, batch_errors, batch_error_list



# def process_batch(batch_df, nexus_deployment, org, project, token):
#     batch_successful_updates = 0
#     batch_errors = 0
#     batch_error_list = []

#     print(f"Starting batch processing, batch size: {len(batch_df)}")  # Confirm batch is being processed

#     for index, row in batch_df.iterrows():
#         print(f"Attempting to process row at index: {index}")  # Indicates loop entry
#         try:
#             # Directly construct an instance without using from_row for now
#             ques_response_item_instance = QuestionnaireResponseItem(
#                 questionnaire_item_uri=row.get('questionnaire_item_uri'),
#                 question=row.get('question'),
#                 value=row.get('value'),
#                 data_type=row.get('data_type'),
#                 response_item_uri=row.get('response_item_uri'),
#                 ques_response_uri=row.get('ques_response_uri')
#             )
#             # Temporarily bypass update_to_nexus to see if instantiation is successful
#             print(f"Successfully instantiated QuestionnaireResponseItem for index {index}")
#             # ques_response_item_instance.update_to_nexus(nexus_deployment, org, project, token)
#             # Simulate a successful update for debugging
#             batch_successful_updates += 1
#         except Exception as e:
#             print(f"Failed to process row at index {index}: {e}")  # Directly print errors
#             batch_errors += 1
#             error_info = {'RecordIndex': index, 'RecordData': row.to_dict(), 'ErrorMessage': str(e)}
#             batch_error_list.append(error_info)

#     print(f"Batch processing completed. Successful: {batch_successful_updates}, Errors: {batch_errors}")
#     return batch_successful_updates, batch_errors, batch_error_list


