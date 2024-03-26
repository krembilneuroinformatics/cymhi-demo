# questionnaire_response_item_data.py

import pandas as pd
import numpy as np

def convert_to_date(val):
    if pd.isna(val) or val == 'nan':
        return val
    try:
        return pd.to_datetime(val).strftime('%m/%d/%Y')
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
        if not pd.isna(val):
            print(f"Failed to convert, replacing with NaN: {val}")
        return np.nan
def create_questionnaire_response_item_df(dataframes, questionnaire_df, patient_df, encounter_df, questionnaire_item_df, ques_response_df, ques_item_answer_option_df, generate_uri_function, nexus_uri_base):
    """
    Creates and returns a DataFrame for questionnaire response items.

    :param dataframes: A dictionary containing dataframes.
    :param questionnaire_df: The DataFrame of questionnaires.
    :param encounter_df: The DataFrame of encounters.
    :param questionnaire_item_df: The DataFrame of questionnaire items.
    :param ques_item_answer_option_df: The DataFrame of question items answer options.
    :param generate_uri_function: The function to generate URIs.
    :param nexus_uri_base: The base URI for Nexus.
    :return: A pandas DataFrame for questionnaire response items.
    """
    consolidated_df = pd.DataFrame()
    columns_to_exclude = ['CENTRE', 'SUBJECT_ID', 'SUBJECT_STATUS', 'SUBJECT_DE_STATUS', 'VISIT_ID', 'VISIT_NAME', 'VISIT_DATE', 'VISIT_OCC', 'FORM_OCCURENCE', 'USER_ID']

    # Loop through each questionnaire name and URI
    for index, row in questionnaire_df.iterrows():
        name = row['questionnaire_name']
        uri = row['questionnaire_uri']

        # Access the DataFrame from the 'dataframes' dictionary using the name as the key
        df = dataframes.get(name)

        if df is not None:
            columns_to_pivot = [col for col in df.columns if col not in columns_to_exclude]
            pivoted_df = pd.melt(df, id_vars=columns_to_exclude, value_vars=columns_to_pivot, var_name='question', value_name='value')
            pivoted_df['value'] = pivoted_df['value'].astype(str)
            pivoted_df['questionnaire_name'] = name
            pivoted_df['questionnaire_uri'] = uri
            consolidated_df = pd.concat([consolidated_df, pivoted_df], ignore_index=True)
            
        
    # Rename columns in encounter_df to lowercase
    consolidated_df.columns = map(str.lower, consolidated_df.columns)
    # List of columns to be dropped
    columns_to_drop = ['user_id']
    consolidated_df= consolidated_df.drop(columns=columns_to_drop)


     # Merge encounter with patient to get subject_id
    enc_df = encounter_df.merge(patient_df[['patient_uri','subject_id']],how='left', on='patient_uri')

    # Define the join columns 
    join_columns = ['centre', 'subject_id', 'subject_status', 'subject_de_status', 'visit_id', 
                    'visit_name', 'visit_date', 'visit_occ', 'form_occurence']

    # Merge with encounter table based on join columns
    merged_df = pd.merge(consolidated_df, enc_df, on=join_columns, how='left')
    # Drop columns_to_exclude
    merged_df.drop(columns=join_columns, inplace=True)

    # Fill missing values in 'encounter_uri' with an empty string
    merged_df['patient_uri'].fillna('', inplace=True)

    # Ensure consistent datatypes
    merged_df['questionnaire_uri'] = merged_df['questionnaire_uri'].astype(str)
    questionnaire_item_df['questionnaire_uri'] = questionnaire_item_df['questionnaire_uri'].astype(str)
    merged_df['question'] = merged_df['question'].astype(str)
    questionnaire_item_df['statistical_name'] = questionnaire_item_df['statistical_name'].astype(str)
    
    # Fetching the ques name from qestionnaire df for ques_item_df
    ques_item_df = questionnaire_item_df.merge(questionnaire_df, on='questionnaire_uri', how='left')
    
    # Merge merged_df with ques_item based on 'questionnaire_uri' and 'question'
    new_table = pd.merge(merged_df, ques_item_df[['questionnaire_name', 'questionnaire_item_uri', 'data_type', 'answer_options_list', 'question_text','statistical_name']],
        how='left',
        left_on=['questionnaire_name', 'question'],
        right_on=['questionnaire_name', 'statistical_name']
    )
    
    # Dropping records where 'questionnaire_item_uri' is null
    new_table = new_table.dropna(subset=['questionnaire_item_uri'])

#     new_table = new_table[['questionnaire_name','questionnaire_item_uri', 'questionnaire_uri', 'encounter_uri', 'question', 'data_type', 'value', 'answer_options_list']]
    
#     # Merge new_table with aop based on 'value' and 'answer_options_list'
#     response_table = pd.merge(
#         new_table, 
#         ques_item_answer_option_df[['questionnaire_item_uri','answer_option_uri']], 
#         how='left', on='questionnaire_item_uri')

    response_table=new_table.copy()
    # Replace various representations of missing data with numpy.nan
    response_table.replace(['nan', 'NaN', 'null', 'Null', 'NULL'], np.nan, inplace=True)

    # Updating values where datatype = Boolean to True and False and blank elsewhere
    response_table.loc[response_table['data_type']=='Boolean','value'] = response_table.loc[response_table['data_type']=='Boolean','value'].apply(lambda x: 'true' if x in [1,1.0, '1.0','1'] else ('false' if x in [0,'0'] else ('' if pd.isna(x) else x)))
    
    
    # Merging it with response table
    response_item = pd.merge(response_table, ques_response_df, how='left',on=['questionnaire_uri','encounter_uri'])
    
    # Apply the generate_uri4 function to create the 'response_tiem_uri' column
    response_item['response_item_uri'] = response_item.apply(lambda row: generate_uri_function(nexus_uri_base, row['encounter_uri'], row['questionnaire_item_uri'], row['ques_response_uri'], row['value']), axis=1)
    
    
    
    # Replace various representations of missing data with numpy.nan
    response_item.replace(['nan', 'NaN', 'null', 'Null', 'NULL',""], np.nan, inplace=True)
    
    # Apply the function to the 'value' column where 'data_type' is 'Date'
    response_item.loc[response_item['data_type'] == 'Date', 'value'] = response_item.loc[response_item['data_type'] == 'Date', 'value'].apply(convert_to_date)

    # Convert the 'value' column to object type
    response_item['value'] = response_item['value'].astype('object')
    
    response_item=response_item[['questionnaire_item_uri','question','data_type','value','response_item_uri','ques_response_uri']]

    return response_item
