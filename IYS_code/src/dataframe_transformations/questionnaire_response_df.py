import pandas as pd

def create_questionnaire_response_df(dataframes, questionnaire_df, patient_df, encounter_df, generate_uri_function, nexus_uri_base):
    """
    Creates and returns a DataFrame for questionnaire responses.

    :param dataframes: A dictionary containing dataframes.
    :param questionnaire_df: The DataFrame of questionnaires.
    :param encounter_df: The DataFrame of encounters.
    :param generate_uri_function: The function to generate URIs.
    :param nexus_uri_base: The base URI for Nexus.
    :return: A pandas DataFrame for questionnaire responses.
    """
    form_names = questionnaire_df['questionnaire_name'].unique().tolist()
    ques_response = pd.DataFrame()

    for table_name in form_names:
        if table_name in dataframes:
            df = dataframes[table_name]
            selected_columns = ['CENTRE', 'SUBJECT_ID', 'SUBJECT_STATUS', 'SUBJECT_DE_STATUS', 'VISIT_ID',
                                'VISIT_NAME', 'VISIT_DATE', 'VISIT_OCC', 'FORM_OCCURENCE', 'USER_ID']
            df_selected = df[selected_columns].copy()
            df_selected['questionnaire_name'] = table_name
            ques_response = pd.concat([ques_response, df_selected], ignore_index=True)

    # Drop duplicates
    ques_response.drop_duplicates(subset=['CENTRE', 'SUBJECT_ID', 'VISIT_ID', 'VISIT_NAME', 'VISIT_DATE', 'VISIT_OCC', 'FORM_OCCURENCE', 'questionnaire_name'], inplace=True)
    ques_response.columns = map(str.lower, ques_response.columns)
    ques_response.drop('user_id', axis=1, inplace=True)
    
    # Merge encounter with patient to get subject_id
    enc_df = encounter_df.merge(patient_df[['patient_uri','subject_id']],how='left', on='patient_uri')

    # Merge with encounter DataFrame
    ques_response = ques_response.merge(enc_df, on=['centre', 'subject_id', 'visit_id', 'visit_occ', 'visit_name', 'visit_date', 
                                                          'form_occurence', 'subject_status', 'subject_de_status'], how='left')

    # Merge with questionnaire DataFrame
    ques_response = ques_response.merge(questionnaire_df[['questionnaire_name', 'questionnaire_uri']], on='questionnaire_name', how='left')

    # Keep only specific columns
    ques_response = ques_response[['questionnaire_name', 'questionnaire_uri', 'encounter_uri', 'patient_uri']]

    # Generate URIs for each questionnaire response
    ques_response['ques_response_uri'] = ques_response.apply(
        lambda row: generate_uri_function(nexus_uri_base, row['questionnaire_uri'], row['encounter_uri']), axis=1
    )

    ques_response.drop(['questionnaire_name'], axis=1, inplace=True)

    return ques_response
