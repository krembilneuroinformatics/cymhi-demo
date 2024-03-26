import pandas as pd

def create_encounter_df(dataframes, questionnaire_df, patient_df, gen_enc_uri_function, nexus_uri_base):
    """
    Creates and returns a DataFrame for encounters.

    :param dataframes: A dictionary containing dataframes.
    :param questionnaire_df: The DataFrame of questionnaires.
    :param patient_df: The DataFrame of patients.
    :param gen_enc_uri_function: The function to generate encounter URIs.
    :param nexus_uri_base: The base URI for Nexus.
    :return: A pandas DataFrame for encounters.
    """
    form_names = questionnaire_df['questionnaire_name'].unique().tolist()
    encounter_df = pd.DataFrame()

    for table_name in form_names:
        if table_name in dataframes:
            df = dataframes[table_name]
            selected_columns = ['CENTRE', 'SUBJECT_ID', 'SUBJECT_STATUS', 'SUBJECT_DE_STATUS', 'VISIT_ID',
                                'VISIT_NAME', 'VISIT_DATE', 'VISIT_OCC', 'FORM_OCCURENCE', 'USER_ID']
            df_selected = df[selected_columns].copy()
            encounter_df = pd.concat([encounter_df, df_selected], ignore_index=True)

    # Drop duplicates and generate URIs
    encounter_df.drop_duplicates(subset=['CENTRE', 'SUBJECT_ID', 'VISIT_ID', 'VISIT_NAME', 'VISIT_DATE', 'VISIT_OCC', 'FORM_OCCURENCE'], inplace=True)
    encounter_df['encounter_uri'] = encounter_df.apply(
        lambda row: gen_enc_uri_function(nexus_uri_base, row['CENTRE'], row['SUBJECT_ID'], row['VISIT_ID'], row['VISIT_OCC'],
                                         row['VISIT_NAME'], row['VISIT_DATE'], row['VISIT_OCC'], row['FORM_OCCURENCE'], row['USER_ID'],
                                         row['VISIT_ID'], row['SUBJECT_STATUS'], row['SUBJECT_DE_STATUS']), axis=1
    )

    # Drop unnecessary columns and rename
    encounter_df.drop("USER_ID", axis=1, inplace=True)
    encounter_df.columns = map(str.lower, encounter_df.columns)

    # Merge with patient URI
    subject_id_to_patient_uri = patient_df[['subject_id', 'patient_uri']].copy()
    encounter_df = encounter_df.merge(subject_id_to_patient_uri, on='subject_id', how='left')
    encounter_df.drop(['subject_id'], axis=1, inplace=True)

    return encounter_df
