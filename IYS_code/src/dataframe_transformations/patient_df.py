import pandas as pd

def handle_invalid_date(date_str):
    try:
        return pd.to_datetime(date_str).date()
    except (TypeError, ValueError):
        return None

def create_patient_df(registration_df, org_name, org_uri, study_title, generate_uri_function, nexus_uri_base):
    """
    Processes the REGISTRATION DataFrame and creates a patient DataFrame.

    :param registration_df: The REGISTRATION DataFrame.
    :param org_name: The name of the organization.
    :param org_uri: The URI of the organization.
    :param study_title: The title of the study.
    :param generate_uri_function: The function to generate URIs.
    :param nexus_uri_base: The base URI for Nexus.
    :return: A pandas DataFrame for patients.
    """
    if org_name == 'YWHO':
        patient = registration_df[['CENTRE', 'SUBJECT_ID', 'DOB', 'Age_AtReg']].copy()
        patient.rename(columns={'CENTRE': 'centre', 'DOB': 'dob', 'SUBJECT_ID': 'subject_id', 'Age_AtReg': 'age_at_reg'}, inplace=True)
    elif org_name == 'Foundry':
        patient = registration_df[['CENTRE', 'SUBJECT_ID', 'rf5', 'rf4']].copy()
        patient.rename(columns={'CENTRE': 'centre', 'SUBJECT_ID': 'subject_id', 'rf5': 'age_at_reg', 'rf4': 'dob'}, inplace=True)

    patient['org_uri'] = org_uri
    patient['study_title'] = study_title
    patient['age_at_reg'] = pd.to_numeric(patient['age_at_reg'], errors='coerce').round().astype('Int64')

    # Generate URI for each patient
    patient['patient_uri'] = patient.apply(lambda row: generate_uri_function(nexus_uri_base, row['study_title'], row['org_uri'], row['subject_id']), axis=1)
    
    patient['dob'] = patient['dob'].apply(handle_invalid_date)
    patient = patient.drop(['study_title', 'centre'], axis=1)
    patient['age_at_reg'] = patient['age_at_reg'].astype(float)
    patient['age_at_reg'] = patient['age_at_reg'].apply(lambda x: int(x) if not pd.isna(x) else None)
    patient['age_at_reg'] = patient['age_at_reg'].astype(str)

    return patient
