import pandas as ps

# dataframe_processing.py

def filter_dataframe_columns(dataframes, dataframe_name, org_name):
    """
    Filters the specified dataframe to keep only the defined columns based on the organization name.

    :param dataframes: A dictionary of dataframes.
    :param dataframe_name: The key for the dataframe to filter.
    :param org_name: The name of the organization to determine the columns to keep.
    :return: None (the dictionary is modified in place).
    """
    columns_to_keep = []

    # Define columns to keep for different organizations
    if org_name == 'YWHO':
        columns_to_keep = ['CENTRE', 'SUBJECT_ID', 'SUBJECT_STATUS', 'SUBJECT_DE_STATUS', 
                           'VISIT_ID', 'VISIT_NAME', 'VISIT_DATE', 'VISIT_OCC', 'FORM_OCCURENCE', 
                           'USER_ID', 'DOB', 'Age_AtReg']
    elif org_name == 'Foundry':
        columns_to_keep = ['CENTRE', 'SUBJECT_ID', 'SUBJECT_STATUS', 'SUBJECT_DE_STATUS', 
                           'VISIT_ID', 'VISIT_NAME', 'VISIT_DATE', 'VISIT_OCC', 
                           'VISIT_SECONDARY_CENTRE_ID', 'VISIT_OWNER_USER_ID', 'FORM_OCCURENCE', 
                           'FORM_DATE_ENTRY_STARTED', 'FORM_DATE_VALIDATED', 
                           'FORM_DATE_VALIDATED_FIRST', 'USER_ID', 'FORM_USERNAME_VALIDATED', 
                           'FORM_USERNAME_VALIDATED_FIRST', 'STATUS', 'rf4', 'rf5']

    if dataframe_name in dataframes and columns_to_keep:
        dataframes[dataframe_name] = dataframes[dataframe_name][columns_to_keep]
    else:
        print(f"DataFrame {dataframe_name} not found in the provided dictionary, or no columns defined for {org_name}.")
