import pandas as pd

def create_questionnaire_df(dhm_df, org_uri, generate_uri_function, nexus_uri_base):
    """
    Creates and returns a DataFrame of questionnaires with unique URIs.

    :param dhm_df: The DHM DataFrame containing 'Form Code'.
    :param generate_uri_function: The function to generate URIs.
    :param nexus_uri_base: The base URI for Nexus.
    :return: A pandas DataFrame for questionnaires.
    """
    questionnaire = pd.DataFrame({'Form Code': dhm_df['Form Code'].unique()})
    questionnaire.rename(columns={'Form Code': 'questionnaire_name'}, inplace=True)
    questionnaire['org_uri']=org_uri

    # Generate URI for each questionnaire
    questionnaire['questionnaire_uri'] = questionnaire.apply(
        lambda row: generate_uri_function(nexus_uri_base, row['questionnaire_name']), axis=1
    )

    return questionnaire
