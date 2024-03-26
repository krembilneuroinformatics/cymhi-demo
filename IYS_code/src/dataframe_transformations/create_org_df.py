import pandas as pd


def create_org_dataframe(org_data, nexus_uri_base, generate_uri_function):
    """
    Creates and returns an organization dataframe.

    :param org_data: A dictionary containing the organization data.
    :param nexus_uri_base: The base URI for Nexus.
    :param generate_uri_function: The function to generate URIs.
    :return: A pandas DataFrame containing the organization data.
    """
    #columns = ['org_id', 'official_name', 'description', 'email', 'website_url', 'org_type', 'province', 'org_uri']
    columns = ['org_id', 'official_name', 'description', 'email', 'website_url', 'province', 'org_uri']
    org_df = pd.DataFrame(columns=columns)

    org_data_df = pd.DataFrame(org_data)
    org_df = org_df.append(org_data_df, ignore_index=True)

    # Apply the generate_uri2 function to each row
    org_df['org_uri'] = [generate_uri_function(nexus_uri_base, row['official_name'], row['org_id']) for index, row in org_df.iterrows()]

    return org_df
