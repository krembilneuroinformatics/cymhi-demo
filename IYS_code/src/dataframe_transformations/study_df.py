import pandas as pd

def create_research_study_df(dataframes, org_uri):
    """
    Creates and returns a DataFrame for research studies.

    :param dataframes: A dictionary of dataframes, including 'DHM'.
    :param org_uri: The URI of the organization.
    :return: A pandas DataFrame containing research study data.
    """
    columns = ['study_title', 'org_uri', 'DATETIME']
    research_study = pd.DataFrame(columns=columns)

    if 'DHM' in dataframes:
        df = dataframes['DHM']
        df = df.dropna(subset=['Study title'])
        unique_study_titles = df['Study title'].unique()

        for study_title in unique_study_titles:
            current_timestamp = pd.Timestamp.now()
            new_row = {'study_title': study_title, 'org_uri': org_uri, 'DATETIME': current_timestamp}
            research_study = research_study.append(new_row, ignore_index=True)

    return research_study
