import pandas as pd

def create_questionnaire_item_df(dataframes, questionnaire_df, generate_uri_function, nexus_uri_base):
    """
    Processes the DHM DataFrame to create a questionnaire item DataFrame.

    :param dataframes: A dictionary containing the 'DHM' DataFrame.
    :param questionnaire_df: The DataFrame of questionnaires.
    :param generate_uri_function: The function to generate URIs.
    :param nexus_uri_base: The base URI for Nexus.
    :return: A pandas DataFrame for questionnaire items.
    """
    # Make a copy of the 'DHM' DataFrame
    questionnaire_item = dataframes['DHM'].copy()

    # Rename columns
    questionnaire_item.rename(columns={
        'Form Code': 'questionnaire_name',
        'Item Code': 'item_code',
        'Study title': 'study_title',
        'Item Code': 'item_code',
        'Statistical Name': 'statistical_name',
        'Page': 'page',
        'Sequence': 'sequence',
        'Data Type': 'data_type',
        'Question Text': 'question_text',
        'UOM': 'uom',
        'List Code': 'answer_options_list',
        'Min warning': 'min_warning',
        'Max warning': 'max_warning',
        'Min error': 'min_error',
        'Max error': 'max_error',
        'Formula - Description': 'formula_description'
    }, inplace=True)


    # Merge with the questionnaire DataFrame
    questionnaire_item = pd.merge(questionnaire_df, questionnaire_item, on='questionnaire_name')

    # Generate URIs for each questionnaire item
    questionnaire_item['questionnaire_item_uri'] = questionnaire_item.apply(
        lambda row: generate_uri_function(nexus_uri_base, row['questionnaire_name'], row['item_code']), axis=1
    )
    # Drop additional columns as required
    columns_to_drop = ['questionnaire_name','org_uri', 'study_title', 'page', 
                       'min_warning', 'max_warning', 'min_error', 'max_error', 
                       'formula_description', 'sequence', 'uom']
    questionnaire_item = questionnaire_item.drop(columns=columns_to_drop, axis=1)

    return questionnaire_item