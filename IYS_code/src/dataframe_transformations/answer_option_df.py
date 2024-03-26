import pandas as pd

def create_answer_option_df(questionnaire_item_df, dataframes, org, generate_uri_function, nexus_uri_base):
    """
    Processes questionnaire items to create an answer option DataFrame.

    :param questionnaire_item_df: The questionnaire item DataFrame.
    :param dataframes: A dictionary of dataframes, including 'DHMListElements'.
    :param org: The organization name ('YWHO' or 'Foundry').
    :param generate_uri_function: The function to generate URIs.
    :param nexus_uri_base: The base URI for Nexus.
    :return: A pandas DataFrame for answer options.
    """
    answer_option = questionnaire_item_df[[ 'questionnaire_uri', 'statistical_name', 
                                           'data_type', 'question_text', 
                                           'answer_options_list', 'questionnaire_item_uri']].copy()

    # Filter based on the organization
    if org == 'YWHO':
        answer_option = answer_option[answer_option['answer_options_list'] != -1]
    elif org == 'Foundry':
        answer_option = answer_option[answer_option['answer_options_list'] != 0]

    # Get the DHMListElements DataFrame
    dhmlist = dataframes['DHMListElements'].copy()
    dhmlist = dhmlist.rename(columns={'List Code': 'answer_options_list'})

    # Merge answer_option with DHMListElements
    ques_item_answer_option = pd.merge(answer_option, dhmlist, on='answer_options_list')

    # Generate URIs for each answer option
    ques_item_answer_option['answer_option_uri'] = ques_item_answer_option.apply(
        lambda row: generate_uri_function(nexus_uri_base, row['questionnaire_item_uri'], row['answer_options_list'], row['Value']), axis=1
    )
    
    #Drop additional columns 
    columns_to_drop = ['questionnaire_uri', 'answer_options_list',
                       'statistical_name','data_type','question_text','Sequence']
    
    ques_item_answer_option = ques_item_answer_option.drop(columns=columns_to_drop, axis=1)

    ques_item_answer_option .rename(columns={'Value':'option_value','Literal':'literal'},inplace= True)
    
    return ques_item_answer_option
