import datetime
import csv
import pandas as pd

def log_errors_to_csv(error_list, org_prefix):
    """
    Logs errors to a CSV file and returns the filename and a DataFrame of errors.

    :param error_list: A list of dictionaries containing error information.
    :param org_prefix: A prefix for the organization to include in the filename.
    :return: Tuple containing the filename of the CSV and the DataFrame of errors.
    """
    # Generate filename based on current date
    current_date = datetime.date.today()
    datestamp = current_date.strftime('%Y%m%d')
    filename = f'{org_prefix}_errors_{datestamp}.csv'

    # Create a DataFrame from error_list
    error_df = pd.DataFrame(error_list)

    # Write errors to the CSV file
    error_df.to_csv(filename, index=False)

    print(f"Errors logged to {filename}")
    return filename, error_df



def read_error_file_and_fetch_records(error_file, main_dataframe):
    """
    Reads an error file and extracts the corresponding records from the main DataFrame.

    :param error_file: The filename of the error CSV.
    :param main_dataframe: The main DataFrame to fetch records from.
    :return: A DataFrame containing the matching records.
    """
    error_df = pd.read_csv(error_file)
    error_indices = error_df['RecordIndex']

    # Use the DataFrame's default index for matching
    matching_records = main_dataframe.loc[main_dataframe.index.isin(error_indices)]
    
    return matching_records
