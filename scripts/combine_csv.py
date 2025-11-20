import os
import pandas as pd

def combine_csv_files(input_folder, output_file):
    """
    Combine all CSV files in the given folder into a single CSV file.

    Args:
    - input_folder: Path to the folder containing the CSV files.
    - output_file: Path where the combined CSV file will be saved.
    """
    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    
    # Initialize an empty list to store DataFrames
    df_list = []

    for csv_file in csv_files:
        # Build the full file path
        file_path = os.path.join(input_folder, csv_file)

        # Read the current CSV file
        print(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)

        # Append the DataFrame to the list
        df_list.append(df)
    
    # Concatenate all DataFrames into one
    combined_df = pd.concat(df_list, ignore_index=True)

    # Write the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined CSV saved to: {output_file}")

# Example usage
input_folder = '/foldername/Collection - US Pet Stores'  # Change this to your folder path
output_file = '/foldername/Collection – US Pet Stores/303288pet_combined_output.csv'  # Path for the output file



combine_csv_files(input_folder, output_file)

