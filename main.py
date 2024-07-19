import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd


def benchmark(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Total time taken by {func.__name__}: {end_time - start_time:.2f} seconds")
        return result

    return wrapper


# Function to read CSV in chunks and process each chunk up to the last column with a header
def read_csv_in_chunks(file_path, encodings, chunksize=100000):
    for enc in encodings:
        try:
            # Read the header row first to determine the last column with a header
            header_df = pd.read_csv(file_path, encoding=enc, nrows=1, low_memory=False)
            last_column_with_header = header_df.columns[header_df.notna().any()].tolist()[-1]

            # Read CSV in chunks up to the last column with a header
            chunk_list = []
            total_rows = 0
            for chunk in pd.read_csv(
                file_path,
                encoding=enc,
                chunksize=chunksize,
                low_memory=False,
                usecols=lambda column: column <= last_column_with_header,
            ):
                total_rows += len(chunk)
                chunk = chunk.loc[:, ~chunk.columns.str.contains("^Unnamed")]
                chunk_list.append(chunk)
            combined_df = pd.concat(chunk_list, ignore_index=True)
            print(f"Total rows read from {file_path}: {total_rows:,}")
            return combined_df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    return None


@benchmark
def main():
    # Directory containing CSV files
    csv_dir = "csv_files"
    # List all CSV files in the directory
    csv_files = [file for file in os.listdir(csv_dir) if file.endswith(".csv")]

    # List of encodings to try
    encodings = ["utf-8", "latin1", "iso-8859-1", "cp1252"]

    # Record start time for reading and processing CSV files
    read_start_time = time.time()

    # Use ThreadPoolExecutor to read files in parallel
    dataframes = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(read_csv_in_chunks, f"{csv_dir}/{file}", encodings): file for file in csv_files}
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                dataframes.append(df)

    # Record end time for reading and processing CSV files
    read_end_time = time.time()

    # Record start time for concatenating DataFrames
    concat_start_time = time.time()

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Record end time for concatenating DataFrames
    concat_end_time = time.time()

    # Record start time for saving the combined DataFrame
    save_start_time = time.time()

    print("Saving the combined data to SQLite database...")
    # Save the combined DataFrame to an SQLite database
    conn = sqlite3.connect("sqlite.db")
    combined_df.to_sql("combined_table", conn, if_exists="replace", index=False)
    conn.execute("VACUUM")  # reclaim disk space when run again
    conn.close()

    # Record end time for saving the combined DataFrame
    save_end_time = time.time()

    print(f"Total number of csv files processed: {len(csv_files)}")
    # Print the number of rows in the combined DataFrame
    print(f"Total rows in the combined DataFrame: {len(combined_df):,}")

    # Print benchmark results for individual sections
    print(f"Time taken to read and process CSV files: {read_end_time - read_start_time:,.2f} seconds")
    print(f"Time taken to concatenate DataFrames: {concat_end_time - concat_start_time:,.2f} seconds")
    print(f"Time taken to save the combined DataFrame: {save_end_time - save_start_time:,.2f} seconds")


if __name__ == "__main__":
    main()
