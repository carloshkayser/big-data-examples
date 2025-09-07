from dask.distributed import Client, LocalCluster

import dask.dataframe as dd
import pandas as pd
import os


def calculate_avg_temp_dask_distributed():
    """
    Calculate the daily average temperature per city using Dask DataFrame with Dask Distributed.
    """

    input_file = "data/city_temperature.csv"

    try:
        # 1. Start a Dask Distributed cluster
        cluster = LocalCluster(n_workers=4, threads_per_worker=2, memory_limit='2GB')
        client = Client(cluster)
        print("Dask cluster started:")
        print(client)

        # 2. Read CSV with Dask
        print(f"Reading data from '{input_file}'...")
        dask_df = dd.read_csv(input_file)

        # 3. Processing and Transformation
        dask_df['date'] = dd.to_datetime(dask_df['datetime']).dt.date
        dask_df['temperature'] = dask_df['temperature'].astype(float)

        # 4. Grouping and Aggregation
        print("Calculating the average temperature per city and day...")
        avg_temp_df = dask_df.groupby(['city', 'date']) \
                              .agg(avg_temp=('temperature', 'mean')) \
                              .reset_index()

        # 5. Sort results
        sorted_results = avg_temp_df.sort_values(by=['city', 'date'])

        # 6. Compute and convert to Pandas
        final_pandas_df = sorted_results.compute()

        # 7. Display results
        print("\nAverage Temperature per City and Day (calculated with Dask Distributed):")
        print(final_pandas_df.to_string(index=False))

        # 8. Write the result to a CSV
        output_file = "output/avg_temp_dask_distributed.csv"

        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        final_pandas_df.to_csv(output_file, index=False)
        print(f"\nResults written to '{output_file}'")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        client.close()
        cluster.close()
        print("Dask cluster shut down.")


if __name__ == "__main__":
    calculate_avg_temp_dask_distributed()
