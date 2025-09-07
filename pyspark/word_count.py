from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession

import pyspark.pandas as ps


def calculate_avg_temp_pyspark_pandas():
    """
    Calculate the daily average temperature per city using PySpark with the Pandas API.
    """

    # 1. Start SparkSession
    # The entry point for programming in Spark with the DataFrame and SQL API.
    spark = SparkSession.builder \
        .appName("AvgTempPandasAPI") \
        .master("local[*]") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

    input_file = "data/city_temperature.csv"

    try:
        # 2. Read the CSV file directly with Spark
        print(f"Reading data from '{input_file}'...")
        ps_df = ps.read_csv(input_file)

        # 3. Processing and Transformation (Equivalent to the Map phase)
        ps_df['date'] = ps.to_datetime(ps_df['datetime']).dt.date
        ps_df['temperature'] = ps_df['temperature'].astype(float)

        # 4. Grouping and Aggregation (Equivalent to the Shuffle and Reduce phases)
        # The .groupby() groups the data by city and date (Shuffle).
        # The .agg() function applies the mean operation on each group (Reduce).
        print("Calculating the average temperature per city and day...")
        avg_temp_df = ps_df.groupby(['city', 'date']) \
                           .agg(avg_temp=('temperature', 'mean')) \
                           .reset_index()  # Converts group indexes into columns

        # 5. Sort the results for consistent display
        sorted_results = avg_temp_df.sort_values(by=['city', 'date'])

        # 6. Display the result
        # .to_pandas() collects the distributed results to the driver node for display.
        # Use with caution on very large datasets. For this example, it is safe.
        final_pandas_df = sorted_results.to_pandas()

        print("\nAverage Temperature per City and Day (calculated with PySpark and Pandas API):")
        print(final_pandas_df.to_string())

        # 6.1 Write the result to a CSV file
        output_file = "avg_temp_pyspark_pandas.csv"
        final_pandas_df.to_csv(output_file, index=False)
        print(f"\nResults written to '{output_file}'")

    except AnalysisException:
        print(f"Error: The file or directory '{input_file}' was not found.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    calculate_avg_temp_pyspark_pandas()
