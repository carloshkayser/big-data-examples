from dask.distributed import Client, LocalCluster
import dask.bag as db
import pandas as pd
import os
import re

if __name__ == "__main__":

    # 1. Start a Dask Distributed cluster
    cluster = LocalCluster(n_workers=4, threads_per_worker=2, memory_limit='2GB')
    client = Client(cluster)
    print("Dask cluster started:")
    print(client)

    # 2. Read TXT as a Dask Bag (lazy, memory-efficient)
    input_file = "data/the_adventures_of_sherlock_holmes.txt"
    print(f"Reading data from '{input_file}'...")
    bag = db.read_text(input_file)

    # 3. Preprocess and split into words using regex (lazy)
    words_bag = (
        bag
        .map(str.lower)  # lowercase
        .map(str.strip)  # remove extra spaces
        .filter(lambda x: x != "")  # remove empty lines
        .map(lambda x: re.findall(r"\b[a-z]+(?:['-][a-z]+)*\b", x))  # extract words
        .flatten()  # explode words into single items
    )

    # 4. Compute word counts
    word_counts = words_bag.frequencies().compute()
    word_counts_df = pd.DataFrame(word_counts, columns=["word", "count"])
    word_counts_df = word_counts_df.sort_values(by="count", ascending=False)

    print(word_counts_df.head(10))

    # 5. Save results to CSV
    output_file = "output/word_count_dask.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    word_counts_df.to_csv(output_file, index=False)
    print(f"Word counts saved to '{output_file}'.")
