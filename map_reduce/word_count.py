from collections import defaultdict

import re

# Map function: Splits each sentence into words and emits (word, 1)
def map_words(sentence):
    words = re.findall(r'\w+', sentence.lower())
    return [(word, 1) for word in words]

# Reduce function: Sums counts for each word
def reduce_counts(word, counts):
    return (word, sum(counts))

# MapReduce
if __name__ == "__main__":

    # Read input text
    with open("data/the_adventures_of_sherlock_holmes.txt", "r") as content:
        text_data = content.readlines()

    # Map phase: Apply map function to each sentence
    mapped = []
    for sentence in text_data:
        mapped.extend(map_words(sentence))
    
    # Shuffle/Sort: Group by word
    grouped = defaultdict(list)
    for word, count in mapped:
        grouped[word].append(count)
    
    # Reduce phase: Sum counts for each word
    results = [reduce_counts(word, counts) for word, counts in grouped.items()]

    result = sorted(results, key=lambda x: x[1], reverse=True)

    print("Showing words with more than 100 counts")
    for word, count in result:
        if count > 100:
            print(f"{word}: {count}")
    
    # Write results to CSV
    output_file = "output/word_count_map_reduce.csv"
    with open(output_file, "w") as f:
        f.write("word,count\n")
        for word, count in result:
            f.write(f"{word},{count}\n")

    print(f"\nResults written to '{output_file}'")
