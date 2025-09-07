from mrjob.job import MRJob
from mrjob.step import MRStep

import re


class MRCityTempAvg(MRJob):
    
    def steps(self):
        """
        Define the MapReduce steps: one mapper and one reducer.
        """
        return [
            MRStep(mapper=self.mapper_temperatures,
                   reducer=self.reducer_temperatures)
        ]

    def mapper_temperatures(self, _, line):
        """
        Map function: Process a line from the input CSV and emit ((city, date), (temperature, 1)).
        Skips header lines and handles malformed data.
        """
        # Skip header or malformed lines
        try:
            datetime_str, city, temp = line.strip().split(",")
            # Extract date from timestamp (e.g., '2025-09-01')
            date = datetime_str.split(" ")[0]
            key = (city, date)
            value = (float(temp), 1)
            yield key, value

        except (ValueError, IndexError):
            # Ignore malformed lines
            pass

    def reducer_temperatures(self, key, values):
        """
        Reduce function: Sum temperatures and counts for each (city, date) key to calculate the average.
        """
        total_temp = 0
        total_count = 0
        for temp, count in values:
            total_temp += temp
            total_count += count
        
        avg_temp = total_temp / total_count if total_count > 0 else 0
        # Emit the key (city, date) and the calculated average temperature
        yield key, avg_temp

if __name__ == '__main__':
    MRCityTempAvg.run()

# python mrjob/avg_temperature.py data/city_temperature.csv > output.txt
# python mrjob/avg_temperature.py -r hadoop hdfs://path/to/city_temperature.csv