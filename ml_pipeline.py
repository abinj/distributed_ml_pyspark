from pyspark.sql import SparkSession

# Instantiate a spark session

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("flights_delay")\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()

# Load raw data

df = spark.read.csv("/home/abin/my_works/datasets/flights.csv", header=True, inferSchema=True, nullValue=' ')

# Clean dataset

to_keep = ['MONTH', 'DAY', 'DAY_OF_WEEK', 'AIRLINE', 'FLIGHT_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'
    , 'TAXI_OUT', 'SCHEDULED_DEPARTURE', 'DEPARTURE_DELAY', 'DISTANCE', 'SCHEDULED_ARRIVAL', 'ARRIVAL_DELAY']

df = df.select(to_keep)

df = df.dropna()

# df.show(20)

# Create Label

df = df.withColumn('label', (df.ARRIVAL_DELAY > 15).cast('integer'))

# df.show(50)

# Balance dataset
# Get number of delayed flights

on_time_flights_count = df.filter(df.label == 0).count()

# Calculate the sub-sampling ratio for the on-time flights

ratio = (df.count() - on_time_flights_count) / on_time_flights_count
print(ratio)

# under sample the redundant class
# Since roughly 20 of the total flights are delayed, taking 20% of 0s and 100% of 1s into dataset
# We are doing this to balance the dataset
df = df.sampleBy('label', {0: ratio, 1: 1})

#df.show(20)










