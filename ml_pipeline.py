from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

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

# under sample the redundant class
# Since roughly 20 of the total flights are delayed, taking 20% of 0s and 100% of 1s into dataset
# We are doing this to balance the dataset
df = df.sampleBy('label', {0: ratio, 1: 1})

#df.show(20)


sub_df = df.groupBy('AIRLINE').agg({"ARRIVAL_DELAY": "avg"}).withColumnRenamed('avg(ARRIVAL_DELAY)', 'AVG_ARRIVAL_DELAY')

df_pandas = sub_df.toPandas()
df_pandas.plot(x='AIRLINE', y= 'AVG_ARRIVAL_DELAY', kind='bar', figsize=(10, 8))
plt.grid(which='major', linestyle='-', linewidth=0.5, color='green')
plt.grid(which='minor', linestyle=':', linewidth=0.5, color='black')
plt.show()












