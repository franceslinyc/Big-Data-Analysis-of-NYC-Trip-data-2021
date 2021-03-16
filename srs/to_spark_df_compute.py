import pyspark
from pyspark.sql import SparkSession
import pprint
import json
from pyspark.sql.types import StructField, StructType 
from pyspark.sql.types import StringType, FloatType, IntegerType, DateType, TimestampType

# Connect to Spark
sc = pyspark.SparkContext() # run Spark applications
#PACKAGE_EXTENSIONS= ('gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar')

bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquerry/pyspark_input'.format(bucket)
output_directory = 'gs://{}/pyspark_demo_output'.format(bucket)

conf={
    # change project id, dataset id, table id
    'mapred.bq.project.id':project,
    'mapred.bq.gcs.bucket':bucket,
    'mapred.bq.temp.gcs.path':input_directory,
    'mapred.bq.input.project.id': "ultra-dimension-300900", 
    'mapred.bq.input.dataset.id': 'trip_data', 
    'mapred.bq.input.table.id': 'yellow_data_2020_20210310_070223', 
}

# Pull table from big query
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf = conf)

# Convert table to a json like object
vals = table_data.values()
vals = vals.map(lambda line: json.loads(line))
#pprint.pprint(vals.first()) # good as of 03/13/2021

# Define a To_numb function 
def To_numb(x):
  x['pickup_hr'] = int(x['pickup_hr'])
  x['pickup_day'] = int(x['pickup_day'])
  x['pickup_mon'] = int(x['pickup_mon'])
  x['pickup_yr'] = int(x['pickup_yr'])
  x['dropoff_hr'] = int(x['pickup_hr'])
  x['dropoff_day'] = int(x['pickup_day'])
  x['dropoff_mon'] = int(x['pickup_mon'])
  x['dropoff_yr'] = int(x['pickup_yr'])
  x['trip_distance'] = float(x['trip_distance'])
  x['PULocationID'] = int(x['PULocationID'])
  x['DOLocationID'] = int(x['DOLocationID'])
  x['total_amount'] = float(x['total_amount'])
  return x

# Apply To_numb function to int or float variables otherwise schema won't work 
vals = vals.map(To_numb)

# Create a dataframe object

# schema
# https://spark.apache.org/docs/3.0.0-preview/sql-ref-datatypes.html
schema = StructType([
   StructField('tpep_pickup_datetime', StringType(), True),  # TimestampType() Not in the form?
   StructField("pickup_hr", IntegerType(), True), 
   StructField("pickup_day", IntegerType(), True),
   StructField("pickup_mon", IntegerType(), True),     
   StructField("pickup_yr", IntegerType(), True),     
   StructField("tpep_dropoff_datetime", StringType(), True), # TimestampType() Not in the form?
   StructField("dropoff_hr", IntegerType(), True), 
   StructField("dropoff_day", IntegerType(), True),
   StructField("dropoff_mon", IntegerType(), True),      
   StructField("dropoff_yr", IntegerType(), True),
   StructField("trip_distance", FloatType(), True),      
   StructField("PULocationID", IntegerType(), True), 
   StructField("DOLocationID", IntegerType(), True),
   StructField("total_amount", FloatType(), True)])

#pprint.pprint(vals.first()) # good as of 03/13/2021

# Initialize spark
# https://spark.apache.org/docs/2.0.0/sql-programming-guide.html#sql
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .getOrCreate()

# Create a df 
df1 = spark.createDataFrame(vals, schema= schema)
df1.repartition(6)                  # partition to 6 partitions 

#pprint.pprint(vals.first())        # good as of 03/09/2020

# Need a To_numb function as well 

# Print few rows of df just to be sure
#pprint.pprint(df1.take(5))  # grab 5 data
#pprint.pprint(df1.describe())        # compute summary statistics; does not work

# Print schema since the variables are still in string 
#pprint.pprint(df1.printSchema()) # look right

# Compute summary statistics
#df1.describe("trip_distance").show() # good
#df1.describe(["trip_distance", "total_amount"]).show() # does not work

df1.describe("trip_distance").show()
df1.describe("total_amount").show()

# Query data
# https://towardsdatascience.com/beginners-guide-to-pyspark-bbe3b553b79f

# # Compute monthly avg trip_distance & total_amount
# df1.select([
#             'trip_distance', 
#             'total_amount'
#             ]
#             ).groupBy('pickup_mon')\
#             .mean()\
#             .show() 

#AttributeError: 'GroupedData' object has no attribute 'describe'


# Consider map individual variable and compute summary statistics; it may be faster?




# Delete the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True) 


## Back to Google Cloud, Week 7
## Upload this file to Storage's cs512_trip
## Create cluster and submit job in Dataproc
## Copy & paste this to Jar files
## gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar

