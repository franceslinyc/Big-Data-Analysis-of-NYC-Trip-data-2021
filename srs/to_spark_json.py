import pyspark
from pyspark.sql import SparkSession
import pprint
import json
from pyspark.sql.types import StructType, FloatType, LongType, StringType, StructField

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
    'mapred.bq.input.dataset.id': 'trip_data',  # update dataset
    'mapred.bq.input.table.id': 'yellow_data_2020_20210310_070223', # update table
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
pprint.pprint(vals.first())




# Delete the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True) 


## Back to Google Cloud, Week 7
## Upload this file to Storage's cs512_trip
## Create cluster and submit job in Dataproc
## Copy & paste this to Jar files
## gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar

