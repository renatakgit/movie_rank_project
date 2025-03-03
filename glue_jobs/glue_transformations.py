import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# retrieve job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# initialize spark and glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# initialize the glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# configure spark setting for max partition size
spark.conf.set("spark.sql.files.maxPartitionBytes", 512 * 1024 * 1024)  # 512MB

# define the schema for incoming json data
df_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("reviewer", StringType(), True),
    StructField("movie", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("review_summary", StringType(), True),
    StructField("review_date", StringType(), True),
    StructField("spoiler_tag", IntegerType(), True),
    StructField("review_detail", StringType(), True),
    StructField("helpful", ArrayType(StringType()), True)
])

# read json data from s3 using the predefined schema
dataFrame = spark.read.schema(df_schema).json("s3://${ACCOUNT_ID}-landing-zone/*.json")

# filter out rows where 'movie' or 'rating' columns are null
formatted_df = dataFrame.filter(
    (dataFrame["movie"].isNotNull()) & 
    (dataFrame["rating"].isNotNull())
)

# data transformations
formatted_df = (
    formatted_df
    .withColumn("review_date", F.to_date(F.col("review_date"), "d MMMM yyyy"))  # convert date format
    .withColumn("rating", F.col("rating").cast("int"))  # cast rating to integer
    .withColumn("spoiler_tag", F.when(F.col("spoiler_tag") == 1, F.lit(True))
                           .when(F.col("spoiler_tag") == 0, F.lit(False))
                           .otherwise(F.lit(None)))  # convert spoiler_tag to boolean
    .withColumn("helpful", F.expr("transform(helpful, x -> cast(x as int))"))  # convert helpful votes to integer list
    .withColumn("review_month", F.date_format(F.col("review_date"), "MM-yyyy"))  # extract month-year from review_date
)

# convert the transformed df into a glue dynamicframe
dynamicFrame = DynamicFrame.fromDF(formatted_df, glueContext, "dynamic_frame")

# write the transformed data back to s3 in parquet format, partitioned by review_month
glueContext.write_dynamic_frame.from_options(
    frame=dynamicFrame,
    connection_type='s3',
    connection_options={
        'path': "s3://${ACCOUNT_ID}-formatted-data", "partitionKeys": ["review_month"]
    },
    format='parquet',
)


job.commit()
