import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

# Initializing Glue Job
args = getResolvedOptions(sys.argv, ['JobName'])
sc = SparkContext()
gluecontext = GlueContext(sc)
spark = gluecontext.spark_session
job = Job(gluecontext)
job.init(args['JobName'], args)

# S3 paths
input_path = "s3://yugan-retail-pipeline/processed/retail_clean.csv"
output_path = "s3:// yugan-retail-pipeline/analytics/retail_clean_parquet/"

# Reading the clean CSV data
df = spark.read.option("header", True).csv(input_path)

# Basic data transformations
df = df.withColumn("total_sales_usd", col("Sales") * col("Price"))

# Writing as Parquet
df.write.mode("overwrite").parquet(output_path)

# Registering in Glue Data Catalog
gluecontext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [output_path]},
    format="parquet"
)

job.commit()
