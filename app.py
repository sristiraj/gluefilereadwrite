import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job


#List of params passed
params = ["data_s3_bucket","data_s3_path","glue_db_name","db_s3_path"]
#Generate glue context entry point
glctxt = GlueContext(SparkContext.getOrCreate())
spark = glctxt.spark_session
logger = glctxt.get_logger()

args = getResolvedOptions(sys.argv, params)

logger.info("Started job for loading data dictionary objects to glue")

#Get all param setting for s3 bucket to lookup for data dictionary files uploaded by user and glue db to use for writing data
s3_bucket = args.get("data_s3_bucket","defaults3")
s3_paths = args.get("data_s3_path","defaults3path").split(",")
glue_db_name = args.get("glue_db_name","default")
db_s3_path = args.get("db_s3_path","s3://default")

logger.info("s3 data paths to traverse and load {}".format(s3_paths))

for path in s3_paths:
    logger.info("Writing data for data {}".format(path))
    df = spark.read.option("inferSchema","true").option("header","true").csv("s3://{}/{}*".format(s3_bucket, path))
    
    tbl_name = path[path.rfind("/", 0)+1:].strip()
    logger.info("Started writing to table {}".format(tbl_name))
    df.write.format("parquet").mode("overwrite").option("header","true").option("path","s3://{}/{}/{}".format(s3_bucket, db_s3_path, tbl_name)).saveAsTable("{}.{}".format(glue_db_name, tbl_name))    
    

logger.info("Loading completed")
