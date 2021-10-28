import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import trim, col
import logging


#List of params passed
params = ["data_s3_bucket","data_s3_path","glue_db_name","db_s3_path","data_file_sep"]
#Generate glue context entry point
# glctxt = GlueContext(SparkContext.getOrCreate())
spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger()

args = getResolvedOptions(sys.argv, params)

logger.info("Started job for loading data dictionary objects to glue")

sep_switcher = {
    "comma":",",
    "pipe":"|",
    "tab":"\\t"
    }
#Get all param setting for s3 bucket to lookup for data dictionary files uploaded by user and glue db to use for writing data
s3_bucket = args.get("data_s3_bucket","defaults3")
s3_paths = args.get("data_s3_path","defaults3path").split(",")
glue_db_name = args.get("glue_db_name","default")
db_s3_path = args.get("db_s3_path","s3://default")
data_file_sep = args.get("data_file_sep","defaults3path").split(",")

print(s3_paths)
print(data_file_sep)
logger.info("s3 data paths to traverse and load {}".format(s3_paths))

for idx, path in enumerate(s3_paths):
    logger.info("Writing data for data {}".format(path))
    sep = sep_switcher.get(data_file_sep[idx],",")
    print(sep)
    read_path = "s3://{}/{}/" if "." not in path else "s3://{}/{}"
    print(read_path.format(s3_bucket, path.strip("/")))
    df = spark.read.option("delimiter",sep).option("header","true").csv(read_path.format(s3_bucket, path.strip("/")))
    if path.strip("/").rfind("/", 0) != -1:
        tbl_name = path[path.strip("/").rfind("/", 0)+1:].strip("/").strip()
    else:
        tbl_name = path.strip("/").strip()
    if "." in tbl_name:
        tbl_name = tbl_name[:tbl_name.find(".")]
    print(tbl_name)    
    logger.info("Started writing to table {}".format(tbl_name))
    df.createOrReplaceTempView("tmp_"+tbl_name)
    new_cols = "select  "
    for col in df.columns:
        new_cols += "trim("+col+") as "+col.strip()+","

    new_cols = new_cols[:len(new_cols)-1] +" from tmp_"+tbl_name
    print(new_cols)    
    df1 = spark.sql(new_cols)
    df1.show()
    df1.write.format("parquet").mode("overwrite").option("header","true").option("path","s3://{}/{}/{}".format(s3_bucket, db_s3_path, tbl_name)).saveAsTable("{}.{}".format(glue_db_name, tbl_name))    
    

logger.info("Loading completed")
