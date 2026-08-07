from pyspark.sql.types import *

def get_schema():
    return StructType([
        StructField("_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("password", StringType(), True),
        StructField("preferences", StringType(), True)
    ])