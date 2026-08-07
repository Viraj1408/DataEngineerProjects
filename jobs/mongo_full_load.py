from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import argparse
import importlib.util

parser = argparse.ArgumentParser()
parser.add_argument("--mongo_uri", required=True)
parser.add_argument("--database", required=True)
parser.add_argument("--collection", required=True)
parser.add_argument("--output_path", required=True)
parser.add_argument("--schema_path", default="NA")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName(f"Mongo_To_Delta_{args.collection}")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
    .getOrCreate()
)

reader = (
    spark.read.format("mongodb")
    .option("spark.mongodb.read.connection.uri", args.mongo_uri)
    .option("database", args.database)
    .option("collection", args.collection)
)

if args.schema_path and args.schema_path != "NA":
    try:
        spec = importlib.util.spec_from_file_location("schema_module", args.schema_path)
        schema_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(schema_module)
        reader = reader.schema(schema_module.get_schema())
    except Exception:
        pass

df = reader.load()

df = (
    df.withColumn("bronze_load_time", f.current_timestamp())
      .withColumn("bronze_load_date", f.current_date())
)

df.write.format("delta").mode("overwrite").save(args.output_path)
