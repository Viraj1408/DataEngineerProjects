from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--bronze_path", required=True)
parser.add_argument("--silver_path", required=True)
parser.add_argument("--primary_key", required=True)
parser.add_argument("--incremental_col", required=True)

args = parser.parse_args()

primary_keys = args.primary_key.split(",")

spark = (SparkSession.builder
         .appName("Bronze_to_Silver_Delta")
         .getOrCreate())

bronze_df = spark.read.format('delta').load(args.bronze_path)

window_spec = Window.partitionBy(*primary_keys) \
                    .orderBy(col(args.incremental_col).desc())

latest_df = (
    bronze_df.withColumn('rownum', row_number().over(window_spec))
             .filter('rownum = 1')
             .drop("rownum")
             .withColumn("silver_load_time", current_timestamp())
             .withColumn("silver_load_date", current_date())
)

if not DeltaTable.isDeltaTable(spark, args.silver_path):

    print("Table not found → creating new Delta table")

    latest_df.write.format("delta") \
        .mode("overwrite") \
        .save(args.silver_path)

else:
    print("Table exists → performing merge")

    silver = DeltaTable.forPath(spark, args.silver_path)

    condition = ""
    for c in primary_keys:
        if condition == "":
            condition = f"target.{c} = source.{c}"
        else:
            condition += f" AND target.{c} = source.{c}"

    (silver.alias("target")
     .merge(latest_df.alias("source"), condition)
     .whenMatchedUpdateAll(
         condition=f"source.{args.incremental_col} > target.{args.incremental_col}"
     )
     .whenNotMatchedInsertAll()
     .execute()
    )