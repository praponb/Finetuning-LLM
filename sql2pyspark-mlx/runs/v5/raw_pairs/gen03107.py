from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("shipped_date") < "2024-07-17")
    .withColumn("status_title", F.initcap(F.trim("status")))
)

agg = (
    valid_shipments
    .groupBy("status")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    agg
    .filter(F.col("max_cost") > 74)
    .select("status", "max_cost")
    .orderBy("status")
)
