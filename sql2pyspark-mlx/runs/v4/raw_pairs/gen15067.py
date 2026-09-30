from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2025-07-31")
    .groupBy("status")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") > 380)
    .select("status", "total_cost")
)
