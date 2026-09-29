from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("cost") <= 89)

summary = (
    valid_shipments
    .groupBy("status")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    summary
    .select("status", "total_cost")
    .orderBy("status")
)
