from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("order_id").isNotNull())

shipments_stats = (
    valid_shipments
    .groupBy("carrier")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    shipments_stats
    .filter(F.col("avg_cost") > 65)
    .select("carrier", "avg_cost")
    .orderBy("carrier")
)
