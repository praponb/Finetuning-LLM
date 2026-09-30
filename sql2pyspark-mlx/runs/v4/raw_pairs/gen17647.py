from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 44) | ~F.col("status").isin(["DELIVERED", "LOST", "RETURNED"]))
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)
