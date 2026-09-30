from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
    .orderBy(F.col("delivered_count").desc())
)
