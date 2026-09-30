from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("status") == "DELIVERED")

shipments_stats = (
    valid_shipments
    .groupBy("carrier")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    shipments_stats
    .orderBy(F.col("carrier").desc(), F.col("cost_range").desc())
    .limit(10)
)
