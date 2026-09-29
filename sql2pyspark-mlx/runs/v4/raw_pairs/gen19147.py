from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("delivered_date") >= "2025-02-01") & (F.col("cost") < 28)) | F.month("shipped_date").isin([3, 5, 8]))
    .groupBy("carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy("avg_cost")
)
