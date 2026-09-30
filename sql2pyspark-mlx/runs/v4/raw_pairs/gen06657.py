from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 87, "high").when(F.col("cost") >= 22, "mid").otherwise("low").alias("cost_bucket"),
        "carrier",
    )
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
    .orderBy("fedex_count")
)
