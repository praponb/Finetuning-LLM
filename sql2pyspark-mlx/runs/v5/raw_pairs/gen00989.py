from pyspark.sql import functions as F

result = (
    df_shipments
    .rollup(
        "carrier",
        F.when(F.col("cost") >= 87, "high").when(F.col("cost") >= 45, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
        F.count("*").alias("shipment_count"),
    )
)
