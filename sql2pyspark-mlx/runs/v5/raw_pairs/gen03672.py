from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 89) | F.col("carrier").isin(["Kerry", "UPS"]))
    .groupBy(
        F.when(F.col("cost") >= 105, "high").when(F.col("cost") >= 30, "mid").otherwise("low").alias("cost_bucket"),
        "carrier",
    )
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.avg("cost").alias("avg_cost"),
    )
)
