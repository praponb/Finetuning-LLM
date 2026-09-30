from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 79, "high").when(F.col("cost") >= 32, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") < 560)
    .select("carrier", "cost_bucket", "kerry_count", "total_cost")
)
