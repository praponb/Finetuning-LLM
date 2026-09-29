from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.when(F.col("cost") >= 85, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
    )
    .orderBy(F.col("cost_tier").desc())
)
