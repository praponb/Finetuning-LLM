from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.date_trunc("week", "last_restock").alias("week_start"))
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.avg("quantity").alias("avg_quantity"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
