from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["order_id", "quantity_tier"], ["order_id"]],
        "order_id",
        "quantity_tier",
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.grouping("order_id").alias("gid"),
    )
    .filter(F.col("avg_unit_price") >= 0)
    .orderBy(F.col("order_id").desc())
)
