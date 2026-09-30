from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quantity_tier", "order_id"], ["quantity_tier"], []],
        "quantity_tier",
        "order_id",
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("total_quantity") < 0)
    .orderBy(F.col("quantity_tier").asc_nulls_last())
)
