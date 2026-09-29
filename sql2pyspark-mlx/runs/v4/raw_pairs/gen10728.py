from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["order_id", "quantity_tier", "product_id"], ["order_id", "quantity_tier"], ["order_id"]],
        "order_id",
        "quantity_tier",
        "product_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.stddev_samp("unit_price").alias("unit_price_stddev_2"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("unit_price_stddev_2").asc_nulls_last())
)
