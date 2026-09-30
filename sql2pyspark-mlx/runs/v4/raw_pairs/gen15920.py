from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("order_id").isin([6, 21, 30]))
    .withColumn("greatest_unit_price_quantity", F.greatest("unit_price", "quantity"))
)

result = (
    base
    .withColumn("greatest_unit_price_quantity_tier", F.when(F.col("greatest_unit_price_quantity") >= 80, "PREMIUM").otherwise("STANDARD"))
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 320, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["greatest_unit_price_quantity_tier", "unit_price_tier"], ["greatest_unit_price_quantity_tier"], []],
        "greatest_unit_price_quantity_tier",
        "unit_price_tier",
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") > 0)
)
