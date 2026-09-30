from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") == 3))
    .withColumn("unit_price_rounded", F.round("unit_price", 2))
)

result = (
    valid
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("avg_unit_price").desc_nulls_first())
)
