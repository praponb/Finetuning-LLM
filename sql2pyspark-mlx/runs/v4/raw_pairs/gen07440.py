from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.avg("unit_price").alias("avg_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("total_unit_price") >= 1100)
    .drop("total_unit_price")
    .orderBy("avg_unit_price", F.col("unit_price_range").desc())
)
