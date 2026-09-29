from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.min("unit_price").alias("min_unit_price"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") > 270)
    .drop("avg_unit_price")
    .orderBy("unit_price_range")
)
