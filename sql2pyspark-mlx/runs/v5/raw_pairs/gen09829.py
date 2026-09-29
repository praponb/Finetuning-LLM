from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(130, 340) | F.col("order_id").isin([8, 23, 32, 38]))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 4)
    .drop("total_count")
    .orderBy("unit_price_range")
)
