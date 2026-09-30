from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 120)
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .orderBy(F.col("unit_price_count").desc(), "order_id")
    .limit(5)
)
