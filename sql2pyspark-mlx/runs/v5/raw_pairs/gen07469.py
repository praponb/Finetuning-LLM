from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([29, 32, 35]))
    .groupBy("order_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .orderBy("order_id")
)
