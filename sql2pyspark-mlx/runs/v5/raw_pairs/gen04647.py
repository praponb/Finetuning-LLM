from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 10) & (F.col("unit_price").between(120, 190) | (F.col("unit_price") > 360)))
    .groupBy("order_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("*").alias("order_item_count"),
    )
    .filter(F.col("unit_price_range") >= 60)
)
