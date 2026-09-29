from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter((F.col("unit_price") >= 170) & (F.col("unit_price") > 310))

agg = (
    filtered_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = agg
