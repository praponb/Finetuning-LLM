from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([6, 10, 34, 38]))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("order_item_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("order_item_count") > 8)
    .orderBy(F.col("quantity_range").desc())
)
