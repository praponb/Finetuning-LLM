from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 11, 15, 34]) & F.col("order_id").isin([5, 6, 31, 38]) & (F.col("product_id") == 15))
    .select(
        "id",
        "product_id",
        "order_id",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)
