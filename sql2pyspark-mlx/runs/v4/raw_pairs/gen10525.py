from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(320, 400) | F.col("product_id").isin([3, 9, 15, 40]))
    .select(
        "id",
        "product_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.ceil("quantity").alias("quantity_whole"),
    )
    .orderBy("quantity_whole", "id")
    .limit(5)
)
