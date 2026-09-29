from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 17) | F.col("product_id").isNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        F.ceil("quantity").alias("quantity_whole"),
    )
    .orderBy("order_id", "id")
    .limit(10)
)
