from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "product_id",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)
