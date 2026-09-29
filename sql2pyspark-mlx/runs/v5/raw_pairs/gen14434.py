from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "order_id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("double").alias("quantity_str"),
        F.ceil("unit_price").alias("unit_price_whole"),
    )
    .orderBy("quantity_str", F.col("unit_price_whole").desc())
)
