from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 39)
    .select(F.col("unit_price").alias("value"), F.lit("order_item").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("shipping_fee").alias("value"), F.lit("order").alias("source"))
    )
    .distinct()
)
