from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee", F.lit("order").alias("source"))
    .union(
        df_order_items
        .select("unit_price", F.lit("order_item").alias("source"))
    )
    .distinct()
    .orderBy("shipping_fee")
)
