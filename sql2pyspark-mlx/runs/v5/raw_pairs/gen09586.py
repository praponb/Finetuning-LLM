from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 10, 25, 28]))
    .select(F.lit("order_item").alias("origin"), "unit_price")
    .unionByName(
        df_orders
        .select(F.lit("order").alias("origin"), F.col("shipping_fee").alias("unit_price"))
    )
    .distinct()
    .orderBy("unit_price")
)
