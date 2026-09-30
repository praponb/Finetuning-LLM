from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 3)
    .select(
        F.lit("order_item").alias("record_type"),
        F.col("unit_price").alias("value"),
    )
    .unionByName(
        df_orders
        .filter(F.col("status") == "RETURNED")
        .select(F.lit("order").alias("record_type"), F.col("shipping_fee").alias("value"))
    )
    .distinct()
    .orderBy("record_type")
)
