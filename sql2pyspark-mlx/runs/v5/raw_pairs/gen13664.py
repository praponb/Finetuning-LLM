from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("shipping_fee") < 35)
    .groupBy("store_id")
    .agg(
        F.min("order_date").alias("first_order_date"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.shipping_fee_range") >= 21)
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("t.first_order_date"),
        F.col("t.shipping_fee_range"),
    )
)
