from pyspark.sql import functions as F

base = df_orders.filter(F.col("shipping_fee") > 39)

orders_stats = (
    base
    .groupBy("status", "channel")
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
)

result = (
    orders_stats
    .select("status", "channel", "store_count", "discount_range")
    .orderBy("channel", "status")
    .limit(50)
)
