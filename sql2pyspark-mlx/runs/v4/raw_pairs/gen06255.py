from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-11-08")))
    .withColumn("is_store", F.when(F.col("channel") == "store", 1).otherwise(0))
    .withColumn("discount_whole", F.floor("discount"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    cleaned
    .select("store_id", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee") < 38)
        .select("store_id", "customer_id")
    )
)
