from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("store_id").isNull() & ((F.col("discount") <= 0.15) | F.col("store_id").isNotNull()))
)

summary = (
    valid_orders
    .groupBy("channel", "status")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = summary
