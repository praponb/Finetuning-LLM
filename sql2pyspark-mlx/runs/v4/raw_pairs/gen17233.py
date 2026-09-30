from pyspark.sql import functions as F

src = df_orders.filter((F.col("amount") > 1460) & (F.col("amount") < 600))

summary = (
    src
    .groupBy("store_id")
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    summary
    .filter(F.col("min_shipping_fee") > 11)
    .select("store_id", "min_shipping_fee", "channel_list")
)
