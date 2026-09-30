from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & F.col("channel").isin(["store", "mobile"]) & F.col("shipping_fee").between(20, 23))
    .groupBy("channel")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
    .orderBy("channel")
)
