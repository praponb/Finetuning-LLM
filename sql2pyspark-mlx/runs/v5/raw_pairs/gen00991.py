from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.discount") < 0.33)
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("o.channel") == "mobile", F.col("o.amount")).otherwise(0)).alias("mobile_amount"),
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
    )
)
