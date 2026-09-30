from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([8, 11, 16, 40]))
    .groupBy(
        F.when(F.col("channel").isin(["store", "partner"]), "tier_1").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
