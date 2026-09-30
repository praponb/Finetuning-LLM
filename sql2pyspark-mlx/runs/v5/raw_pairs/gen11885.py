from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .groupBy(
        F.when(F.col("orders.channel").isin(["mobile", "store"]), "group_a").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("payments.status")))).alias("status_values"),
    )
    .orderBy("channel_group")
)
