from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([18, 21, 40])), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("shipping_fee") >= 34, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list_2"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").asc_nulls_first())
)
