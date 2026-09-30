from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([6, 22])), "left_anti")
)

result = (
    eligible
    .groupBy("status", F.date_trunc("QUARTER", "order_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.max(F.when(F.col("status") == "SHIPPED", F.col("discount"))).alias("shipped_max_discount"),
    )
    .orderBy(F.col("shipped_max_discount").desc_nulls_first())
)
