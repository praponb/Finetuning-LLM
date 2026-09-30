from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("amount").asc_nulls_last())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_windows = (
    df_orders
    .select(
        "*",
        F.sum("amount").over(w).alias("cumulative_amount"),
        F.sum("amount").over(w2).alias("running_amount"),
        F.dense_rank().over(w3).alias("amount_dense_rank"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

kept = (
    with_last
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 18), "left_anti")
)

summary = (
    kept
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("running_amount", 0.5).alias("running_amount_p50"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
)

result = (
    summary
    .select(
        "quarter_start",
        "running_amount_p50",
        "amount_quartiles",
        "channel_list",
        "corr_discount_amount",
        "store_count",
        "avg_discount",
    )
    .orderBy(F.col("store_count").desc_nulls_last())
)
