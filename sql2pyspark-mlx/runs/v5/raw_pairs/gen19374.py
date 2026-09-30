from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").asc_nulls_last())
w_part = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .select(
        "*",
        F.concat_ws("/", "status", "method").alias("status_method_key"),
        F.round((F.col("amount") * 1.15), 2).alias("amount_adjusted"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "method", "amount", "status")
)

with_windows = (
    latest
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        F.max("amount").over(w_part).alias("running_max_amount"),
        F.avg("amount").over(w_part).alias("running_avg_amount"),
        F.min("amount").over(w_part).alias("running_min_amount"),
        F.count("amount").over(w_part).alias("running_count_amount"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.running_avg_amount",
        F.col("p.discount"),
        F.col("p.shipping_fee"),
    )
)

report = enriched.select("id", "order_id", "running_avg_amount", "discount", "shipping_fee")

result = (
    report
    .select("id", "running_avg_amount", "discount", "shipping_fee")
    .orderBy(F.col("discount").desc_nulls_first())
)
