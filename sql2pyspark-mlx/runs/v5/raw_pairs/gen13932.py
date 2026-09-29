from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel")
w2 = Window.partitionBy("channel").orderBy(F.col("amount").asc_nulls_first())
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.channel"))
)

metrics = (
    joined
    .select(
        "*",
        F.avg("amount").over(w).alias("status_avg_amount"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "status_avg_amount",
        "amount_cume_dist",
        F.concat_ws("-", "channel", "method").alias("channel_method_key"),
    )
)

filtered = (
    cleaned
    .filter(F.col("amount") >= 1540)
    .select(
        "*",
        (F.col("amount_cume_dist") * F.col("status_avg_amount")).alias("amount_cume_dist_times_status_avg_amount"),
    )
)

cleaned_payments = (
    filtered
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "status",
        "status_avg_amount",
        "amount_cume_dist",
        "channel_method_key",
        "amount_cume_dist_times_status_avg_amount",
        F.lower("status").alias("status_lower"),
        F.lower("status").alias("status_lower_2"),
    )
)

pivoted = (
    cleaned_payments
    .groupBy("order_id")
    .pivot("status", ["SUCCESS", "REFUNDED", "FAILED"])
    .agg(F.sum("amount_cume_dist_times_status_avg_amount"))
)

result = (
    pivoted
    .join(quantity_by_order_id, on="order_id", how="left")
    .select("order_id", "FAILED", F.col("avg_order_item_quantity"))
    .orderBy(F.col("FAILED").desc())
)
