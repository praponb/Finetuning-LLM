from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("method").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method")
w_part = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("running_count_discount", "id")
w5 = Window.partitionBy("method").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_max_discount", "id").rowsBetween(-5, -1)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-11-15 14:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.channel"), F.col("p.status").alias("order_status"))
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.order_date").between("2023-10-22", "2025-10-01"))
    .select(
        "s.*",
        F.col("p.amount").alias("order_amount"),
        F.col("p.discount"),
        F.col("p.status").alias("order_status_2"),
    )
)

enriched_payments = (
    joined
    .select(
        "id",
        "method",
        "amount",
        "status_title",
        "method_filled",
        "channel",
        F.min("discount").over(w_ordered).alias("running_min_discount"),
        F.max("discount").over(w_ordered).alias("running_max_discount"),
        F.count("discount").over(w_ordered).alias("running_count_discount"),
        F.sum("discount").over(w_ordered).alias("running_sum_discount"),
        F.avg("discount").over(w2).alias("method_avg_discount"),
    )
)

scored = (
    enriched_payments
    .select(
        "id",
        "method",
        "method_filled",
        "running_min_discount",
        "running_max_discount",
        "running_count_discount",
        "running_sum_discount",
        "method_avg_discount",
        F.sum("running_sum_discount").over(w_part).alias("running_sum_running_sum_discount"),
        F.min("running_sum_discount").over(w_part).alias("running_min_running_sum_discount"),
        F.avg("running_sum_discount").over(w_part).alias("running_avg_running_sum_discount"),
        F.count("running_sum_discount").over(w_part).alias("running_count_running_sum_discount"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "method_filled",
        "running_min_discount",
        "running_sum_discount",
        "method_avg_discount",
        "running_min_running_sum_discount",
        "running_avg_running_sum_discount",
        "running_count_running_sum_discount",
        "event_seq",
        F.sum("running_min_discount").over(w5).alias("cumulative_running_min_discount"),
        F.avg("method_avg_discount").over(w6).alias("method_avg_discount_avg_5_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "method_filled",
        "running_min_discount",
        "running_sum_discount",
        "method_avg_discount",
        "running_count_running_sum_discount",
        "event_seq",
        "cumulative_running_min_discount",
        "method_avg_discount_avg_5_prior",
    )
    .orderBy(F.col("event_seq").desc(), F.col("id").desc())
)
