from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").desc(), "id")
w_running = Window.partitionBy("method").orderBy("running_avg_amount", "id")
w4 = Window.partitionBy("status", "method").orderBy("running_avg_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method", "status").orderBy(F.col("running_max_amount").desc(), "id")
w6 = Window.orderBy("running_max_amount", "id")
w_part = Window.orderBy("running_avg_amount", "id")
w8 = Window.partitionBy("method")
w9 = Window.partitionBy("status").orderBy(F.col("running_max_amount_bucket").desc(), "id")

with_windows = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "running_avg_amount",
        "running_sum_amount",
        "running_max_amount",
    )
)

enriched = (
    ranked
    .filter(F.col("status") == "REFUNDED")
    .select(
        "*",
        F.lead("running_sum_amount", 1).over(w_running).alias("next_running_sum_amount"),
        F.last("running_max_amount", ignorenulls=True).over(w4).alias("last_known_running_max_amount"),
        F.ntile(10).over(w5).alias("running_max_amount_bucket"),
        F.ntile(4).over(w_running).alias("running_avg_amount_bucket"),
    )
)

with_windows_payments = (
    enriched
    .select(
        "id",
        "status",
        "running_max_amount",
        "next_running_sum_amount",
        "running_max_amount_bucket",
        F.lag("next_running_sum_amount", 2).over(w6).alias("prev_next_running_sum_amount"),
        F.row_number().over(w_part).alias("event_seq"),
        F.count("running_sum_amount").over(w8).alias("method_count_running_sum_amount"),
    )
)

deduped = (
    with_windows_payments
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "status",
        "running_max_amount",
        "next_running_sum_amount",
        "prev_next_running_sum_amount",
        "event_seq",
        "method_count_running_sum_amount",
    )
)

result = (
    deduped
    .groupBy(
        F.when(F.col("prev_next_running_sum_amount") >= 8900, "LARGE").otherwise("SMALL").alias("prev_next_running_sum_amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("event_seq_per_success"),
    )
    .orderBy(F.col("failed_rows").desc())
)
