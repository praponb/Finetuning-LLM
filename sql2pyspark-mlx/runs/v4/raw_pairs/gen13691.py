from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "method")
w4 = Window.orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("method", "order_id").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy(F.col("session_no").desc(), "id")
w8 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w9 = Window.partitionBy("order_id").orderBy(F.col("num_methods").desc())

valid = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .select("*", F.col("paid_at").cast("date").alias("paid_at_day"))
)

with_dims = (
    valid
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.amount").alias("order_amount"))
)

flagged = (
    with_dims
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

enriched = (
    sessionized
    .select(
        "*",
        F.count("order_amount").over(w3).alias("order_id_count_order_amount"),
        F.last("order_amount", ignorenulls=True).over(w4).alias("last_known_order_amount"),
        F.avg("amount").over(w5).alias("amount_avg_5_prior"),
        F.last("session_start_flag", ignorenulls=True).over(w6).alias("last_known_session_start_flag"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

filtered = (
    enriched
    .filter((F.col("paid_at") > F.lit("2023-05-08 05:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .withColumn("event_seq_times_session_start_flag", (F.col("event_seq") * F.col("session_start_flag")))
)

base = (
    filtered
    .filter(F.col("order_amount") >= 1530)
    .select("*", F.trim("method").alias("clean_method"))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

stats = (
    latest
    .groupBy(
        "order_id",
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
        "session_no",
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.avg(F.when(F.col("method") == "paypal", F.col("last_known_order_amount"))).alias("paypal_avg_last_known_order_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount_avg_5_prior"))).alias("success_total_amount_avg_5_prior"),
    )
    .withColumn("num_methods_dense_rank", F.dense_rank().over(w9))
)

result = (
    stats
    .select(
        "order_id",
        "session_start_flag_tier",
        "session_no",
        "num_methods",
        "bank_transfer_rows",
        "paypal_avg_last_known_order_amount",
        "success_total_amount_avg_5_prior",
    )
    .orderBy(F.col("paypal_avg_last_known_order_amount").desc())
)
