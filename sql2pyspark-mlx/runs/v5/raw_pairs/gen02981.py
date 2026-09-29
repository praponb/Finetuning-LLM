from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id", "status").orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("order_id").orderBy("last7_avg_amount", "id")
w_running = Window.orderBy("last7_max_amount", "id")
w_ordered = Window.partitionBy("order_id").orderBy("next_last7_max_amount", "id")
w6 = Window.partitionBy("status").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("status") == "REFUNDED"))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.sum("amount").over(w_hist).alias("last7_sum_amount"),
        F.avg("amount").over(w_hist).alias("last7_avg_amount"),
        F.max("amount").over(w_hist).alias("last7_max_amount"),
        F.min("amount").over(w_hist).alias("last7_min_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

with_windows_payments = (
    with_windows
    .filter(F.col("last7_max_amount") >= 1500)
    .withColumn("last7_avg_amount_bucket", F.ntile(10).over(w3))
    .withColumn("next_last7_max_amount", F.lead("last7_max_amount", 1).over(w_running))
)

metrics = (
    with_windows_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "status_title",
        "last7_sum_amount",
        "amount_share",
        "last7_avg_amount_bucket",
        "next_last7_max_amount",
        F.lead("last7_min_amount").over(w_ordered).alias("next_last7_min_amount"),
        F.lag("last7_min_amount").over(w_ordered).alias("prev_last7_min_amount"),
        F.lag("last7_avg_amount_bucket").over(w_ordered).alias("prev_last7_avg_amount_bucket"),
        F.lead("last7_avg_amount_bucket").over(w_ordered).alias("next_last7_avg_amount_bucket"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.first("last7_max_amount", ignorenulls=True).over(w6).alias("first_known_last7_max_amount"),
    )
)

filtered = (
    metrics
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-07-20 22:00:00"))) & (F.col("amount") < 1250))
    .withColumn("seq_no_scaled", F.round((F.col("seq_no") * 100), 2))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "High").when(F.col("seq_no") >= 4, "Medium").otherwise("Low"))
)

summary = (
    filtered
    .groupBy(F.date_trunc("quarter", "paid_at").alias("quarter_start"))
    .agg(
        F.min("paid_at").alias("started_at"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.max("paid_at").alias("ended_at"),
    )
    .filter(F.col("num_status") > 1)
)

result = (
    summary
    .select("quarter_start", "started_at", "num_status", "ended_at")
    .orderBy(F.col("quarter_start").desc(), F.col("started_at").desc())
)
