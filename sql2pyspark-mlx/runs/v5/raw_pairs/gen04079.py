from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") > F.lit("2023-11-09 19:00:00").cast("timestamp")))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

matched_keys = (
    islands
    .select("order_id", "id")
    .intersect(
        df_payments
        .filter(F.col("method").isin(["card", "cash"]))
        .select("order_id", "id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("paid_at") >= F.lit("2023-09-14").cast("date"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("session_id_times_amount", (F.col("session_id") * F.col("amount")))
)

filtered_payments = (
    base
    .filter((F.col("paid_at") > F.lit("2023-09-17 08:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "session_id",
        "status_prefix",
        "session_id_times_amount",
        F.length("status").alias("status_length"),
        F.round((F.col("session_id_times_amount") / F.col("amount")), 2).alias("session_id_times_amount_per_amount"),
    )
)

grouped = (
    filtered_payments
    .groupBy(
        F.when(F.col("amount") >= 1340, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count_if(F.col("status") == "SUCCESS").alias("success_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    grouped
    .select(
        "amount_tier",
        "activity_month",
        "distinct_status",
        "amount_stddev",
        "success_events",
        "status_list",
    )
    .orderBy("success_events", F.col("activity_month").desc())
)
