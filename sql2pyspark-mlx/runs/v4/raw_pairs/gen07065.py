from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.orderBy("amount", "id")
w6 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "method").orderBy(F.col("amount_scaled").desc_nulls_last(), "id")
w9 = Window.partitionBy("method").orderBy("is_new_burst", "id").rowsBetween(-2, -1)
w10 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("method").orderBy(F.col("amount_scaled").desc(), "id")
w12 = Window.partitionBy("method", "status").orderBy("method_avg_session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("amount").desc(), "id")
w14 = Window.partitionBy("order_id").orderBy("first_known_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-06-12 01:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("amount_scaled", F.round((F.col("amount") * 0.9), 2))
    .withColumn("status_method", F.concat("status", F.lit(" - "), "method"))
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

metrics = (
    islands
    .select(
        "*",
        F.avg("session_no").over(w3).alias("method_avg_session_no"),
        F.count("amount_scaled").over(w4).alias("amount_scaled_count_7d"),
        F.row_number().over(w5).alias("event_seq"),
        F.first("amount", ignorenulls=True).over(w6).alias("first_known_amount"),
        F.last("method", ignorenulls=True).over(w7).alias("last_known_method"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .withColumn("session_no_avg_2_prior", F.avg("session_no").over(w9))
    .withColumn("cumulative_amount", F.sum("amount").over(w10))
    .withColumn("amount_scaled_bucket", F.ntile(10).over(w11))
)

filtered = (
    scored
    .filter(F.col("paid_at") > F.lit("2024-04-21 19:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_scaled",
        "is_new_burst",
        "session_no",
        "method_avg_session_no",
        "event_seq",
        "first_known_amount",
        "last_known_method",
        "row_num",
        "session_no_avg_2_prior",
        "cumulative_amount",
        "amount_scaled_bucket",
        F.floor("cumulative_amount").alias("cumulative_amount_whole"),
        F.col("event_seq").cast("double").alias("event_seq_str"),
    )
)

filtered_payments = (
    filtered
    .filter(F.col("event_seq").between(4, 5))
    .withColumn("method_title", F.initcap(F.trim("method")))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

result = (
    filtered_payments
    .withColumn("cumulative_session_no_avg_2_prior", F.sum("session_no_avg_2_prior").over(w12))
    .withColumn("amount_bucket", F.ntile(4).over(w13))
    .withColumn("running_cumulative_amount", F.sum("cumulative_amount").over(w14))
)
