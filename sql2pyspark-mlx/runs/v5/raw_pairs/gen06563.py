from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("status", "method").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method").orderBy(F.col("session_start_flag").asc_nulls_last(), "id")

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-06-02 14:00:00")))
    .select(
        "*",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.avg("amount").over(w2).alias("running_avg_amount"),
        F.sum("amount").over(w2).alias("running_sum_amount"),
        F.count("amount").over(w2).alias("running_count_amount"),
        F.sum("amount").over(w3).alias("running_amount"),
    )
)

with_gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w5))

ranked = (
    sessions_payments
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .cube(
        F.col("p.status").alias("order_status"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.running_count_amount")).alias("avg_running_count_amount"),
        F.round(F.sum(F.col("a.session_no")), 2).alias("total_session_no"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("total_session_no").desc())
)
