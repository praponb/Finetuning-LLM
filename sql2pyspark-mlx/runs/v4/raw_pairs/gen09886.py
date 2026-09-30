from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("amount") > 580)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.upper("method").alias("method_upper"),
    )
)

with_prev = cleaned.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .groupBy(
        "method",
        F.date_trunc("WEEK", "prev_paid_at").alias("week_start"),
        "order_id",
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.countDistinct("method").alias("distinct_methods"),
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
    )
    .orderBy(F.col("method").asc_nulls_last())
)
