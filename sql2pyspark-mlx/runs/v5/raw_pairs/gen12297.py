from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("island_id", "id")

base = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select("a.*", F.col("o.order_date"))
)

scored = (
    with_dims
    .withColumn("running_count_amount", F.count("amount").over(w_part))
    .withColumn("running_avg_amount", F.avg("amount").over(w_part))
    .withColumn("running_sum_amount", F.sum("amount").over(w_part))
    .withColumn("running_is_refunded", F.sum("is_refunded").over(w_part_2))
)

flagged = (
    scored
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "is_refunded",
        F.sum("session_start_flag").over(w4).alias("island_id"),
    )
)

scored_payments = (
    sessions_payments
    .filter(~F.col("status").isin(["SUCCESS", "FAILED"]))
    .withColumn("running_max_is_refunded", F.max("is_refunded").over(w5))
    .withColumn("running_min_is_refunded", F.min("is_refunded").over(w5))
    .withColumn("running_avg_is_refunded", F.avg("is_refunded").over(w5))
    .withColumn("running_sum_is_refunded", F.sum("is_refunded").over(w5))
    .withColumn("island_id_bucket", F.ntile(3).over(w6))
)

result = (
    scored_payments
    .groupBy(F.date_trunc("week", "paid_at").alias("week_start"))
    .agg(
        F.min("paid_at").alias("started_at"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .orderBy(F.col("distinct_status").asc_nulls_first())
)
