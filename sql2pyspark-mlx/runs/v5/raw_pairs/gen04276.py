from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("paid_at", "id")
w7 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("activity_month").orderBy(F.col("web_rows").desc())

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

scored = (
    sessionized
    .filter(F.col("method").isin(["card", "cash", "promptpay"]))
    .select(
        "id",
        "order_id",
        "paid_at",
        F.max("burst_id").over(w3).alias("burst_id_max_7d"),
        F.first("method", ignorenulls=True).over(w4).alias("first_known_method"),
        F.sum("burst_id").over(w5).alias("burst_id_sum_90d"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.paid_at",
        "src.burst_id_max_7d",
        "src.first_known_method",
        F.col("d.discount"),
        F.col("d.channel"),
    )
)

flagged_2 = (
    joined
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w6).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_payments = flagged_2.withColumn("burst_id", F.sum("new_group_flag").over(w7))

grouped = (
    sessionized_payments
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.percentile_approx("burst_id_max_7d", 0.5).alias("burst_id_max_7d_p50"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .withColumn("web_rows_dense_rank", F.dense_rank().over(w8))
)

result = (
    grouped
    .select(
        "activity_month",
        "week_start",
        "web_rows",
        "burst_id_max_7d_p50",
        "web_rows_dense_rank",
    )
)
