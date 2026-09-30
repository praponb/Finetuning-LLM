from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("days_since_paid_at", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("order_id").orderBy(F.col("days_since_paid_at").desc(), "id")
w3 = Window.orderBy("days_since_paid_at", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("days_since_paid_at_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("paid_at", "id")
w8 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status").orderBy(F.col("days_since_paid_at_max_30d").desc_nulls_last())

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "days_since_paid_at",
        F.avg("days_since_paid_at").over(w).alias("days_since_paid_at_avg_5_prior"),
        F.ntile(10).over(w2).alias("days_since_paid_at_bucket"),
        F.avg("days_since_paid_at").over(w3).alias("days_since_paid_at_avg_6_prior"),
        F.max("days_since_paid_at").over(w4).alias("days_since_paid_at_max_30d"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "days_since_paid_at_bucket",
        "days_since_paid_at_max_30d",
        F.min("days_since_paid_at").over(w5).alias("last4_min_days_since_paid_at"),
        F.max("days_since_paid_at").over(w5).alias("last4_max_days_since_paid_at"),
        F.count("days_since_paid_at").over(w5).alias("last4_count_days_since_paid_at"),
        F.avg("days_since_paid_at_avg_6_prior").over(w6).alias("days_since_paid_at_avg_6_prior_avg_7d"),
    )
)

with_prev = scored.withColumn("prev_paid_at", F.lag("paid_at").over(w7))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "paid_at",
        "status",
        "days_since_paid_at_max_30d",
        "last4_min_days_since_paid_at",
        "last4_max_days_since_paid_at",
        "days_since_paid_at_avg_6_prior_avg_7d",
        F.sum("new_group_flag").over(w8).alias("island_id"),
    )
)

latest = (
    islands
    .withColumn("rn", F.rank().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "last4_min_days_since_paid_at",
        "last4_max_days_since_paid_at",
        "island_id",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("last4_min_days_since_paid_at") >= 67, "HIGH").otherwise("LOW").alias("last4_min_days_since_paid_at_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("last4_min_days_since_paid_at").alias("avg_last4_min_days_since_paid_at"),
        F.count("*").alias("row_count_2"),
        F.round(F.avg("last4_min_days_since_paid_at"), 2).alias("avg_last4_min_days_since_paid_at_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
