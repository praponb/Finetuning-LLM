from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("method", "status")
w5 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method", "status").orderBy(F.col("running_island_id").desc_nulls_last())
w7 = Window.partitionBy("method").orderBy("paid_at", "id")
w8 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("method").isin(["promptpay", "cash"]), "Group A").otherwise("Other").alias("method_group"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "method_group",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

scored = (
    sessionized
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.avg("island_id").over(w4).alias("method_avg_island_id"),
        F.last("method", ignorenulls=True).over(w5).alias("last_known_method"),
        F.sum("island_id").over(w5).alias("running_island_id"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

with_gaps_2 = (
    deduped
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w7).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w7) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps_2.withColumn("session_no", F.sum("is_new_session").over(w8))

metrics = (
    sessions_payments
    .withColumn("last3_max_method_avg_island_id", F.max("method_avg_island_id").over(w_part))
    .withColumn("last3_sum_method_avg_island_id", F.sum("method_avg_island_id").over(w_part))
    .withColumn("last3_avg_method_avg_island_id", F.avg("method_avg_island_id").over(w_part))
)

flagged = (
    metrics
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w7).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w7) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = (
    flagged
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "island_id",
        "method_avg_island_id",
        "is_new_session",
        "session_no",
        "last3_max_method_avg_island_id",
        "last3_sum_method_avg_island_id",
        "last3_avg_method_avg_island_id",
        F.sum("new_group_flag").over(w8).alias("session_id"),
    )
)

daily = (
    sessions_payments_2
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .rollup("year_start")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("method") == "cash", F.col("last3_sum_method_avg_island_id")).otherwise(0)).alias("cash_last3_sum_method_avg_island_id"),
        F.sum(F.when(F.col("method") == "paypal", F.col("amount"))).alias("paypal_total_amount"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    daily
    .select("year_start", "cash_last3_sum_method_avg_island_id", "gid")
    .orderBy(F.col("year_start").desc())
)
