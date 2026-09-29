from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("is_cancelled", "id")
w_hist = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())
w_recent = Window.partitionBy("plan").orderBy(F.col("is_cancelled").asc_nulls_last())
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("status", "plan").orderBy(F.col("is_cancelled").desc_nulls_last())

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 28, 33]) & (F.col("end_date") > F.lit("2023-04-21").cast("date")))
    .withColumn("is_cancelled", F.when(F.col("status") == "CANCELLED", 1).otherwise(0))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "plan", "monthly_fee", "status", "is_cancelled")
)

remaining_keys = (
    deduped
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") < "2024-04-01")
        .select("status")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("is_cancelled") > 56)
    .select(
        "plan",
        "monthly_fee",
        "status",
        "is_cancelled",
        F.percent_rank().over(w_hist).alias("monthly_fee_percent_rank"),
        F.dense_rank().over(w_recent).alias("is_cancelled_dense_rank"),
    )
)

with_windows_subscriptions = (
    with_windows
    .withColumn("status_avg_monthly_fee", F.avg("monthly_fee").over(w4))
    .withColumn("plan_avg_monthly_fee", F.avg("monthly_fee").over(w5))
    .withColumn("is_cancelled_dense_rank_share", F.round((F.col("is_cancelled_dense_rank") / F.sum("is_cancelled_dense_rank").over(w4)), 4))
    .withColumn("is_cancelled_cume_dist", F.cume_dist().over(w6))
)

result = (
    with_windows_subscriptions
    .groupBy("status")
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.sum("plan_avg_monthly_fee").alias("total_plan_avg_monthly_fee"),
        F.avg("status_avg_monthly_fee").alias("avg_status_avg_monthly_fee"),
    )
)
