from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("end_date", "id")
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_avg_5").desc(), "id")
w6 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_avg_5").desc(), "id")
w7 = Window.orderBy(F.col("prev_monthly_fee").asc_nulls_last())
w8 = Window.partitionBy("status").orderBy("latest_rank_plus_prev_monthly_fee", "id").rowsBetween(-2, -1)
w_recent = Window.partitionBy("status").orderBy(F.col("prev_monthly_fee").desc(), "id")
w10 = Window.partitionBy("status").orderBy("prev_monthly_fee_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_rank", F.rank().over(w))
    .withColumn("monthly_fee_avg_5", F.avg("monthly_fee").over(w2))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w3))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 1).over(w4))
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "status",
        "monthly_fee_rank",
        "monthly_fee_avg_5",
        "prev_monthly_fee",
        "latest_rank",
    )
)

valid = (
    deduped
    .filter(F.col("monthly_fee_rank") < 5)
    .select(
        "*",
        (F.col("latest_rank") + F.col("prev_monthly_fee")).alias("latest_rank_plus_prev_monthly_fee"),
    )
)

scored = (
    valid
    .select(
        "id",
        "status",
        "monthly_fee_rank",
        "monthly_fee_avg_5",
        "latest_rank",
        F.dense_rank().over(w7).alias("prev_monthly_fee_dense_rank"),
        F.avg("prev_monthly_fee").over(w8).alias("prev_monthly_fee_avg_2_prior"),
        F.ntile(4).over(w_recent).alias("prev_monthly_fee_bucket"),
        F.ntile(5).over(w_recent).alias("prev_monthly_fee_bucket_2"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "status",
        "monthly_fee_rank",
        "monthly_fee_avg_5",
        "latest_rank",
        "prev_monthly_fee_dense_rank",
        "prev_monthly_fee_avg_2_prior",
        F.sum("monthly_fee_rank").over(w10).alias("running_sum_monthly_fee_rank"),
        F.count("monthly_fee_rank").over(w10).alias("running_count_monthly_fee_rank"),
        F.min("monthly_fee_rank").over(w10).alias("running_min_monthly_fee_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "status",
        "running_sum_monthly_fee_rank",
        "running_min_monthly_fee_rank",
    )
    .unpivot(
        ids=["id", "status"],
        values=["running_sum_monthly_fee_rank", "running_min_monthly_fee_rank"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
