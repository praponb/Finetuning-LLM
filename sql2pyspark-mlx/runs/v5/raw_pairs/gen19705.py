from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc_nulls_first())
w4 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").asc_nulls_first())
w6 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("plan").orderBy("monthly_fee_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("monthly_fee_avg_3", "id")
w9 = Window.partitionBy("status")

latest = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "plan",
        "status",
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.percent_rank().over(w3).alias("monthly_fee_percent_rank"),
        F.sum("monthly_fee").over(w4).alias("cumulative_monthly_fee"),
        F.dense_rank().over(w5).alias("monthly_fee_dense_rank"),
        F.avg("monthly_fee").over(w6).alias("monthly_fee_avg_3"),
    )
)

with_windows = (
    metrics
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]))
    .select(
        "*",
        F.min("monthly_fee_dense_rank").over(w7).alias("running_min_monthly_fee_dense_rank"),
        F.avg("monthly_fee_dense_rank").over(w7).alias("running_avg_monthly_fee_dense_rank"),
        F.count("monthly_fee_dense_rank").over(w7).alias("running_count_monthly_fee_dense_rank"),
        F.max("monthly_fee_dense_rank").over(w7).alias("running_max_monthly_fee_dense_rank"),
        F.ntile(3).over(w8).alias("monthly_fee_avg_3_bucket"),
    )
)

remaining_keys = (
    with_windows
    .select("status")
    .exceptAll(
        df_subscriptions
        .filter(~F.col("plan").isin(["enterprise", "basic"]))
        .select("status")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("cumulative_monthly_fee") != 853)
    .select(
        "id",
        "plan",
        "status",
        "last_known_monthly_fee",
        "monthly_fee_percent_rank",
        "monthly_fee_dense_rank",
        "running_max_monthly_fee_dense_rank",
        "monthly_fee_avg_3_bucket",
        F.coalesce("running_avg_monthly_fee_dense_rank", F.lit(0)).alias("running_avg_monthly_fee_dense_rank_filled"),
        F.col("monthly_fee_avg_3_bucket").cast("string").alias("monthly_fee_avg_3_bucket_num"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("running_avg_monthly_fee_dense_rank_filled").over(w9).alias("status_avg_running_avg_monthly_fee_dense_rank_filled"),
    )
)

result = (
    scored
    .select(
        "plan",
        F.col("last_known_monthly_fee").alias("LAST_KNOWN_MONTHLY_FEE"),
        F.col("monthly_fee_percent_rank").alias("MONTHLY_FEE_PERCENT_RANK_VAL"),
        "running_avg_monthly_fee_dense_rank_filled",
        F.col("running_max_monthly_fee_dense_rank").alias("RUNNING_MAX_MONTHLY_FEE_DENSE_RANK"),
    )
    .unpivot(
        ids=["plan"],
        values=["LAST_KNOWN_MONTHLY_FEE", "MONTHLY_FEE_PERCENT_RANK_VAL", "running_avg_monthly_fee_dense_rank_filled", "RUNNING_MAX_MONTHLY_FEE_DENSE_RANK"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
