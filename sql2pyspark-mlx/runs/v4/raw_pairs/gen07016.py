from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee_adjusted").desc(), "id")
w2 = Window.partitionBy("plan", "customer_id")
w_running = Window.partitionBy("customer_id").orderBy("latest_rank", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w5 = Window.partitionBy("customer_id").orderBy("latest_rank", "id")
w6 = Window.partitionBy("status").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id")
w8 = Window.orderBy("plan_sum_monthly_fee_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status", "customer_id")
w10 = Window.partitionBy("customer_id", "plan").orderBy(F.col("row_num").desc(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") * 0.9), 2).alias("monthly_fee_adjusted"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.sum("monthly_fee_adjusted").over(w2).alias("plan_sum_monthly_fee_adjusted"),
        F.avg("monthly_fee_adjusted").over(w_running).alias("monthly_fee_adjusted_avg_3_prior"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("seq_no"),
        F.sum("monthly_fee").over(w6).alias("running_monthly_fee"),
    )
)

latest_subscriptions = (
    metrics
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

metrics_subscriptions = (
    latest_subscriptions
    .withColumn("cumulative_monthly_fee_adjusted_avg_3_prior", F.sum("monthly_fee_adjusted_avg_3_prior").over(w8))
    .withColumn("status_stddev_samp_seq_no", F.stddev_samp("seq_no").over(w9))
)

deduped = (
    metrics_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

daily = (
    deduped
    .groupBy(
        "customer_id",
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("cumulative_monthly_fee_adjusted_avg_3_prior")).otherwise(0)).alias("cancelled_cumulative_monthly_fee_adjusted_avg_3_prior"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
    )
)

result = (
    daily
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "seq_no_tier",
        "row_seq_p95",
        "cancelled_cumulative_monthly_fee_adjusted_avg_3_prior",
        F.col("min_account_balance"),
    )
    .orderBy(
        F.col("cancelled_cumulative_monthly_fee_adjusted_avg_3_prior").asc_nulls_last(),
    )
)
