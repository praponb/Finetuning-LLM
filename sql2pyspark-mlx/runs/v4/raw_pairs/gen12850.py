from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("status")
w_grp = Window.partitionBy("status", "customer_id")
w4 = Window.partitionBy("account_type").orderBy("balance", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("next_balance").desc(), "id")
w6 = Window.partitionBy("account_type")
w7 = Window.partitionBy("status", "account_type").orderBy("next_balance", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("customer_id").orderBy(F.col("status_sum_balance_2").desc(), "id")
w9 = Window.partitionBy("account_type").orderBy("next_balance", "id").rowsBetween(-6, -1)
w10 = Window.partitionBy("customer_id").orderBy("balance_bucket_avg_5_prior", "id").rowsBetween(-6, Window.currentRow)
w11 = Window.partitionBy("customer_id").orderBy("account_type_stddev_samp_balance_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("account_type").orderBy(F.col("balance_bucket_avg_5_prior").desc_nulls_first())
w13 = Window.partitionBy("customer_id").orderBy("balance_bucket", "id").rowsBetween(-3, -1)
w14 = Window.partitionBy("status").orderBy(F.col("row_seq").desc_nulls_first())
w15 = Window.partitionBy("status").orderBy(F.col("status_sum_balance_2_avg_3_prior").desc_nulls_first())

metrics = (
    df_accounts
    .withColumn("balance_bucket", F.ntile(5).over(w_part))
    .withColumn("status_sum_balance", F.sum("balance").over(w2))
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("status_sum_balance_2", F.sum("balance").over(w_grp))
    .withColumn("next_balance", F.lead("balance", 2).over(w4))
)

enriched = (
    metrics
    .withColumn("next_balance_bucket", F.ntile(5).over(w5))
    .withColumn("account_type_stddev_samp_balance_bucket", F.stddev_samp("balance_bucket").over(w6))
    .withColumn("balance_bucket_avg_5_prior", F.avg("balance_bucket").over(w7))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w9))
    .withColumn("next_balance_avg_6", F.avg("next_balance").over(w10))
)

with_windows_accounts = (
    with_windows
    .withColumn("running_status_sum_balance", F.sum("status_sum_balance").over(w11))
    .withColumn("balance_bucket_avg_5_prior_percent_rank", F.percent_rank().over(w12))
    .withColumn("status_sum_balance_2_avg_3_prior", F.avg("status_sum_balance_2").over(w13))
)

remaining_keys = (
    with_windows_accounts
    .select("status", "id")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["CREDIT", "SAVINGS"]))
        .select("status", "id")
    )
)

verified = (
    with_windows_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_accounts = (
    verified
    .withColumn("row_seq_cume_dist", F.cume_dist().over(w14))
    .withColumn("status_sum_balance_2_avg_3_prior_dense_rank", F.dense_rank().over(w15))
)

daily = (
    enriched_accounts
    .cube(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.max(F.when(F.col("status") == "OPEN", F.col("next_balance_avg_6"))).alias("open_max_next_balance_avg_6"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.round(F.avg("next_balance_bucket"), 2).alias("avg_next_balance_bucket"),
        F.min("balance_bucket_avg_5_prior").alias("min_balance_bucket_avg_5_prior"),
    )
    .filter(F.col("open_max_next_balance_avg_6") >= 0)
)

result = (
    daily
    .select(
        "activity_month",
        "quarter_start",
        "year_start",
        "open_max_next_balance_avg_6",
        "min_balance_bucket_avg_5_prior",
    )
)
