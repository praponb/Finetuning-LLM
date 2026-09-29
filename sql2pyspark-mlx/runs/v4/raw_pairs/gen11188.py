from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("activity_month", F.date_trunc("month", "opened_date"))
    .withColumn("balance_tier", F.when(F.col("balance") >= 86000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["activity_month", "balance_tier"], ["activity_month"], []],
        "activity_month",
        "balance_tier",
    )
    .agg(
        F.stddev_samp("balance").alias("balance_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.stddev_samp("balance").alias("balance_stddev_2"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("avg_balance").desc())
)
