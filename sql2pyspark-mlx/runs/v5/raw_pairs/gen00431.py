from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("customer_id", "account_type")
    .exceptAll(
        df_accounts
        .filter(F.col("opened_date") >= "2024-09-14")
        .select("customer_id", "account_type")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["week_start", "activity_month"], ["week_start"]],
        "week_start",
        "activity_month",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("checking_balance") >= 0)
)
