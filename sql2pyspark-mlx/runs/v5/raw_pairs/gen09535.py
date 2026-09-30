from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .withColumn("account_type_upper", F.upper("account_type"))
    .withColumn("opened_date_period", F.date_trunc("week", "opened_date"))
)

result = (
    filtered
    .withColumn("balance_tier", F.when(F.col("balance") >= 76000, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date_period"))
    .rollup("balance_tier", "customer_id", "year_start")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "SAVINGS", 1)), F.lit(0)), 2).alias("balance_per_savings"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("num_account_types")
)
