from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
    )
    .orderBy(F.col("balance_stddev").desc())
)
