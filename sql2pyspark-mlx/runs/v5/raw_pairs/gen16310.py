from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-11").cast("date"))
    .withColumn("abs_balance", F.abs("balance"))
    .withColumn("balance_rounded", F.round("balance", 0))
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.round(F.sum("balance_rounded") / F.nullif(F.count(F.when(F.col("account_type") == "LOAN", 1)), F.lit(0)), 2).alias("balance_rounded_per_loan"),
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.percentile_approx("balance_rounded", 0.95).alias("balance_rounded_p95"),
        F.countDistinct("status").alias("distinct_status"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
    )
    .orderBy(F.col("distinct_status").desc_nulls_first())
)
