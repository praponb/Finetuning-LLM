from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("account_type").isin(["LOAN", "SAVINGS"]))

status_summary = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
)

result = (
    status_summary
    .select("status", "min_opened_date", "pct_closed")
    .orderBy("min_opened_date", "status")
    .limit(5)
)
