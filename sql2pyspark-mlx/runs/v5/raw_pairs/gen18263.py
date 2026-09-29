from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("closed_count") >= 2)
)
