from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT"]) & ((F.col("status") == "OPEN") | F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"])))
)

summary = (
    valid_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
)

result = (
    summary
    .select("opened_month", "pct_savings")
    .orderBy(F.col("pct_savings").desc())
)
