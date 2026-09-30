from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN", "CHECKING"]) & (F.col("account_type") != "CREDIT") & (F.col("opened_date") >= "2023-09-03"))
)

accounts_stats = (
    recent_accounts
    .groupBy("status", "account_type")
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)

result = (
    accounts_stats
    .filter(F.col("pct_frozen") < 43)
    .select("status", "account_type", "loan_count", "pct_frozen")
    .orderBy(F.col("pct_frozen").desc())
)
