from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.min("balance").alias("min_balance"),
    )
)
