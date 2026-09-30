from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("pct_credit") < 75)
    .orderBy(F.col("opened_month").desc())
)
