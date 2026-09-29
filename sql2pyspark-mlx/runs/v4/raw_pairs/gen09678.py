from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .orderBy("avg_balance")
)
