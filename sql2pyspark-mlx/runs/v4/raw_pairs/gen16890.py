from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
    )
    .filter(F.col("savings_count") >= 64)
    .select("opened_month", "savings_count")
    .orderBy(F.col("opened_month").desc())
    .limit(10)
)
