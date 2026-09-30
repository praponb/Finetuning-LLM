from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "SAVINGS")
    .groupBy("status", F.month("opened_date").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
    .orderBy("opened_month", "status")
)
