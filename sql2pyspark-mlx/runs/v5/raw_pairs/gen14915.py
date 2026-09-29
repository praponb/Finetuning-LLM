from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.month("opened_date").isin([2, 8, 9]))
    .withColumn("clean_account_type", F.trim("account_type"))
)

agg = (
    valid_accounts
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
)

result = (
    agg
    .filter(F.col("avg_balance") >= 69000)
    .orderBy("customer_id")
    .limit(5)
)
