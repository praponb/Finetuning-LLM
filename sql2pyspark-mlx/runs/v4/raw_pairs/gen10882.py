from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("balance") < 71000) & F.col("customer_id").isin([1, 33, 40]) & (F.col("status") == "CLOSED"))
    .withColumn("balance_scaled", F.round((F.col("balance") * 0.9), 2))
)

accounts_stats = (
    filtered_accounts
    .groupBy("status")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
)

result = accounts_stats
