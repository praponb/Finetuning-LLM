from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT"]) & F.col("status").isin(["CLOSED", "OPEN"]))
    .groupBy("account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.count("*").alias("account_count"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") > 330000)
    .drop("total_balance")
    .orderBy("account_type")
)
