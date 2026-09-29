from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
    .filter(F.col("credit_balance") < 60)
    .select("status", "credit_balance")
    .orderBy("credit_balance", F.col("status").desc())
)
