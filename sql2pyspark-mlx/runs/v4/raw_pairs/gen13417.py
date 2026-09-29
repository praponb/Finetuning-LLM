from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "CREDIT"]))
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
    )
    .filter(F.col("loan_balance") >= 34)
)
