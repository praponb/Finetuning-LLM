from pyspark.sql import functions as F

result = (
    df_accounts
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
    )
)
