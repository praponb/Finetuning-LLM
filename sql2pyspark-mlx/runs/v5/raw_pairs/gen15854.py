from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 69000)
    .select(
        "status",
        "account_type",
        "customer_id",
        F.trim("account_type").alias("clean_account_type"),
    )
    .orderBy("account_type", F.col("clean_account_type").desc())
)
