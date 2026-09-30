from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "account_type",
        "status",
        "balance",
        F.lower("account_type").alias("account_type_lower"),
    )
    .orderBy("balance", F.col("account_type").desc())
)
