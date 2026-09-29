from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull() & F.col("balance").isNotNull() & F.col("status").isNotNull())
    .select(
        "id",
        "balance",
        "status",
        "account_type",
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
        F.lower("account_type").alias("account_type_lower"),
    )
)
