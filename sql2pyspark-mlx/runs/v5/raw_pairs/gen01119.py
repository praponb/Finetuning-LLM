from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 48000) & (F.col("balance") >= 24000) & (F.col("account_type") == "SAVINGS"))
    .select(
        "account_type",
        "status",
        "balance",
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
    )
)
