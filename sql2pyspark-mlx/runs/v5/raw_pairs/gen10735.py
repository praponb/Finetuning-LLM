from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "account_type",
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
    )
)
