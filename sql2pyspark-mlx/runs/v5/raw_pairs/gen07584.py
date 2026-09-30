from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "status",
        "balance",
        F.concat("account_type", F.lit(" - "), "status").alias("account_type_status"),
    )
)
