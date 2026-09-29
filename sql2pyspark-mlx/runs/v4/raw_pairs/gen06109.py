from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "status",
        "account_type",
        "opened_date",
        F.trim("account_type").alias("clean_account_type"),
    )
)
