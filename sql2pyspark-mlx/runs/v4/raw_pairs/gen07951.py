from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2023-09-01") & (F.col("status") == "FROZEN"))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.length("account_type").alias("account_type_length"),
    )
)
