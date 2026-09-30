from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.concat_ws("/", "status", "account_type").alias("status_account_type_key"),
    )
)
