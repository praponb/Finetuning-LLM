from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "customer_id",
        F.concat_ws("/", "account_type", "status").alias("account_type_status_key"),
    )
)
