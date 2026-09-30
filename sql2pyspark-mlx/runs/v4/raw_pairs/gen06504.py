from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "status",
        "balance",
        "account_type",
        F.date_trunc("MONTH", "opened_date").alias("opened_date_period"),
    )
)
