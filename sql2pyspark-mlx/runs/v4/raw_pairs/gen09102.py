from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)
