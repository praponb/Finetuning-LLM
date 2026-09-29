from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "status")
    .agg(
        F.min("balance").alias("min_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
)
