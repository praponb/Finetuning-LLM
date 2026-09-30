from pyspark.sql import functions as F

result = (
    df_accounts
    .cube("account_type", "customer_id")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
