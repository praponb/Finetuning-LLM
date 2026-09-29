from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "account_type")
    .agg(
        F.min("balance").alias("min_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
