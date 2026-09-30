from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)
