from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.min("balance").alias("min_balance"))
    .filter(F.col("min_balance") > 70000)
)
