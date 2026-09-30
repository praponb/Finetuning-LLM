from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "FROZEN"]))
    .groupBy("account_type", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.min("opened_date").alias("min_opened_date"))
)
