from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.max("opened_date").alias("last_opened_date"),
    )
)
