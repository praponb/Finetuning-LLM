from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum("balance").alias("total_balance"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)
