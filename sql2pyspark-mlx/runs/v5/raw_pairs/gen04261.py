from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2024-07-01") | F.col("status").isNotNull())
    .select("id", "balance", F.last_day("opened_date").alias("opened_date_month_end"))
)
