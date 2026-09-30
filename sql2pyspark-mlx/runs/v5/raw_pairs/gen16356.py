from pyspark.sql import functions as F

result = (
    df_accounts
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count("*").alias("account_count"),
    )
    .orderBy("last_opened_date")
)
