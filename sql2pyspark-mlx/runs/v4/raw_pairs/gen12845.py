from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("*").alias("account_count"),
    )
)
