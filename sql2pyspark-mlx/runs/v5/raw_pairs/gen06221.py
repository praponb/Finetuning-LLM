from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.max("opened_date").alias("latest_opened_date"))
)
