from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(F.min("opened_date").alias("min_opened_date"))
)
