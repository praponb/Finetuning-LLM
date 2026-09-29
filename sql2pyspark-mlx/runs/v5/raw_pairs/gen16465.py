from pyspark.sql import functions as F

result = (
    df_accounts
    .rollup("status", F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("first_opened_date")
)
