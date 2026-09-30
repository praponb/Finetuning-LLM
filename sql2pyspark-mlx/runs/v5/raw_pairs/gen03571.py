from pyspark.sql import functions as F

result = (
    df_accounts
    .rollup(F.month("opened_date").alias("opened_month"), "account_type")
    .agg(F.count("*").alias("row_count"))
    .orderBy("account_type")
)
