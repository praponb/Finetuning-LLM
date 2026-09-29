from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.countDistinct("balance").alias("unique_customers"))
    .orderBy(F.col("unique_customers").desc())
)
