from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.year("opened_date") >= 2024) & (F.col("status") != "CLOSED"))
    .groupBy("status")
    .agg(F.countDistinct("balance").alias("unique_customers"))
    .orderBy("status", F.col("unique_customers").desc())
)
