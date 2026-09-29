from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 44000) & (F.col("opened_date") >= "2023-09-23"))
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
    .filter(F.col("closed_count") >= 1)
)
