from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-07-30")
    .groupBy("status", F.month("opened_date").alias("opened_month"))
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") < 324000)
    .select("status", "opened_month", "total_balance")
    .orderBy("opened_month")
)
