from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 53000)
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") < 449000)
    .orderBy("total_balance", "opened_month")
    .limit(5)
)
