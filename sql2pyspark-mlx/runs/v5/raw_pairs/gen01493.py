from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") > 217000)
    .select("opened_month", "total_balance")
    .orderBy(F.col("opened_month").desc())
    .limit(10)
)
