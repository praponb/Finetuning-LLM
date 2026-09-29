from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("balance").between(26000, 69000) & (F.col("balance") >= 71000) & (F.col("status") == "OPEN"))
)

agg = (
    valid_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.avg("balance").alias("avg_balance"))
)

result = agg.filter(F.col("avg_balance") >= 47000)
