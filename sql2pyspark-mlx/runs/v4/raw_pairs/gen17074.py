from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 66000)
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .pivot("status", ["OPEN", "FROZEN", "CLOSED"])
    .agg(F.max("balance"))
)
