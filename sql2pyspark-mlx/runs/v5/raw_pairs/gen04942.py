from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CHECKING", "CREDIT"]) & (F.col("opened_date") > "2025-05-01"))
    .groupBy("status")
    .agg(F.min("balance").alias("min_balance"))
    .orderBy("min_balance")
)
