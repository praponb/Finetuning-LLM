from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .orderBy(F.col("avg_balance").desc())
)
