from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & (F.col("status") != "OPEN"))
    .groupBy("status")
    .agg(F.min("balance").alias("min_balance"))
)
