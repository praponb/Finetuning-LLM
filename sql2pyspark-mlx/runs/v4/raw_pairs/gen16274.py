from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 6)
    .select("account_type", "cnt")
)
