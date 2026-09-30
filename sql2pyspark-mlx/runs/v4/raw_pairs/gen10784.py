from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 5)
    .select("account_type", "total_count")
    .orderBy(F.col("account_type").desc(), "total_count")
)
