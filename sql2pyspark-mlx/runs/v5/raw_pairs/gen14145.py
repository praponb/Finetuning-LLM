from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .groupBy("status")
    .agg(F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"))
    .filter(F.col("credit_count") > 6)
    .select("status", "credit_count")
    .orderBy("status")
)
