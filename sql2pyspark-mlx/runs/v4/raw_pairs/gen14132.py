from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") >= "2024-12-30") & (~F.col("account_type").isin(["SAVINGS", "LOAN"]) | (F.col("customer_id") == 13)))
    .groupBy("account_type")
    .agg(F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"))
    .orderBy(F.col("open_count").desc())
)
