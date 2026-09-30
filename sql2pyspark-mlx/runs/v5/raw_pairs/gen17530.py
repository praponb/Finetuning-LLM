from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 5, 9]) | F.col("customer_id").isin([9, 13, 34, 40]))
    .select("id", "opened_date", "status", F.trim("status").alias("clean_status"))
    .orderBy("opened_date", F.col("status").desc(), "id")
    .limit(10)
)
