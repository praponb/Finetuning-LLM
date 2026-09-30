from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 26)
    .groupBy("status")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
    .filter(F.col("open_count") > 4)
    .orderBy("status")
    .limit(3)
)
