from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([3, 25, 32, 35]))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"))
)
