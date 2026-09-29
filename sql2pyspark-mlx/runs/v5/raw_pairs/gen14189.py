from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([2, 3, 6]))
    .groupBy("customer_id")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc())
)
