from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1700)
    .select(F.col("revenue").alias("value"))
    .subtract(df_transactions.select(F.col("amount").alias("value")))
    .orderBy("value")
)
