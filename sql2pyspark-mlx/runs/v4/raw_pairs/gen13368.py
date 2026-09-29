from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1100)
    .select("amount")
    .subtract(df_store_sales.select(F.col("revenue").alias("amount")))
    .orderBy("amount")
)
