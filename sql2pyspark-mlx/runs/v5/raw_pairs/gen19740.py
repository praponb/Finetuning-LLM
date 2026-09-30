from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.col("amount").alias("value"))
    .subtract(df_store_sales.select(F.col("revenue").alias("value")))
)
