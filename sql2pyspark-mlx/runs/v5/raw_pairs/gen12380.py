from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .agg(F.min("amount").alias("min_amount"))
)
