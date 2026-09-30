from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([6, 24, 28]))
    .groupBy("method")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") < 560)
    .orderBy("method")
    .limit(20)
)
