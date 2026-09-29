from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") < 2)
    .orderBy(F.col("order_id").desc())
)
