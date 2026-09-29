from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(F.count("*").alias("num_payments"))
    .filter(F.col("num_payments") > 9)
    .select("status", "method", "num_payments")
    .orderBy("num_payments")
)
