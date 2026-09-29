from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.year("paid_at").alias("paid_year"))
    .agg(F.max("amount").alias("max_amount"))
    .orderBy(F.col("max_amount").desc())
)
