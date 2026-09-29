from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.count("amount").alias("amount_count"))
    .orderBy(F.col("amount_count").desc())
)
