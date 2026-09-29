from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-05-01")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy(F.col("stddev_amount").desc())
)
