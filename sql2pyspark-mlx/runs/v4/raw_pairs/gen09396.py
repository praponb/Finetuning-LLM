from pyspark.sql import functions as F

result = (
    df_payments
    .rollup("status", F.to_date("paid_at").alias("paid_day"))
    .agg(F.min("paid_at").alias("first_paid_at"))
)
