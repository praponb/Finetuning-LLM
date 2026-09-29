from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
