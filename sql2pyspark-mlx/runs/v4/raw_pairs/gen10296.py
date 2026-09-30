from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 590)
    .orderBy(F.col("paid_hour").desc())
)
