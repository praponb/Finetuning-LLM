from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
        F.max("paid_at").alias("latest_paid_at"),
    )
    .filter(F.col("card_count") > 72)
)
