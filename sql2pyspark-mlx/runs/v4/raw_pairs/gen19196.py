from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
    )
    .orderBy(F.col("paid_quarter").desc())
    .limit(1)
)
