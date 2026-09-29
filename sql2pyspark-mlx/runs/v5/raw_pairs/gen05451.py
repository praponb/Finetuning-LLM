from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1180, 1400) & (F.col("method") == "cash"))
    .groupBy("status", F.year("paid_at").alias("paid_year"))
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("paid_at").alias("last_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy("avg_amount", "status", "paid_year")
    .limit(10)
)
