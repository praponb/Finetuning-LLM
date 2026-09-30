from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([6, 17, 38, 40]) & (F.col("amount") <= 1460) & F.hour("paid_at").between(11, 18))
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy(F.col("total_amount").desc())
)
