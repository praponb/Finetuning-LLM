from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "paypal") & F.col("order_id").isin([29, 38]))
    .groupBy("status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("status").desc())
)
