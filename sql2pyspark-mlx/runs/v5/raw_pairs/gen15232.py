from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & F.col("order_id").isin([4, 25, 35, 38]))
    .withColumn("status_lower", F.lower("status"))
)

agg = (
    recent_payments
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
    )
)

result = agg.orderBy(F.col("cash_amount").desc(), "paid_year")
