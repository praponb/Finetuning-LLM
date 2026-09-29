from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1080, 1220) & (F.col("method") == "paypal"))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
    .drop("cnt")
    .orderBy("pct_paypal", "status")
)
