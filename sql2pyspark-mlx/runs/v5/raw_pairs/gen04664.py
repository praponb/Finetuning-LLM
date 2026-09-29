from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "promptpay"]) & (F.to_date("paid_at") >= "2024-09-01"))
    .groupBy("status", F.year("paid_at").alias("paid_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
    )
    .orderBy(F.col("paypal_count").desc(), "status", "paid_year")
    .limit(3)
)
