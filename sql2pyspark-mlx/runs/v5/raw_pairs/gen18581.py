from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .pivot("status", ["EXPIRED", "CANCELLED"])
    .agg(F.max("monthly_fee").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "customer_id",
        F.col("EXPIRED_amount").alias("expired_amount"),
        F.col("EXPIRED_n").alias("expired_n"),
        F.col("CANCELLED_amount").alias("cancelled_amount"),
        F.col("CANCELLED_n").alias("cancelled_n"),
    )
)
