from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter((F.col("status") != "REFUNDED") & (F.col("paid_at") < "2025-02-18") & (F.col("amount") >= 750))
)

agg = (
    valid_payments
    .groupBy(
        F.when(F.col("amount") >= 1290, "high").when(F.col("amount") >= 370, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("amount").alias("amount_count"))
)

result = agg.select("amount_bucket", "amount_count")
