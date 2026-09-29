from pyspark.sql import functions as F

valid_payments = df_payments.filter((F.col("order_id") == 29) & (F.col("amount") > 690))

summary = (
    valid_payments
    .groupBy(
        "method",
        F.when(F.col("amount") >= 1410, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("*").alias("payment_count"),
        F.count("amount").alias("amount_count"),
    )
)

result = summary
