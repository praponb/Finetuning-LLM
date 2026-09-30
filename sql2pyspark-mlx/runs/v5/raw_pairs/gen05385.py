from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("status") == "FAILED") & F.col("method").isin(["paypal", "cash", "card"]) & F.col("order_id").isNotNull())
)

summary = (
    base
    .groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    summary
    .filter(F.col("amount_count") >= 6)
    .orderBy(F.col("order_id").desc())
)
