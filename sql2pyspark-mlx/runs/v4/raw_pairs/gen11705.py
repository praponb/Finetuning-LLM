from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "order_id",
        F.when(F.col("amount") >= 1690, "high").when(F.col("amount") >= 250, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
    )
    .filter(F.col("pct_paypal") >= 39)
    .orderBy(
        F.col("pct_paypal").desc(),
        F.col("success_count").desc(),
        "order_id",
        "amount_bucket",
    )
    .limit(3)
)
