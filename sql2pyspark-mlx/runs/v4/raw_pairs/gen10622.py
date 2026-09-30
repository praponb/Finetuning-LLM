from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("order_id") == 18) & (F.col("order_id") == 24) & F.col("method").isin(["bank_transfer", "cash", "promptpay"]))
)

payments_stats = (
    recent_payments
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
    )
)

result = (
    payments_stats
    .select("method", "pct_paypal")
    .orderBy(F.col("method").desc())
)
