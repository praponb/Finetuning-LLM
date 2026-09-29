from pyspark.sql import functions as F

valid_payments = df_payments.filter((F.col("method") == "bank_transfer") | F.col("method").isNull())

payments_stats = (
    valid_payments
    .groupBy(
        "method",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = payments_stats.orderBy(F.col("stddev_amount").desc(), "status_group")
