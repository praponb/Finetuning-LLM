from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([6, 32]) & F.col("method").isin(["cash", "paypal", "bank_transfer"]))
    .groupBy(
        "status",
        F.when(F.col("status").isin(["FAILED", "SUCCESS"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .orderBy("status")
)
