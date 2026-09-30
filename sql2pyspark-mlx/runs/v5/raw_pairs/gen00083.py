from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & ~F.col("method").isin(["card", "cash", "bank_transfer"]))
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("method")
)
