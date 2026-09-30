from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("method").isin(["promptpay", "paypal"]), "core").otherwise("other").alias("method_group"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
    )
    .filter(F.col("cash_count") > 52)
)
