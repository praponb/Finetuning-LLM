from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([11, 19, 22, 40]))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
        F.countDistinct("amount").alias("unique_statuss"),
        F.count("amount").alias("amount_count"),
    )
)
