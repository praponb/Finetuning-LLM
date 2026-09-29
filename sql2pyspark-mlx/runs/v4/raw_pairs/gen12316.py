from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") & (F.col("method").isNull() | F.col("method").isin(["card", "cash", "promptpay"])))
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
    )
    .filter(F.col("pct_bank_transfer") >= 44)
)
