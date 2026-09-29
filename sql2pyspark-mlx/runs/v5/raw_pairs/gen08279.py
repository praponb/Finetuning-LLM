from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([13, 18]) | F.col("amount").between(1260, 1580))
)

status_summary = (
    base
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    status_summary
    .filter(F.col("pct_cash") >= 67)
    .orderBy(F.col("amount_count").desc(), "pct_cash")
)
