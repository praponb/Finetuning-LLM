from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") < "2025-01-01") & (F.col("method") == "cash"))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
    )
    .orderBy(F.col("status").desc())
)
