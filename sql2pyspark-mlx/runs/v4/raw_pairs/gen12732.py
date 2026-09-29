from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("amount") >= 620) & (F.to_date("paid_at") < "2025-08-08") & (F.col("amount") > 880))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

method_summary = (
    base
    .groupBy("method", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
)

result = (
    method_summary
    .select("method", "status", "pct_card")
    .orderBy(F.col("pct_card").desc(), "method", "status")
    .limit(1)
)
