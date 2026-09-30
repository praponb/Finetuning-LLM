from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.hour("paid_at").between(6, 10) & ~F.col("status").isin(["FAILED", "REFUNDED"]))
    .withColumn("status_lower", F.lower("status"))
)

paid_month_summary = (
    base
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("paid_month"),
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    paid_month_summary
    .filter(F.col("stddev_amount") > 62)
    .select("paid_month", "paid_cohort", "pct_failed", "stddev_amount")
)
