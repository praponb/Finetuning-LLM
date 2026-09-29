from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .groupBy(
        "status",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
    .orderBy(F.col("status").desc())
)
