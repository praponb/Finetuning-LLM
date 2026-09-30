from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "method",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("unique_methods") > 3)
    .orderBy(F.col("paid_cohort").desc(), "method")
    .limit(5)
)
