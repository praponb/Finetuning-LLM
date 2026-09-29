from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
        "is_flagged",
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 36)
    .select("txn_cohort", "is_flagged", "stddev_amount", "avg_amount")
    .orderBy("txn_cohort")
)
