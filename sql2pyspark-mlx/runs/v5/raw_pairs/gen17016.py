from pyspark.sql import functions as F

src = (
    df_transactions
    .filter((F.col("txn_time") < "2024-10-24") & F.col("is_flagged") & F.col("is_flagged"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)

agg = (
    src
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
        "is_flagged",
    )
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    agg
    .select("txn_cohort", "min_amount")
    .orderBy("min_amount", "txn_cohort")
)
