from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("is_flagged") | (F.col("amount") >= 2400))

summary = (
    valid_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"))
)

result = (
    summary
    .select("txn_cohort", "fee_count")
    .orderBy("txn_cohort")
)
