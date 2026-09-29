from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") | F.col("is_flagged"))
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 3200, "high").when(F.col("amount") >= 1300, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.max("txn_time").alias("last_txn_time"))
)
