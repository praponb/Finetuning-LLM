from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") != "Lazada")
    .groupBy(
        "is_flagged",
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") >= 3500)
)
