from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1100, 2700))
    .groupBy(
        "is_flagged",
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
        F.quarter("txn_time").alias("txn_quarter"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
