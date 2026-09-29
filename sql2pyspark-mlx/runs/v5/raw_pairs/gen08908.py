from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 19)
    .select(
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
    .orderBy("txn_time_day")
)
