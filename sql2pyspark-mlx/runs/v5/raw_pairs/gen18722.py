from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

txn_month_summary = (
    filtered_transactions
    .groupBy(
        F.month("txn_time").alias("txn_month"),
        F.when(F.col("is_flagged_flag") >= 71, "high").when(F.col("is_flagged_flag") >= 17, "mid").otherwise("low").alias("is_flagged_flag_bucket"),
    )
    .agg(F.count("amount").alias("amount_count"))
)

result = txn_month_summary.select("txn_month", "is_flagged_flag_bucket", "amount_count")
