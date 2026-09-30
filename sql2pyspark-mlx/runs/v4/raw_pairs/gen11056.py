from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 4300, "PREMIUM").otherwise("STANDARD"))
    .cube("activity_month", "account_id", "amount_tier")
    .agg(
        F.min("txn_time").alias("started_at"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
        F.max("txn_time").alias("ended_at"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("account_id").desc())
)
