from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Starbucks", "Amazon"]))
    .withColumn("txn_time_day", F.col("txn_time").cast("date"))
)

agg = (
    filtered_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
)

result = (
    agg
    .filter(F.col("total_amount") < 11000)
    .select("txn_day", "total_amount", "pct_fee")
)
