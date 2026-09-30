from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Starbucks")
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "is_flagged")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("stddev_amount") > 26)
    .select("txn_month", "is_flagged", "stddev_amount", "min_amount")
)
