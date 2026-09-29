from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 4000)
    .groupBy("txn_type")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 7)
    .select("txn_type", "min_amount", "total_count")
    .orderBy("total_count")
)
