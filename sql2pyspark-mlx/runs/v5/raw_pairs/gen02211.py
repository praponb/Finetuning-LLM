from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") < 4)
    .orderBy(F.col("fee_count").desc())
)
