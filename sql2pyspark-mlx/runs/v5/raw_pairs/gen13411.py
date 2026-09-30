from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 3200) | (F.col("txn_type") == "FEE"))
    .groupBy("merchant")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.max("amount").alias("max_amount"),
    )
)
