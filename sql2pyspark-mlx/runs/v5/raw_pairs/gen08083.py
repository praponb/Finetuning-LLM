from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") < 11000)
    .orderBy("total_amount", F.col("account_id").desc())
    .limit(20)
)
