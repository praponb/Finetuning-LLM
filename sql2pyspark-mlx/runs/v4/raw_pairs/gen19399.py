from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("*").alias("row_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("row_count") > 4)
)
