from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy(F.col("total_amount").desc())
)
