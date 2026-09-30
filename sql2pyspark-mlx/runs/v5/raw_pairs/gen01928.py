from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 3)
)
