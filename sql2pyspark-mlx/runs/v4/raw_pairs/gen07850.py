from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("total_count").desc())
)
