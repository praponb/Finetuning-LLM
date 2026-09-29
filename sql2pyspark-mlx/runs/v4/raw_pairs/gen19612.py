from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", F.trim("merchant").alias("clean_merchant"))
    .orderBy("clean_merchant", F.col("merchant").desc())
)
