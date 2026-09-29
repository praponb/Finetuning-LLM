from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"))
    .orderBy("7_eleven_count")
)
