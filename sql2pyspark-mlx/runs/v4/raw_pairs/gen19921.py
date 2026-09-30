from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "CREDIT") & ((F.col("amount") >= 2000) | (F.col("merchant") == "Amazon")))
    .groupBy("is_flagged")
    .agg(F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"))
    .filter(F.col("7_eleven_count") > 3)
    .orderBy(F.col("7_eleven_count").desc(), "is_flagged")
    .limit(1)
)
