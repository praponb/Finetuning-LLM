from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 15))
    .groupBy("is_flagged")
    .agg(F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"))
    .filter(F.col("7_eleven_count") >= 4)
    .select("is_flagged", "7_eleven_count")
    .orderBy("7_eleven_count")
)
