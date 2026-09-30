from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2400) | ~F.col("is_flagged"))
    .withColumn("is_7_eleven", F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))
    .withColumn("txn_type_lower", F.lower("txn_type"))
    .orderBy("amount", "id")
    .limit(10)
)
