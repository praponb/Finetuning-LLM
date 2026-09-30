from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2025-09-20") & ((F.col("merchant") == "Lazada") | (F.col("amount") < 1400)))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("txn_type_prefix", F.substring("txn_type", 1, 2))
)
