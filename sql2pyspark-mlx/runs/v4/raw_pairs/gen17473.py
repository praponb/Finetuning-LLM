from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") & F.hour("txn_time").between(9, 12))
    .withColumn("amount_adjusted", F.round((F.col("amount") * 100), 2))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)
