from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("is_flagged") & ((F.col("amount") >= 2400) | F.col("amount").between(1300, 3800)))
)

agg = (
    valid_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    agg
    .select("txn_hour", "max_amount")
    .orderBy("txn_hour")
)
