from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Amazon") | (F.col("txn_time") < "2024-08-27"))
    .select("id", "is_flagged")
    .orderBy(F.col("id").desc())
)
