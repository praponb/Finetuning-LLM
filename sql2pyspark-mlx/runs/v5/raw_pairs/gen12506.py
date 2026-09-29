from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 11) | (F.col("txn_type") == "CREDIT"))
    .select("id", "txn_type", "amount")
    .orderBy(F.col("id").desc())
)
