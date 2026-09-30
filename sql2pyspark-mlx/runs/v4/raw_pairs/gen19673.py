from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .select("txn_type")
    .unionByName(df_transactions.filter(F.col("is_flagged")).select("txn_type"))
    .orderBy("txn_type")
)
