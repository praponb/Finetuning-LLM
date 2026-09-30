from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([21, 34, 38]) & F.col("txn_type").isin(["FEE", "CREDIT", "DEBIT"]))
    .select("account_id", "txn_type", F.upper("txn_type").alias("txn_type_upper"))
    .orderBy("txn_type_upper")
)
