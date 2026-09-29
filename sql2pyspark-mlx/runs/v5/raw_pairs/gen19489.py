from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Amazon"]))
    .select("txn_type", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type") == "DEBIT")
        .select("txn_type", "merchant")
    )
    .distinct()
    .orderBy("txn_type")
)
