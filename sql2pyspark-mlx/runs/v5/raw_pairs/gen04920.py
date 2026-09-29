from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "Starbucks", "Lazada"]))
    .select("txn_type", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("amount") >= 3700)
        .select("txn_type", "merchant")
    )
    .orderBy("txn_type")
)
