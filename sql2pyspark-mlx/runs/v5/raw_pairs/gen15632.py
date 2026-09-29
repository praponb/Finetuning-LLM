from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1100)
    .select("txn_type")
    .intersect(
        df_transactions
        .filter(F.col("txn_type").isin(["REFUND", "CREDIT"]))
        .select("txn_type")
    )
)
