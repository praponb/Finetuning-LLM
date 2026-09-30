from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "REFUND"]))
    .select("id", "merchant", "txn_type")
    .intersect(
        df_transactions
        .filter(F.col("merchant").isin(["Lazada", "Shell", "Starbucks"]))
        .select("id", "merchant", "txn_type")
    )
)
