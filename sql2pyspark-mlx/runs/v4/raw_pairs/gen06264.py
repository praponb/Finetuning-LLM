from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type", "merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type") != "DEBIT")
        .select("txn_type", "merchant")
    )
)
