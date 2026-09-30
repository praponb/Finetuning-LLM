from pyspark.sql import functions as F

result = (
    df_transactions
    .select("account_id", "txn_type")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([2, 22]))
        .select("account_id", "txn_type")
    )
)
