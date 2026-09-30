from pyspark.sql import functions as F

result = (
    df_transactions
    .select("account_id", "txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant") == "Starbucks")
        .select("account_id", "txn_type")
    )
)
