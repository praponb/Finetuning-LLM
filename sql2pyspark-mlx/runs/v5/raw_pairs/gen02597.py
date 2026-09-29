from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([3, 37]))
    .select("id", "txn_type", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("merchant") == "Grab")
        .select("id", "txn_type", "merchant")
    )
    .distinct()
)
