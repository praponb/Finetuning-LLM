from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .select("id", "merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([23, 30, 39]))
        .select("id", "merchant", "txn_type")
    )
    .distinct()
)
