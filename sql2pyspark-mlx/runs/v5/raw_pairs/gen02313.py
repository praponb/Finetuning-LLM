from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 23, 29]))
    .select("id", "txn_type", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "txn_type", "account_id")
    )
    .distinct()
)
