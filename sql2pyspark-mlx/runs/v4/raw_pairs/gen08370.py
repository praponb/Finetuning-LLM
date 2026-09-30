from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2024-02-01")
    .select("account_id", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("account_id", "txn_type")
    )
    .orderBy("txn_type")
)
