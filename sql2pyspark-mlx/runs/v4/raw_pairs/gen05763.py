from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([2, 35]))
    .select("id", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("txn_time") < "2023-08-09")
        .select("id", "txn_type")
    )
    .distinct()
)
