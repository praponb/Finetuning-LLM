from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([12, 28]))
    .select("account_id", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([9, 14]))
        .select("account_id", "txn_type")
    )
    .distinct()
    .orderBy(F.col("txn_type").desc())
)
