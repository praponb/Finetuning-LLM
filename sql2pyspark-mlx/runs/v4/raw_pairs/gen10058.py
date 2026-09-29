from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type", "account_id")
    .exceptAll(
        df_transactions
        .filter(F.col("merchant").isNull())
        .select("txn_type", "account_id")
    )
)
