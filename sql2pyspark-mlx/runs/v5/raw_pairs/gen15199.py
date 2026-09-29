from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type", "id")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_time") >= "2025-02-12")
        .select("txn_type", "id")
    )
)
