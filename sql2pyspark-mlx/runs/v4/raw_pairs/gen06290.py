from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_time") >= "2023-08-01")
        .select("txn_type")
    )
)
