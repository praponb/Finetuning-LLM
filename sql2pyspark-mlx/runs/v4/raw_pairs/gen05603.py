from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-09-01")
    .select("merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("txn_time") >= "2023-06-01")
        .select("merchant", "txn_type")
    )
)
