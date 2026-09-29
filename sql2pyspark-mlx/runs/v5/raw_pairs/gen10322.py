from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Grab")
    .select("id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") >= "2024-02-01")
        .select("id", "txn_type")
    )
)
