from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        F.upper("txn_type").alias("txn_type_upper"),
    )
    .orderBy("txn_type_upper")
)
