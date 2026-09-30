from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.hour("txn_time").alias("txn_time_hour"),
    )
    .orderBy(F.col("txn_time_hour").desc())
)
