from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("txn_type") == "FEE"))
    .select(
        "id",
        "txn_time",
        "txn_type",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.upper("txn_type").alias("txn_type_upper"),
    )
)
