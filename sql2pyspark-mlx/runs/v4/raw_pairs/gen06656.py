from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("txn_type") == "CREDIT") | F.col("account_id").isin([8, 22])))
    .select(
        "id",
        "txn_time",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
    .orderBy(F.col("txn_time").desc(), "id")
    .limit(1)
)
