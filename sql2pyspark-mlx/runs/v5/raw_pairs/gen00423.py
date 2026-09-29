from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select(
        "txn_time",
        F.weekofyear("txn_time").alias("txn_time_weekofyear"),
        F.col("amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("txn_time_weekofyear").desc())
)
