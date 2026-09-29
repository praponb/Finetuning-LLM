from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        F.when(F.col("merchant") == "Lazada", 1).otherwise(0).alias("is_lazada"),
    )
    .orderBy(F.col("txn_time").desc())
)
