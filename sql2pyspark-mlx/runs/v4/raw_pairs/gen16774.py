from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_type",
        F.when(F.col("merchant") == "Grab", 1).otherwise(0).alias("is_grab"),
    )
    .orderBy("txn_type")
)
