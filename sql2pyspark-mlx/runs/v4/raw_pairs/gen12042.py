from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .select(
        "id",
        "txn_type",
        F.concat("txn_type", F.lit("/"), "merchant").alias("txn_type_merchant"),
    )
    .orderBy("id")
    .limit(20)
)
