from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select("txn_type", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("txn_time") < "2024-01-26")
        .select("txn_type", "merchant")
    )
    .distinct()
    .orderBy(F.col("merchant").desc())
)
