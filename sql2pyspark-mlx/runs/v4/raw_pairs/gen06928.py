from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select("id", "account_id", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type") == "CREDIT")
        .select("id", "account_id", "merchant")
    )
    .distinct()
)
