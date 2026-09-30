from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", "account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type") == "CREDIT")
        .select("merchant", "account_id")
    )
)
