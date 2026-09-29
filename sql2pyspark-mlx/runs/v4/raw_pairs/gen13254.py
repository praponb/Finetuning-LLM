from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .select("merchant", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "Shell")
        .select("merchant", "account_id")
    )
)
