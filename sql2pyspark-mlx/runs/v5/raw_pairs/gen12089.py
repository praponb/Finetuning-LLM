from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1200)
    .select("account_id")
    .intersect(
        df_transactions
        .filter(F.col("txn_type") == "DEBIT")
        .select("account_id")
    )
)
