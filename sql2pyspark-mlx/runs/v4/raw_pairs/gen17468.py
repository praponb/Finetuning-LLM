from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1700)
    .select("txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([7, 25, 31]))
        .select("txn_type")
    )
)
