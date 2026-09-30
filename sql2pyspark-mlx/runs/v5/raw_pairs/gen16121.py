from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "CREDIT", "DEBIT"]))
    .select("id", "merchant", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([6, 38]))
        .select("id", "merchant", "account_id")
    )
)
