from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 29, 32, 33]))
    .select("merchant")
    .unionByName(
        df_transactions
        .filter(~F.col("txn_type").isin(["CREDIT", "REFUND", "FEE"]))
        .select("merchant")
    )
    .distinct()
)
