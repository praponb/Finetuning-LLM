from pyspark.sql import functions as F

result = (
    df_transactions
    .select("account_id", "merchant")
    .intersectAll(
        df_transactions
        .filter(F.hour("txn_time").between(12, 17))
        .select("account_id", "merchant")
    )
)
