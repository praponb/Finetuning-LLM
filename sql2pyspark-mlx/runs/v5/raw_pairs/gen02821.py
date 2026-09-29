from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .select("id", "merchant", "account_id")
    .intersect(
        df_transactions
        .filter(F.to_date("txn_time") >= "2024-04-22")
        .select("id", "merchant", "account_id")
    )
)
