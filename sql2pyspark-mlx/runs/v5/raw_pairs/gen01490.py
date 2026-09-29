from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 2300)
    .select("merchant", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("merchant", "account_id")
    )
)
