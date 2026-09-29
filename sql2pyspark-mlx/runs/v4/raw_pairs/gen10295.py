from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([27, 29]))
        .select("id")
    )
)
