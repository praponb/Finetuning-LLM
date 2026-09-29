from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([23, 31]))
    .select(
        "id",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
