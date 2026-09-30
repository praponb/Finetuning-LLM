from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1100)
    .select(
        "id",
        "is_flagged",
        "amount",
        F.col("amount").cast("int").alias("amount_int"),
    )
)
