from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "bank_transfer") | F.col("amount").between(670, 740))
    .select(
        "id",
        "method",
        "amount",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)
