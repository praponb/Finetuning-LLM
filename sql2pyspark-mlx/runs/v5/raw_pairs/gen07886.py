from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1030, 1320) & (F.col("order_id") == 12))
    .select(
        "id",
        "order_id",
        "method",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.round("amount", 1).alias("amount_rounded"),
    )
)
